---
title: Watches
description: Follow ordered changes, checkpoint consumers, and make derived data catch up without rescanning source state.
---

# Watches

This tutorial continues the object, metadata, authorisation, and index path introduced in [Buckets and Objects](/tutorials/buckets-and-objects/), [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/), and [Authorisation Grants and Revokes](/tutorials/authorisation/). By this point you have a tenant profile named `acme`, a `documents` bucket, and at least one object key under `tutorial/`.

A watch is Anvil's way to say: "show me committed changes after this position". That position is a cursor or revision. A consumer reads events, updates its own derived state, and stores a checkpoint only after its work is durable. The result is a worker that can restart safely without listing the whole bucket, rebuilding every index, or guessing which changes happened while it was offline.

Applications should use the streaming public API directly for production consumers. The `anvil watch`, `anvil authz watch`, `anvil index query`, and `anvil personaldb watch` commands in this page are manual helpers over those APIs. Keep [Watches and Derived Data](/learn/watches-and-derived-data/), [Indexes and Query](/learn/indexes-and-query/), [Public CLI](/reference/public-cli/), [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) nearby while you read.

This page teaches watches as restartable maintenance streams, not live notifications for user interfaces. You will see how cursors, revisions, checkpoints, derived writes, and replay fit together across object, authz, index, and PersonalDB surfaces.

## Prerequisites and checkpoint storage

A watch is useful only if the consumer stores checkpoints somewhere durable. Before running the examples, decide where your worker would write its checkpoint: an object, a database row in your application, a PersonalDB projection record, or another durable control record. A terminal scrollback cursor is enough for a tutorial, but not for a production worker.

Watches are public-plane reads unless the specific stream is an admin/system stream. The examples use `anvil` and tenant tokens. Do not use the private admin API to consume ordinary object, authz, index, or PersonalDB watches for an application worker. Use admin diagnostics when the watch infrastructure itself is unhealthy.

## Why watches exist

Without a watch, a worker that maintains a search index, cache, projection, or export has two poor choices. It can rescan source data repeatedly, which becomes expensive and can race with concurrent writes. Or it can remember local timestamps and hope they line up with committed Anvil state, which is unsafe in a distributed system.

A watch stream gives the worker an ordered feed of committed changes. The worker asks for events after cursor `123`, processes event `124`, writes its derived update, and then records `124` as its checkpoint. If it crashes before recording the checkpoint, it sees `124` again after restart. That is fine if the derived write is idempotent. If it records `124` before its derived write is durable, it can skip work permanently. The checkpoint must therefore mean "processed through here", not "seen on the wire".

Cursors are scoped to a stream. An object prefix cursor is not an authz revision. An index partition cursor is not a PersonalDB group cursor. Some streams use a single `uint64` cursor. Authz tuple watches use a `revision`. Some derived streams expose a split `cursor_low` and `cursor_high`; store both numbers exactly as returned.

## Watch surfaces available today

The public API has more watch RPCs than the public CLI currently exposes. That is important when you are designing a worker: use the API when you need the full event envelope, subject details, hashes, low/high cursors, or an API-only stream.

`ObjectService.WatchPrefix` watches object changes in one bucket under one key prefix. The current CLI exposes it as `anvil watch prefix`. It is the main tutorial surface because it is the watch most application workers use first. It reports object writes, deletes, versions, and metadata through the API; the CLI prints only `cursor`, `event_type`, and `object_key`.

`BucketService.WatchBucketMetadata` watches bucket records and bucket policy changes. That is different from an object watch: a bucket watch tells you that the bucket itself changed, not that an object under a prefix changed. For a named bucket, the API checks `bucket:watch` on that bucket. The current public CLI does not expose a bucket metadata watch command.

`AuthService.WatchAuthzTupleLog` watches relationship tuple writes and removals by authz revision. The current CLI exposes it twice: `anvil watch authz` and `anvil authz watch`. Both call the tuple-log watch. API-only authz watches also exist for namespace changes and derived-authz lag; the public CLI does not expose those today.

`IndexService.WatchIndexDefinition` watches index definition lifecycle events such as create, update, disable, and drop. `IndexService.WatchIndexPartition` watches derived index partition events. These are for index workers and operators who need to see whether derived state is moving, not for reading source objects directly.

`PersonalDbService.WatchPersonalDbGroup` watches committed PersonalDB group changes. The current CLI exposes it as `anvil watch personaldb` and `anvil personaldb watch`. The API also has a PersonalDB projection watch; the current public CLI does not expose that projection watch.

There are other public API watch-style surfaces, such as git-source watches, but they do not have tutorial CLI coverage here. This page stays with the commands that exist in the current public CLI.

## Prerequisites for the examples

Watches are tenant-public API reads. They are authenticated unless a specific service path says otherwise; public-read buckets do not make watch streams anonymous. The profile you use must have the exact public policy scope for the stream you open.

For the examples below, the narrow useful scopes are:

| Purpose | Public policy action | Resource checked today |
| --- | --- | --- |
| Watch object changes in `documents` | `object:list` | `documents` |
| Watch bucket metadata through the API | `bucket:watch` | `documents` |
| Write the demo object, if you create one | `object:write` | `documents/tutorial/watch-demo.json` |
| Watch index definitions or partitions in `documents` | `index:watch` | `documents` |
| Query an index in `documents` | `index:read` | `documents` |
| Watch tuple changes in the `document` namespace | `authz:watch` | `document` |
| Watch a PersonalDB group | `personaldb:watch` | `tenant-<tenant_id>/<database_id>` |

Do not use wildcard grants to make a watch demo convenient. A watch often feeds other systems, so over-broad watch access can leak object names, metadata, tuple changes, or operational timing even when it never returns an object body.

The object upload command in this page is illustrative for the same reason it was in earlier tutorials: the current public CLI upload path may still need broader bucket discovery than the exact object write scope because it discovers bucket ids through `ListBuckets`. If your local profile does not have that helper permission, use an object you already wrote or use an API/client path that supplies the mutation context directly.

## Tail object changes under a prefix

Start with an object prefix watch. It is the easiest way to see the checkpoint pattern because the stream is tied to the object log for one bucket.

```bash
anvil --profile acme watch prefix documents tutorial/ --after-cursor 0
```

This calls `ObjectService.WatchPrefix` for bucket `documents`, prefix `tutorial/`, and cursor `0`. A successful connection proves that the profile can authenticate, the caller has `object:list` on `documents`, the bucket exists in the tenant, and Anvil can stream the durable object-watch snapshot followed by live events.

If nothing has changed after cursor `0`, the command may wait. That is not failure; it means the stream is open and no matching event has arrived yet. If it fails with permission denied, fix the `object:list` grant for the bucket. If it fails with `UnauthorizedReservedNamespace`, check that the prefix does not start with `_anvil/`. If it fails because the bucket or region is unavailable, return to the earlier bucket and local-mesh setup.

In another shell, create a small object under the watched prefix if your profile has the required write authority:

```bash
printf '{"event":"watch-demo"}\n' > watch-demo.json
anvil --profile acme object put watch-demo.json s3://documents/tutorial/watch-demo.json
```

This write is not part of the watch API. It is an ordinary authenticated object write. A successful write proves Anvil committed a new current version for that key. The watch command should then print a line shaped like:

```text
124	put	tutorial/watch-demo.json
```

The exact event type is implementation data, so do not build a parser that only accepts the word `put`. In the API response, the event also carries fields such as version id, ETag, size, delete-marker state, creation time, and the common watch envelope. The CLI prints a compact line for human inspection.

Object mutation API responses also carry a `watch_cursor`. The current `anvil object put` helper does not print that cursor, so the manual tutorial path gets the cursor from the watch stream. Application code should normally keep the cursor returned by the write response when it needs immediate query catch-up.

## Store a checkpoint and restart from it

A production worker stores a checkpoint in its own durable state, not in an environment variable. For a tutorial, a local file is enough to show the rule.

```bash
CHECKPOINT_FILE=.anvil-watch-documents-tutorial.cursor
AFTER_CURSOR="$(cat "$CHECKPOINT_FILE" 2>/dev/null || printf '0')"

anvil --profile acme watch prefix documents tutorial/ --after-cursor "$AFTER_CURSOR" |
while IFS=$'\t' read -r cursor event_type object_key; do
  printf 'processing cursor=%s event=%s key=%s\n' \
    "$cursor" "$event_type" "$object_key"

  # In a real worker, update derived state first. Store the cursor last.
  printf '%s\n' "$cursor" > "$CHECKPOINT_FILE"
done
```

This command proves a simple restart contract. The first line loads the last saved cursor or starts at `0`. The watch asks Anvil for events after that cursor. The loop processes each event and writes the cursor only after the example's "derived work" has completed. Here the derived work is just printing a line; in a real worker it might update a search segment, invalidate a cache key, publish a projection manifest, or enqueue a downstream job.

Stop the command with `Ctrl-C`, inspect the cursor, and restart from the same checkpoint:

```bash
LAST_CURSOR="$(cat .anvil-watch-documents-tutorial.cursor)"
anvil --profile acme watch prefix documents tutorial/ --after-cursor "$LAST_CURSOR"
```

This proves the resume path. Events at or before the saved cursor should not be replayed by the stream. If your worker crashed before saving the latest cursor, it may replay the last event it had already seen. That is why derived writes must be idempotent. Use stable derived keys, mutation ids, compare-and-swap preconditions, or "upsert by source version" records so repeated processing is harmless.

## Use watch cursors for query correctness

Indexes are derived data. An object write commits before every index necessarily has processed it. For a dashboard that tolerates slight staleness, that may be acceptable. For a workflow that just wrote an object and must query an index that includes it, ask the query to prove catch-up.

The API field is `require_caught_up_to_watch_cursor` on `IndexService.QueryIndex`. The current CLI exposes it as `--require-caught-up-to-watch-cursor`:

The example query uses `typed_predicates_json`, which is an array of predicates over fields already materialised by a typed JSON index. In this example, the index must have a field named `status`; the predicate asks for rows whose materialised JSON value equals the string `ready`. The catch-up flag then asks Anvil to prove that the answer is not older than the watch cursor.

```bash
anvil --profile acme index query documents documents_by_status \
  --typed-predicates-json '[{"field":"status","op":"eq","value":"ready"}]' \
  --require-caught-up-to-watch-cursor "$LAST_CURSOR" \
  --lag-timeout-ms 5000
```

Use an index name and predicate that match the index you created in your own tutorial chain. A successful query proves more than "the index returned results". It proves the caller has `index:read` on `documents`, the index exists, the query JSON is accepted, and the index generation used for the answer has caught up to at least the object watch cursor you supplied.

If the command fails with `IndexLagging`, the service is protecting you from a stale answer. You can wait and retry, increase the lag timeout if the workload is expected to catch up quickly, or choose an explicitly stale-tolerant path. Do not treat `IndexLagging` as "no rows matched".

The API response includes catch-up fields such as `source_watch_cursor_high`, `index_watch_cursor_applied`, `is_caught_up`, and `lag_record_count_hint`. The current CLI prints hits and pagination tokens, but it does not display those catch-up fields. Use the API directly if your worker needs to record or alert on exact lag values.

## Watch authorisation changes

Relationship authorisation has its own ordered log. Tuple writes and removals advance an authz revision, not an object cursor. A cache that stores permitted objects, a derived userset index, or a long-running export should treat authz changes as first-class invalidation input.

```bash
anvil --profile acme watch authz document --after-revision 0
```

This calls `AuthService.WatchAuthzTupleLog` for the `document` namespace. A successful connection proves the caller has `authz:watch` on `document` and can read tuple-log events in the tenant's default authz scope. The CLI prints compact lines shaped like:

```text
42	add	document:documents/tutorial/welcome.txt#viewer
```

Store the last fully processed revision the same way you store an object cursor. On restart, pass it back with `--after-revision`:

```bash
AUTHZ_REVISION="$(cat .anvil-watch-document-authz.revision 2>/dev/null || printf '0')"
anvil --profile acme watch authz document --after-revision "$AUTHZ_REVISION"
```

The API event includes the subject kind, subject id, reason, writer, record hash, timestamp, and envelope. The current CLI does not print all of those fields, so it is good for tailing and demonstrations but not enough for a complete audit export or cache-invalidation worker that must distinguish every subject.

`anvil authz watch document --after-revision 0` is an equivalent helper for the same tuple-log API. It does not watch schema binding changes. Use the API-only namespace watch when you need namespace/schema invalidation events, and use the API-only derived-lag watch when you need to monitor authz-derived index lag.

## Watch index maintenance

Index watches are about derived index state, not source object bodies. Use them when you operate index builders, deployment automation, or diagnostics that need to know whether definitions or partitions changed.

```bash
anvil --profile acme watch index-definition documents --after-cursor 0
```

This calls `IndexService.WatchIndexDefinition` for the `documents` bucket. A successful connection proves the caller has `index:watch` on `documents`. The stream emits when an index definition is created, updated, disabled, or dropped. If you change an index in another shell, the watch should print a cursor and event type.

Partition watches use split cursors because derived index partitions use a wider cursor space. The current CLI requires a partition id argument. Use a partition id from your index worker, manifest, or API response; do not guess it in production.

```bash
PARTITION_ID=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

anvil --profile acme watch index-partition \
  documents documents_by_status "$PARTITION_ID" \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

This command is illustrative unless the index and partition id exist in your environment. A successful stream proves `index:watch` and that the index definition is present and enabled. The CLI prints `cursor_low:cursor_high` and an event type. Store the low and high cursor components together; using only one half loses the resume position.

Index definition watches, partition watches, and `require_caught_up_to_watch_cursor` solve different problems. Definition watches tell you the configuration changed. Partition watches tell you derived index storage moved. Query catch-up proves that a specific answer is not older than a source object cursor you care about.

## Watch PersonalDB groups when you use PersonalDB

PersonalDB has its own committed log and projection model. If your application uses PersonalDB groups, the group watch is the stream that tells projection builders and sync monitors that a committed head moved.

```bash
anvil --profile acme watch personaldb customer-notes \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

This calls `PersonalDbService.WatchPersonalDbGroup` for `customer-notes` in the authenticated tenant. The CLI decodes the tenant id from the bearer token and sends it in the request. A successful connection proves the caller is in the same tenant as the requested group and has `personaldb:watch` for the group, or an allowed relationship-authorisation path for the watcher relation.

The equivalent family-specific helper is:

```bash
anvil --profile acme personaldb watch customer-notes \
  --after-cursor-low 0 \
  --after-cursor-high 0
```

Both commands stream group-level events and print `cursor_low:cursor_high` plus an event type. The API also supports `WatchPersonalDbProjection`, but the current public CLI does not expose a projection-watch command. Use the API directly for projection-specific workers.

## Handle lag, gaps, and restarts deliberately

A watch stream is long-lived, so consumers need normal failure handling. Network connections close. Tokens expire. Workers deploy. Live broadcast buffers can be overrun. Storage retention and compaction policies can eventually make very old cursors unsuitable for incremental catch-up.

The recovery loop is simple but strict: reconnect with the last durable checkpoint, replay any events Anvil returns, and write the next checkpoint only after derived work is durable. If the service reports that the watch fell behind the retained live event window, restart from the checkpoint. If the checkpoint is too old for the retained source history, rebuild or repair the derived state from committed source records and then publish a fresh checkpoint. Do not advance a cursor just to silence an error.

A derived worker should store enough context with the cursor to make mistakes obvious: tenant id, stream type, bucket or namespace, prefix, index name or database id, cursor or revision, worker version, and the time the checkpoint was written. That extra context prevents accidentally resuming an authz watch with an object cursor or processing `documents` events into a projection for another bucket.

## What to take forward

Use watches when a system needs to keep derived state aligned with committed Anvil state. Store checkpoints after work is durable. Expect replay and make processing idempotent. Use object prefix watches for object-derived work, bucket watches for bucket metadata through the API, authz tuple watches for relationship changes, index watches for derived index maintenance, and PersonalDB watches for PersonalDB log/projection workflows. When query correctness depends on a write you just saw, pass the relevant source object cursor with `require_caught_up_to_watch_cursor` instead of hoping the index has caught up.

## Success and failure cues

A watch connection proves the caller can open the stream; it does not prove the worker has processed anything. The durable checkpoint is your evidence of processing. If a worker repeats an event after restart, that is normal when it crashed before checkpointing. If it skips work, look for checkpoints written before side effects became durable. If the stream reports a gap or unavailable cursor, rebuild or repair from source rather than inventing a later cursor.

## Where to go next

Pair watches with [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/) when a worker needs replayable domain events, and with [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/) when only one worker should process a shard at a time. For index-specific lag and rebuild evidence, continue to [Repair and Diagnostics](/tutorials/repair-and-diagnostics/).
