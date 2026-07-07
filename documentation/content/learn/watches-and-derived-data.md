---
title: Watches and Derived Data
description: Understand Anvil watch streams, cursors, checkpoints, lag, catch-up, derived indexes, projections, repair, and current watch-surface limits.
---

# Watches and Derived Data

A system that stores data usually grows a second problem: other things want to keep a view of that data. A search index wants object text. A typed query index wants JSON fields. A permission cache wants authorisation tuples. A PersonalDB projection wants accepted changesets. An audit exporter wants append-stream records. A static catalogue wants object and link state.

Those views are **derived data**. They are useful because they make reads fast, but they are safe only when they can prove which source changes they include. A watch is Anvil's API pattern for that proof: "give me committed events after this cursor".

Read this page after [CoreStore](/learn/corestore/), [Object Model](/learn/object-model/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), and [Indexes and Query](/learn/indexes-and-query/). The hands-on tutorial is [Watches](/tutorials/watches/). Related tutorials include [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/), [Indexes, Path Metadata, and Typed Query](/tutorials/indexes-path-metadata-and-typed-query/), [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/), [PersonalDB](/tutorials/personaldb/), and [Repair and Diagnostics](/tutorials/repair-and-diagnostics/). Operators should also read [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/), [Index Operations](/operators/index-operations/), [PersonalDB Operations](/operators/personaldb-operations/), and [Repair and Diagnostics](/operators/repair-and-diagnostics/). Command syntax is in [Public CLI](/reference/public-cli/), index definition and query JSON are in [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/), and permission strings are in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## Source records come first

Anvil treats committed source records as truth. Object versions, bucket metadata events, append-stream records, authorisation tuples, PersonalDB log entries, index definitions, gateway routing records, and mesh lifecycle records are source or control records. They are the history from which other views are built.

Derived data is any maintained view of those records:

```text
object versions       -> path, metadata, typed JSON, full-text, vector, hybrid indexes
authz tuple log       -> derived usersets and permission labels
PersonalDB commits    -> projections and replica catch-up state
append stream records -> audit exports, delivery ledgers, timelines
bucket metadata       -> bucket administration views and routing decisions
```

A derived view should never become a private, untraceable truth. It should carry enough evidence to answer: which source stream did I read, through what cursor, under which definition, with which authorisation revision, and where is the durable manifest or checkpoint that proves it?

## What a watch does

A watch is an ordered feed of committed changes after a cursor. The consumer supplies its last durable position, receives later events, performs its own work, and advances its checkpoint only after that work is durable.

The safe loop is:

```text
read durable checkpoint
open watch after checkpoint
for each event:
  read or validate referenced source state if needed
  update derived data idempotently
  publish derived manifest, segment, export, or projection
  store checkpoint after the derived update is durable
```

If the worker crashes after processing an event but before writing the checkpoint, it will see that event again after restart. That is acceptable when processing is idempotent. If the worker stores the checkpoint before the derived update is durable, it can skip work forever. A checkpoint must mean "processed through here", not "received on the socket".

## Cursors and revisions are scoped

A cursor is a position in one watch stream. It is not a global timestamp. Different watch families use different cursor shapes:

| Watch family | Position shape | What it means |
| --- | --- | --- |
| Object prefix | `uint64 cursor` | Object-watch event position for a bucket/prefix. |
| Bucket metadata | `uint64 cursor` | Bucket metadata event position. |
| Authz tuple log | `uint64 revision` | Relationship-authorisation revision. |
| Index definition | `uint64 cursor` | Index definition lifecycle event position. |
| Index partition | `cursor_low` and `cursor_high` | Derived index partition event position. |
| PersonalDB group/projection | `cursor_low` and `cursor_high` | PersonalDB log or projection watch position. |
| Git source | `cursor_low` and `cursor_high` | Git-source event position; API exists, CLI support is not exposed in the current tutorial path. |
| Append stream | `record_sequence` | Append streams use `ReadAppendStream` and `TailAppendStream`, not the generic `anvil watch` surface. |

Store the whole position exactly as returned. Do not put an authz revision into an object watch. Do not store only the low half of a split cursor. Do not treat a cursor from a staging tenant as valid for production. A useful checkpoint record includes the tenant, stream family, bucket or namespace, prefix or partition, cursor, consumer id, software version, and time written.

The API responses often include a `WatchEventEnvelope`. That envelope carries common evidence such as watch stream id, partition family, partition id, low/high cursor, mutation id, record kind, object reference, revisions, payload hash, and emitted time. The current CLI prints compact human-readable lines and does not expose the full envelope. Use the API for production consumers that need complete evidence.

## Replay versus tail

A watch has two phases in practice. First it replays retained events after the supplied cursor. Then it tails newer events as they arrive.

For example, `ObjectService.WatchPrefix` reads a durable snapshot of object-watch events after `after_cursor`, then listens for live object events. `AuthService.WatchAuthzTupleLog` reads tuple-log history after `after_revision`, then listens for live tuple events. Some derived watches, such as index partition, authz namespace, authz derived lag, and git-source watches, poll durable records at intervals rather than using the live broadcast channel.

The user-facing rule is the same either way: you can reconnect from your last checkpoint. The implementation detail matters when you diagnose lag. A live-broadcast watch can report that the client fell behind the retained live window. A polling watch may simply keep asking storage for events after the last cursor. In both cases, the consumer should treat the cursor as the recovery boundary.

Current replay windows are not unlimited. Many watch RPCs read an initial batch capped at 1000 events. A consumer that has been offline long enough to accumulate more backlog than the single watch call can safely cover should use a rebuild or repair path, or an API that explicitly pages the underlying history where available. Do not assume one long-lived stream is a permanent archive reader.

## Durable checkpoints

The current public watch APIs do not provide a general tenant-facing checkpoint service. The CLI examples in [Watches](/tutorials/watches/) store cursors in local files only to demonstrate the rule. Production applications should store checkpoints in their own durable system: an Anvil object, an append stream record, a database row, a manifest CAS record, or another store with the same failure discipline.

Anvil itself has internal CoreStore-backed watch checkpoint support for derived workers such as index builders. Those checkpoints include stream id, partition, consumer id, cursor, source manifest hash, generation, writer node, hash, and signature. Updating them requires an ownership fence so a stale node cannot advance another builder's checkpoint. That mechanism is source-grounded, but it is not exposed as a general public CLI workflow today.

If your worker also uses [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/), treat the lease cursor and the watch cursor carefully. The lease service stores a monotonic progress cursor for a named task. It does not automatically prove that your derived output was written. The safe pattern is to write the derived output under the right preconditions, then checkpoint the task and watch position.

## Lag and catch-up

Lag is the distance between latest committed source state and the derived state a reader is using. Small lag is normal: a write can commit before a full-text indexer extracts text or before a vector builder produces embeddings. Persistent lag is operational evidence: a worker is slow, stopped, under-provisioned, blocked by a bad source record, or missing a required capability.

Anvil exposes lag in two ways. Watch streams let a consumer know its own last processed cursor. Index query responses include fields such as `source_watch_cursor_high`, `index_watch_cursor_applied`, `is_caught_up`, and `lag_record_count_hint`. The API exposes those fields directly; the current CLI helper sends the same query flags but prints hits and pagination rather than the full lag evidence. Query requests can also require catch-up:

```bash
anvil --profile acme index query documents documents_by_status \
  --typed-predicates-json '[{"field":"status","op":"eq","value":"ready"}]' \
  --require-caught-up-to-watch-cursor "$OBJECT_CURSOR" \
  --lag-timeout-ms 5000
```

This uses `IndexService.QueryIndex.require_caught_up_to_watch_cursor`. A successful result proves the index generation used for the answer has applied at least the source object cursor you supplied. It does not prove the index definition is the one your product intended, that the caller is allowed to see every source object, or that later writes have already been indexed. If the index has not reached the supplied cursor, the service can fail with `IndexLagging` rather than silently returning stale results.

Do not overstate `lag_timeout_ms`. The field exists on the current API and CLI, but current query paths primarily compare required cursors with materialised segment source cursors and return lag status when they are behind. Treat it as a consistency hint/limit for the query surface you are using, not as a promise that every index kind will block until fresh under all conditions.

## Derived indexes and projections

Indexes are the most visible derived data in Anvil. A typed JSON index derives field segments from object bodies or append records. A full-text index derives postings from extracted text. A vector index derives vectors from embeddings. A hybrid index combines several signals. All of those outputs should be traceable to a source cursor, index definition, generation, segment hashes, and authorisation context.

The index builder code publishes derived proofs and watch checkpoints. The proof records the index id, kind, partition, source watch stream, source cursor, source manifest hash, generation, segment hashes, and builder node. That is why query responses can report generation and applied cursor instead of merely saying "the index exists".

PersonalDB follows the same principle with different source records. A PersonalDB group watch reports committed group events. Projection watches report projection-specific progress, source log indices, hashes, definition hashes, and authz revision. The current public CLI exposes group watches, but not projection watches. Application projection workers should use the API directly when they need projection-level evidence.

Authz-derived state is similar. Tuple-log watches give the source revisions. Namespace watches tell derived systems when schema or namespace state changes. Derived-lag watches report how far an authz-derived index is behind. The tuple-log watch is exposed by the public CLI; namespace and derived-lag watches are API-only in the current repo.

## Backpressure and data-loss signals

A slow consumer should slow itself, not silently skip events. The current streaming handlers send events through bounded channels. Several live-broadcast surfaces report `DataLoss`-style errors when the receiver falls behind the retained live event window, with messages such as "watch fell behind retained live event window".

That message does not mean Anvil deliberately lost committed source truth. It means the streaming consumer can no longer rely on that live watch connection to have delivered every event after its last in-memory position. The correct response is to stop processing from the broken stream, reopen from the last durable checkpoint, and replay. If replay from that checkpoint is no longer available or the backlog exceeds what the watch surface can deliver safely, run a rebuild or repair from source records.

Backpressure also belongs in your design. If each event triggers slow external I/O, batch carefully, bound concurrency, and store checkpoints only after the external side effect is durable or explicitly idempotent. A watch worker that keeps accepting events while its output system is down is only building a larger correctness problem.

## Repair and rebuild

A rebuild is a controlled way to recreate derived state from source records. A repair is a controlled way to detect and correct drift. Both are different from blindly advancing a cursor.

Use repair or rebuild when:

```text
checkpoint is too old for incremental replay
source cursor and derived manifest disagree
segment hash or proof hash does not match
index generation omitted source records
PersonalDB projection state cannot be verified
an authz revoke must invalidate stale derived permission state
```

Repairs should not invent source records. If an object version, tuple, append record, or PersonalDB commit was never accepted, repair must not create it to satisfy a derived view. The safe repair direction is source to derived: read committed source records, rebuild or validate the derived artefact, publish a new generation or finding, and record evidence.

The tenant-facing repair tutorial is [Repair and Diagnostics](/tutorials/repair-and-diagnostics/). Operators should pair that with [Repair and Diagnostics](/operators/repair-and-diagnostics/) and [Index Operations](/operators/index-operations/).

## Watch surfaces today

The current repo exposes these public/API watch surfaces:

| Surface | API | Public CLI coverage |
| --- | --- | --- |
| Object prefix | `ObjectService.WatchPrefix` | `anvil watch prefix` |
| Bucket metadata | `BucketService.WatchBucketMetadata` | No direct public CLI command today |
| Authz tuple log | `AuthService.WatchAuthzTupleLog` | `anvil watch authz` and `anvil authz watch` |
| Authz namespace | `AuthService.WatchAuthzNamespace` | API only today |
| Authz derived lag | `AuthService.WatchAuthzDerivedLag` | API only today |
| Index definition | `IndexService.WatchIndexDefinition` | `anvil watch index-definition` |
| Index partition | `IndexService.WatchIndexPartition` | `anvil watch index-partition` |
| PersonalDB group | `PersonalDbService.WatchPersonalDbGroup` | `anvil watch personaldb` and `anvil personaldb watch` |
| PersonalDB projection | `PersonalDbService.WatchPersonalDbProjection` | API only today |
| Git source | `GitSourceService.WatchGitSource` | API only today |
| Append stream replay/tail | `ReadAppendStream` and `TailAppendStream` | `anvil stream read` and `anvil stream tail`, not `anvil watch` |

The table is intentionally precise. Do not document commands that are not present. If a workflow needs bucket metadata watches, PersonalDB projection watches, authz namespace watches, or git-source watches, use the API until the CLI grows those helpers.

## Security and authorisation

Watches are read surfaces, and read surfaces can leak information. An object watch reveals object keys, event types, version ids, sizes, ETags, delete-marker state, and timing. An authz watch reveals relationship changes. An index watch reveals derived maintenance activity. A PersonalDB watch reveals commit timing and hashes.

Current checks reflect that risk. Object prefix watches require `object:list` on the bucket. Bucket metadata watches require `bucket:watch`. Index watches require `index:watch`. Authz tuple watches require `authz:watch` for the namespace or configured resource. PersonalDB watches require `personaldb:watch` or the relevant relationship-authorisation path. Public-read buckets do not make watch streams anonymous.

Use narrow grants. A service account that exports one project prefix should not automatically be able to watch every object name in the bucket if your product model treats names as sensitive. Where current checks are coarser than the ideal model, document the coarse boundary and compensate with bucket layout, separate tenants, or narrower application roles.

## What to take forward

Watches are how Anvil connects source records to derived data without rescanning everything. A watch event is not complete until the consumer's derived output and checkpoint are durable. Cursors are scoped, checkpoints must be stored carefully, and replay must be safe. Lag is normal until a reader requires catch-up. Repair rebuilds derived views from source records; it does not invent source truth. The API exposes more watch detail than the current CLI, so production workers should use the API when they need envelopes, split cursors, exact lag, or API-only watch families.
