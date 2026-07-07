---
title: Writes, Consistency, and Fences
description: Understand Anvil write visibility, object versions, idempotency, compare-and-swap, mutation context, task leases, fence tokens, mutation batches, and watch cursors.
---

# Writes, Consistency, and Fences

A storage system is easy to understand when one client writes one file and then reads it back. It becomes difficult when a network timeout hides whether a write committed, two editors save the same object, an index builder restarts halfway through a batch, or an old worker wakes up after another worker has taken over its task.

Anvil's write model is built around explicit evidence for those cases. A writer does not only send bytes. It also sends identity context, retry context, and, when correctness requires it, preconditions. The server creates immutable versions, moves current pointers, records watch cursors, and rejects stale work when the supplied evidence no longer matches committed state.

This page is the conceptual companion to [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/), [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/), [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/), and [Watches](/tutorials/watches/). It builds on [Object Model](/learn/object-model/) and [CoreStore](/learn/corestore/), then prepares you for [Watches and Derived Data](/learn/watches-and-derived-data/) and [Indexes and Query](/learn/indexes-and-query/). Command syntax lives in [Public CLI](/reference/public-cli/), and permission strings live in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## Source truth and derived truth

Anvil separates source records from derived views.

The source truth for an object key is its version history and current pointer. A successful object write creates a new version record. A current read follows the current pointer. A pinned read asks for a specific `version_id`. A delete without a version id writes a delete marker and moves the current pointer to that marker, so current reads behave as not found while earlier versions can still exist in history.

Derived views are built from source records. Object watches, indexes, full-text search, vector search, authz-derived usersets, PersonalDB projections, and operational diagnostics may process committed source events after the source write returns. That lag is not a corruption bug; it is the reason Anvil exposes cursors and catch-up checks.

The resulting operating principle is:

```text
source write committed != every derived view has already caught up
```

If your workflow needs the object itself, read the object. If it needs a search result, index generation, projection, or external consumer to include the write, carry the returned cursor and require the derived surface to catch up where that surface supports it.

## What an object write makes visible

A normal native object write through `ObjectService.PutObject` starts with metadata, then streams bytes. The metadata includes the bucket, object key, optional content type, user metadata, and a `NativeMutationContext`. If the request is authorised and passes validation, Anvil writes the payload, creates a new object version, publishes object metadata, moves the key's current pointer, and returns evidence:

| Response field | What it is for |
| --- | --- |
| `version_id` | The exact committed object version. Use this for pinned reads and version preconditions. |
| `etag` | An opaque validator for the visible object representation. Use it for ETag preconditions; do not parse it. |
| `mutation_id` | The server-side mutation identifier for tracing and audit correlation. |
| `payload_hash` and `record_hash` | Integrity evidence for the payload and metadata record. |
| `authz_revision` | The relationship-authorisation revision observed by the write path. |
| `index_policy_snapshot` | The index policy snapshot associated with the object version. |
| `watch_cursor` | The object-watch position produced by this mutation. Store it if a downstream consumer or query must catch up. |

Readers should not observe a half-written object as current. They see the previous committed state, the new committed state, or a failure. That is different from saying every possible read in every region and every derived view is globally instantaneous. Current implementation paths are strongest around the committed source record and the serving placement for that bucket; cross-region routing, gateways, and derived views have their own documented behaviour and limits.

## Current pointer, versions, and last-writer-wins

The current pointer is the mutable part of an object key. Versions are immutable committed states. When you overwrite `documents/report.txt`, the old version is not edited in place. Anvil creates a new version and makes the current pointer refer to it.

If two writers both write without preconditions, the later successful write can become current. That may be exactly what you want for a cache, generated preview, or last-saved draft. It is not safe for a collaborative document, package manifest, approval record, or any state where an older client must not silently erase a newer decision.

For those workflows, the client should read the current version first, remember its `version_id` or `etag`, and send a precondition on the later mutation. A failed precondition is useful information: another writer changed the source record before you. The correct response is to reload, merge, ask a user, or abandon the stale update. Do not treat it as a storage outage that should be blindly retried.

## Mutation context is part of the write contract

Native object mutations carry `NativeMutationContext`. It is not decorative metadata. It tells the server which tenant, bucket, principal, request, precondition, authorisation revision, and idempotency key the caller believes it is using.

| Field | Tutorial meaning |
| --- | --- |
| `tenant_id` | Must match the authenticated token's tenant. A caller cannot write for another tenant by changing this field. |
| `bucket_id` | Must match the named bucket. This prevents a stale or confused client from applying a context to the wrong bucket. |
| `principal` | Must match the authenticated principal. It is checked; it is not a way to impersonate another app. |
| `request_id` | A caller-supplied operation identifier useful for tracing. It must be present. |
| `precondition` | A current-pointer precondition string such as `none`, `version:<uuid>`, or `etag:<etag>`. |
| `authz_zookie_optional` | Optional authorisation revision requirement. If supplied, Anvil requires the tenant authz log to have reached that revision before accepting the write. |
| `idempotency_key` | A stable retry key for this logical mutation. Reuse it for retries of the same operation, not for unrelated work. |

Current public CLI helpers construct this context for you, but they usually choose a fresh idempotency key and `precondition: "none"`. That is fine for smoke tests. It is not enough for correctness-sensitive application writes, because the CLI does not currently let you pin a version, reuse a retry key deliberately, or set an authz zookie. Production code should use the public API or a client library.

## Idempotency answers the timeout question

A timeout is ambiguous. The server may have committed the write and lost the response, or it may not have received the request at all. Idempotency lets the client make a retry safe by saying: "this retry is the same logical operation as the first attempt".

For native object mutations, Anvil stores an idempotency record keyed by tenant, bucket, principal, and `idempotency_key`, and validates that the same key is being used for the same mutation target. If the original operation committed, a later retry with the same context and target can return the stored response. If the same idempotency key is reused for a different target or parameter set, the server rejects it rather than guessing which effect the caller wanted.

Idempotency is not a substitute for preconditions. It prevents duplicate effects from one logical request being retried. It does not stop two different logical requests from racing. Use an idempotency key to answer "did my retry already commit?" Use CAS or version preconditions to answer "is the object still in the state I read?"

CoreStore streams have a lower-level version of the same rule: appending a stream record with the same idempotency key and same payload can replay the original append receipt, while reusing the key with different bytes is an idempotency conflict. Higher-level APIs use this pattern internally, but application code should rely on the public API fields rather than writing CoreStore records directly.

## Compare-and-swap and object preconditions

Compare-and-swap, usually shortened to CAS, means: change this mutable pointer only if it still has the value I expect. The pointer might be an object current pointer, a link generation, a manifest revision, an index generation head, a lease ref, or an internal CoreStore ref.

Native object mutations expose current-pointer preconditions as a string in `NativeMutationContext.precondition`:

| Precondition | Meaning today |
| --- | --- |
| `none` | Do not check the current pointer. Last-writer-wins may occur. |
| `exists` | Continue only if the key currently has a non-deleted object. |
| `not_exists`, `not-exists`, or `absent` | Continue only if the key currently has no non-deleted object. |
| `version:<uuid>` | Continue only if the current object version id matches the supplied UUID. |
| `etag:<etag>` | Continue only if the current object ETag matches. Quotes around the ETag are ignored for comparison. |

Several APIs also accept structured `WritePrecondition` values. An object-version precondition can require another object to be at a particular version or require it not to exist. A lease-fence precondition can require the caller still to hold a task lease. `PatchJsonObject`, append-stream writes, stream segment sealing, `CompareAndSwapManifest`, and `MutationBatch` use this structured shape.

The S3-compatible gateway maps relevant HTTP conditional headers such as ETag preconditions onto the same idea for supported operations. S3 is still a gateway over Anvil's model; the native API is the clearest way to express all current Anvil write preconditions.

## Manifest CAS

Many applications need a small mutable manifest rather than a single object body. A package registry may publish immutable blobs under versioned keys, then update `latest`. A document system may publish a JSON index of the current approved version. A build system may publish a manifest that points at several artefacts.

`ObjectService.CompareAndSwapManifest` models that as a numeric revision. The caller sends:

```text
manifest_key: "releases/current.json"
expected_revision: 7
manifest_json: "{ ... }"
mutation_context: <NativeMutationContext>
precondition: <optional WritePrecondition>
```

Anvil reads the current manifest revision for that key. If it is still `7`, it appends the new manifest body and returns revision `8`. If another writer already published revision `8`, the request fails with `Manifest revision mismatch`. The failure means your product logic needs to reread the manifest and decide again.

There is no current public CLI helper for manifest CAS. Treat it as an API feature. The CLI can upload ordinary manifest-shaped objects, but an ordinary upload is not the same as revision-checked manifest CAS.

## Task leases and fence tokens

CAS protects a specific mutable pointer. It does not by itself answer whether a background worker still owns the work it is about to publish. A worker can read a watch cursor, spend minutes building an index segment, lose its lease, and still have enough ordinary object permission to write stale output unless the write itself checks ownership.

A task lease is a temporary claim on a named tenant task. The current public Coordination service exposes acquire, checkpoint, commit, read, and force-release methods, and the public CLI exposes matching `anvil lease` helpers. The stored owner is derived from authenticated claims: tenant id, principal kind, principal id, and actor instance id. The acquire request's `owner_label` is only a diagnostic label for humans. Checkpoint and commit requests do not contain an owner field, so they cannot spoof another owner by copying a string.

When a lease is acquired, Anvil returns a fence token. The first token is `1`. If the lease expires and a different owner acquires the same task while the old state is still present, the fence token increases. A stale worker using the old token then fails checkpoint, commit, and any write protected by `WritePrecondition.lease_fence`.

Current task leases also have practical limits:

| Limit | What it means for applications |
| --- | --- |
| TTL cap | Public task leases are capped by `task_lease_ttl_secs`, defaulting to 300 seconds. The CLI default is 30 seconds. Renew by acquiring again with the same authenticated owner before expiry. |
| Commit semantics | Commit validates owner, fence, expiry, and monotonic cursor, then removes the active lease record. It is not a permanent completed-job ledger. Store completed progress in your own durable state if you need it later. |
| Force release | Force release removes the active lease record for recovery. It does not prove the old process stopped, roll back ordinary writes, or commit work. Use it sparingly and pair it with fenced write paths. |
| CLI visibility | The CLI prints only a compact subset of the lease record. Automated workers should use the API response. |

Force release deserves special care. Because it deletes the active named lease record, it is an operational break-glass action, not a normal handoff protocol. Normal expiry-and-takeover preserves enough state to advance the token; force-release workflows should assume the old process may still be alive and should design their data writes to be idempotent, version-checked, and observable.

## Fenced mutations

A fenced mutation says: perform this write only if this authenticated caller still owns task `T` with fence token `F`.

The current public Object API supports that check through `WritePrecondition.lease_fence`. Before running the protected operation, the service reads the named task lease in the authenticated tenant, derives the owner from the caller's claims, checks owner equality, checks the fence token, and checks that the lease has not expired. If any of those checks fail, the mutation fails before that operation starts.

This is the difference between a safe worker and a hopeful worker:

```text
hopeful worker:
  read lease
  do work
  ordinary object put
  checkpoint lease

fenced worker:
  acquire lease and remember fence token
  do work
  submit write with lease_fence and object/version preconditions
  checkpoint or commit only after output is durable
```

The first pattern can publish stale output if ownership changes between the read and the put. The second pattern makes ownership part of the write condition.

The current public CLI can acquire, read, checkpoint, commit, and force-release task leases, but it does not expose `ObjectService.MutationBatch` or structured `WritePrecondition` flags for object writes. Use the CLI to learn and inspect lease state. Use the API when stale-worker rejection is part of your correctness story.

## Mutation batches: two layers, different promises

The phrase "mutation batch" appears at two layers, and they should not be blurred.

At the CoreStore layer, a `CoreMutationBatch` groups ref updates and stream appends in one scope partition under shared preconditions. Cross-partition atomic mutation is rejected today. Reads and watches filter transaction-linked records so uncommitted stream records are not exposed as source truth. This is the internal substrate described in [CoreStore](/learn/corestore/).

At the public Object API layer, `ObjectService.MutationBatch` is a convenience request shape for a set of object, append-stream, task-lease, and manifest operations. It validates the native mutation context, checks the supplied `WritePrecondition`, applies authorisation for task-lease operations, records an idempotency response for the request, and returns operation receipts.

Do not overstate the public batch as a general rollback transaction. In the current implementation, high-level operations are executed through their existing service paths after preconditions and locks are acquired. If you need retry-safe workers, design each operation to be idempotent and add explicit object-version, manifest-revision, and lease-fence checks. Use the batch to put the checks and receipts in one API flow, not to assume arbitrary multi-object rollback semantics.

Supported public batch operations today include:

| Operation | What it is useful for |
| --- | --- |
| `put_object` | Publish output bytes under the batch's bucket. |
| `patch_json_object` | Apply a JSON merge patch to an object, optionally from a base version. |
| `delete_object` | Delete the current object or a specified version. |
| `append_stream_record` | Append an audit, event, or history record to an append stream. |
| `checkpoint_task_lease` | Advance progress while retaining the lease. |
| `commit_task_lease` | Validate final progress and remove the active lease. |
| `compare_and_swap_manifest` | Publish a revision-checked manifest update. |

## Watch cursors connect writes to consumers

A write response's `watch_cursor` is the position of the object-watch event caused by that write. A consumer can store that cursor after it has safely processed the event, then restart from `after_cursor` instead of rescanning a whole bucket. This is how derived systems avoid both missed updates and expensive full scans.

Cursors are surface-specific. Object prefix watches use numeric cursors. Append streams use record sequences. Authz tuple watches use revisions. Index and PersonalDB watches have their own cursor shapes. Treat a cursor as an opaque checkpoint for the surface that returned it, not as a global clock for every Anvil feature.

Index queries expose the consistency boundary explicitly. `QueryIndexRequest.require_caught_up_to_watch_cursor` lets a caller require an index to have applied at least a named source watch cursor where supported. The response includes fields such as `source_watch_cursor_high`, `index_watch_cursor_applied`, `is_caught_up`, and `lag_record_count_hint`. If the index has not caught up within the requested timeout, the query can fail or report lag rather than pretending stale search is fresh truth.

At the CoreStore layer, ref preconditions can record a `source_watch_cursor` and reject a mutation if that cursor is no longer retained. That is mostly an internal correctness tool for derived publishers. Application code should normally carry the public watch or query cursors returned by the relevant API.

## What Anvil guarantees today, and what it does not

It is safer to describe consistency by naming the condition being protected than by using one broad label.

| Condition | Current practical meaning |
| --- | --- |
| Committed object visibility | A current object read sees committed object state, not a half-uploaded payload. |
| Version history | Each object write creates a distinct version id; deletes create delete-marker behaviour for current reads. |
| Current-pointer CAS | Native mutation preconditions can reject writes when the current version, ETag, existence, or absence no longer matches. |
| API idempotency | Native mutation idempotency can replay a committed response for the same logical request and reject key reuse for a different target. |
| Manifest CAS | Manifest updates advance only from the expected revision. |
| Task-lease ownership | Checkpoint, commit, and lease-fenced writes derive owner from authenticated claims and reject owner mismatch, stale token, and expiry. |
| CoreStore batch scope | Internal mutation batches are scoped to one partition; cross-partition atomic mutation is not currently supported. |
| Derived view lag | Indexes, projections, and consumers may lag source writes; use cursors and catch-up fields where available. |

The model does not make every operation serialisable across every region, gateway, and derived system. It does not make a CLI smoke test equivalent to an API workflow with stable idempotency keys and preconditions. It does not make `object:write` imply lease ownership. It does not make force release prove that a dead worker is truly dead. It does not make an index query current unless the index has caught up to the source cursor you care about.

Those limitations are intentional boundaries to design around. When a workflow requires stronger behaviour, add the evidence: version preconditions for object edits, manifest revisions for manifests, idempotency keys for retries, lease fences for workers, and watch cursors for derived maintenance.

## A practical write loop

A robust worker or application save loop usually looks like this:

```text
1. Read the source state and keep its version id, manifest revision, or cursor.
2. Prepare the new payload or derived output.
3. Submit the write with a stable idempotency key.
4. Add object-version, manifest, and lease-fence preconditions that express what must still be true.
5. Store the returned version id, mutation id, and watch cursor.
6. Advance your own checkpoint only after the write or derived output is durable.
7. On precondition failure, reread and make a product decision before retrying.
```

For an interactive editor, the checkpoint may be a saved `version_id` in the user's session. For a package publisher, it may be a manifest revision. For an index builder, it may be the last object watch cursor applied. For an audit exporter, it may be the append-stream sequence last delivered to an external system. The mechanism changes, but the habit is the same: carry evidence forward and write only if the evidence still matches.

## Operational implications

Operators should expect correctness failures to be specific. `ObjectVersionPreconditionFailed`, `Manifest revision mismatch`, `LeaseHeld`, `LeaseExpired`, `StaleFence`, `LeaseOwnerMismatch`, idempotency conflict, watch cursor expiry, and index lag each mean a different thing. Collapsing them into "storage error" hides the action to take.

Use [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/) for lagging consumers, [CoreStore Operations](/operators/corestore-operations/) for storage substrate issues, [Repair and Diagnostics](/operators/repair-and-diagnostics/) for projection repair, and [Security Hardening](/operators/security-hardening/) for keeping public and admin planes separate. Do not repair lost updates by editing files under `STORAGE_PATH`; use the API, repair tools, or a documented restore path.

## What to take forward

Writes in Anvil are versioned, cursor-producing mutations over source records. Safe clients make their assumptions explicit. Idempotency handles retry ambiguity. CAS and object-version preconditions prevent lost updates. Manifest CAS protects mutable JSON heads. Task leases decide who owns background work. Fence tokens reject stale owners at mutation time. Watch cursors connect source writes to derived consumers.

The API exposes these concepts more completely than the current CLI. Use CLI commands as manual helpers and smoke tests. Use the public API for production code that must be correct under retries, races, stale workers, and derived-data lag.

## Choosing retry behaviour

A safe retry repeats the same logical operation with the same idempotency key and the same intended precondition. It should not generate a new business operation every time a network timeout occurs. For example, a renderer writing `reports/42/output.pdf` should use a stable request id or idempotency key derived from report id and render generation, then treat a precondition failure as evidence that another writer changed the object.

Fences add one more guarantee for background work. A worker that acquired a lease at fence token `7` can publish output only while token `7` is still valid for that task. If the lease expires and another node acquires token `8`, late writes from token `7` must fail. This is why Anvil background maintenance is described as in-process leased work rather than a best-effort daemon loop.
