---
title: Task Leases and Fenced Mutations
description: Coordinate tenant background work with lease ownership, fence tokens, checkpoints, and API-level write preconditions.
---

# Task Leases and Fenced Mutations

This tutorial builds on [Watches](/tutorials/watches/) and [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/). Watches and append streams give you ordered work to process. Task leases help you decide which worker is currently allowed to process a named unit of work. Fenced mutations help that worker prove, at write time, that it still owns the lease.

Applications should use the public Coordination and Object APIs directly for production workers. The `anvil lease` commands below are manual helpers over the current `CoordinationService` task-lease methods; they are useful for learning and operations. The command reference is [Public CLI](/reference/public-cli/), the scope strings are in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and the consistency model is introduced in [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/).

## What a task lease is

A task lease is a temporary claim on a named task inside one tenant. The task id is your application name for the work, such as `import-acme-docs`, `projector-audit-events`, or `rebuild-search-billing`. Only one active security owner can hold that task at a time.

The stored owner is derived from the authenticated token, not from a caller-controlled owner field. In the current service, the owner includes the tenant id, principal kind, principal id, and actor instance id from the token. The `owner_label` on acquire is only a diagnostic label for humans. It cannot impersonate another owner, and checkpoint/commit requests do not contain an owner field at all.

The fence token is the correctness boundary. When a task is acquired for the first time, the fence token starts at `1`. If the lease expires and another owner later acquires it, the fence token increases. A stale worker that still remembers an old token can no longer checkpoint, commit, or pass an API lease-fence precondition.

A checkpoint is progress while a lease is still active. A commit validates the active owner and fence, returns the completed cursor in the response, and removes the active lease record. Current task-lease commit is not a durable job-completion ledger by itself; if your application needs a permanent completed cursor, write that cursor to your own manifest, append stream, object, or database as part of the workflow.

## When leases are different from ordinary writes

An ordinary object write proves that the caller has `object:write` for the object key at the time of the write. It does not prove that the caller still owns a background task. A stale worker can still perform an ordinary object write if it still has object write authority.

A fenced mutation adds a second condition: "perform this write only if my lease fence is still current". The current public API supports this through `MutationBatchRequest.precondition.lease_fence` on the Object service. That path can combine object operations, append-stream records, task-lease checkpoint/commit operations, object-version preconditions, and manifest compare-and-swap operations under one request shape.

The current public CLI does not expose `MutationBatch`. Use the CLI to inspect and exercise lease lifecycle. Use the API when correctness depends on object writes or append records being rejected for stale lease owners.

## Prerequisites and current limits

The examples use a task called `import-acme-docs`. Task ids must be safe path components: non-empty, not `.` or `..`, no slash, and no control characters. The resource string checked by public policy is `task_lease/<task_id>`.

Use narrow grants rather than broad ones:

| Purpose | Public policy action | Resource checked today |
| --- | --- | --- |
| Acquire, checkpoint, or commit this task | `coordination:lease_write` | `task_lease/import-acme-docs` |
| Read this task lease | `coordination:lease_read` | `task_lease/import-acme-docs` |
| Force-release this task lease | `coordination:lease_admin` | `task_lease/import-acme-docs` |
| Write task output objects | `object:write` | The exact bucket/key being written |

A production worker that writes objects and checkpoints the lease also needs the object or stream permissions for those writes. The lease permission does not grant data access, and data access does not grant lease ownership.

The CLI shown here exposes named task leases. The lower-level ownership-fence API also exists in the Coordination service for internal partition ownership, but there is no public CLI surface for those ownership methods in the current repo.

## Acquire a lease

A task lease is associated with a task kind, a partition family, and a partition id. Anvil validates that `partition_id` is a 32-byte hex string. These fields are mostly labels and routing information for operators; the task id is the public policy resource.

Set a tutorial partition id:

```bash
PARTITION_ID=0000000000000000000000000000000000000000000000000000000000000001
```

Acquire the lease for 30 seconds:

```bash
anvil --profile acme lease acquire import-acme-docs object-import object-prefix "$PARTITION_ID" \
  --owner-label importer-1 \
  --source-cursor-low 0 \
  --source-cursor-high 0 \
  --ttl-nanos 30000000000
```

This calls `CoordinationService.AcquireTaskLease`. A successful response proves that the caller authenticated, had `coordination:lease_write` on `task_lease/import-acme-docs`, the task id and partition id were valid, and no other active security owner currently held the task. The CLI prints a compact row containing the task id, fence token, owner principal id, and checkpoint cursor.

It does not prove that the source queue has work, that your worker has permission to write output objects, or that the lease will remain valid until the worker finishes. The lease expires at the server-calculated expiry time unless the same owner reacquires it first.

The public API and CLI use nanoseconds for task-lease TTL. The CLI default is 30 seconds. The server caps public task leases by `task_lease_ttl_secs`, which defaults to 300 seconds; requesting `0` or a negative value asks for the server cap, and requesting more than the cap is reduced to the cap.

Save the fence token from the output before continuing:

```bash
FENCE_TOKEN=1
```

## Read the current lease

Read the stored lease:

```bash
anvil --profile acme lease read import-acme-docs
```

This calls `CoordinationService.ReadTaskLease`. A successful response proves that the caller has `coordination:lease_read` for the task. The CLI prints `found=true` and the compact lease row when a lease record exists.

Read does not decide whether your worker should continue. The API response includes expiry timestamps, owner details, source cursor, checkpoint cursor, lease epoch, lease hash, and lease signature, but the current CLI prints only a subset. If you need to make automated decisions about expiry, use the API response rather than parsing the CLI row.

An expired lease can still be present until it is reacquired, committed, or force-released. Treat expiry as a time condition, not as proof that the record has disappeared.

## Checkpoint progress

Checkpoint after processing records up to cursor `125`:

```bash
anvil --profile acme lease checkpoint import-acme-docs "$FENCE_TOKEN" 125 0
```

This calls `CoordinationService.CheckpointTaskLease`. The two cursor arguments are the low and high halves of a `u128` cursor. For ordinary `u64` watch cursors or append stream sequences, put the value in the low half and use `0` for the high half.

A successful checkpoint proves that the caller is the same security owner that holds the active lease, the fence token still matches, the lease has not expired, and the checkpoint did not move backwards. It updates the active lease's checkpoint cursor.

It does not prove that output objects, derived indexes, or external side effects are complete. Only checkpoint after the work represented by that cursor has been made durable somewhere safe. If a worker crashes after processing but before checkpointing, the replacement should replay from the last stored checkpoint and make its side effects idempotent.

If checkpoint fails with `StaleFence`, another acquisition has moved the fence. If it fails with `LeaseOwnerMismatch`, the token used for checkpoint is not the same security owner as the one that acquired the lease. If it fails with `LeaseExpired`, stop writing and try to acquire a fresh lease before resuming.

## Renew by acquiring as the same owner

There is no separate `lease renew` command in the current public CLI. Re-run acquire with the same authenticated owner before expiry if the worker needs more time:

```bash
anvil --profile acme lease acquire import-acme-docs object-import object-prefix "$PARTITION_ID" \
  --owner-label importer-1 \
  --source-cursor-low 125 \
  --source-cursor-high 0 \
  --ttl-nanos 30000000000
```

When the same security owner reacquires an active lease, the current implementation keeps the same fence token and extends the lease. The lease epoch changes, and the checkpoint cursor is at least the supplied source cursor. If a different owner tries to acquire while the lease is active, the service returns `LeaseHeld`.

Be careful with short-lived access tokens. Because the owner includes the token actor instance id when the token has one, reacquiring or checkpointing with a different token instance can be treated as a different owner. Keep a stable worker identity for one lease cycle, or design the worker to stop and reacquire cleanly when its token changes.

## Commit when the unit is complete

Commit after all work through cursor `150` is durable:

```bash
anvil --profile acme lease commit import-acme-docs "$FENCE_TOKEN" 150 0
```

This calls `CoordinationService.CommitTaskLease`. A successful response proves that the caller still owns the active lease, the fence token matches, the lease has not expired, and the committed cursor did not move backwards relative to the current checkpoint. The service then removes the active lease and returns the previous lease in the response. The CLI prints `committed=true` and the previous compact lease row.

Commit does not by itself write your application result, close an append stream, or force an index to catch up. If the completed cursor must be durable after the lease disappears, store it in your own state. A common pattern is to use the API-only mutation batch path to write the output object or append-stream record and checkpoint or commit the lease under the same lease-fence precondition.

After a successful commit, `anvil lease read import-acme-docs` should normally print `found=false` unless another worker has already acquired the task again.

## Recover from stale or abandoned workers

If a worker dies, the lease eventually expires. Another worker can acquire the same task after expiry; the new lease gets a higher fence token. The old worker's checkpoint, commit, and fenced mutation attempts then fail because the old fence is stale.

Read the lease first when debugging:

```bash
anvil --profile acme lease read import-acme-docs
```

If the current owner is still alive, do nothing. If the owner is gone and waiting for TTL is acceptable, let the lease expire naturally. If an operator needs to clear the task immediately, use force release:

```bash
anvil --profile acme lease force-release import-acme-docs
```

This calls `CoordinationService.ForceReleaseTaskLease`. A successful response proves that the caller has `coordination:lease_admin` for the task. It prints whether a lease was released and, when present, the previous lease row.

Force release does not commit work, roll back partial writes, or prove that the old worker stopped. It only removes the current lease record. Use it for recovery, not as a normal worker loop. A stale worker with ordinary object-write permission can still write unless its writes use the API lease-fence precondition.

## Use fenced mutations in the API

For correctness-sensitive workers, checking a lease and then doing a normal object write is not enough. The lease may expire or be taken over between those two calls. Use `ObjectService.MutationBatch` with a `WritePrecondition.lease_fence` when the write itself must be rejected for stale owners.

The current API fields are:

```text
MutationBatchRequest.bucket_name
MutationBatchRequest.mutation_context
MutationBatchRequest.precondition.lease_fence.task_id
MutationBatchRequest.precondition.lease_fence.fence_token
MutationBatchRequest.precondition.object_versions[]
MutationBatchRequest.operations[]
```

Supported batch operations include `put_object`, `patch_json_object`, `delete_object`, `append_stream_record`, `checkpoint_task_lease`, `commit_task_lease`, and `compare_and_swap_manifest`. Object-version preconditions let you require an object to be at a specific version or require it not to exist. The manifest operation has its own `expected_revision`, which is a compare-and-swap guard for manifest-style state.

A mutation batch with `precondition.lease_fence` proves, before the operations run, that the caller is the same security owner, the fence token matches, and the lease has not expired. If the fence is stale, the batch fails with `StaleFence` instead of writing output.

Do not overstate the current batch semantics. The service checks the batch preconditions and then executes the requested operations; it is not exposed in the CLI and should not be treated as a general rollback transaction in documentation. Design each operation to be idempotent, and use object-version or manifest CAS guards when repeated attempts must not duplicate work.

## Relate leases to append streams and index lag

A lease cursor is opaque to Anvil. It can represent an append stream sequence, an object watch cursor, an index partition cursor, or an application-specific progress number. The lease service only stores and compares it for monotonic progress on checkpoint and commit.

For append-stream consumers, use the last processed `record_sequence` as a cursor and store it in the low half. For object-watch consumers, use the watch cursor. If the worker updates a typed JSON index or depends on one, a lease checkpoint does not prove the index has caught up. Use index query catch-up fields where supported, and design the worker to tolerate derived-data lag.

A robust retry loop looks like this: acquire or renew the lease, read from the last durable checkpoint, process a bounded batch, write side effects through API preconditions, checkpoint only after those side effects are durable, and commit or release operationally when the unit is complete. If any stale-fence or owner-mismatch error appears, stop immediately and let the current owner continue.

## What to take forward

Use task leases to coordinate who may work, not as a substitute for data permissions. Use fence tokens to prevent stale owners from making progress. Use checkpoints for restartable progress, and store completed state somewhere durable if you need it after commit removes the lease. Use direct API mutation batches with `lease_fence`, object-version preconditions, and manifest CAS for correctness-sensitive writes, because the current public CLI only exposes the lease lifecycle and not fenced data mutations.
