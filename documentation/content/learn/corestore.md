---
title: CoreStore
description: Understand Anvil's unified durable substrate: blobs, refs, streams, mutation batches, watches, fences, root state, feature records, and current distribution limits.
---

# CoreStore

CoreStore is the durable substrate underneath Anvil. It is not a tenant-facing database, a gateway protocol, or a command-line feature. It is the internal layer that gives objects, metadata, indexes, authorisation, PersonalDB, gateways, leases, audit records, and mesh lifecycle state one shared way to become durable, recoverable, watchable, and repairable.

You do not need to be a storage-engine specialist to understand the idea. Anvil has many product features, but their source records should not each invent a private file format, private transaction rule, private replication rule, and private repair path. CoreStore supplies a small set of storage primitives. Feature code then layers its own schemas on top of those primitives.

Read this page after [Object Model](/learn/object-model/). It prepares you for [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Indexes and Query](/learn/indexes-and-query/), [Authorisation](/learn/authorisation/), [Gateways](/learn/gateways/), [PersonalDB](/learn/personaldb/), and [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/). Operators should pair it with [CoreStore Operations](/operators/corestore-operations/), [Backup and Recovery](/operators/backup-and-recovery/), and [Repair and Diagnostics](/operators/repair-and-diagnostics/).

## Why Anvil avoids separate feature stores

A storage system starts to drift when every feature writes its own durable files. Object payloads might have one crash story, indexes another, authorisation tuples another, gateway upload sessions another, and mesh routing records another. That can look convenient in early development because each feature can choose a local layout. It becomes dangerous when you need to answer operational questions:

```text
Which records committed before the crash?
Which index generation was built from which object cursor?
Which authorisation revision was used for this query page?
Did a stale worker publish state after losing its lease?
Can repair rebuild this view from source records?
Can backup restore objects, indexes, authz, and routing together?
```

CoreStore exists so those questions have one family of answers. Immutable bytes become CoreStore blobs. Mutable heads become generationed refs. Ordered history becomes streams. Multi-record visibility goes through mutation batches. Workers that own background work carry fence tokens. Watches expose cursors so consumers can resume instead of rescanning everything.

The goal is not to make every feature have the same payload format. A full-text segment is different from a vector segment, and a PersonalDB changeset is different from an authz tuple. The goal is that their durable state is written, checked, watched, recovered, and repaired through the same substrate.

## The basic primitives

CoreStore is easiest to remember as three nouns plus the coordination rules around them:

| Primitive | What it means | Typical Anvil use |
| --- | --- | --- |
| Blob, or CoreObject | Immutable bytes addressed and verified by hash, with a manifest that records size, encoding, placement, and mutation evidence. | Object bodies, sealed stream segments, index segments, PersonalDB snapshots and changeset payloads, gateway package blobs. |
| Ref, or CoreRef | A named mutable pointer with a generation and compare-and-swap preconditions. | Current object heads, index generation manifests, task lease state, gateway tags, root catalog heads. |
| Stream, or CoreStream | An ordered sequence of records with sequence numbers, cursors, event hashes, and optional idempotency keys. | Object metadata history, bucket metadata history, append stream records, authz tuple logs, audit trails, lifecycle records, watch inputs. |
| Mutation batch | A committed group of ref updates and stream appends under shared preconditions in one scope partition. | Publish an object metadata event and current pointer together; advance a derived generation after writing its segment; append audit evidence with state change. |
| Fence | A time-bounded ownership token bound to the authenticated principal. | Reject stale workers, index builders, compaction jobs, and lease-protected mutations. |
| Watch cursor | A resumable position in committed stream history. | Derived index maintenance, audit consumers, projection builders, operational tailing. |
| Root catalog and quorum profile | Control records for durable roots, placement groups, and quorum expectations. | Mesh root state, control-plane recovery, future distributed placement behaviour. |

Those words appear throughout Anvil because they are the implementation vocabulary beneath the product vocabulary. An object current pointer is a ref-like idea. An append stream is stream-shaped state. A search index generation is blobs plus a published ref. A background task lease is a ref and fence problem.

## What a blob proves

A CoreStore blob is immutable bytes plus evidence for reading them back. The current local backend hashes the bytes with SHA-256, splits them with a Reed-Solomon `4+2` profile, writes shard files under local replica directories, and writes a manifest as a quorum-read control record. The manifest records the object hash, logical size, region id, encoding profile, shard placements, creation time, and mutation id.

When CoreStore reads a blob, it does not trust a filename alone. It reads the manifest, checks that the manifest matches the requested hash and size, verifies shard hashes and stored sizes, reconstructs missing data shards when enough shards remain, and then verifies the final byte hash before returning data.

That concrete behaviour matters even if future backends place shards on real nodes instead of local directories. The contract is that a blob reference is content evidence, not a pointer to whichever file happens to be on disk today. Operators should therefore treat `STORAGE_PATH` as durable CoreStore state, not as a cache that can be hand-edited. The local layout currently lives under the server-owned `_core/` directory inside the configured storage path.

## What a ref proves

A ref is a named pointer to a target, with a generation. Updating a ref is a compare-and-swap operation: the writer can require that the ref is absent, present, at a particular generation, or still pointing at a particular target. If the current state does not match, the update fails.

This is the same idea developers use at the object API level when they say "write this object only if the current version is still X". CoreStore applies it to lower-level heads: current object refs, manifest refs, index generation refs, fence records, root catalog refs, and other mutable pointers.

A CAS failure is useful evidence, not an infrastructure panic. It means another committed mutation changed the ref first. The safe response is to reread, decide whether the intended change is still valid, and retry with new evidence only if the product logic permits it. See [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/) and [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/) for the tenant-facing version of this pattern.

## What a stream proves

A stream is ordered history. Each stream record has a sequence number, a cursor, a payload hash, an event hash, and the previous event hash. That gives consumers more than a list of JSON lines. It gives them a chain they can replay and a cursor they can store after their own derived work is durable.

A stream append can carry an idempotency key. If a caller retries the same append with the same key and same payload, CoreStore can return the original receipt. If the same key is reused with different bytes, CoreStore rejects it as an idempotency conflict. This is the lower-level version of the retry-safety rule public APIs use for writes.

Streams can be sealed into segment blobs for compaction or archival, but sealing a segment is not the same as closing the logical stream. Current conformance tests check that a stream can still accept later records after a segment is sealed. This distinction matters for append streams and audit logs: segment sealing is a storage maintenance operation; stream closure would be a separate product-level state if a feature supports it. The tutorial [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/) explains the tenant-facing model.

## Transactions and mutation batches

A mutation batch groups visible state changes. In the current CoreStore API, a `CoreMutationBatch` has a transaction id, a scope partition, a committed principal, preconditions, and operations. Operations are currently ref updates and stream appends. They must belong to the same scope partition; cross-partition atomic mutation is deliberately rejected today.

The point of the batch is visibility. An object write should not publish a current pointer without the metadata event that explains it. An index builder should not publish a generation head before its segment and source cursor evidence are durable. A lease-protected worker should not update data if its fence is stale at commit time.

A failed batch should not leave a committed transaction record or visible stream/ref state. A successful batch writes a committed transaction record and its visible updates. Reads and watches filter transaction-linked records so uncommitted work does not appear as source truth.

This is why CoreStore is more than a blob store. The hard part in application storage is not merely storing bytes; it is deciding which bytes, metadata, heads, and events become visible together.

## Watches and cursors

CoreStore watches are built from committed stream records. A watch request names a stream prefix and an optional cursor. CoreStore returns later events in cursor order, with enough information for a consumer to know the stream id, sequence, event type, event hash, payload hash, transaction id, and creation time.

Higher-level Anvil watches wrap this idea for specific features. Object watches expose object keys and delete markers. Authz tuple watches expose relationship changes. Index maintenance consumes object and authz histories. PersonalDB watches expose group changes. The common pattern is the same: a consumer stores its last processed cursor only after its own derived state is safe.

A cursor is not an eternal right to replay from the beginning of time. The implementation can reject a source cursor if the referenced stream position is no longer retained, and code surfaces that as a `WatchCursorExpired`-style failure. Consumers should be prepared to fall back to a rebuild from source records when their checkpoint is too old. See [Watches and Derived Data](/learn/watches-and-derived-data/) and [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/) for operational patterns.

## Fences and stale-worker rejection

A fence is a claim that a particular authenticated principal owns a piece of work for a bounded time. CoreStore fence records include the fence name, owner principal, fence token, expiry time, and update time. The local implementation caps the requested TTL at 120 seconds.

The important security property is that the owner is not just a caller-supplied string. Mutation-batch fence checks derive authority from the committed principal and compare it with the fence record. If an impersonating worker sends the same task id or owner text but is not the authenticated principal that holds the fence, the fenced mutation fails. If the old worker's token has expired or a later owner has acquired a newer token, the stale mutation fails.

Fences are used where CAS alone is not enough. CAS can prevent lost updates to one pointer, but a background worker often reads input, performs expensive work, and later tries to publish several results. A fence lets the commit path ask: "is this still the worker that owns the task?" The public tutorial [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/) describes the application-facing pattern and current exposure gaps.

## Reserved namespaces and private storage

CoreStore is internal. Tenants should never write CoreStore files directly, and public object keys should never be used as disguised system records. Anvil protects both boundaries.

At the API level, object keys under Anvil-owned prefixes such as `_anvil/meta/`, `_anvil/index/`, `_anvil/authz/`, `_anvil/watch/`, `_anvil/personaldb/`, `_anvil/git/`, and `_anvil/tmp/` are reserved. Public object reads, writes, lists, and gateway requests reject those namespaces before ordinary object logic can treat them as user data. This prevents tenants from reading relationship tuples, forging index state, or depending on internal layouts.

At the storage-path level, the server owns `_core/` and `tmp/` under `STORAGE_PATH`. `_core/` contains CoreStore replicas, blobs, manifests, refs, streams, transactions, locks, and staging data. `tmp/` may hold transient upload staging before a write commits. Operators may back up or restore these directories as part of a controlled runbook, but application code should not interpret or mutate them as product data.

The permission vocabulary for public resources is documented in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/). The operational rule is simpler: use the public API for tenant data, use the private admin API for operator lifecycle, and do not bypass either plane by editing CoreStore state by hand.

## Feature records layered on top

Feature-specific schemas still exist. CoreStore does not make a vector segment look like an authz tuple or a PersonalDB snapshot. It gives each feature a durable home and a shared set of correctness rules.

| Feature family | How it layers on CoreStore today |
| --- | --- |
| Object payloads | Bodies are written as blobs; object metadata records and current heads are CoreStore-backed journal/ref state. |
| Buckets and object links | Bucket metadata, object-link descriptors, generations, and delete markers are durable records rather than side files. |
| Append streams and audit | Records are stream entries; sealed segments are blobs; cursors support replay. |
| Task leases | Lease state uses refs, blob-backed records, and fence-token checks. |
| Authorisation | Namespace schemas, tuple logs, and derived userset indexes are CoreStore-backed and revision-aware. |
| Path, typed, full-text, vector, and hybrid indexes | Derived segments are blobs; generation manifests and source cursor evidence are published through refs and journals. |
| PersonalDB | Changeset payloads, commit certificates, heads, snapshots, projections, and watch records are CoreStore-backed. |
| Mesh and gateways | Region, cell, node, bucket locator, host alias, gateway mount, credential, package blob, tag, upload session, and audit records are CoreStore-backed. |
| Repair and diagnostics | Findings and repair evidence should point back to source records and be durable enough for audit. |

This table describes storage layering, not necessarily a complete public protocol surface for every feature. For example, S3-compatible object access is implemented as a gateway surface, while package gateway pages currently focus on foundations and modelling unless a specific protocol handler is present. See [Gateways](/learn/gateways/), [S3-Compatible Gateway](/tutorials/s3-gateway/), and [Package Gateway Foundations](/tutorials/package-gateway-foundations/) for the current product surfaces.

## Distribution and current implementation honesty

CoreStore's API is designed to hide placement and replication details from feature code. The types already model object manifests, placements, root catalogs, and quorum profiles. The current local backend, however, is the first backend used by tests and single-node development. It simulates control replicas as local files, uses five local control replicas with a read/write quorum of three, and writes data shards under local replica directories.

That means you should not read this page as a claim that all future multi-region replication, remote shard placement, network quorum, automatic shard repair, or cross-region proxy behaviour is production-complete today. The code is structured so those capabilities can live behind the same CoreStore API, but the current documentation should stay honest about what is implemented.

For operators, the practical consequence is conservative: protect `STORAGE_PATH`, back it up consistently, do not edit `_core/` manually, monitor CoreStore read/write errors, and treat derived indexes as rebuildable from source records rather than as the only copy of truth. The operator pages [CoreStore Operations](/operators/corestore-operations/), [Observability](/operators/observability/), [Backup and Recovery](/operators/backup-and-recovery/), and [Repair and Diagnostics](/operators/repair-and-diagnostics/) cover those responsibilities.

## What may live outside CoreStore

Not every file touched by the process is authoritative durable feature state. Some files are allowed to exist outside CoreStore because they are temporary, bootstrap-only, or process-local:

- upload staging before the write commits;
- scratch files used to build a PersonalDB snapshot or index segment before publishing it;
- process caches that can be rebuilt;
- operator bootstrap files such as initial credentials or node identity material;
- build artefacts outside the running server's durable state.

The test is whether deleting the file changes committed Anvil truth. If deleting a scratch file merely forces a retry, it may be outside CoreStore. If deleting it loses an object version, tuple, index generation, gateway mount, lease, route, audit event, or PersonalDB commit, it belongs in CoreStore-backed state.

## Reading CoreStore failures

CoreStore failures are usually specific. Collapsing them into "storage error" makes incidents harder to resolve.

| Failure shape | What it usually means |
| --- | --- |
| Ref generation or target mismatch | Another writer changed the mutable head first; reread before deciding whether to retry. |
| Stream head mismatch | The append expected a previous sequence/hash that is no longer current. |
| Idempotency conflict | The same idempotency key was reused with different bytes. |
| Fence precondition failed | The worker no longer owns the task, used the wrong principal, or let the fence expire. |
| Watch cursor expired | The consumer checkpoint is older than retained stream history; rebuild from source records. |
| Read quorum failure | Enough matching local control replicas were not available or did not agree. |
| Blob hash or manifest mismatch | Data or manifest integrity checks failed; treat as repair/restore territory. |

These errors are part of the design. They tell a caller or operator which correctness condition failed. A CAS mismatch is not the same as corrupted shards. An expired cursor is not the same as missing authorisation. A fence failure is not a reason to force-publish stale worker output.

## What to take forward

CoreStore is the reason Anvil can present many features without becoming many unrelated stores. Objects, links, indexes, watches, append streams, task leases, authorisation records, PersonalDB state, gateway records, mesh routing, audit, and repair evidence all build on the same durable vocabulary: blobs for immutable bytes, refs for mutable heads, streams for ordered history, mutation batches for visibility, fences for ownership, and cursors for replay.

The model is intentionally API-first and server-owned. Application code should use Anvil APIs. Operators should use admin APIs and documented runbooks. Feature code should persist durable truth through CoreStore primitives. Derived views should be rebuildable from CoreStore-backed source records. That discipline is what keeps Anvil explainable when a write races, a worker crashes, a query lags, a region drains, or an operator has to prove what happened.
