---
slug: /architecture/storage
title: 'Deep Dive: CoreStore Storage'
description: How Anvil stores objects, metadata, streams, indexes, authz state, PersonalDB records, and gateway records through one durable CoreStore substrate.
tags: [architecture, deep-dive, storage, corestore, erasure-coding, indexing]
---

# Deep Dive: CoreStore Storage

> **TL;DR:** Anvil persists every authoritative feature record through CoreStore. Object bodies, stream segments, refs, metadata, indexes, authorisation state, PersonalDB state, mesh control records, gateway credentials, and audit records share one durable storage model instead of each subsystem inventing its own persistence path.

CoreStore is Anvil's internal storage boundary. Public APIs still expose familiar buckets, keys, object versions, S3-compatible operations, search APIs, watch streams, PersonalDB APIs, and admin operations. Internally, those features are mapped onto three CoreStore primitives:

| Primitive | Purpose | Examples |
| --- | --- | --- |
| `CoreObject` | Immutable bytes addressed by content hash and manifest. | Object payloads, sealed stream segments, index segments, PersonalDB snapshots, package blobs. |
| `CoreStream` | Ordered append-only records with gap-free sequence numbers and hash chaining. | Object metadata journals, bucket journals, authz tuple logs, task queues, append streams, audit logs. |
| `CoreRef` | Compare-and-swap mutable pointers with generations and optional fence preconditions. | Current object heads, index generation heads, task lease state, gateway tags, PersonalDB heads. |

A feature may keep specialised record formats, but those records must live inside these primitives. A full-text segment and a PersonalDB snapshot are different formats, yet both are immutable `CoreObject`s with durable manifests. A bucket journal and an audit log have different event bodies, yet both are `CoreStream`s. A gateway tag and an object head point at different records, yet both are `CoreRef`s.

## Why one store matters

Before a system becomes large, it is tempting to give each feature a convenient local file or state table. That works briefly and then creates several independent durability systems: one for object data, one for metadata, one for indexes, one for leases, one for authorisation, one for package registries, and one for audits. Each one then needs its own replication, recovery, fencing, compaction, backup, and repair rules.

Anvil avoids that split. CoreStore solves the hard storage problems once:

- immutable data is content-addressed and erasure-coded;
- ordered changes are appended through streams;
- mutable heads use compare-and-swap refs;
- stale writers are rejected with fence preconditions;
- watch cursors are derived from committed stream records;
- recovery can enumerate committed CoreStore state instead of scanning feature-specific local files.

The scale benefit is direct: adding a new feature should add a record format and query path, not a new database.

## Write path

A normal object write follows this shape:

```text
client PUT object
  -> authenticate principal
  -> authorise object write
  -> stage upload bytes locally while streaming
  -> CoreStore.put_blob(payload) writes the immutable body
  -> build object metadata frame
  -> build current-object ref update
  -> CoreStore.commit_mutation_batch(metadata append + ref update)
  -> watch stream exposes the committed mutation
  -> index workers consume the watch cursor
```

The temporary upload file is not authoritative. It exists only while a request is being assembled. Recovery depends on the CoreObject manifest, stream records, and refs committed by CoreStore.

## Erasure coding and manifests

Large immutable values are split into shards according to the active quorum profile. The current local implementation writes deterministic replica directories for control records and erasure shards under the CoreStore-owned storage area. A committed object manifest records the logical hash, size, shard set, placement profile, and the information needed to reconstruct the object.

The important rule is not the exact shard filename. It is the boundary: a caller never treats a local file as the object. The object is the `CoreObject` identified by its manifest and content hash. If enough shards are unavailable to satisfy the read quorum, reads fail closed instead of silently returning partial data.

Small objects may be represented more compactly, but they still enter the system through CoreStore. The optimisation changes layout, not authority.

## Streams

CoreStream records are the source of truth for ordered facts. Each record carries a sequence number, kind, payload hash, previous event hash, optional transaction id, and cursor. The stream must be gap-free. If recovery sees a gap or a hash-chain break, the consumer stops and repair takes over.

Streams are used for:

- object metadata and directory changes;
- bucket state;
- authz tuples and namespace changes;
- task queue records;
- append stream entries;
- gateway audit entries;
- PersonalDB group watches;
- index partition watches.

Sealing a segment compacts older records into an immutable CoreObject segment. It does not close the logical stream. New records continue after the sealed range.

## Refs and mutation batches

CoreRefs are mutable heads. A ref update must name the expected generation, target, absence, presence, source cursor, authz revision, or fence precondition required by the operation. A stale precondition rejects the whole mutation before any visible record is published.

Related ref and stream changes use `CoreMutationBatch`. A batch is atomic inside its partition. For example, an object write publishes object metadata and updates the current object ref together. A task claim can update lease state only if the authenticated caller still holds the fence. A gateway tag can move only if the expected generation still matches.

This prevents half-published state such as an object version with no current ref, an index head pointing at a missing segment, or a stale worker committing over a newer owner.

## What belongs outside CoreStore

Only narrow bootstrap or scratch state may be outside CoreStore:

- process identity and keypair files needed before the node can join the mesh;
- transient upload staging before bytes are committed;
- temporary SQLite scratch files used to build a PersonalDB snapshot before the snapshot bytes are stored as a CoreObject;
- caches that can be discarded and rebuilt from committed state.

If losing a file would lose committed Anvil state, that file must be CoreStore state.

## Feature mapping

| Feature | CoreStore mapping |
| --- | --- |
| Object payloads | `CoreObject` payload manifests and shard maps. |
| Object metadata | `CoreStream` mutation journal plus current-object `CoreRef`s and sealed directory segments. |
| Buckets | `CoreStream` bucket journals and control refs. |
| Object links | Link records in metadata streams plus generation-checked refs. |
| Append streams | User-visible append records in CoreStreams and sealed segment CoreObjects. |
| Task leases | Fence and lease records in CoreRefs and streams. |
| Authorisation | Namespace schemas, tuples, derived userset indexes, and lag watches in CoreStore. |
| Path and typed indexes | Materialised segment CoreObjects and generation refs. |
| Full text search | Posting segment CoreObjects with source cursor and authz revision. |
| Vector search | Vector segment CoreObjects with HNSW graph data and permission-aware filtering. |
| PersonalDB | Changeset payloads, commit certificates, snapshots, row indexes, projections, and watches in CoreStore. |
| Source and model artefacts | Git/source manifests, indexes, model records, and ingestion state in CoreStore. |
| Gateway records | Mounts, credentials, repository records, blobs, tags, upload sessions, and audits in CoreStore. |

## Recovery model

Recovery starts from CoreStore root and partition state. The node reads root catalog and quorum information, validates manifests, replays streams, checks refs, and rebuilds derived read models when needed. A derived read model is allowed to be missing or stale; it is not allowed to become the source of truth.

The release tests prove that committed object payloads survive through CoreStore refs, stream visibility is gated by committed transaction records, object manifests require quorum, and feature families named by the RFC have CoreStore-backed persistence.
