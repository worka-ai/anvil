---
title: Architecture Overview
description: A contributor-oriented guide to Anvil's CoreStore-centred architecture, API planes, durability model, indexing model, watches, gateways, and release status.
---

# Architecture Overview

This section explains how Anvil works internally. The Learn book teaches the product model from first principles, the Tutorials book shows concrete operations, and the Operators book explains how to run a deployment. Architecture is different: it is for contributors, operators reviewing a release, and engineers deciding whether Anvil's storage design is stable enough to build on.

The short version is that Anvil has one durable storage centre. Metadata is stored in RocksDB through CoreMeta. Tiny payloads may be inlined into RocksDB when they fit the inline payload policy. Larger durable bytes go through the CoreStore byte pipeline and are written as erasure-coded shard data. Feature-specific writers exist for objects, streams, indexes, authorisation, PersonalDB, registry/gateway records, and mesh control records, but those writers do not get to invent independent durable storage systems.

That division is deliberate. RocksDB is a local metadata engine, not the object store. The erasure-coded byte pipeline is the durable blob plane, not a separate feature path. Indexes and streams can have specialised binary formats because their read patterns are specialised, but their metadata, locators, generations, and commit evidence still flow through the same CoreStore model.

## The two storage planes

Anvil's storage implementation has two cooperating planes.

The first plane is **CoreMeta**. It is implemented with RocksDB and holds metadata records, current heads, version descriptors, transaction state, lease and fence rows, index definitions, index segment locators, authz tuple pages, boundary records, PersonalDB locators, mesh records, registry records, materialisation cursors, refcounts, and observability rows. CoreMeta is replicated logically: an owner sends deterministic CoreMeta row batches to metadata replicas, replicas persist those rows into their own RocksDB instances, and root publication only happens after the required quorum evidence exists.

The second plane is the **CoreStore byte pipeline**. It stores payload-like durable bytes: object bodies, large stream payloads, large writer segments, index segment bodies, PersonalDB snapshots and pages, source packs, gateway blobs, registry blobs, and other feature output that is too large or inappropriate for RocksDB. Bytes are staged, transformed by the pipeline, split into blocks, erasure-coded, written to shard placements, and then referenced from CoreMeta. The root-visible metadata never points at missing bytes.

```text
public/admin/gateway request
        |
        v
 authentication, authorisation, validation, routing
        |
        v
 feature writer: object | stream | index | authz | personaldb | registry | mesh
        |
        +--> CoreMeta rows in RocksDB
        |       heads, versions, definitions, locators, cursors, leases, roots
        |
        +--> CoreStore byte pipeline when payload/segment is larger than inline policy
                stage -> compress -> encode -> place -> write shards -> record manifest
```

The most important release property is that the feature writer is not allowed to treat a local JSON file, a local SQLite file, or an index-specific directory as final durable truth. Scratch files can exist while a request is in flight. Operator exports can exist outside the server storage path. Test fixtures can write temporary files. But final Anvil state must be recoverable from CoreMeta plus the byte pipeline.

## Inline payloads

The current inline policy is intentionally conservative. RocksDB values have a 64 KiB maximum encoded CoreMeta value cap. Raw object payloads are eligible for inline storage only up to the configured tiny-object threshold, which defaults to 32 KiB. Stream record index payloads are capped lower because they sit on the stream index path. This means the public statement is not "every file up to 64 KiB is stored in RocksDB". The correct statement is:

> CoreMeta stores metadata and bounded tiny payloads. RocksDB has a 64 KiB encoded value ceiling, and ordinary raw payload inline eligibility defaults to 32 KiB.

That gives small-object workloads a fast path while preventing RocksDB from becoming the large-payload store. If a payload or writer segment does not fit the inline policy, it follows normal byte-pipeline rules and is stored through the erasure-coded blob plane.

## Root visibility and read-after-write

A write is visible only after its safety evidence is complete. For a large object this means the byte pipeline has produced enough shard receipts for the storage class, the CoreMeta batch has reached metadata quorum, the commit certificate evidence has itself been persisted, and the root generation has advanced. For small inline writes, the byte plane may not be involved, but the CoreMeta quorum and root publication rules still matter.

Normal object `PUT`, `HEAD`, and `GET` paths should be read-after-write consistent after a finalised write returns. Explicit transactions are different: writes carrying a transaction id are staged and are not visible until `CommitTransaction`. Index reads are also different. An index query reads the latest materialised index generation available to that query path. A caller that needs a freshness proof can supply a watch cursor and require the index to be caught up to that cursor or fail.

## API planes

Anvil exposes a public plane and an admin plane. They may run in the same process, but they are different trust surfaces.

The public plane is tenant-facing. Tenant applications use it for buckets, objects, object links, public policy, tenant application credentials, relationship authorisation, indexes, watches, append streams, task leases, PersonalDB, tenant diagnostics, tenant repair, and gateways. It may be exposed to application networks and, depending on deployment, to the internet.

The admin plane is private. Operators use it for tenant creation, first credential handover, system policy grants, topology lifecycle, routing repair, secret-envelope rotation, global diagnostics, global repair, and administrative audit. The admin plane is authorised through the built-in system realm. It is not a local storage writer and should not need direct access to a server storage directory.

The CLI split follows the same rule. `anvil` is the public tenant CLI. `anvil-admin` is the private operator CLI.

## Indexes as derived writer output

Indexes are not separate databases. They are derived writer outputs tied back to source records, source cursors, authorisation revisions, boundary state, and index definitions. Every index has an index definition in CoreMeta and one or more segment records in CoreMeta. The segment bytes themselves follow the standard writer-output rule: inline if tiny, otherwise erasure-coded.

Anvil currently supports path, metadata-filter, typed JSON, full-text, vector, hybrid, PersonalDB row metadata, and git-source index families. The common idea is always the same:

1. Select source records.
2. Extract fields, terms, vectors, or protocol-specific rows.
3. Write an immutable segment using a format suited to the query path.
4. Publish a segment locator and generation through CoreMeta.
5. Query through the planner, intersecting index candidates with boundary and authorisation candidates where applicable.
6. Apply final visibility checks before returning results.

The current full-text implementation uses BM25 scoring over tokenised postings and supports phrase queries. The current vector implementation uses an HNSW graph. Typed JSON and metadata indexes use typed field/value structures so equality, range, prefix, existence, and ordering predicates can be planned without scanning every object body.

## Streams and watches

Append streams are ordered by record sequence. A stream record can carry metadata and a payload. Payload bytes use the CoreStore storage rules: small records may inline, larger payloads go through the byte pipeline. Stream metadata and stream record indexes are CoreMeta rows. Tail APIs stream records to callers; internally some tails and watch surfaces use polling over persisted state rather than a pure push path.

Watches are the bridge between source writes and derived consumers. Prefix watches produce object changes under a bucket prefix. Index, authz, PersonalDB, and other watches let builders and applications resume from durable cursors. Prefix watches are prefix-scoped today; richer predicate filtering belongs in indexed query APIs rather than in the basic prefix watch surface.

## Mesh communication

Metadata quorum traffic now uses persistent bidirectional gRPC streams for CoreMeta replication. The stream keeps request ids and pending responses, applies timeouts, evicts failed streams, and reconnects on retryable failures. TCP_NODELAY is enabled on listeners. Shard writes currently use cached internal gRPC calls, and shard reads use streaming responses. This is enough for the current release contract, with room for more payload-oriented streaming optimisation later.

Liveness is timeout-and-reconnect based rather than application-heartbeat based on every stream. Operators should use the observability surfaces and request-level timeouts when diagnosing a silent peer rather than assuming every idle stream emits heartbeat records.

## Gateways

Gateways adapt external protocols to Anvil's model. S3-compatible object access maps to tenants, buckets, keys, object versions, metadata, public policy, and authorisation. Static host aliases and object links map to the same object and routing model. Gateway foundation records for package-shaped protocols are represented as CoreStore-backed registry/gateway records so future adapters do not force S3 assumptions into the core.

This is the architectural rule for contributors: a gateway can translate a protocol, but it cannot define a second storage or security model.

## Release status

This release is architecturally ready from the storage-layout perspective. Metadata and bounded inline payloads are in RocksDB. Larger durable bytes flow through the byte pipeline. Index segment bodies follow the same storage rules as other writer outputs. The admin plane is private and system-realm authorised. Query paths use the shared planner and final visibility gates for the live index families.

There are staged extensions that improve expressiveness or performance without changing the storage foundation. Full-text search currently exposes BM25 and phrase behaviour rather than the entire future boolean grammar. Hybrid search currently uses a fixed blend of full-text, vector, and freshness signals rather than the full future fusion language. Prefix watches are prefix filtered, not arbitrary predicate filtered. Mesh metadata traffic uses persistent streaming; shard write traffic still has further streaming optimisation available. These are API and execution-surface improvements, not reasons to redesign CoreMeta or the byte pipeline.

For performance context and the exact release posture, read [Release Architecture Status](/architecture/release-status/).
