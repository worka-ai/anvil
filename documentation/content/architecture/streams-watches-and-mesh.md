---
title: Streams, Watches, and Mesh Transport
description: How append streams, prefix watches, derived maintenance, and CoreMeta mesh replication fit into Anvil's architecture.
---

# Streams, Watches, and Mesh Transport

Streams and watches make committed change visible without asking every consumer to rescan every object. Mesh transport makes the same durable change survive node and placement boundaries. These systems are related, but they are not the same thing.

An append stream is application data. A prefix watch is a committed-change feed. CoreMeta replication is an internal metadata quorum protocol. Shard transfer is the blob-plane distribution protocol. Treating these separately keeps the design understandable.

## Append streams

An append stream is an ordered sequence of records. Each record has a sequence number, metadata, optional content type, optional user metadata JSON, and optional payload bytes. Readers can ask for records after a sequence number and can include or omit payload bytes.

Stream record payloads follow CoreStore storage rules. Tiny payloads may be inlined according to the inline policy. Larger payloads are stored through the byte pipeline. Stream heads and stream record indexes are CoreMeta rows.

Append streams are suitable for audit trails, durable attempts, event histories, coordination logs, and application-visible ordered facts. They are not Anvil's internal metadata WAL. CoreMeta relies on RocksDB's WAL locally and on logical CoreMeta replication between nodes.

## Prefix watches

A prefix watch follows committed object changes in one bucket under one prefix. It begins with a snapshot for the requested cursor position and then streams live events. The current filter shape is intentionally simple: bucket, prefix, and cursor. If an application needs a metadata predicate, typed predicate, full-text condition, or vector condition, that belongs in an index query rather than in the base prefix watch.

A watch cursor is durable evidence that a consumer has seen source changes up to a point. A derived worker should checkpoint only after its own derived output is durable. If a worker checkpoints before the output is safe, recovery can skip required work.

## Derived maintenance watches

Index definitions, index partitions, authz tuple logs, PersonalDB groups, and other derived systems have watch or watch-like surfaces. Some are pushed through live streams. Some currently poll persisted CoreStore state at a bounded interval. Both shapes are acceptable as long as the consumer sees committed records, carries durable cursors, and fails clearly on lag or incompatible state.

## CoreMeta replication streams

CoreMeta replication is internal. It moves deterministic metadata row batches from an owner to metadata replicas. The current implementation uses persistent bidirectional gRPC streams for the CoreMeta quorum path. Each request carries a request id, pending responses are tracked, timeouts are enforced, failed streams are evicted, and retryable failures reopen the stream.

This was a major performance improvement because metadata commit paths previously paid more connection/request overhead than necessary. The public consequence is lower write latency for tenant, app, grant, bucket, object, and authz operations.

## Blob shard transport

Blob shard writes and reads are separate from CoreMeta replication. A large object write is encoded into shards, then shard writes are sent to the selected placements. Reads reconstruct from the storage-class read quorum. Current shard reads use streaming responses. Shard writes use cached internal requests and can be further optimised later without changing object manifests or RocksDB layout.

## Liveness model

Current CoreMeta streams detect dead ends through request timeouts, stream closure, and reconnect logic. The protocol does not require an application-level heartbeat record for every idle stream in this release. Operators should rely on request latency, timeout counts, peer connection state, replication lag, and diagnostics when deciding whether a peer is healthy.

Future heartbeat work can improve operator visibility and failure detection speed, but it does not require changing CoreMeta row formats or the blob storage layout.
