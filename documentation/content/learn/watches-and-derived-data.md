---
title: Watches And Derived Data
description: Learn watch streams, derived indexes, cursors, lag, replay, and recovery.
---

# Watches And Derived Data

**What this page achieves:** you will understand how Anvil turns durable mutations into current indexes, projections, notifications, and operational views without broad rescans.

Derived data is data computed from source data. A full text index is derived from object content. A directory listing is derived from object keys. A PersonalDB projection is derived from committed changesets. An authorization userset index is derived from relationship tuples.

Derived data is useful because it is fast to query. It is dangerous if it silently drifts from the source. Anvil uses watch streams, cursors, manifests, and validation to keep derived data trustworthy.

## The source of truth

The source of truth is the durable mutation stream: object writes, metadata changes, tuple writes, PersonalDB commits, source artifact records, and control-plane updates. Derived systems consume that stream.

An index is allowed to be behind. It is not allowed to pretend it is current when it is not. That distinction is central to Anvil's design.

## What a watch stream is

A watch stream is an ordered feed of changes. A watcher subscribes to a bucket, prefix, index family, authorization namespace, PersonalDB group, or another scoped source. Each event has a position called a cursor.

A simplified event might look like:

```json
{
  "cursor": "0000000000000042",
  "bucket": "documents",
  "key": "tenants/acme/contracts/contract-42.pdf",
  "version": "v7",
  "operation": "put",
  "metadata_changed": true
}
```

A consumer stores the last cursor it processed. If it restarts, it resumes from that cursor. If it falls behind retention, Anvil can rebuild from a manifest and then resume from a known point.

## Derived maintenance flow

The general flow is the same for many subsystems:

```text
client write
  -> durable mutation record
  -> watch event
  -> derived builder consumes event
  -> builder updates segment or projection
  -> builder checkpoints cursor
  -> query path uses current generation when consistency requirements are met
```

This flow is used by directory indexes, metadata indexes, full text indexes, vector indexes, authorization derived usersets, PersonalDB projections, source artifact indexes, and operational timelines.

## Cursors and generations

A cursor says how far a consumer has read. A generation says which sealed output is published. A manifest ties source records to derived output.

For example, a full text index generation might say:

- source bucket: `documents`;
- source manifest: `m-123`;
- processed cursor: `c-9001`;
- tokenizer version: `en-gb-v2`;
- output segment hash: `...`.

When a query asks for a consistency level, Anvil can check whether the index generation is new enough.

## Lag is an operational signal

Lag is the distance between the newest source mutation and the cursor processed by a derived system. Short lag during bursts is normal. Persistent lag means the deployment needs attention: more CPU, memory, IO, task lease capacity, or index-specific tuning.

Anvil surfaces lag because hiding it creates false confidence. A user interface can show loading for strong consistency. An operator can alert on sustained lag. A developer can choose appropriate consistency requirements per query.

## Replay and repair

If a derived segment is missing, corrupt, or built from the wrong source generation, Anvil does not trust it. It rebuilds from durable source records and manifests. Repair is possible because base data and mutation records are authoritative.

This is why Anvil treats derived data as maintained output, not hand-written state. A rebuild produces a new generation with proof of its source cursor and manifest.

## What you can do after this page

You should be able to explain watches, cursors, generations, lag, replay, and why derived data must prove its source. Next, learn how PersonalDB uses the same durability and authorization principles for local-first SQLite data.
