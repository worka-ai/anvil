---
title: Watches And Derived Data
description: Learn watch streams, cursors, derived indexes, lag, replay, repair, and why Anvil avoids blind rescans.
---

# Watches And Derived Data

**What this page gives you:** a model for change streams and derived data. You will learn how Anvil keeps indexes, projections, source views, and operational timelines current without asking every consumer to rescan buckets.

Derived data is data computed from source data. A full text index is derived from object content. A metadata index is derived from object metadata. A vector index is derived from embeddings. A PersonalDB projection is derived from committed changesets. An authorisation userset index is derived from relationship tuples.

Derived data is useful because it is fast to query. It is dangerous if it silently drifts from the source. A search index that missed a delete is unsafe. A projection that skipped a commit is wrong. A permissions index that ignored a tuple update can expose private data.

Anvil uses watch streams, cursors, manifests, and validation to make derived data explicit.

## Source events

The source of truth is the durable mutation stream. Mutations include:

- object writes and deletes;
- metadata updates;
- authorisation tuple changes;
- index definition changes;
- PersonalDB commits;
- source artefact records;
- control-plane updates.

Each committed mutation has an ordered position. That position is the thing watchers use to know where they are.

## Watch stream

A watch stream is an ordered feed of changes. A watcher subscribes to a scope such as a bucket, prefix, index family, authorisation namespace, or PersonalDB group. The watcher receives events and stores a cursor.

A cursor is a bookmark:

```text
processed everything up to position 0000000000001842
```

If the watcher restarts, it resumes from that cursor. It does not guess by listing the whole bucket again.

## Derived consumer loop

Every derived system follows the same shape:

```text
load last cursor
  -> read watch events after cursor
  -> validate event shape and source versions
  -> update derived structure
  -> write manifest/proof
  -> commit new cursor
```

The cursor is committed only after derived work is durable. That rule makes retries safe. If a process crashes halfway through, it repeats work instead of falsely claiming completion.

## Lag

Lag is how far a derived consumer is behind the source stream. A short lag after heavy writes is normal. Persistent lag is an operational signal.

Examples:

- a metadata index is 500 events behind;
- a full text index has not extracted a large PDF yet;
- a vector index is waiting for embeddings;
- a PersonalDB projection has not processed the latest commit;
- an authorisation userset index has not consumed a tuple update.

Applications can choose behaviour based on lag and required consistency. Some screens can show slightly stale suggestions. A permission-sensitive workflow should wait for the required authorisation revision.

## Generations and manifests

A derived index generation is a published version of an index. It should include a manifest explaining:

- which source stream it covers;
- which cursor it reached;
- which index definition and parameters were used;
- which segment files belong to it;
- which hashes prove integrity.

Queries should route to a valid generation. If a new generation fails validation, Anvil should keep serving the previous valid generation or report readiness according to the requested consistency.

## Repair

Repair is not magic mutation. A repair process should produce findings: what was checked, what failed, what source cursor or manifest was involved, what was rebuilt, and whether operator action remains.

A good repair system is explainable. It does not silently rewrite state and ask operators to trust it.

## Leopard-style acceleration

Anvil's watch model enables acceleration: consumers keep local derived structures warm by following changes as they happen. Instead of waiting for a query and then scanning source data, Anvil lets indexes and projections stay close to current state continuously.

This is essential for large deployments. At high object counts, scanning is the failure mode. Watch-driven maintenance turns the cost into incremental work tied to durable source events.

## What you can do after this page

You should be able to explain watches, cursors, lag, generations, manifests, replay, repair, and why derived data must prove its source. Next, learn how PersonalDB applies the same ideas to local-first SQLite data.
