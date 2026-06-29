---
title: Watches And Derived Data
description: Learn how Anvil streams changes and keeps indexes current without rescanning.
---

# Watches And Derived Data

**Goal:** understand watches, cursors, derived data, and why Anvil can keep search, authorization, and projections current without repeatedly scanning buckets.

A watch is a stream of changes. Instead of asking "what changed since I last scanned everything?", a consumer asks Anvil to send committed mutations after a cursor.

A cursor is a durable position in a stream. If a consumer processes events through cursor 1000 and saves that checkpoint, it can resume at 1001 after a restart.

## Why watches matter

Imagine a bucket with billions of objects. A background indexer that repeatedly scans the whole bucket wastes enormous IO. Worse, it may never catch up if the bucket changes quickly.

A watch-driven indexer works differently:

1. it reads a manifest or checkpoint;
2. it subscribes to mutations after a cursor;
3. it applies each mutation to its derived index;
4. it records the cursor it has processed;
5. if it falls too far behind, it rebuilds from a known manifest and resumes.

This pattern is how Anvil maintains full text indexes, vector indexes, directory indexes, derived authorization usersets, source indexes, and PersonalDB projections.

## Derived data

Derived data is data computed from base data. A full text posting list is derived from object text. A vector index is derived from embeddings. A PersonalDB projection is derived from source rows. A relationship index is derived from tuple writes.

Derived data must prove what source it came from. Anvil records source cursor, source manifest hash, generation, and segment hashes. If those do not validate, the derived index is invalid and must rebuild.

## Leopard-style acceleration

Anvil uses the watch pattern to maintain precomputed relationship and query indexes. This is similar in spirit to Leopard-style acceleration: expensive graph or query work is moved from request time to controlled derived maintenance, while request-time checks use precomputed structures that are tied to a known source revision.

The benefit is not just speed. It is bounded, explainable speed. A permission check can say which authorization revision it used. A search result can say which source generation and index cursor produced it.

## What happens when a watcher falls behind

Watch streams retain a window. If a consumer asks for a cursor older than the retained window, Anvil returns a data-loss style error for that watch. The consumer must rebuild from the relevant manifest and then resume from the manifest checkpoint.

That failure mode is deliberate. Silent gaps would be worse. A search index that missed events but still claimed to be current would be incorrect.

## What you can do now

You should now be able to explain why watches are the backbone of Anvil's derived systems and why cursors, manifests, and proofs matter for correctness.
