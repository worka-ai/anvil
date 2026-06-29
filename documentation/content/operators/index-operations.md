---
title: Index Operations
description: Monitor and repair Anvil path, metadata, full text, vector, authz, source, and PersonalDB indexes.
---

# Index Operations

**What this page gives you:** an operator's guide to index health. You will learn what index lag means, why generations matter, when rebuilds are required, and how to reason about full text, vector, authorisation, and PersonalDB derived state.

Indexes are maintained views over source facts. They make queries fast, but they must stay tied to durable source data. A healthy deployment needs visibility into each derived subsystem, not just object byte storage.

## Index families

Anvil maintains several kinds of derived structures:

- directory and path indexes;
- metadata indexes;
- full text indexes;
- vector indexes;
- source artefact indexes;
- authorisation derived userset indexes;
- PersonalDB projections;
- media extraction outputs.

A healthy metadata index does not imply a healthy vector index. Each family has its own lag, generation, and repair surface.

## Lag

Lag measures how far an index is behind its source stream. It should be monitored per index family and scope.

Persistent lag can indicate:

- insufficient CPU or IO;
- an embedding or extraction bottleneck;
- a stuck watch cursor;
- invalid source data;
- repeated validation failure;
- an overloaded node selected for too much background work.

Alert thresholds should match product expectations. A collaboration product may need near-immediate metadata and authorisation updates. A media archive may tolerate longer vector indexing lag.

## Generations

A generation is a published version of an index. It should have a manifest that identifies source cursor, index definition, segment files, hashes, and validation status.

Queries should use valid generations. Rebuilding a new generation should not destroy the previous valid generation until the new one is proven and published.

## Full text operations

Full text indexes depend on tokenization, language handling, extraction, and snippet policy. Changing tokenization or snippet behaviour is an index definition change. Rebuild affected generations so ranking and snippets are consistent.

Snippets are sensitive. Verify snippet storage and result exposure follow authorisation policy.

## Vector operations

Vector indexes require careful compatibility:

- embedding model identity must match;
- dimension must match exactly;
- distance metric must match intended semantics;
- HNSW memory usage must be planned;
- candidate fetching must account for authorisation filtering;
- segment compaction must preserve source cursor proof.

If any of model, dimension, or metric changes, create a new index generation. Do not mix incompatible vectors.

## Repair findings

Repair should produce findings. A finding should say:

- which bucket, index, or generation is affected;
- which invariant failed;
- which source cursor or manifest was involved;
- whether automatic repair was attempted;
- whether operator action is required.

Do not accept silent mutation as repair. Operators need evidence.

## What you can do after this page

You should be able to monitor index lag, reason about generations, rebuild derived structures, and interpret repair findings. Next, learn backup and recovery.
