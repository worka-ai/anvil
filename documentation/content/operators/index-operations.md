---
title: Index Operations
description: Operate metadata, full text, vector, authorization, and PersonalDB derived indexes.
---

# Index Operations

**Goal:** monitor index health, understand lag, and recover derived data safely.

Anvil indexes are derived from durable base mutations. That means an index can be rebuilt, but it must never silently claim to be current if it missed events.

## The operator model

Every derived subsystem has three important facts:

| Fact | Meaning |
| --- | --- |
| Source manifest | The durable source state the index was built from. |
| Source cursor | The watch position processed by the index. |
| Generation | The sealed version of the index output. |

If these facts do not validate, the index is invalid and Anvil rebuilds it.

## Monitor lag

Monitor lag for:

- directory indexes;
- full text indexes;
- vector indexes;
- authorization derived usersets;
- PersonalDB projections;
- source indexes;
- media extraction outputs.

Small lag during bursts is normal. Persistent lag means the deployment needs more CPU, memory, IO, or task lease capacity.

## Rebuild behavior

A rebuild should be explicit and observable. The system reads a source manifest, derives the index from source records, writes a new index generation, records proof, and then publishes the generation. Queries should either use the previous valid generation or report rebuilding status according to API consistency requirements.

## Vector index operations

Vector indexes have additional operational needs:

- embedding model identity must be stable;
- vector dimension must match index definition;
- distance metric must match query semantics;
- memory budget must cover HNSW graph loading and query candidate expansion;
- authorization filtering can require more internal candidates than the user requested.

## Full text operations

Full text indexes depend on tokenization and extraction. Changing tokenizer configuration is an index definition change. Treat it as a new generation and rebuild.

## What to alert on

Alert when:

- index lag exceeds product SLO;
- derived proof validation fails;
- watch consumers repeatedly overrun retention;
- vector dimension mismatch errors occur;
- authorization derived indexes cannot catch up;
- projection builders repeatedly fail the same source cursor.
