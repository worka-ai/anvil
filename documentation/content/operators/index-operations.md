---
title: Index Operations
description: Monitor, repair, and reason about Anvil directory, metadata, text, vector, authz, and PersonalDB indexes.
---

# Index Operations

**What this page achieves:** you will understand how to operate Anvil's derived indexes, what lag means, how rebuilds work, and what to alert on.

An index is derived from durable source mutations. This means it can be rebuilt, but it must prove which source cursor and manifest it represents. Operators should treat indexes as maintained production systems, not invisible caches.

## The three facts every derived system must expose

| Fact | Meaning | Operator question |
| --- | --- | --- |
| Source manifest | The source records used to build the output. | Can this generation prove its inputs? |
| Source cursor | The latest mutation consumed. | How far behind is it? |
| Generation | The published derived output. | Which version are queries using? |

If any fact is missing or invalid, the index is not trustworthy.

## Index families

Monitor each family separately:

- directory/path indexes;
- metadata indexes;
- full text indexes;
- vector indexes;
- source artifact indexes;
- authorization derived userset indexes;
- PersonalDB projections;
- media extraction outputs.

A healthy metadata index does not imply the vector index is healthy. A current vector index does not imply authorization derived usersets are current.

## Lag

Lag is how far a derived consumer is behind its source stream. A short burst is normal after heavy writes. Persistent lag means the system cannot keep up.

Useful lag metrics include:

- `full_text_indexing_lag`;
- `vector_indexing_lag`;
- `authz_derived_index_lag`;
- `personaldb_projection_lag`;
- `watch_stream_lag`.

Alert thresholds should match product expectations. A document archive might tolerate seconds or minutes. A collaborative application may need read-your-write behavior for specific flows.

## Rebuilds

A rebuild creates a new index generation from durable source data:

```text
read source manifest
  -> validate source records and hashes
  -> build derived segment
  -> write segment and proof
  -> publish new generation
  -> update query routing
```

During rebuild, queries should use the previous valid generation or report index readiness according to requested consistency. A corrupt generation must not silently serve results.

## Vector index operations

Vector indexes need special attention:

- embedding model identity must match the index definition;
- vector dimension must match exactly;
- distance metric must match query semantics;
- HNSW graph memory must be planned;
- authorization filtering may require fetching more candidates than the user requested;
- segment compaction must preserve source cursor proof.

If dimension or model identity changes, treat it as a new index generation. Do not mix incompatible vectors.

## Full text operations

Full text indexes depend on tokenization, extraction, language handling, and stored snippet policy. A tokenizer change is an index definition change. Rebuild affected generations so ranking and query behavior are consistent.

Snippets must follow authorization rules. A private snippet is private data.

## Repair findings

Repair should produce findings rather than silently mutating state. A finding should explain:

- which bucket/index/generation is affected;
- what validation failed;
- which source manifest or cursor is involved;
- what automatic repair was attempted;
- whether operator action is required.

## What you can do after this page

You should be able to explain index lag, generations, rebuilds, vector-specific risks, and repair findings. Next, learn backup and recovery.
