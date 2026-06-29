---
title: Search And Indexes
description: Define Anvil indexes and query objects with metadata, full text, vector, and hybrid search.
---

# Search And Indexes

**Goal:** turn object data into queryable application experiences without scanning buckets or leaking unauthorized results.

Search starts with an index definition. The definition tells Anvil which objects to index, which fields to extract, which tokenizer or embedding model to use, and which filters are supported.

## Metadata index example

A metadata index supports structured filters:

```json
{
  "name": "documents-by-customer",
  "kind": "metadata",
  "prefix": "tenants/",
  "fields": [
    { "name": "customer", "type": "keyword" },
    { "name": "document_type", "type": "keyword" },
    { "name": "created_at", "type": "timestamp" }
  ]
}
```

Use metadata indexes for dashboards, lifecycle jobs, retention policies, and scoped application lists.

## Full text index example

A full text index extracts text from object bodies or structured row envelopes:

```json
{
  "name": "document-body-text",
  "kind": "full_text",
  "prefix": "tenants/",
  "source": { "object_body": true },
  "tokenizer": { "language": "en", "lowercase": true, "positions": true }
}
```

Use full text search when users type words they expect to appear in the result.

## Vector index example

A vector index stores embeddings:

```json
{
  "name": "media-semantic-search",
  "kind": "vector",
  "prefix": "tenants/",
  "modality": "image",
  "embedding_model": "clip-vit-large-patch14",
  "dimension": 768,
  "distance": "cosine"
}
```

Use vector search when users search by meaning, similarity, or example content.

## Hybrid query example

Hybrid queries combine filters, text, vector, and authorization:

```json
{
  "prefix": "tenants/acme/documents/",
  "metadata_filter": {
    "document_type": "contract",
    "status": "signed"
  },
  "text_query": "renewal clause",
  "vector_query": {
    "embedding": [0.012, -0.441, 0.087],
    "top_k": 50
  },
  "limit": 10,
  "consistency": "latest_authorized"
}
```

Anvil returns only authorized results. Each result includes enough identity to fetch, display, or audit the matching object version.

## Operational contract

After an index is created, Anvil maintains it from watch streams. Query APIs expose index lag and source cursor information. If an index is rebuilding, clients should show a clear loading or degraded-result state rather than silently mixing old and new data.

## Testing search

Test search with:

- at least one object that should match;
- one object outside the prefix;
- one object blocked by authorization;
- one object with similar text but wrong metadata;
- one updated object to verify watch-driven refresh;
- one deleted object to verify it disappears from results.
