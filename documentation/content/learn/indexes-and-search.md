---
title: Indexes And Search
description: Learn indexing, full text search, vector search, and hybrid ranking before using Anvil search APIs.
---

# Indexes And Search

**Goal:** understand what an index is, why search needs more than one index type, and how Anvil combines them safely.

An index is a data structure built to answer a specific question quickly. Without an index, a system may need to inspect every object. With the right index, it can jump directly to a small candidate set.

Anvil supports several index families because applications ask different kinds of questions.

| Question | Best index family |
| --- | --- |
| What objects live under this prefix? | Directory/path index |
| Which objects have `status = signed`? | Metadata index |
| Which documents contain `payment terms`? | Full text index |
| Which images are visually similar to this one? | Vector index |
| Which results best match text, vector, filters, and freshness? | Hybrid search |

## Full text search

Full text search turns natural language text into searchable tokens. A document such as:

```text
The invoice is overdue and requires approval.
```

might produce tokens like:

```text
invoice, overdue, requires, approval
```

A full text index records which documents contain which terms, how often they appear, and where they appear. At query time, Anvil can rank documents by how strongly they match the query.

Full text search is good when the user knows words that should appear in the result. It is not enough when the user describes meaning in different words or searches across image/audio/video embeddings.

## Vector search

A vector is a list of numbers that represents meaning. An embedding model can turn text, image, audio, or video into a vector. Similar meanings produce nearby vectors.

For example, these phrases may have vectors close to each other even though they do not share many words:

```text
"late payment"
"invoice is overdue"
"unpaid balance"
```

Anvil stores vector indexes using HNSW, a graph-based approximate nearest-neighbor algorithm. HNSW is used because exact vector search over large collections is too expensive: comparing one query vector against every stored vector does not scale.

Anvil owns the persistent vector segment format. The vector engine is an implementation detail behind Anvil's index API, which keeps repair, manifests, authorization filtering, and segment verification under Anvil control.

## Metadata filters

Search usually needs filters. A user might search for `contract renewal` only inside `customer=Acme`, only under a project prefix, and only for documents created this year. Metadata filters reduce the candidate set before final ranking.

Filters are not merely UI conveniences. They protect cost and relevance. A vector query across every object in every tenant would be too broad. A vector query scoped to a tenant, bucket, prefix, metadata condition, and authorization revision is a production query.

## Hybrid search

Hybrid search combines multiple scores. Anvil uses text score, vector score, metadata/path filters, freshness, and authorization filtering to produce a final result set.

The point is not to make every query complicated. The point is to let one API handle real product search:

- exact terms from full text;
- semantic similarity from vectors;
- structured filters from metadata;
- predictable scope from paths;
- safe result visibility from authorization.

A result is useful only if the caller is allowed to see it. Anvil applies authorization before returning results.

## Watch-driven index maintenance

Indexes must stay current. Anvil emits watch events from object and metadata mutations. Index builders consume those events and checkpoint their cursor. If a builder falls too far behind, it rebuilds from a manifest and resumes from a known checkpoint.

This is what makes indexes trustworthy. They are not ad hoc caches. They are derived data with recorded source cursors and proof of which source generation they represent.

## What you can do now

You should now be able to explain the difference between directory, metadata, full text, vector, and hybrid indexes. Next, learn why authorization must be integrated with storage and search rather than bolted on after query results are returned.
