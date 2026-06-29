---
title: Indexes And Search
description: Learn directory indexes, metadata indexes, full text search, vector search, hybrid ranking, and authorization-safe search.
---

# Indexes And Search

**What this page gives you:** an introduction to indexing and search for readers who have never built a search system. You will learn why indexes exist, how full text and vector search differ, why hybrid search exists, and why authorization must be part of every query.

An index is a maintained shortcut. Without an index, a system answers a question by scanning everything. With an index, it keeps a structure that lets it jump to likely answers.

A phone book is an index. You could find a person by reading every page, but alphabetical ordering lets you jump to a surname. Storage systems use the same idea for object keys, metadata fields, words, vectors, relationships, and database projections.

## Directory and path indexes

A directory index answers questions about prefixes:

```text
list objects under tenants/acme/projects/p-123/documents/
```

The system should not scan every object in every bucket to answer that. It maintains key order and prefix structures so listing one area is fast.

Path indexes are why key design matters. A clear prefix becomes a fast query boundary.

## Metadata indexes

A metadata index answers structured questions:

```text
status = signed
customer = Acme Ltd
document_type = contract
created_at >= 2026-01-01
```

Metadata indexes are best for exact filters, ranges, sorting, and facets. They are not good at understanding free-form human language. Use full text search for words and vector search for meaning.

## Full text search

Full text search starts with text. The system tokenizes the text into terms, records where those terms appear, and stores statistics that help rank results.

For example, a document containing:

```text
The overdue invoice includes updated payment terms.
```

may produce terms such as:

```text
overdue, invoice, includes, updated, payment, terms
```

A query for `payment terms` should rank this document higher than one that mentions only `payment` once. Full text search can also support snippets, highlighting, language-aware tokenization, and phrase queries.

The important security point: snippets are data. A snippet from a private document is private data. Anvil treats full text results as authorized object exposure, not a harmless search side channel.

## Vector search

A vector is a list of numbers. An embedding model converts text, images, audio, or video into vectors where similar meanings are close together.

These phrases may have nearby vectors even though they use different words:

```text
cancel my subscription
close my account
stop billing me
```

Vector search is useful when exact words are not enough. Users can search by meaning, compare images, find similar audio, or retrieve related video segments.

A brute-force vector search compares the query vector to every stored vector. That becomes expensive at scale. Anvil uses graph-based approximate nearest-neighbor indexing with Rust-native HNSW support. Approximate search means the system finds high-quality candidates quickly rather than proving the mathematical nearest neighbor by scanning everything.

Vector indexes have strict contracts:

- the embedding model identity must match;
- the vector dimension must match;
- the distance metric must match the application meaning;
- authorization filtering must happen before results are exposed;
- index generations must prove which source objects they cover.

## Hybrid search

Hybrid search combines signals. A product query often needs several kinds of evidence:

```text
text: "renewal notice"
vector: meaning of "contract renewal obligations"
metadata: document_type = contract, status != archived
prefix: tenants/acme/
ranking: text score + vector similarity + freshness
```

A hybrid query should not be a pile of separate result lists merged in application code. The merge must preserve authorization, deduplication, source versions, and consistency. Anvil's search model exists so those constraints live in the storage platform.

## Authorization-safe search

Search can leak data even when object reads are protected. A caller might learn that a private document exists from:

- result counts;
- facet counts;
- snippets;
- vector neighbors;
- timing differences;
- autocomplete suggestions;
- watch notifications;
- metadata-only listings.

Anvil evaluates authorization as part of query execution. Search results, counts, snippets, and derived rows must be filtered before exposure. Applications should not run a broad admin query and then filter in memory.

## Index freshness and readiness

Indexes are derived data. A write commits first; index builders then consume watch events and update index generations. Usually this is quick, but it is still a process with lag and failure modes.

Anvil exposes readiness and lag so applications can choose correct user experience:

- show a loader until the index reaches a required cursor;
- allow weaker consistency for non-critical suggestions;
- block a workflow until a required derived view is current;
- alert operators when lag is persistent.

## What you can do after this page

You should be able to explain directory indexes, metadata indexes, full text search, vector search, hybrid ranking, authorization-safe result filtering, and index readiness. Next, learn the authorization model that protects all of those surfaces.
