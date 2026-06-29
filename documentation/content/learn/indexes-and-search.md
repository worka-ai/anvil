---
title: Indexes And Search
description: Learn indexes, full text search, vector search, hybrid ranking, and authorization-safe queries.
---

# Indexes And Search

**What this page achieves:** you will understand why indexes exist, what full text and vector search mean, and how Anvil combines search with permissions and object versions.

An index is a prepared data structure for answering a question quickly. Without an index, a system may have to inspect every object. With the right index, it can jump directly to the likely answers.

The simplest index is a phone book. You could find a person by reading every page, but the alphabetical order lets you jump to the right area. Storage systems use the same idea for keys, metadata, words, vectors, and relationships.

## Index families in Anvil

Anvil has multiple index families because applications ask different questions.

| Question | Index family |
| --- | --- |
| Which objects are under this prefix? | Directory/path index |
| Which objects have `status = signed`? | Metadata index |
| Which documents contain `payment terms`? | Full text index |
| Which images or paragraphs are semantically similar? | Vector index |
| Which source artifact introduced this file? | Source/artifact index |
| Which users may see this object? | Authorization derived index |
| Which database rows changed for this projection? | PersonalDB projection index |

The important point is that these indexes are derived from durable source mutations. They are not casual caches. They have source cursors, manifests, generations, and repair rules.

## Full text search

Full text search helps when a user knows words that should appear in the result. Anvil tokenizes text, records terms, and ranks matches.

A document containing:

```text
The invoice is overdue and requires approval.
```

might produce searchable terms such as:

```text
invoice, overdue, requires, approval
```

A full text index records which object versions contain which terms, where the terms appear, and enough statistics to rank matches. A query for `overdue invoice` should rank that document higher than one that mentions only `invoice` once.

Full text search is not just for text files. Text can be extracted from PDFs, office documents, audio transcripts, video transcripts, or structured JSON fields.

## Vector search

A vector is a list of numbers. An embedding model converts text, images, audio, or video into vectors where similar meanings are close together. This lets a user search by meaning rather than exact words.

For example, these phrases may be close in vector space:

```text
late payment
invoice is overdue
unpaid balance
```

A brute-force vector search compares a query vector to every stored vector. That becomes too expensive at scale. Anvil uses HNSW, a graph-based approximate nearest-neighbor index, to find good candidates quickly while keeping the persistent segment format under Anvil control.

Approximate search means the system optimizes for fast, high-quality nearest results rather than mathematically comparing every vector. For application search, that tradeoff is usually the right one.

## Metadata filters and path scope

Search is rarely global. A user usually searches inside a tenant, project, folder, media type, date range, language, status, or object class. Metadata and path filters narrow the candidate set before final scoring.

A production query often looks conceptually like this:

```text
within bucket documents
under prefix tenants/acme/projects/p-123/
where document_type = contract
where status != archived
matching text "renewal clause"
near vector embedding(query)
visible to user amy at auth revision r42
```

Anvil treats all those constraints as part of the same query plan. Search results are not useful unless they are authorized, current enough for the requested consistency, and traceable to object versions.

## Hybrid ranking

Hybrid search combines signals. Text score, vector similarity, metadata filters, path scope, freshness, and authorization all contribute to the final result set.

Hybrid ranking exists because users mix exact and fuzzy intent. A legal user may type an exact clause name but also expect conceptually related language. A media user may search with words but expect image similarity. Anvil gives those use cases one search model rather than forcing applications to merge unrelated result lists by hand.

## Authorization-safe search

Search must not leak existence. If a user is not allowed to read a private document, a search query must not reveal that the document exists through result titles, counts, facets, snippets, vector neighbors, or timing side effects.

Anvil applies authorization filtering as part of query execution. It also tracks the authorization revision used to expose results. If a query requires a consistency level the authorization index has not reached, Anvil waits, reports index readiness, or uses the configured consistency path rather than silently returning unsafe results.

## Watch-driven index maintenance

An object write produces watch events. Index builders consume those events and checkpoint their cursor. If an index builder is behind, Anvil can report lag. If a segment fails validation, Anvil rebuilds it from manifests and source records.

This is the difference between a production index and a best-effort cache. Operators can ask: which source cursor does this index represent, and can it prove that claim?

## What you can do after this page

You should be able to explain directory indexes, metadata indexes, full text search, vector search, hybrid ranking, and authorization-safe query execution. Next, learn the authorization model itself.
