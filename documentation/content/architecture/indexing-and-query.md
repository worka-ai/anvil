---
title: Indexing and Query Architecture
description: How Anvil builds and queries path, metadata, typed JSON, full-text, vector, hybrid, PersonalDB, and git-source indexes.
---

# Indexing and Query Architecture

Indexes in Anvil are derived views over source records. They are built so a query can narrow the candidate set before reading payloads, while still preserving final visibility checks. An index is not a second source of truth. If an index is stale or damaged, it can be diagnosed, repaired, or rebuilt from source records and watch cursors.

Every index has three layers.

First, the **definition layer** stores the index name, kind, selectors, extractors, authorisation mode, and build policy. These rows live in `cf_index_defs`.

Second, the **segment layer** stores immutable writer output. Segment records live in `cf_index_rows` and point at the actual segment bytes. Segment bytes are ordinary CoreStore writer output: they may be inline if tiny, otherwise they are stored through the erasure-coded byte pipeline.

Third, the **query layer** reads the relevant segment generation, creates index candidates, intersects those candidates with boundary and authorisation candidates where applicable, and performs final result visibility checks before responding.

## Path and metadata-filter indexes

Path and metadata-filter indexes are backed by typed field segments generated from current object metadata. They are useful for object-listing-shaped queries that should not scan every object body.

A path index uses object key information and supports prefix-shaped narrowing. A metadata-filter index indexes user metadata fields and supports equality filters. Both use the `TypedFieldSegment` writer format because metadata values can be normalised into typed field/value rows.

The segment contains a field catalog, sorted column data, field value index rows, range fences, and row-by-ordinal records. CoreMeta stores the index definition and the segment locator. The segment body follows normal writer-output storage rules.

## Typed JSON indexes

Typed JSON indexes extract typed values from JSON object bodies, object user metadata, or append records. They are the structured query path for applications that want object-native storage plus predictable field queries.

Typed predicates currently support equality, membership, ranges, prefix matching, existence, and null/missing checks. Query order can be ascending or descending over indexed fields. Boundary predicates can also participate in planning where the index and boundary state can be compared safely.

The important rule is that the JSON body is source data; the typed segment is derived. A query can use the derived segment to avoid broad payload reads, but final checks still ensure the returned object is visible and still matches the required request shape.

## Full-text indexes

Full-text indexes use a full-text segment. The segment stores field catalog information, analyser configuration, a term dictionary, compressed postings blocks, postings-by-term rows, and stored-field records. Query execution tokenises the query, reads matching postings, and scores candidates with BM25. Phrase query mode uses stored token positions.

The current public behaviour is plain text query plus phrase mode, with BM25 scoring. Future boolean query grammar can be added over the same term dictionary and postings architecture without changing the storage foundation.

Full-text extraction can read selected object body text, JSON pointer text, object keys, content type fields, metadata fields, media transcript output, and other configured text sources. Production embedding or media extraction must be configured explicitly; development-only deterministic providers are not a production search model.

## Vector indexes

Vector indexes use a vector segment with an HNSW graph. The segment stores vector header data, vector blocks, HNSW graph data, entrypoints, id maps, entry-by-id rows, delete bitmaps, and HNSW-by-node rows.

A vector definition declares dimensions, metric, modality, embedding provenance, chunking, and HNSW parameters. Vectors may be caller supplied, extracted from JSON bodies, read from raw vector payloads, or produced by a configured embedding provider. Query execution reads the segment, searches the HNSW graph, and then routes candidates through the common planner and final visibility checks.

## Hybrid indexes

Hybrid search combines text and vector signals. In the current implementation, hybrid query reads full-text and vector candidates, blends them with a fixed scoring recipe, and returns a single result list. The purpose is practical: many application searches need both lexical relevance and semantic similarity, but the caller should not have to manually merge two unrelated result sets.

The current fusion language is intentionally narrower than the full future design. That is an API expressiveness point rather than a storage layout issue. The full-text and vector segments already follow the same CoreStore writer rules and can support richer fusion semantics later.

## PersonalDB and git-source indexes

PersonalDB row metadata indexes support local-first SQLite witness and projection workflows. Git-source indexes support source-pack and repository-shaped workflows. They use the same architectural pattern: source records are canonical, index segments are derived, segment locators are CoreMeta rows, and segment bodies follow writer-output storage rules.

## Query planning and authorisation

Anvil query planning uses candidate sets. Index readers produce candidates. Boundary readers produce candidates. Authorisation readers produce candidates where the authorisation mode can be represented that way. The planner intersects compatible candidate sets and rejects incompatible generation combinations instead of silently mixing stale evidence.

Candidate pruning is not the only safety mechanism. Final result visibility remains mandatory. That final check is what prevents an index segment, cache, or stale candidate set from returning an object the caller should not see.

Page tokens are scoped. They bind the caller, tenant, bucket, index, index generation, root generation, query shape, predicate hash, order hash, boundary generation, and authorisation revision. Reusing a token after changing the query or caller should fail rather than produce surprising pagination.

## Freshness

Index queries are immediately useful after an index generation exists, but they are not the same as reading the just-written object head. A write can be visible to object reads before a derived index has caught up. For workflows that need freshness evidence, the public query request can require the index to be caught up to a watch cursor. If the index has not reached that cursor, Anvil returns a lag/freshness failure instead of pretending the result is complete.
