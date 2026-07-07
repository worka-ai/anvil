---
title: Indexes and Query
description: Understand Anvil indexes as derived data, from selectors and extractors through query visibility, pagination, lag, diagnostics, and repair.
---

# Indexes and Query

An index is a maintained shortcut. Without one, a service that wants "all open invoices for Acme, ordered by due date" has to scan object bodies, parse JSON, check metadata, apply permissions, and sort the result every time. That is simple at ten objects and unusable at millions.

Anvil indexes answer those questions by keeping derived structures beside the source records. A path index keeps object-key navigation cheap. A typed JSON index keeps queue and dashboard predicates cheap. A full-text index keeps word search cheap. A vector index keeps semantic similarity cheap. The important part is that none of those structures becomes the source of truth. They are rebuildable views over committed Anvil records, with generations, cursors, diagnostics, and authorisation checks around them.

Read this page with [Object Model](/learn/object-model/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), and [Authorisation](/learn/authorisation/). The practical tutorials are [Path, Metadata, and Typed Query Indexes](/tutorials/indexes-path-metadata-and-typed-query/), [Full-Text Search](/tutorials/full-text-search/), [Vector Search](/tutorials/vector-search/), and [Hybrid Search](/tutorials/hybrid-search/). Exact JSON grammar lives in [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/), CLI syntax in [Public CLI](/reference/public-cli/), and permission strings in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/). Operators should also read [Index Operations](/operators/index-operations/) and [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/).

## Source records and derived rows

The source record is the thing Anvil accepted durably: an object version, current-object metadata, an append record, an authorisation tuple, a PersonalDB commit, or another record family. An index row is a derived fact about one of those records. For example, an object body might be:

```json
{
  "invoice_id": "inv-1001",
  "customer_id": "acme",
  "state": "open",
  "due_at": "2026-07-31T00:00:00Z"
}
```

A typed JSON index may materialise `customer_id`, `state`, and `due_at` as fields. A full-text index may materialise terms from a `body` field. A vector index may materialise an embedding from an `embedding` array. If the object is rewritten or deleted, those rows must eventually move forward to match the source stream.

That is why Anvil index responses carry evidence such as index generation, authorisation revision, source watch cursor, applied cursor, and page token binding. A query result is useful only when you know both what it found and what version of the derived view it used.

## The index definition has three build-time questions

Creating an index is not the same as asking a query. The definition tells Anvil how to build a materialised view. Query requests later ask that view a question. Keep those phases separate.

`selector_json` answers: which source records are eligible? For object-backed indexes, the common selector is an object-key `prefix`, optionally with a `content_type`. This is a build-time boundary. A query-time `path_prefix` can narrow within selected rows, but it cannot reach outside the selector.

```json
{
  "prefix": "documents/",
  "content_type": "application/json"
}
```

`extractor_json` answers: what should be extracted from each selected source record? It is most visible in full-text and hybrid indexes, where it can say "index the whole UTF-8 body", "index this JSON Pointer", "index this metadata field", or "index these named text fields". For path and metadata-filter indexes it is normally `{}` today. For typed JSON indexes, the current implementation defines typed fields in `build_policy_json` rather than `extractor_json`. For vector indexes, the vector extractor also lives in `build_policy_json`; current validation rejects a separate vector extractor for kind `vector`.

`build_policy_json` answers: how should the derived structure be built? For full-text it records tokenisation and position options. For typed JSON it records field names, JSON Pointer extractors, required fields, source kind, and default ordering. For vector it records the vector schema, embedding provenance, dimension, modality, normalisation, and HNSW options. For hybrid it contains both the full-text and vector build policy parts.

These JSON fields are strings on the API and CLI, but they are not arbitrary decoration. They define the contract between source data, index builders, query semantics, and future repair. Use [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/) for the full supported shapes rather than copying fragments between index kinds.

## The main index kinds

Different index kinds optimise different questions. Choosing the wrong one usually creates either slow queries or a derived view that cannot prove the answer you want.

| Kind | What it indexes | Typical query | Important current behaviour |
| --- | --- | --- | --- |
| `path` | Current object metadata and keys. | Prefix navigation with optional exact metadata filters. | Query rejects text and vector inputs. Rows are ordered by object key and source identity. |
| `metadata_filter` | Current object metadata and keys. | Exact equality over user metadata, optionally narrowed by path. | Query requires `metadata_filters_json`; use `path` if you only need prefix browsing. |
| `typed_json` | Named JSON values from supported source kinds such as current objects or append records. | Equality, range, existence, `in`, and stable ordering. | `typed_predicates_json` and `typed_order_json` must be arrays. `object_version` is accepted by validation, but the current builder does not scan every historical version. |
| `full_text` | Tokenised text extracted from object payloads, metadata, or supported text sources. | `query_text`, optionally `phrase`, path prefix, and metadata filters. | Current direct query is term/phrase search, not a boolean query language. |
| `vector` | Embedding vectors extracted from payloads or produced by a configured provider. | Numeric `query_vector`, optionally path prefix and metadata filters. | Query vector dimension must match the built segment; the CLI does not embed natural-language queries for you. |
| `hybrid` | Full-text and vector material under one definition. | Text, vector, or both, with path prefix and metadata filters. | Current direct hybrid scoring uses fixed weights; callers do not configure weights per query. |

The API enum also contains `personaldb_row_metadata` and `git_source`. They are accepted definition kinds, but the generic Index service does not currently materialise or query them through the normal `QueryIndex` path. PersonalDB and Git source have their own service surfaces; do not document those enum values as general-purpose index types until the implementation grows that support.

## Query inputs are not build policy

Query-time fields ask questions of rows that already exist. `path_prefix` narrows object keys represented in the materialised rows. `metadata_filters_json` performs exact JSON equality against object user metadata. `typed_predicates_json` filters typed JSON rows, and `typed_order_json` controls typed ordering. `query_text` searches full-text material. `query_vector` searches vector material. `page_token` resumes a previous query page.

A useful mental model is:

```text
selector_json      -> build-time source boundary
extractor_json     -> build-time extraction description
build_policy_json  -> build-time materialisation rules
query fields       -> read-time question over built rows
```

If a source object was excluded by `selector_json`, no query field can bring it back. If a typed field was not listed in `build_policy_json`, `typed_predicates_json` cannot filter on it. If a full-text extractor did not include a title field, searching for words in the title cannot work. Conversely, making the selector too broad and relying on query filters for every product boundary creates larger derived state, more lag, and harder repair.

## Authorisation is part of the query plan

Indexes are not a bypass around object security. Public policy scopes decide who may manage or query an index. Relationship authorisation and object read scopes decide which protected hits are visible when the index uses the default `inherit_object` mode.

Index administration checks scoped actions such as `index:create`, `index:update`, and `index:delete` on `bucket/index_name`. Listing, querying, and diagnostics currently check `index:read` on the bucket name, not on a specific index definition. That bucket-level read scope is coarse; design roles and buckets with that current boundary in mind.

The `authorization_mode` on the definition controls per-hit visibility:

| Mode | Query visibility rule | Safe use |
| --- | --- | --- |
| `inherit_object` | The caller needs `index:read` to ask the question, and each returned object must also pass object read or the built-in object-reader relationship check. | Default for object-derived names, metadata, text, vectors, and private search. |
| `index_only` | The query path does not re-check object read for each hit; `index:read` on the bucket is enough. | Only for derived rows safe for every bucket-level index reader. |
| `public` | Same current query effect as `index_only`; it does not make the public API anonymous by itself. | Only for deliberately public derived catalogues. |

Filtering after results leave Anvil is not equivalent. A full-text hit, object key, vector score, or page count can leak information even without the object body. The index service must participate in authorisation before it returns hits.

There are current limits to know. Prefix-style object read scopes may be expanded into concrete object labels for query filtering, but very broad prefixes can exceed the current permission-set cap and fail with `AuthzPermissionSetTooLargeForPrefixScope`. Use narrower buckets, prefixes, or relationship tuples rather than expecting one broad prefix grant to be efficient forever.

## Filters, ordering, and ranking

Path and metadata-filter indexes are deterministic list-like indexes. They return rows ordered by object key and source identity. A `path` query can use `path_prefix`; a `metadata_filter` query must also include `metadata_filters_json`. Metadata filters are exact JSON equality. The string `"20"` is not the number `20`, and equality over an object or array means exact JSON equality, not containment.

Typed JSON indexes are structured list-like indexes. Predicates are an array where each entry names a field, an operator, and a value or values. Supported operators include equality, `in`, less-than/greater-than comparisons, existence, and null checks. Ordering is also an array. Ties are broken by source identity so pagination remains stable. Typed comparisons compare JSON values: numbers as numbers, strings lexicographically, booleans as booleans, and mixed types by their JSON representation. Use consistently formatted timestamp strings, such as RFC 3339 UTC, when string ordering must match time ordering.

Full-text indexes are score-like indexes. They tokenise selected text and rank matches with BM25-style scoring. Phrase queries require positions in the built policy. Current direct full-text query does not implement a general boolean syntax; treat `query_text` as token/phrase input and use path or metadata filters for hard narrowing.

Vector indexes are score-like indexes over numeric embeddings. The query vector must come from the same model space, dimension, modality, normalisation, and metric contract as the indexed vectors. The server can call configured command-based embedding providers for `object_body_utf8` extraction during build, and it has a deterministic `test_only` provider for tests when explicitly enabled. The current CLI query path accepts a numeric `--vector`; it does not turn user text into a query embedding.

Hybrid indexes combine full-text and vector signals in one index definition. A direct hybrid query may send text, vector, or both. When both are present, current scoring uses a fixed recipe: text, vector, and freshness signals are normalised and combined with implementation-defined weights. If your product needs per-query weighting or a more complex structured plan, use the API-level query-spec surface where it fits today, and verify the plan diagnostics. The public CLI currently exposes direct `anvil index query`, not a query-spec helper.

## Pagination binds the query shape

Anvil page tokens are opaque. A token is not merely an offset; it binds the caller, tenant, bucket, index, generation, definition version, authorisation revision, predicate hash, order hash, and last sort position. Current direct index tokens expire after 15 minutes. This prevents an old cursor from being reused after the query shape or permission context changed.

Use a `next_page_token` only with the same logical query. Changing the text, vector, filters, order, prefix, caller, bucket, index, or definition should make the token invalid rather than silently skipping rows, duplicating rows, or leaking data from a stale authorisation view.

Score-like indexes and list-like indexes paginate differently internally. Full-text, vector, and hybrid results page by score plus an object-version tie-breaker. Path and typed JSON results page by their deterministic sort tuple plus source identity. The practical rule is the same: keep the token opaque and keep the query stable.

## Lag and catch-up

Indexes are maintained asynchronously from source records. A successful object write does not mean every index has already materialised the new version. A builder must see the source event, extract data, publish a segment, and advance its proof or checkpoint.

For metadata-backed and typed JSON indexes, direct `QueryIndex` responses currently report useful source cursor fields: `source_watch_cursor_high`, `index_watch_cursor_applied`, `is_caught_up`, and `lag_record_count_hint`. A caller can send `require_caught_up_to_watch_cursor` with a decimal cursor; if the source or materialised segment is behind that cursor, Anvil returns `IndexLagging` instead of stale results.

`lag_timeout_ms` exists on the API and CLI, but current direct index query implementations do not generally wait for catch-up. Treat it as a limit/hint for surfaces that implement waiting, not as proof that a query blocked until fresh.

Full-text, vector, and hybrid direct query responses currently report caught-up status as true with zero cursor fields rather than meaningful object-source lag. For those index kinds, handle freshness operationally: watch source changes, watch index partition progress where available, inspect diagnostics, retry after expected build delay, or design the product to show an indexing state.

## Diagnostics and repair

Diagnostics explain why a definition exists but a row is absent or incomplete. A full-text extractor may fail because the object body is not UTF-8. A typed JSON index may reject an object because a required JSON Pointer is missing. A vector index may record a dimension mismatch or a missing provider. The public Index API exposes `ListIndexDiagnostics`, and the CLI helper is `anvil index diagnostics`.

No diagnostics in one page does not prove the index is correct. It proves only that no matching diagnostic records were returned for that request. You still need to check selection, source data, authorisation visibility, materialised generation, and lag.

Repair is the controlled path for drift. `RepairService.RepairIndex` and the public `anvil repair run index` helper can inspect and, when requested, rebuild index state from source records. Repair should move derived data towards committed source truth; it should not invent source objects or grant visibility to satisfy a broken index. Use [Repair and Diagnostics](/tutorials/repair-and-diagnostics/) and [Index Operations](/operators/index-operations/) for the operational flow.

## Current limits to design around

The index model is intentionally broader than every current helper surface. The public API is primary; the CLI is a manual helper over common calls. Today that means several practical limits:

```text
list, query, and diagnostics use coarse bucket-level index:read checks
full-text, vector, and hybrid direct queries do not expose meaningful catch-up cursors
direct full-text search is token/phrase search, not a boolean query language
direct hybrid scoring uses fixed weights
direct full-text, vector, and hybrid queries do not evaluate typed_predicates_json
query-spec exists in the API, but there is no public CLI query-spec command today
personaldb_row_metadata and git_source are enum values, not generic materialised QueryIndex paths
```

These are not reasons to avoid indexes. They are reasons to choose boundaries deliberately, prefer the API in production code, and keep diagnostics, watches, and repair in the design from the start.

## What to take forward

An Anvil index is a derived, authorised, repairable view over source records. `selector_json` decides what can enter the view, `extractor_json` and `build_policy_json` decide what is materialised, and query fields ask questions of the materialised rows. Pick the index kind that matches the question, keep `inherit_object` unless the derived data is deliberately safe for every index reader, treat page tokens as bound and opaque, and use catch-up, diagnostics, watches, and repair to make freshness and correctness explicit.
