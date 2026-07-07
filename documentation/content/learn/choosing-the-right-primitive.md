---
title: Choosing the Right Primitive
description: Decide when to use Anvil objects, metadata, indexes, search, streams, watches, leases, PersonalDB, links, gateways, public delivery, and admin topology operations.
---

# Choosing the Right Primitive

Anvil gives you several ways to store, find, publish, and coordinate data. The difficult part is rarely naming a feature. It is deciding which record is the source of truth, which views are derived from it, which API can express the required correctness, and which authorisation boundary protects the result.

A good design starts with the thing that must survive failure. A document body, an audit event, a due job, a search hit, a local SQLite commit, and a public download URL are different shapes of truth. They should not all be forced into one table, one bucket prefix, one index, or one gateway protocol.

Read this chapter with [Object Model](/learn/object-model/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), [Indexes and Query](/learn/indexes-and-query/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Authorisation](/learn/authorisation/), [Gateways](/learn/gateways/), [PersonalDB](/learn/personaldb/), [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Public CLI](/reference/public-cli/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/).

## Begin with the source record

Ask what the system must be able to replay or prove later. If the answer is "these bytes existed at this key", use an object version. If the answer is "these events happened in this order", use an append stream. If the answer is "this worker still owned the task when it wrote output", use a task lease and fenced mutation. If the answer is "this local SQLite changeset was accepted at this log index", use PersonalDB. If the answer is "these objects matched this query at this derived generation", use an index result with lag and authorisation evidence.

Derived data is then built around that source. Metadata, typed indexes, full-text indexes, vector indexes, hybrid search, watches, projections, static aliases, and package catalogues are all useful because they make source records easier to read. They become dangerous when they are treated as independent truth.

## Objects and versions: durable bytes at stable names

Use **objects** for documents, media files, generated reports, package artefacts, model weights, manifests, snapshots, and any payload whose main identity is a tenant, bucket, key, and version. A PDF at `projects/p-123/documents/d-456/original.pdf` is an object. A video rendition at `media/m-22/renditions/720p.mp4` is an object. A package tarball under a digest key is an object.

An object is not a filesystem inode and not a database row. Slashes in keys help prefix listing and routing, but they do not create directories or directory permissions. Updating an object creates a new version and moves the current pointer; it should not be used as an append-only event log when sequence order matters.

The source of truth is the object version and current pointer. Authorisation normally uses public policy actions such as `object:read`, `object:write`, `object:delete`, and `object:list`, with relationship-authorisation checks available for object read visibility. Current CLI helpers are useful for smoke tests, but some production fields still require the API: content type and rich metadata on upload, write preconditions, complete version-history inspection, pinned link targets, and full mutation context.

Use [Buckets and Objects](/tutorials/buckets-and-objects/) and [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/) when you need hands-on object modelling. Use [S3-Compatible Gateway](/tutorials/s3-gateway/) only when the client is already S3-shaped; the native Object API remains the richer contract.

## Metadata: compact facts about objects

Use **metadata** for small operational facts that should travel with an object: content type, document state, customer id, capture time, checksum label, source system, retention class, or media dimensions. Metadata lets a `HEAD`, listing, diagnostic, or index builder understand the object without downloading the whole body.

Metadata is not the place for the whole business document. If the source record is a JSON contract, store the contract in the object body and extract query fields into an index. If the source record is a 200 MB video transcript, store the transcript as an object or extracted text object and index the relevant text.

The source of truth is still the object version that carries the metadata. Metadata reads and listings are authorised as object reads or listings because names and labels can leak private information. The current public CLI upload helper does not expose all metadata fields; production writers should use the API or client library when metadata is part of the contract. See [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/) and [Object Model](/learn/object-model/).

## Typed indexes: structured questions over source records

Use a **typed JSON index** when a product asks for structured predicates and ordering: open invoices by due date, jobs available before now, documents by `customer_id`, packages by version metadata, or audit events by actor and time. The source might be current object JSON, object metadata, or append-stream records, depending on the definition.

A typed index is not a second copy of the document and not a queue by itself. If a due-work item is stored as JSON, the object or append record remains the source. The typed index is the fast way to ask "which items are due?" or "which documents have status signed?" The worker still needs object versions, task leases, or mutation preconditions to update state safely.

Authorisation has two layers: callers need `index:read` on the bucket under the current implementation, and `inherit_object` indexes also filter each returned hit by object visibility. The current index read scope is coarse at bucket level; it is not per-index for query and diagnostics. Query correctness can use `require_caught_up_to_watch_cursor` where the index kind exposes meaningful source cursors. The CLI requires valid array syntax for typed predicates and order, while the full API exposes more response evidence. See [Indexes and Query](/learn/indexes-and-query/) and [Path, Metadata, and Typed Query Indexes](/tutorials/indexes-path-metadata-and-typed-query/).

## Full-text indexes: words and phrases

Use a **full-text index** when users search human language: document text, support tickets, comments, OCR output, transcripts, source files, or package descriptions. Tokenisation turns text into searchable terms; phrase support depends on positions being built into the index.

Full-text search is not structured filtering and not a boolean query language today. Put hard boundaries such as project, document type, state, or date into metadata or typed fields, then combine them with text search where supported. Do not expect `query_text` to behave like a general SQL `WHERE` clause or a web-search boolean syntax.

The source of truth is the object or text source that the index extracts from. Authorisation follows the index mode, usually `inherit_object` for private corpora. Current direct full-text query paths do not expose meaningful catch-up cursors in the same way typed object indexes do, so production products should show indexing state, use watches and diagnostics, and tolerate lag. See [Full-Text Search](/tutorials/full-text-search/) and [Index Operations](/operators/index-operations/).

## Vector indexes: similarity in embedding space

Use a **vector index** when similarity matters more than exact words: semantic document retrieval, related media, image or audio segment similarity, recommendation-like discovery, or retrieval-augmented generation over a private corpus. A vector is a numeric embedding produced by a model or supplied by the caller. Dimension, modality, normalisation, and metric must match between indexed vectors and query vectors.

A vector index is not magic meaning and not a text embedder in the CLI. If the query is natural language, something must produce the query embedding. Production embeddings require a configured provider or caller-supplied vectors. Deterministic/test embeddings are for tests and are not production-quality semantic embeddings.

The source of truth is the original object and the embedding contract used to derive or store the vector. Authorisation should usually inherit object visibility because vector hits and scores can leak corpus contents. Current CLI query accepts numeric vectors and diagnostics are essential for dimension/provider failures; direct vector query catch-up evidence is currently limited compared with typed indexes. See [Vector Search](/tutorials/vector-search/) and [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/).

## Hybrid search: combined ranking signals

Use **hybrid search** when good ranking needs more than one signal. A private legal corpus might need exact filters for matter and jurisdiction, full-text matches for clauses, vector similarity for semantic recall, and freshness or path scope for tie-breaking. A support system might combine typed filters, ticket text, and embeddings from past resolutions.

Hybrid search is not a reason to ignore source design. Text and vectors still come from source objects. Metadata and path prefix still provide hard narrowing. Authorisation still controls which hits can be returned. If all you need is "list files under this folder" or "find jobs due before now", a path, metadata, or typed index is simpler.

Current direct hybrid scoring combines text, vector, and freshness signals with implementation-defined weights; do not document per-query configurable weights unless you are using an API surface that actually supports your plan. Full-text/vector lag limitations also apply. See [Hybrid Search](/tutorials/hybrid-search/) after [Full-Text Search](/tutorials/full-text-search/) and [Vector Search](/tutorials/vector-search/).

## Append streams: ordered event history

Use **append streams** when sequence is the product: audit logs, delivery attempts, build logs, workflow timelines, import histories, integration callbacks, or state-transition history. A stream record says "this event happened after the previous event in this stream". That is a different truth from "this JSON object is the current state".

An append stream is not an object watch. Watches report that Anvil source records changed; append streams are application-owned source records. It is also not a queue by itself. A stream can feed workers, but worker ownership, retry, and checkpoint policy need leases or application state.

The source of truth is the stream identity and ordered record sequence. Authorisation currently uses object-style resources: creating, appending, or sealing checks object write on the stream key; reading/tailing uses read/list checks as implemented. CLI gaps matter: create/append/seal helpers may need broader bucket-list ability because of mutation-context lookup, the CLI generates fresh idempotency keys, there is no stream-list helper to recover a lost stream id, and sealing a segment is not logical stream closure. See [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/).

## Watches: keeping derived work current

Use **watches** when something must maintain derived data without rescanning everything. Search builders, cache invalidators, exporters, audit processors, PersonalDB projection monitors, and repair monitors all need to know what changed after their last checkpoint.

A watch is not an archive and not a durable checkpoint service by itself. It is a stream of committed changes after a cursor. Your consumer must store its checkpoint after its own side effect is durable. If a consumer falls behind a retained live window, it should restart from the last durable checkpoint or rebuild from source.

The source of truth is the watched source stream: object changes, bucket metadata, authz tuple revisions, index partitions, PersonalDB groups, or append stream sequences. Authorisation is read-like and can reveal names, hashes, timings, and relation changes; public-read does not make watches anonymous. Current CLI coverage is uneven: object prefix, authz tuple log, index definition/partition, and PersonalDB group watches are exposed; bucket metadata, PersonalDB projection, authz namespace/derived-lag, and git-source watches are API-only today. See [Watches and Derived Data](/learn/watches-and-derived-data/) and [Watches](/tutorials/watches/).

## Task leases and fenced mutations: safe workers

Use **task leases** when multiple workers might process the same named unit of work. Use **fenced mutations** when stale workers must be rejected at the moment they write output. A due-work system might store job records as JSON objects, query due jobs with a typed index, acquire `task_lease/send-email-batch-42`, and then write result objects or append audit records only if the lease fence is still current.

A task lease is not data permission. A worker with a lease still needs object, stream, index, or PersonalDB authority for the data it touches. Ordinary object writes are not automatically fenced; a stale worker with object write permission can still write unless the write uses the API lease-fence precondition.

The source of truth is the lease record for ownership and the source record being mutated for application state. Authorisation uses `coordination:lease_read`, `coordination:lease_write`, and `coordination:lease_admin` on `task_lease/<task_id>`, plus the normal permissions for data writes. Current public CLI exposes lease lifecycle helpers, but not `ObjectService.MutationBatch` with `lease_fence`; production fenced mutations require the API. See [Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/) and [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/).

## PersonalDB: local SQLite with witnessed history

Use **PersonalDB** when the application has local SQLite state that must synchronise through a shared, witnessed log: desktop/mobile offline apps, collaborative local-first data, replicated workspace databases, or server-built SQLite projections. The client keeps SQLite locally. Anvil witnesses changesets, stores certificates, advances heads, emits watches, builds snapshots, and enforces group and row-effect authorisation.

PersonalDB is not a hosted SQL server and not a better object bucket for binary files. Attachments, previews, package artefacts, and media assets usually belong in objects, with PersonalDB rows referencing their object keys or versions. PersonalDB is for ordered SQLite changesets and replica catch-up.

The source of truth is the PersonalDB group log chain: changeset payloads, log records, commit certificates, heads, snapshots, and projection definitions. Authorisation includes group actions such as `personaldb:create`, `personaldb:read`, `personaldb:commit`, and `personaldb:watch`, plus row-effect actions `personaldb:insert`, `personaldb:update`, and `personaldb:delete` on derived row resources. Current CLI support is not a complete sync client: submit cannot provide all required production fields, catch-up output is compact, projection watch is API-only, and snapshot restore/download is not exposed as a complete public workflow. See [PersonalDB](/learn/personaldb/) and [PersonalDB Tutorial](/tutorials/personaldb/).

## Links, public access, and static hosting: stable delivery names

Use **links** when a stable name should move without copying bytes: `releases/latest.tar.gz`, `sites/www`, `models/current`, or `reports/latest.pdf`. Use **public-read** and **static hosting** when anyone who can reach the public surface should be able to read selected object data through HTTP/S3/static routes. Use a dedicated public bucket whenever possible.

Links are not copies, and public access is not a bypass. A link changes alias metadata; it does not duplicate or delete the target. Public-read lets unauthenticated readers fetch matching data from public surfaces; it does not grant writes, expose the admin API, or relax reserved namespace checks. Static hosting is object delivery through routing, not a full web server with every website feature.

The source of truth is the target object version or current pointer, the link descriptor and generation, the bucket public-read state, and any host-alias routing record. Authorisation uses object read/write/delete for links and bucket write for public-read changes; tenant-owned host aliases require bucket authority, while operator host-alias lifecycle belongs on the admin plane. Current gaps include bucket-wide public-read in the public CLI, no CLI target-version flag for pinned links, redirect-link delivery not generally implemented as HTTP `3xx`, and limited static-site behaviours such as no automatic index fallback unless documented for the route you use. See [Public Access](/tutorials/public-access/), [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/), and [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/).

## S3 gateway: compatibility for object tooling

Use the **S3-compatible gateway** when existing tools already speak S3 and the job is object-shaped: migration, backup tools, bulk upload/download, range reads, copy, multipart upload, or simple metadata round-trips. An app credential maps to an S3 access key and secret, and the gateway translates supported S3 operations into Anvil object operations.

The S3 gateway is not the core security model and not the full Anvil API. It cannot express relationship schemas, native watches, typed query definitions, PersonalDB commits, task leases, fenced mutations, repair workflows, or rich metadata in the way the native APIs can. AWS IAM policy documents, ACLs, lifecycle rules, notification configuration, object tags, CORS, and website configuration are not Anvil's control plane today.

The source of truth remains the Anvil bucket, object versions, metadata, links, public-read state, and authorisation checks. S3 signing authenticates an Anvil app principal; unsigned read-side paths are only for deliberate public-read behaviour. Use the native API when correctness depends on Anvil-specific fields. See [Gateways](/learn/gateways/) and [S3-Compatible Gateway](/tutorials/s3-gateway/).

## Package and static delivery: model today, gateway later

Use objects, links, metadata, indexes, public/private policy, and optionally the S3 gateway to model package artefacts today. Store immutable blobs under digest keys, write version manifests as ordinary objects, move `latest` or channel names with links, index manifests for catalogue queries, and use public-read or authenticated reads according to product policy.

Do not claim a package registry protocol exists unless the implemented gateway exists. Current package gateway code includes foundational internal records for repositories, blobs, tags, upload sessions, credentials, mounts, access tokens, and audit, but there are no tenant-facing Docker Registry v2, npm, PyPI, Maven, Cargo, `anvil package`, or `anvil registry` surfaces in the current repo.

The source of truth for today's package-like workflows is still Anvil objects and links. Authorisation uses the same object, bucket, index, app, and relationship-authorisation model as other tenant data. See [Package Gateway Foundations](/tutorials/package-gateway-foundations/) and [Gateway Operations](/operators/gateway-operations/).

## Admin and topology operations: operator control, not tenant data

Use **admin/topology operations** for storage tenants, initial tenant handover, system app provisioning, regions, cells, nodes, routing projections, system host-alias lifecycle, admin diagnostics, admin repair, secret-envelope work, and mesh lifecycle. These operations change the environment in which tenant data lives.

Admin operations are not an application data modelling primitive. Do not use the private admin API to upload tenant package artefacts, move a `latest` link, write objects, repair missing product permissions, or bypass public policy scopes. The public plane is for tenants and applications; the admin plane is for operators and system-realm authority.

The source of truth is mesh/control-plane state, CoreStore-backed control records, routing projections, lifecycle descriptors, and admin audit records. Authorisation uses the built-in system realm, not tenant public policy scopes. Current gaps matter: region activation requires real checkpoints but lacks a production-friendly checkpoint-generation command, drain completion workflows are incomplete in exposed CLI surfaces, and cross-region proxying is partial. See [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/), [Admin Plane](/operators/admin-plane/), [Topology Planning](/operators/topology-planning/), and [Admin CLI](/reference/admin-cli/).

## Concrete designs

A private document system usually starts with objects. Store the original file, extracted text, previews, and manifests under project-shaped prefixes. Put compact state and labels in metadata. Use typed indexes for dashboards such as "contracts awaiting signature", full-text for keyword search, vector or hybrid search for semantic discovery, and `inherit_object` authorisation so search results do not leak private documents. Use watches to keep derived processors current, append streams for document audit history, and task leases for OCR or ingestion workers.

A media pipeline can store originals and renditions as objects, track transcoding jobs as JSON objects queried by a typed index, record every transcoder attempt in an append stream, and serve approved public renditions through a dedicated public bucket or host alias. Vector indexes may help find visually or semantically related assets, but the source bytes and approved-public state still live in objects and metadata.

A durable audit log should not be a mutable object overwritten after every event. Use an append stream for ordered audit events, store consumer checkpoints after exports are durable, optionally build a typed index over append records for investigation, and keep Anvil's service-recorded audit events separate from your application audit stream.

A due work queue is usually a composition, not one primitive. Store the job as an object or append a creation event, index due fields with a typed JSON index, use a task lease so one worker owns a job or partition, and write completion state with API preconditions or a fenced mutation. Do not rely on in-memory locks or index query results alone for ownership.

A local-first app should use PersonalDB for the SQLite history and objects for large attachments. The client edits local SQLite, submits changesets to Anvil, stores commit certificates and heads, catches up from its durable head, and tails PersonalDB watches. Attachments, exports, and static previews remain object data referenced from rows.

A private search corpus should combine object storage, relationship authorisation, and indexes. Store source documents as objects, extract text and embeddings through well-defined pipelines, use full-text/vector/hybrid indexes with `inherit_object`, and pass catch-up requirements where supported when a user must see a write they just made. If full-text or vector lag is visible, show indexing state instead of pretending the corpus is complete.

A package or static delivery workflow should keep immutable artefacts as objects, move channels with links, publish only deliberate buckets or prefixes, and use S3 or static hosting as delivery adapters. The admin API should stay out of tenant publishing; operators manage routing and host lifecycle, while tenants publish data through public APIs.

## What to take forward

Choose the primitive by the proof you need later. Objects prove versions of named bytes. Metadata gives compact labels for those versions. Indexes answer derived questions. Full-text, vector, and hybrid search rank content in different ways. Append streams prove ordered events. Watches keep derived systems from rescanning. Leases and fences prove the current worker still owns the job. PersonalDB proves accepted SQLite history. Links and public delivery give stable read names without copying bytes. Gateways adapt outside protocols to Anvil; they do not replace the native model. Admin topology operations shape the deployment and must remain on the private operator plane.

## A quick design review script

For every new feature, write one sentence each for source of truth, derived views, caller authority, retry key, freshness requirement, and repair path. If the source of truth is "whatever the latest index says", the design is probably inverted. If the repair path is "rerun an admin command from the application", the plane boundary is probably wrong. If the retry key is random per attempt, the operation is probably not safely retryable.
