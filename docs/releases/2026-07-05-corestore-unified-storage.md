# Anvil 2026-07-05: CoreStore unified storage, authz-aware query, and gateway-neutral foundations

This release is the CoreStore release. It rewrites Anvil's internal durability model so every authoritative feature record goes through one storage substrate: immutable CoreObjects, ordered CoreStreams, and generationed CoreRefs with CAS and fence preconditions.

The public product shape remains direct: objects, buckets, versions, S3-compatible access, native gRPC APIs, search, watches, PersonalDB, source artefacts, model artefacts, and admin operations. The change is underneath those APIs. Anvil no longer lets each subsystem own a private durable file format with its own recovery story. Feature-specific formats still exist, but their persistence is CoreStore-backed.

## Why this matters for scale

A storage system that has separate durability paths for objects, metadata, indexes, authorisation tuples, task queues, package records, and audit logs eventually has to solve replication and recovery many times. That is expensive operationally and risky architecturally. CoreStore gives Anvil one place to enforce:

- erasure-coded immutable object storage;
- quorum-readable manifests and refs;
- append-only ordered streams;
- compare-and-swap mutable heads;
- authenticated fence tokens for stale-writer rejection;
- committed transaction visibility;
- watch cursors for derived maintenance;
- repair from source records instead of feature-local files.

That is the design connection back to RFC 0006: storage is the core, and every derived feature sits above it.

## CoreStore primitives

CoreStore exposes three primitives internally.

`CoreObject` stores immutable bytes. Object payloads, sealed stream segments, full-text segments, vector segments, typed-field segments, PersonalDB snapshots, source indexes, package blobs, and model artefact records all use this path.

`CoreStream` stores ordered records. Object metadata, bucket state, authz tuples, task queues, append streams, index events, PersonalDB watches, mesh control records, gateway audit records, and admin audit records use streams.

`CoreRef` stores mutable heads. Current object pointers, index generation heads, gateway tags, PersonalDB heads, ownership records, and lifecycle refs use compare-and-swap generations with optional fence preconditions.

## Transaction and visibility hardening

Mutation batches publish refs and stream records atomically within a partition. Failed preconditions do not leak half-written visible state. Stream records are visible only after the transaction record is committed. CoreObject manifests are replicated as quorum control records, and reads fail closed when quorum is unavailable.

This release includes tests proving failed mutation batches do not publish refs, streams, or transaction records; manifests require quorum; and stale fences cannot commit protected writes.

## Authorisation-aware query planning

Search and index queries now bind authorisation into the plan instead of filtering after the fact as an application concern. Path, typed field, full-text, vector, and hybrid queries carry source IDs and authorisation labels. The query planner intersects candidate sets with permission sets before returning object keys, snippets, scores, or page tokens.

Page tokens bind query shape, index generation, authz revision, principal context, and mesh context. Reusing a token under a different query shape is rejected.

## Materialised indexes

The release distinguishes clearly between source records and derived indexes. Objects and append records are source facts. Index segments are durable materialised views. Index generations publish only after their segment objects and source cursor proof are durable.

The implemented materialised indexes include:

- path and directory segments;
- typed field and range segments;
- full-text posting segments with phrase-capable positions where enabled;
- vector segments with Rust-native HNSW graph support;
- hybrid query plans combining text, vector, filters, and permissions;
- authz derived userset indexes;
- PersonalDB row and projection indexes.

## Reserved namespace denial

The `_anvil/` namespace is Anvil-owned. This release enforces hard denial across native object APIs and S3-compatible APIs. Public callers cannot read, head, write, delete, list, watch, copy, compose, patch, multipart-upload, append, seal, or range-read reserved paths. Forged query parameters or copy sources do not create internal authority.

## Mesh and lifecycle state

Mesh routing and lifecycle records are now CoreStore-backed control records. Region, cell, node, host alias, bucket locator, tenant locator, object link, routing projection, diagnostics, repair findings, and audit records share the CoreStore durability model.

Cross-region object proxying preserves the original authenticated principal. The receiving region validates the internal proxy token and then authorises the object operation as the original principal, not as a broad system identity.

## Gateway-neutral storage model

S3 remains a supported gateway, but it no longer defines the core model. Gateway records are represented as Anvil records first: mounts, credentials, repositories, blobs, tags, upload sessions, token challenges, and audits. The same gateway store can support S3-compatible object access, static host aliases, container registry records, Rust crate registry records, npm package records, PyPI records, and Maven records without adding a new persistence engine for each protocol.

The release-supported public gateway is S3-compatible object access. The CoreStore gateway foundation records are included so additional protocol handlers can map requests into the same mount, credential, repository, blob, tag, and audit model.

## CLI surfaces

Anvil ships two command-line tools.

`anvil-cli` is the user/application CLI. It configures profiles, obtains tokens, manages buckets and objects where authorised, performs delegated auth operations, and drives ingestion flows.

`admin` is the network admin CLI. It talks to the admin listener, not the storage directory. Operators use it to create tenants, create and rotate app credentials, grant and revoke policy, create buckets, toggle public access, manage region/cell/node lifecycle, manage object links and host aliases, run repair, inspect diagnostics, list audit events, and rotate server-side encrypted envelopes.

Every mutating admin command carries an audit reason and request context. The server owns storage and encryption keys; the CLI does not.

## Verification performed

Release-branch verification included:

- `cargo test --workspace`;
- CoreStore conformance tests for durable feature-family coverage;
- local storage guard tests proving authoritative feature writes do not bypass CoreStore-owned paths;
- transaction visibility tests;
- CoreObject manifest quorum tests;
- reserved namespace native API tests;
- reserved namespace S3 gateway tests;
- authz-aware full-text, vector, hybrid, path, and typed-field query tests;
- gateway mount and credential store tests;
- internal proxy principal-preservation tests;
- mesh lifecycle and routing tests;
- PersonalDB commit, snapshot, projection, watch, and repair tests;
- Rust client live native API tests.

## Operational impact

Operators should treat `STORAGE_PATH` as the durable CoreStore state directory. It contains committed storage state, not a cache. Backups and recovery plans should protect CoreStore roots, manifests, shards, refs, streams, and sealed segments together.

The admin API should stay on an internal listener. Public API traffic may expose native and S3-compatible object access according to deployment policy, but administrative mutation surfaces belong on `ADMIN_LISTEN_ADDR` with explicit admin credentials.
