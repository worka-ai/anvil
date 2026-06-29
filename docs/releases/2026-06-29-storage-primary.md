# Anvil Storage Primary Release

This release is a fundamental rewrite of Anvil into an object-native storage platform. Anvil now treats object bytes, metadata, indexes, watches, relationship authorisation, source artefacts, model artefacts, and PersonalDB witnessing as one coherent storage system.

## Release Summary

Anvil now ships as:

- a Docker-hosted server image published to GitHub Container Registry;
- release binaries for the server, CLI, and admin tooling;
- the `anvil-storage` Rust client crate for application developers;
- a Fission-built documentation site published through GitHub Pages;
- a public S3-compatible gateway backed by the same native object, metadata, authorisation, and indexing model as the gRPC API.

The central architectural change is the move away from an external relational metadata store. Anvil-owned state is stored and recovered through Anvil's object-native journals, manifests, segments, checkpoints, fences, and derived indexes. The result is a storage system where data, metadata, indexes, and recovery evidence live together and can be moved, replicated, watched, repaired, and audited as storage state.

## Object Storage and S3 Compatibility

The release includes bucket and object APIs for creating buckets, writing objects, reading objects, listing prefixes, deleting objects, copying objects, composing payloads, retaining versions, and reading ranges. The S3 gateway supports standard client flows including signed requests, object reads and writes, list operations, metadata, range reads, preconditions, and multipart-compatible behaviours.

Request signing and object visibility are enforced at the gateway boundary. SigV4 requests are checked for freshness, and private object reads check authorisation before metadata lookup so callers cannot distinguish missing objects from objects they are not allowed to inspect.

## Metadata, Paths, and Queryable Storage

Anvil maintains object metadata and path-oriented indexes as storage-native structures. Applications can rely on predictable object keys, metadata fields, prefix navigation, and derived indexes without building separate metadata tables outside the store.

The metadata model supports high-level application navigation patterns such as workspace timelines, artefact lists, frame indexes, source bundles, logs, screenshots, model files, and PersonalDB records. These are stored as objects with deterministic paths and can be indexed, watched, authorised, and recovered through Anvil itself.

## Full Text Search

Full text indexing is integrated into the object store. Text can be extracted from object bodies or derived media transcripts, indexed into full text segments, and queried with result filtering that respects authorisation.

The search path supports tokenised lookup, phrase-oriented query behaviour, ranking metadata, and safe filtering so the query system does not expose objects that the caller cannot read.

## Vector Search for Text, Audio, Image, and Video

Vector search is implemented as native storage structures rather than an external vector database dependency. Vector segments, graph validation, modality tracking, and nearest-neighbour query paths are part of the Anvil index model.

The release supports vector index flows for text-derived embeddings and media-derived embeddings, including audio and video modalities. This lets applications store media once and build retrieval experiences over the same object namespace.

## Relationship Authorisation

Anvil includes a Zanzibar-style relationship authorisation system. It stores relationship tuples, namespace definitions, caveat hashes, derived usersets, tuple watches, and authorisation indexes inside Anvil.

Authorisation is enforced across object reads, object listings, S3 gateway access, indexes, source artefacts, PersonalDB operations, and internal reserved namespaces. Reserved `_anvil/*` state is fail-closed and is only writable through Anvil-owned internal paths.

## Watch Streams and Derived Maintenance

The release includes durable watch streams with cursor support. Watches allow applications, derived index builders, projections, operators, and repair routines to catch up from known positions without rescanning whole buckets.

Watch-backed subsystems include object metadata, bucket metadata, authz tuples, authz derived lag, git source indexes, index partitions, PersonalDB groups, and PersonalDB projections. Cursor validation and checkpoint logic are covered by the test suite.

## PersonalDB Witnessing

PersonalDB support is now a first-class part of Anvil. Anvil can witness SQLite changesets, validate changeset effects, issue commit certificates, maintain group heads, create snapshots, expose catch-up APIs, build authorised projections, and support projection writeback.

The PersonalDB path gives local-first applications a server-side witness and storage layer without making application servers own the database replication protocol. Changesets, certificates, snapshots, row indexes, projection definitions, and repair evidence are persisted as Anvil storage state.

## Source and Model Artefacts

Anvil includes storage and query support for source and model artefacts. Git pack and source-index paths support source bundle storage, tree/blob lookup, watch cursors, and reproducibility workflows.

Model artefact flows include Hugging Face ingestion, model manifests, indexed files, diagnostics, and object uploads into Anvil-managed buckets. This enables model and source assets to share the same object, metadata, search, watch, and authorisation infrastructure.

## Rust Client

The `anvil-storage` crate is the release-supported Rust client. It exposes the public Anvil gRPC services needed by application code and packages generated protocol bindings for those services.

Internal node-to-node services and shard-management messages are intentionally not exposed through the Rust client crate. Bearer token metadata is marked sensitive, and client debug output redacts credentials.

## Documentation Site

The documentation site is built with Fission and published independently of the server release workflow. It contains a marketing home page, a progressive learning path, tutorials, developer guides, operator guides, and reference material.

The docs teach Anvil concepts from first principles: object storage, keys and metadata, indexes and search, authorisation, watches, PersonalDB, native APIs, S3 compatibility, deployment, identity, backup, recovery, and package publishing.

## Release Verification

The release branch passed:

- Rust formatting checks;
- full workspace tests;
- S3 gateway tests;
- distributed reconstruction tests;
- Docker cluster tests;
- Hugging Face ingestion e2e tests;
- Rust client live API tests;
- docs site checks;
- Rust client crates.io dry-run packaging;
- release-blocking security re-audit for token handling, public client API exposure, SigV4 freshness, and object existence behaviour.

