# Anvil

Anvil is a production object storage platform for teams that need storage to do more than accept bytes. It stores objects, but it also keeps metadata indexes, full text search, vector search, relationship authorization, watch streams, source artifacts, and PersonalDB witnessing under one coherent system.

Most product teams start with a simple requirement: upload something and read it later. The next requirements arrive quickly: list recent project files, filter by metadata, search inside documents, find similar media, protect every result with fine-grained authorization, update live views when data changes, and sync local-first SQLite data without losing consistency. When those capabilities are assembled from unrelated systems, application code becomes the place where storage correctness is glued together.

Anvil moves those concerns into the storage layer.

## What Anvil Solves

An object store is a system that stores bytes under names. That foundation is useful, but modern applications also need to ask questions about stored data:

- Which objects belong to this tenant, project, user, or timeline?
- Which objects match this metadata filter?
- Which documents contain this phrase?
- Which images, audio clips, videos, or text records are semantically similar?
- Which results may this caller see?
- Which indexes have caught up to this write?
- Which local database changes were witnessed, certified, projected, and made visible?

Anvil treats these as storage questions rather than application glue. Object identity, metadata, versions, authorization, watches, indexes, and recovery evidence are part of one product model.

## Core Capabilities

- **Object storage:** buckets, keys, object versions, checksums, range reads, multipart flows, and S3-compatible access for existing tooling.
- **Metadata and path indexes:** predictable key layouts and queryable metadata for fast listings, filters, facets, and operational navigation.
- **Full text search:** tokenization, ranking, snippets, and authorization-safe search results over object text and extracted content.
- **Vector search:** semantic retrieval over text, images, audio, and video using vector segments and Rust-native nearest-neighbor indexing.
- **Relationship authorization:** Zanzibar-style tuples, permissions, caveats, and fail-closed reserved namespaces protecting every exposure path.
- **Watch streams:** durable change streams with cursors so indexes, projections, applications, and operators can catch up without rescanning everything.
- **PersonalDB witnessing:** SQLite changeset verification, commit certificates, snapshots, row metadata, authorized projections, and catch-up support for local-first applications.
- **Source and model artifacts:** storage patterns for source packs, build outputs, logs, screenshots, model manifests, media derivatives, and reproducibility records.

## Documentation

The public documentation lives in `documentation/` and is built with Fission.

Start with the learning path if you are new to object storage, indexing, search, authorization, watches, or PersonalDB:

```sh
fission site serve --project-dir documentation
```

Build and check the static site:

```sh
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
```

The documentation is structured as a progressive guide:

- `documentation/content/learn/` teaches the concepts from first principles.
- `documentation/content/tutorials/` shows how to perform Anvil operations in Rust, Java, Node.js, and Python.
- `documentation/content/developers/` shows how to build applications with Anvil.
- `documentation/content/operators/` explains deployment, identity, indexing operations, backup, recovery, and release work.
- `documentation/content/reference/` gives exact configuration, CLI, package, and error references.
- `documentation/src/app.rs` contains the custom Fission marketing home page.

## Development Checks

Run the workspace checks before publishing or changing core behavior:

```sh
cargo fmt --all -- --check
cargo test --workspace
```

S3 compatibility is a release priority. Changes to request signing, streaming uploads, object operations, metadata, authorization, or bucket behavior should include focused S3 gateway tests and then the full workspace test suite.

## Release Surfaces

An Anvil release is expected to include:

- the server image;
- Rust crates and CLI packages;
- TypeScript and Python client package metadata and generated clients when enabled;
- the Fission documentation site;
- S3 compatibility smoke tests;
- operator-facing release notes and verification evidence.

See `documentation/content/operators/release-checklist.md` for the release process.

## License

Anvil is licensed under the Apache 2.0 License. See `LICENSE`.
