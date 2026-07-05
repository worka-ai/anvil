# Anvil

Anvil is a production object storage platform that makes storage, search, authorisation, live change streams, and local-first database witnessing part of one system.

Most applications begin with a simple storage requirement: put bytes under a name and read them later. Real products quickly need more. They need predictable paths, fast listings, metadata filters, text search, semantic retrieval, access checks on every result, live updates when data changes, auditable background indexing, and a way to sync local-first SQLite data without turning the application server into a pile of bespoke storage glue.

Anvil moves those concerns into the storage layer. It stores objects, maintains indexes, evaluates relationship authorisation, exposes durable watches, and witnesses PersonalDB changesets using a single object-native architecture.

## What Anvil Provides

- **Object storage:** buckets, keys, versions, checksums, range reads, multipart upload flows, copy/delete operations, S3-compatible access, and native gRPC APIs.
- **Object-native metadata:** bucket/object state, manifests, journals, derived index records, diagnostics, source artefacts, and PersonalDB state are stored inside Anvil rather than delegated to an external relational metadata store.
- **Path and metadata indexes:** predictable key layouts, prefix navigation, metadata selectors, filters, and facets for application and operator workflows.
- **Full text search:** tokenisation, ranking, phrase support, snippets, and result filtering that respects authorisation before exposing matches.
- **Vector search:** Rust-native vector segment storage and nearest-neighbour search for text, image, audio, and video-derived embeddings.
- **Relationship authorisation:** Zanzibar-style tuples, namespace rules, caveats, derived usersets, fail-closed internal namespaces, and permission checks on object, index, source, and PersonalDB paths.
- **Watch streams:** durable cursor-based change streams used by applications, derived indexes, projections, and recovery tooling to catch up without rescanning whole buckets.
- **PersonalDB witnessing:** SQLite changeset validation, commit certificates, snapshots, row metadata, authorised projections, projection writeback, repair evidence, and catch-up APIs for local-first applications.
- **Source and model artefacts:** storage patterns for git packs, source indexes, model manifests, Hugging Face ingestion, media extraction diagnostics, and reproducibility records.
- **Operational tooling:** an admin CLI, an application CLI, Docker image publication, S3 compatibility tests, release checks, and Fission-built documentation.


## Storage Architecture

Anvil's durable boundary is CoreStore. CoreStore has three primitives: immutable `CoreObject`s, ordered `CoreStream`s, and compare-and-swap `CoreRef`s. Objects, metadata, indexes, authorisation, PersonalDB, mesh routing, gateway records, task leases, audits, and repair evidence all persist through those primitives.

This keeps S3 compatibility in the right place. S3 is a gateway for existing object tools; it is not the internal storage model. Native APIs, S3-compatible APIs, search, watches, and admin operations all resolve into the same object, authorisation, and CoreStore path.

## Release Surfaces

This release ships Anvil through these supported surfaces:

- **Server:** Docker image and release binaries. The server is not published to crates.io.
- **Rust client:** the `anvil-storage` crate, published to crates.io.
- **CLI/admin binaries:** packaged from the server build for operators.
- **Documentation site:** a Fission static site published independently from code releases.
- **Protocol bindings:** generated gRPC bindings are packaged inside the Rust client; internal node-to-node services are not exposed by the public client API.

## Quick Start

Run the server from Docker:

```sh
docker pull ghcr.io/worka-ai/anvil:latest
docker run --rm -p 50051:50051 -p 9000:9000 ghcr.io/worka-ai/anvil:latest
```

Add the Rust client to an application:

```toml
[dependencies]
anvil-storage = "0.2"
```

Use the client:

```rust
use anvil_storage::{AnvilClient, proto::ListBucketsRequest};

# async fn example(endpoint: String, token: String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
let client = AnvilClient::connect_with_bearer(endpoint, token).await?;
let response = client.buckets().list_buckets(ListBucketsRequest {}).await?;
println!("{} buckets", response.into_inner().buckets.len());
# Ok(())
# }
```

Existing S3-compatible tools can also speak to Anvil through the S3 gateway when configured with Anvil-issued credentials.

## Documentation

The public documentation lives in `documentation/` and is built with Fission.

Start the local documentation site:

```sh
fission site serve --project-dir documentation
```

Build and check the static site:

```sh
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
```

The documentation is structured as a progressive guide:

- `documentation/content/learn/` teaches object storage, metadata, indexing, search, authorisation, watches, and PersonalDB from first principles.
- `documentation/content/tutorials/` shows each operation using release-supported client surfaces.
- `documentation/content/developers/` explains how to build applications directly on Anvil.
- `documentation/content/operators/` covers deployment, identity, indexing operations, backup, recovery, and release work.
- `documentation/content/reference/` defines configuration, CLI commands, package surfaces, and security errors.
- `documentation/src/app.rs` contains the Fission marketing home page.

## Development Checks

Run these checks before publishing or changing core behaviour:

```sh
cargo fmt --all -- --check
cargo test --workspace
cargo publish --dry-run -p anvil-storage
```

Security-sensitive changes should include focused tests for the affected path and then the full workspace test suite. Request signing, token handling, reserved namespaces, object existence behaviour, relationship authorisation, S3 gateway behaviour, watch cursors, and PersonalDB witnessing are release-critical surfaces.

## Release Process

The release process is intentionally split by surface:

1. Open a PR containing the source, documentation, CI, and release-note changes.
2. Merge to `main` after CI passes.
3. Let GitHub Actions build and publish the Docker image.
4. Let the independent documentation workflow publish the Fission static site.
5. Publish the Rust client crate with `cargo publish -p anvil-storage`.
6. Create the GitHub release with detailed notes and links to the Docker image, crate, documentation, and verification evidence.

See `documentation/content/operators/release-checklist.md` for the operator checklist.

## License

Anvil is licensed under the Apache 2.0 License. See `LICENSE`.
