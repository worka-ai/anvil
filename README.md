# Anvil

Anvil is a production object storage platform with indexing, search, relationship authorisation, durable watches, and PersonalDB witnessing built into the storage layer.

A normal object store can put bytes under a key and read them later. Product systems usually need much more: predictable path layouts, version history, metadata filters, full text search, vector retrieval, access checks on every result, live change streams, background repair evidence, static delivery, S3-compatible tooling, and a way to sync local-first SQLite applications without adding a separate database service. Anvil treats those capabilities as storage responsibilities instead of leaving every application to rebuild them.

## Current Capabilities

- **Object storage:** tenants, buckets, keys, object versions, current pointers, delete markers, checksums, range reads, copy/delete operations, multipart flows, native gRPC APIs, and S3-compatible object access.
- **CoreStore durability:** objects, metadata, indexes, authorisation records, PersonalDB state, mesh lifecycle records, gateway records, leases, audit events, and repair findings persist through immutable objects, ordered streams, and compare-and-swap refs.
- **Indexes and search:** path indexes, metadata-filter indexes, typed JSON indexes, full text indexes, vector indexes, and hybrid indexes with documented selector, extractor, build-policy, query, diagnostics, pagination, and catch-up shapes.
- **Authorisation:** public policy scopes for tenant app credentials, Zanzibar-style relationship authorisation for tenant product data, and private system-realm relations for the admin API.
- **Watch streams:** cursor-based streams for source changes and derived maintenance so applications and background workers can catch up without rescanning everything.
- **PersonalDB witnessing:** SQLite changeset validation, commit certificates, catch-up, snapshots, projection records, row metadata, and repair evidence for local-first applications.
- **Gateways:** native public API, S3-compatible object access, static host-alias routing, object links, and CoreStore-backed gateway foundation records.
- **Operations:** Docker-first server deployment, separate public/admin listeners, tenant and app provisioning, least-privilege grants, topology lifecycle, diagnostics, repair, audit listing, and release gates.

## Architecture

CoreStore is Anvil's durable boundary. It has three primitives:

| Primitive | Purpose |
| --- | --- |
| `CoreObject` | Immutable bytes: payloads, index segments, snapshots, source packs, gateway blobs. |
| `CoreStream` | Ordered facts: object mutations, authz tuple logs, audit events, append records, PersonalDB commits. |
| `CoreRef` | CAS heads: current object pointers, index generations, PersonalDB heads, routing state, leases. |

Feature-specific formats still exist where useful, but durable truth goes through those primitives. S3 is a gateway, not the storage model. The native API, S3 gateway, search, watches, PersonalDB, and admin workflows all resolve back to Anvil tenants, buckets, resources, authorisation, and CoreStore records.

## Release Surfaces

- **Server:** Docker image and release binaries. The server crate is not published to crates.io.
- **Rust client:** `anvil-storage = "0.2.4"` on crates.io.
- **CLIs:** `anvil` for tenant/public operations and `anvil-admin` for private admin-plane operations.
- **Documentation:** Fission static site in `documentation/`, published by a separate docs workflow.
- **Protocol bindings:** generated gRPC bindings are packaged with the Rust client.

## Quick Start

Anvil is Docker-first. Set `ANVIL_IMAGE` to the image published for the release you want to run, preferably pinned by tag or digest:

```sh
export ANVIL_IMAGE="ghcr.io/<owner>/anvil:v0.2.4"
docker pull "$ANVIL_IMAGE"
```

For a real local setup, use the Docker tutorial because it also covers server secret material, first-start bootstrap, public/admin listener separation, and token flow:

```sh
fission site serve --project-dir documentation
# then open /tutorials/setup-local-anvil/
```

Add the Rust client to an application:

```toml
[dependencies]
anvil-storage = "0.2.4"
```

Use the client with a bearer token minted by Anvil:

```rust
use anvil_storage::{proto::ListBucketsRequest, AnvilClient};

async fn example(
    endpoint: String,
    token: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = AnvilClient::connect_with_bearer(endpoint, token).await?;
    let response = client.buckets().list_buckets(ListBucketsRequest {}).await?;
    println!("{} buckets", response.into_inner().buckets.len());
    Ok(())
}
```

Existing S3-compatible tools can use the S3 gateway with Anvil-issued app credentials when the operation is object-shaped. Use the native API or Rust client for Anvil-specific features such as typed indexes, watches, PersonalDB, relationship authorisation, task leases, and repair workflows.

## Documentation

The documentation is organised as four books:

- `documentation/content/learn/` teaches the concepts from first principles.
- `documentation/content/tutorials/` walks through concrete operations.
- `documentation/content/operators/` covers deployment and production operation.
- `documentation/content/reference/` documents the CLIs, authorisation action/resource strings, and index/query JSON shapes.

Build and check the site locally:

```sh
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
```

## Development Checks

Run these before changing core behaviour or opening a release PR:

```sh
cargo fmt --all -- --check
./scripts/release-gates.sh
```

`release-gates.sh` includes storage hardening checks, docs hardening, release-note rendering, the Fission documentation build, the Rust client publish dry run, and the workspace test suite. Security-sensitive changes should also include focused tests for the affected path before the full gate.

## Release Process

The release flow is designed so PR and release testing use the same gates:

1. Open a PR containing source, docs, workflow, README, and blog/release-note changes.
2. Merge to `main` only after CI passes.
3. Tag the release.
4. Let the release workflow test the Docker image, publish the tested image, publish `anvil-storage` if the version is new, render release notes from the release blog post, and create the GitHub release.
5. Let the independent documentation workflow publish the Fission static site from `documentation/`.

See `documentation/content/operators/release-readiness-checklist.md` for the operator checklist.

## License

Anvil is licensed under the Apache 2.0 License. See `LICENSE`.
