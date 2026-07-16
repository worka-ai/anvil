# Anvil Clients

This directory contains native API client packages generated from the public projection of `anvil-core/proto/anvil.proto`. The current release ships the Rust client only; the TypeScript and Python packages are kept as source previews.

## Rust / crates.io

Package: `anvil-storage`

```sh
cargo test -p anvil-storage
cargo publish --dry-run -p anvil-storage
```

The package ships generated protocol bindings, bearer-token helpers, and typed service-client constructors.

## TypeScript / npm

Not published in this release.

## Python / PyPI

Not published in this release.

## Synchronising proto files

After editing `anvil-core/proto/anvil.proto`, run:

```sh
scripts/sync-client-protos.sh
```

This writes the public client proto projection into each client package before release. Node-to-node CoreStore internals are intentionally omitted from client packages.
