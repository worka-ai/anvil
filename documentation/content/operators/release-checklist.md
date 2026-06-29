---
title: Release Checklist
description: Build, test, package, and publish Anvil server, CLI, and clients.
---

# Release Checklist

**Goal:** release Anvil artifacts with repeatable checks for server images, Rust crates, npm clients, and Python clients.

Use this checklist when preparing an Anvil release.

## Source checks

Run:

```bash
cargo fmt --all -- --check
cargo test --workspace
```

Run external smoke suites when Docker and network access are available:

```bash
ANVIL_RUN_DOCKER_E2E=1 cargo test -p anvil-storage --test docker_cluster_test -- --nocapture
ANVIL_RUN_HF_E2E=1 cargo test -p anvil-storage --test hf_ingestion_e2e -- --nocapture
```

## Docker checks

Build the image and run a smoke test that covers:

- container boot;
- health/readiness;
- tenant and app provisioning;
- token acquisition;
- S3 bucket create;
- S3 PUT, GET, HEAD, LIST, DELETE;
- reserved namespace rejection;
- native auth and index checks.

## Rust crates

Publish crates in dependency order:

1. `anvil-storage-core`
2. `anvil-storage-test-utils` if publishing test support
3. `anvil-storage-cli`
4. `anvil-storage`

Run dry-runs first:

```bash
cargo publish --dry-run -p anvil-storage-core
cargo publish --dry-run -p anvil-storage-cli
cargo publish --dry-run -p anvil-storage
```

Dependent dry-runs require dependencies to exist in the registry, so publish order matters.

## npm client

From `clients/typescript`:

```bash
npm ci
npm test
npm pack --dry-run
npm publish --access public
```

Verify that the package includes the generated TypeScript entry points and `proto/anvil.proto`.

## Python client

From `clients/python`:

```bash
python -m build
python -m twine check dist/*
python -m twine upload dist/*
```

Verify that the wheel includes the proto file and generated gRPC modules.

## Documentation

Build the static documentation site:

```bash
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
```

Publish the generated `documentation/target/fission/site` directory to the configured static host.
