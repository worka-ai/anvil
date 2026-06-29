---
title: Release Checklist
description: Build, test, package, publish, and verify Anvil server, CLI, clients, and documentation.
---

# Release Checklist

**What this page achieves:** you will have a repeatable release process for server images, Rust crates, TypeScript clients, Python clients, and documentation.

A release is not only a tag. It is a set of artifacts that users can install and operators can deploy. Each artifact must be built from the intended source, tested, packaged, published, and smoke-tested after publication.

## Source verification

Run source checks first:

```bash
cargo fmt --all -- --check
cargo test --workspace
```

These checks prove the workspace builds and tests in the local environment. They do not replace Docker, client package, or deployment smoke tests.

## External smoke suites

Run environment-dependent suites when Docker and network access are available:

```bash
ANVIL_RUN_DOCKER_E2E=1 cargo test -p anvil-storage --test docker_cluster_test -- --nocapture
ANVIL_RUN_HF_E2E=1 cargo test -p anvil-storage --test hf_ingestion_e2e -- --nocapture
```

Classify any ignored test before release. An ignored test is either deliberately external, moved into CI with a required environment flag, or a release blocker.

## Docker image

Build the image with a fixed version tag and run a smoke test that covers:

- container boot;
- health/readiness;
- tenant and app provisioning;
- token acquisition;
- S3 bucket create;
- S3 PUT, GET, HEAD, LIST, DELETE;
- reserved namespace rejection;
- native object API;
- basic metadata index query;
- authorization tuple check.

A container that starts is not enough. It must prove the object and security surfaces work.

## Rust crates

Publish Rust crates in dependency order:

1. `anvil-storage-core`
2. `anvil-storage-test-utils` when publishing test support
3. `anvil-storage-cli`
4. `anvil-storage`

Run dry-runs before publishing:

```bash
cargo publish --dry-run -p anvil-storage-core
cargo publish --dry-run -p anvil-storage-cli
cargo publish --dry-run -p anvil-storage
```

Dependent dry-runs may require already-published versions. If so, publish in order and verify each package page before moving to the next.

## TypeScript package

From `clients/typescript`:

```bash
npm ci
npm test
npm pack --dry-run
npm publish --access public
```

Verify the package contains generated TypeScript entry points, the protocol file, type declarations, and README content that explains how to connect.

## Python package

From `clients/python`:

```bash
python -m build
python -m twine check dist/*
python -m twine upload dist/*
```

Verify the wheel contains generated gRPC modules, protocol files, type hints where available, and documentation for authentication and endpoint configuration.

## Documentation site

Build and verify the documentation site:

```bash
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
```

The published site should explain concepts, guide developers, guide operators, and provide exact reference material. Do not ship a release whose docs are only command snippets.

## Post-publication verification

After publishing:

1. Pull the Docker image by tag and run the smoke test.
2. Install the Rust CLI from the published crate and run basic commands.
3. Install the npm package in a fresh project and call a read-only API.
4. Install the Python package in a fresh virtual environment and call a read-only API.
5. Open the documentation site and verify navigation/search.
6. Record artifact versions, checksums, and release notes.

## What you can do after this page

You should be able to release Anvil with a repeatable process that proves every published artifact is usable, not merely uploaded.
