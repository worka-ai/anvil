---
title: Release Checklist
description: Build, test, package, publish, and verify Anvil server images, Rust crates, TypeScript clients, Python clients, and documentation.
---

# Release Checklist

**What this page gives you:** a repeatable release process. You will know which artifacts must be built, which tests prove them, and what to verify after publishing.

A release is not a tag. A release is a set of installable artifacts that operators and developers can use: server image, Rust crates, CLI, TypeScript client, Python client, protocol files, and documentation.

## Source verification

Run source checks first:

```bash
cargo fmt --all -- --check
cargo test --workspace
```

These checks prove the workspace builds and tests locally. They do not replace Docker, client package, S3 gateway, or deployment smoke tests.

## External smoke suites

Run environment-dependent suites when Docker and required network access are available:

```bash
ANVIL_RUN_DOCKER_E2E=1 cargo test -p anvil-storage --test docker_cluster_test -- --nocapture
ANVIL_RUN_HF_E2E=1 cargo test -p anvil-storage --test hf_ingestion_e2e -- --nocapture
```

Ignored tests must be classified before release. An ignored test is either deliberately external, moved into CI with a required environment flag, or a release blocker.

## Docker image

Build the production image with a fixed version tag. Then run a smoke test that proves:

- container boots;
- health/readiness reports ready;
- tenant and application credentials can be created;
- token acquisition works;
- S3 bucket and object operations work;
- signed streaming upload works;
- reserved namespace access is rejected;
- native object API works;
- metadata index query works;
- authorization tuple check works;
- metrics and logs are emitted.

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

## TypeScript and Python clients

For TypeScript:

```bash
cd clients/typescript
npm ci
npm test
npm pack --dry-run
```

For Python:

```bash
cd clients/python
python -m build
python -m twine check dist/*
```

Each client package should include generated protocol bindings, examples, README content, and authentication guidance.

## Documentation

Build and check the Fission documentation site:

```bash
fission site check --project-dir documentation --release
fission site build --project-dir documentation --release
```

The site must teach concepts, guide developers, guide operators, and provide exact reference material. Command lists alone are not release documentation.

## Post-publication verification

After publishing:

1. Pull the image by tag and rerun smoke tests.
2. Install the Rust CLI from the published crate and run read/write checks.
3. Install the TypeScript client in a fresh project and call a read-only API.
4. Install the Python client in a fresh virtual environment and call a read-only API.
5. Open the documentation site and verify navigation and search.
6. Record versions, checksums, release notes, and known limitations.

## What you can do after this page

You should be able to release Anvil as a coherent product instead of a collection of uploaded files.
