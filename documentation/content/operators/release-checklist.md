---
title: Release Checklist
description: Build, test, package, publish, and verify Anvil server images, release binaries, the Rust client crate, and documentation.
---

# Release Checklist

**What this page gives you:** a repeatable release process. You will know which artefacts must be built, which tests prove them, and what to verify after publishing.

A release is not a tag. A release is a set of installable artefacts that operators and developers can use: server image, release binaries, the Rust client crate, protocol files, and documentation. This release intentionally ships only the Rust native client; non-Rust client packages are not release artefacts yet.

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
ANVIL_RUN_DOCKER_E2E=1 cargo test -p anvil-server --test docker_cluster_test -- --nocapture
ANVIL_RUN_HF_E2E=1 cargo test -p anvil-server --test hf_ingestion_e2e -- --nocapture
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
- authorisation tuple check works;
- metrics and logs are emitted.

## Rust client crate

The only crates.io artefact in this release is the Rust client crate:

1. `anvil-storage`

The server, admin binary, CLI implementation, core implementation crates, and test utilities are not published to crates.io in this release. Server distribution is through the Docker image and release binaries.

Validate the client crate before publishing:

```bash
cargo test -p anvil-storage
cargo test -p anvil-server --test rust_client_tests
cargo publish --dry-run -p anvil-storage
```

The first command proves the package API and generated bindings build in isolation. The second command starts a live Anvil node and proves the published Rust client can create a bucket, list buckets, stream an object upload, and stream the object back through the native API.

The TypeScript, Python, Java, and Maven surfaces are not release blockers for this release.

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
2. Download or extract the release CLI binary and run read/write checks.
3. Install the `anvil-storage` Rust client crate in a fresh project and call a read-only API.
4. Open the documentation site and verify navigation and search.
5. Record versions, checksums, release notes, and known limitations, including that non-Rust native clients are not part of this release.

## What you can do after this page

You should be able to release Anvil as a coherent product instead of a collection of uploaded files.
