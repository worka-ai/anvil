---
title: Package Publishing
description: Release artifacts produced by the Anvil project.
---

# Package Publishing

**Goal:** understand the release artifacts Anvil publishes and what each one is for.

Anvil releases include server images, Rust crates, TypeScript clients, Python clients, generated protocol files, and static documentation.

## Docker image

The Docker image runs the Anvil server and includes the CLI binaries needed for smoke testing. The runtime image is minimal and does not include the Rust toolchain.

## Rust crates

| Package | Purpose |
| --- | --- |
| `anvil-storage-core` | Core types, generated gRPC bindings, storage engines, auth, indexes, and service implementation. |
| `anvil-storage-cli` | Reusable CLI implementation. |
| `anvil-storage` | Server binary, admin binary, S3 gateway, and release package. |
| `anvil-storage-test-utils` | Test harness utilities for integration tests and downstream validation. |

## TypeScript package

`anvil-storage-client` provides a packaged JavaScript/TypeScript client surface and includes `proto/anvil.proto` for tools that generate clients at application build time.

## Python package

`anvil-storage-client` for Python packages generated gRPC modules and the Anvil proto so Python services can call the native API directly.

## Documentation site

The documentation site is a Fission static site. It contains custom Fission pages for product narrative and Markdown content routes for guide and reference material.
