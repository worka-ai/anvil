---
title: Package Publishing
description: Published Anvil artifacts and how each package is meant to be used.
---

# Package Publishing

**What this page achieves:** you will understand what Anvil publishes, which audience each artifact serves, and how the artifacts fit together.

Anvil releases ship multiple artifacts because different users enter the system in different ways. Operators need a server image. Rust developers may want crates. TypeScript and Python developers need packaged clients. Everyone needs documentation tied to the same release.

## Docker image

The Docker image runs the Anvil server. It is the standard deployment artifact for container environments. A release image should be pinned by version and verified with smoke tests before production rollout.

The image is expected to expose the native API and S3-compatible gateway according to runtime configuration.

## Rust crates

| Package | Purpose |
| --- | --- |
| `anvil-storage-core` | Core types, generated protocol bindings, storage engines, auth, indexes, PersonalDB services, and implementation internals. |
| `anvil-storage-cli` | Reusable CLI implementation for user and admin command surfaces. |
| `anvil-storage` | Server binary, admin binary, S3 gateway, and top-level release crate. |
| `anvil-storage-test-utils` | Test harness utilities for integration tests and downstream validation when published. |

Publish crates in dependency order so downstream packages can resolve versions cleanly.

## TypeScript client

The TypeScript package gives JavaScript and TypeScript applications a native API client surface. It should include generated code, type declarations, the protocol file, and examples showing authentication and a basic read/write flow.

Use it when a Node.js service, web backend, or tooling script needs native Anvil APIs rather than S3 compatibility alone.

## Python client

The Python package gives Python services and data workflows a native API client. It should include generated gRPC modules, protocol files, package metadata, and examples for connecting and issuing basic calls.

Use it for data import, automation, analysis, model artifact workflows, and service integration.

## Documentation site

The documentation site is part of the release. It teaches the model, guides developers, guides operators, and provides reference material. It should match the behavior of the released binaries and clients.

## Artifact relationship

A typical adoption path is:

```text
operator deploys Docker image
  -> administrator creates tenant/application credentials
  -> developer installs Rust/TypeScript/Python client or configures S3 tool
  -> application writes objects and metadata
  -> operator monitors indexes, authz, watches, and PersonalDB health
```

Each artifact supports one part of that path.

## What you can do after this page

You should be able to identify which Anvil artifact a user needs and verify that a release includes all expected server, client, and documentation outputs.
