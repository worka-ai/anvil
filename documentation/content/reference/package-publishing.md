---
title: Package Publishing
description: Published Anvil artifacts and how each package is meant to be used.
---

# Package Publishing

**What this page gives you:** a reference for the artifacts an Anvil release publishes and the audience for each artifact.

Anvil ships multiple packages because developers and operators enter the system through different tools. Operators need a server image. Rust users may install crates and CLI tools. TypeScript and Python users need client packages. Everyone needs documentation matching the release.

## Docker image

The Docker image runs the Anvil server. It exposes the native API and S3-compatible gateway according to runtime configuration. It should be pinned by version and smoke-tested before production rollout.

## Rust crates

| Package | Purpose |
| --- | --- |
| `anvil-storage-core` | Core types, protocol bindings, storage engines, auth, indexes, PersonalDB services, and implementation internals. |
| `anvil-storage-cli` | CLI implementation for user and admin command surfaces. |
| `anvil-storage` | Server binary, admin binary, S3 gateway, and top-level release crate. |
| `anvil-storage-test-utils` | Test utilities for integration tests and downstream validation when published. |

Publish in dependency order.

## TypeScript client

The TypeScript package is for Node.js services, web backends, automation, and developer tooling that need native Anvil APIs rather than S3 compatibility alone. It should include generated code, type declarations, protocol files, and examples.

## Python client

The Python package is for data workflows, importers, automation, model artifact tooling, and service integration. It should include generated gRPC modules, protocol files, package metadata, and connection examples.

## Documentation site

The documentation site is part of the release. It teaches concepts, guides developers, guides operators, and provides reference material tied to the released behavior.

## Artifact relationship

```text
operator deploys Docker image
  -> administrator creates tenant/application credentials
  -> developer installs a client or configures S3 tooling
  -> application writes objects and metadata
  -> indexes, authz, watches, and PersonalDB services maintain derived state
  -> operators monitor health and recoverability
```

## What you can do after this page

You should be able to identify which Anvil artifact a user needs and verify that a release includes every expected server, client, and documentation output.
