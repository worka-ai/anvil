---
title: Package Publishing
description: Published Anvil artefacts and how each package is meant to be used.
---

# Package Publishing

**What this page gives you:** a reference for the artefacts an Anvil release publishes and the audience for each artefact.

Anvil ships multiple packages because developers and operators enter the system through different tools. Operators need a server image. Rust users may install crates and CLI tools. This release ships the Rust native client first; other language clients are outside the release scope. Everyone needs documentation matching the release.

## Docker image

The Docker image runs the Anvil server. It exposes the native API and S3-compatible gateway according to runtime configuration. It should be pinned by version and smoke-tested before production rollout.

## Rust crates

| Package | Purpose |
| --- | --- |
| `anvil-storage-client` | Public Rust native API client with generated protocol bindings, bearer-token helpers, and typed service-client constructors. |
| `anvil-storage-core` | Core types, storage engines, auth, indexes, PersonalDB services, and implementation internals. |
| `anvil-storage-cli` | CLI implementation for user and admin command surfaces. |
| `anvil-storage` | Server binary, admin binary, S3 gateway, and top-level release crate. |
| `anvil-storage-test-utils` | Test utilities for integration tests and downstream validation when published. |

Publish in dependency order.

## Non-Rust clients

TypeScript, Python, Java, and Maven packages are not part of this release. Keep their source packages clearly marked as unreleased until they have dedicated packaging, smoke tests, and publication pipelines.

## Documentation site

The documentation site is part of the release. It teaches concepts, guides developers, guides operators, and provides reference material tied to the released behaviour.

## Artefact relationship

```text
operator deploys Docker image
  -> administrator creates tenant/application credentials
  -> developer installs a client or configures S3 tooling
  -> application writes objects and metadata
  -> indexes, authz, watches, and PersonalDB services maintain derived state
  -> operators monitor health and recoverability
```

## What you can do after this page

You should be able to identify which Anvil artefact a user needs and verify that a release includes every expected server, client, and documentation output.
