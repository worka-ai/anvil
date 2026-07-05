---
title: Package Publishing
description: Published Anvil artefacts and how each package is meant to be used.
---

# Package Publishing

**What this page gives you:** a reference for the artefacts an Anvil release publishes and the audience for each artefact.

Anvil ships multiple packages because developers and operators enter the system through different tools. Operators need a server image. Rust developers may install the Rust client crate. Operators use the Docker image and release binaries. This release ships the Rust native client first; other language clients are outside the release scope. Everyone needs documentation matching the release.


## Supported gateway surfaces

Anvil's core model is gateway-neutral. A gateway is a protocol adapter that maps a request into an Anvil principal, tenant, bucket, resource, authorisation scope, object or repository prefix, and CoreStore-backed record family.

| Gateway surface | Release status | What it stores |
| --- | --- | --- |
| Native gRPC API | Supported | Buckets, objects, indexes, watches, authorisation, PersonalDB, source/model artefacts, repair, and diagnostics. |
| S3-compatible object API | Supported | Bucket and object operations for tools that already speak S3. |
| Static host aliases | Supported as routing/control records | Hostname-to-bucket/prefix mappings used by object delivery paths. |
| Object links | Supported | Symlink-like object aliases such as movable `latest` pointers. |
| Registry gateway foundation | Supported as internal records | Gateway mounts, credentials, repositories, blobs, tags, upload sessions, token challenges, and audits. |

Container registry, Rust crate registry, npm, PyPI, and Maven protocol handlers use the registry gateway foundation when those protocol endpoints are enabled in a later release. They are not separate storage systems and they do not get private durable files.

## Docker image

The Docker image runs the Anvil server. It exposes the native API and S3-compatible gateway according to runtime configuration. It should be pinned by version and smoke-tested before production rollout.

## Rust crate

| Package | Purpose | Published in this release |
| --- | --- | --- |
| `anvil-storage` | Public Rust native API client with generated protocol bindings, bearer-token helpers, and typed service-client constructors. | Yes |
| `anvil-server` | Internal workspace package for server, admin binary, and S3 gateway builds. | No |
| `anvil-storage-core` | Internal workspace package for storage engines, auth, indexes, PersonalDB services, and implementation internals. | No |
| `anvil-storage-cli` | Internal workspace package for CLI implementation. | No |
| `anvil-storage-test-utils` | Internal workspace package for integration tests and downstream validation. | No |

Only `anvil-storage` is a crates.io package for this release. The server is released as a Docker image and release binaries, not as a crates.io server crate.

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
