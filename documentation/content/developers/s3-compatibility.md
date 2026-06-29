---
title: S3 Compatibility
description: Use S3-compatible clients with Anvil while understanding what compatibility does and does not cover.
---

# S3 Compatibility

**What this page achieves:** you will know when S3 compatibility is the right interface, how it maps to Anvil, and where native APIs are required.

S3 compatibility means tools that know the S3 object API can talk to Anvil for common object operations. This is valuable because many backup tools, data importers, SDKs, and command-line workflows already understand S3-style buckets, keys, objects, metadata, range reads, and multipart uploads.

Compatibility does not mean Anvil becomes only an S3 service. Anvil has native features that S3 does not express: index definitions, relationship authorization schemas, watch streams, vector search, PersonalDB witnessing, and structured administrative diagnostics.

## What maps cleanly

These concepts map directly:

| S3 concept | Anvil concept |
| --- | --- |
| Bucket | Bucket |
| Object key | Object key |
| Object body | Object bytes |
| User metadata | Object metadata |
| ETag/checksum | Version/hash validation surface |
| LIST prefix | Directory/prefix index query |
| GET range | Object byte range read |
| Conditional write/read | Version and precondition checks |

Use S3 clients for straightforward object movement: ingest files, export artifacts, sync backups, or integrate software that already expects an S3 endpoint.

## What needs native APIs

Use native APIs for:

- creating index definitions;
- querying metadata indexes beyond basic object listing;
- full text, vector, and hybrid search;
- managing relationship authorization schemas and tuples;
- subscribing to watch streams;
- PersonalDB group open, commit, snapshot, and projection APIs;
- source artifact and model ingestion workflows;
- structured diagnostics and repair operations.

S3 clients cannot express those operations because the S3 protocol does not contain those concepts.

## Reserved namespace behavior

Anvil internal paths under `_anvil/` are not accessible through S3. This includes GET, HEAD, LIST, PUT, COPY, multipart operations, DELETE, and conditional variants. The gateway rejects those paths before normal object authorization.

This protects internal metadata, index material, authorization tuples, watch checkpoints, and PersonalDB state. If you need operational insight, use structured native or admin APIs.

## Authentication

S3-compatible clients normally sign requests. Anvil verifies the request identity and maps it to tenant/application authorization. Keep S3 credentials scoped to the minimum buckets and prefixes required.

Do not give broad write credentials to generic automation. A sync tool that only uploads release artifacts should not have rights to delete source snapshots or inspect database group state.

## Example: object import

A simple import flow is:

```text
create bucket with native or admin API
  -> configure scoped S3 credentials
  -> run existing uploader against Anvil endpoint
  -> verify object count and hashes
  -> create or update index definitions with native API
  -> query imported metadata/search through native API
```

The S3 tool moves bytes. Native Anvil APIs make those bytes searchable, authorized, watchable, and operationally visible.

## Compatibility test expectations

A production-compatible S3 surface should prove:

- bucket create/list/delete where supported;
- object PUT/GET/HEAD/LIST/DELETE;
- metadata round trip;
- range reads;
- conditional reads and writes;
- multipart upload and abort behavior;
- reserved namespace rejection;
- authorization failures do not leak object existence.

## What you can build after this page

You should be able to decide when S3 compatibility is enough and when native APIs are required. Next, learn how object metadata and indexes support application-level queries.
