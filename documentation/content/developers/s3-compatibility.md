---
title: S3 Compatibility
description: Use S3-compatible clients with Anvil while understanding the boundary between object compatibility and native Anvil features.
---

# S3 Compatibility

**What this page gives you:** a practical understanding of what S3 compatibility means, what maps cleanly to Anvil, what requires native APIs, and how to use S3-compatible clients safely.

S3 compatibility means tools that speak the S3 object API can use Anvil for common object operations. That matters because many systems already know how to upload, download, list, range-read, and multipart-upload objects through an S3-style interface.

Compatibility does not mean Anvil is only an S3 service. S3 does not contain concepts for native index definitions, relationship authorisation schemas, watch streams, vector search, PersonalDB witnessing, or structured repair. Those features use Anvil's native API.

## What maps cleanly

| S3 concept | Anvil concept | Notes |
| --- | --- | --- |
| Bucket | Bucket | Policy and placement boundary. |
| Object key | Object key | Path-like application identifier. |
| Object body | Object bytes | Durable data for one version. |
| User metadata | Object metadata | Queryable when indexed through native definitions. |
| ETag/checksum | Version/hash surface | Useful for integrity and preconditions. |
| LIST prefix | Directory/prefix query | Backed by Anvil path indexes. |
| GET range | Range read | Authorisation and version rules still apply. |
| Multipart upload | Multipart object assembly | Useful for large artefacts. |
| Conditional headers | Preconditions | Prevent stale or conflicting operations. |

Use S3-compatible clients for importers, backup tools, artefact uploaders, data pipelines, and existing libraries that only need object movement.

## What requires native APIs

Use native APIs for:

- creating and updating index definitions;
- querying metadata indexes beyond basic prefix lists;
- full text, vector, and hybrid search;
- managing relationship authorisation schemas and tuples;
- subscribing to watches;
- opening PersonalDB groups and submitting commits;
- reading PersonalDB projections;
- creating source and model artefact manifests;
- inspecting diagnostics and repair findings.

A common pattern is to import bytes through S3, then define indexes and query them through the native API.

## Authentication and request signing

S3-compatible requests are signed. Anvil verifies the signature, maps the credential to an Anvil identity, and then evaluates authorisation for the requested bucket, key, method, and metadata exposure.

Keep S3 credentials narrow. A tool that uploads build artefacts should not have permission to delete unrelated objects, read private prefixes, or manage authorisation state.

## Streaming and multipart behaviour

Large clients often stream request bodies or use multipart uploads. Anvil verifies signed streaming payloads and validates multipart assembly. This matters because large uploads should be safe even when bodies arrive in chunks and clients retry interrupted operations.

A production smoke test should cover:

- signed single-part PUT;
- signed streaming PUT;
- multipart create, upload part, complete, and abort;
- object metadata round trip;
- range GET;
- conditional requests;
- list prefix behaviour;
- delete and head behaviour where permitted.

## Reserved namespaces

Anvil internal paths under `_anvil/` are not public objects. S3 gateway operations reject them before normal object handling. This includes `GET`, `HEAD`, `LIST`, `PUT`, `COPY`, multipart operations, `DELETE`, conditional variants, and range reads.

Applications must not use `_anvil/` as a product prefix. Operators should treat attempts to access reserved namespaces as caller bugs or suspicious activity.

## Example import flow

```text
operator creates bucket and scoped credentials
  -> importer uploads files through S3-compatible client
  -> client verifies counts and checksums
  -> application/native job writes index definitions
  -> Anvil indexes metadata, text, or vectors
  -> application queries authorised results through native API
```

The S3 tool moves bytes. Anvil's native model turns those bytes into searchable, authorised, watchable product data.

## What you can build after this page

You should be able to connect existing S3-compatible tools to Anvil safely and know when to switch to native APIs for indexing, search, watches, authorisation, PersonalDB, and diagnostics.
