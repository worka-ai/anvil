---
title: Object Model
description: Understand Anvil storage tenants, buckets, object keys, bodies, metadata, versions, current pointers, delete markers, links, reserved namespaces, prefixes, and placement.
---

# Object Model

Anvil is easiest to understand if you start with its object model rather than with a gateway protocol or a command. An object is not just a file in a directory. It is tenant-owned data with a stable key, a committed version history, protected metadata, authorisation state, placement, watches, and derived views that other features can build from.

This page gives you the vocabulary used throughout the rest of Learn. The practical sequence is [Buckets and Objects](/tutorials/buckets-and-objects/), [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/), and [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/). For read-side behaviour, see [Reads, Listing, and Links](/learn/reads-listing-and-links/). For write preconditions, idempotency, and fences, see [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/).

## The shape of stored data

A normal object address has three visible parts:

```text
tenant -> bucket -> object key
```

The object key then has a current pointer, and the current pointer selects one committed object version:

```text
tenant acme
  bucket documents
    key projects/p-123/contracts/main.pdf
      current -> version 01J...
        body bytes
        metadata
        ETag
        mutation id
        authorisation/index/watch evidence
```

That structure explains most Anvil operations. A read without a version id asks for the current version. A pinned read asks for a specific version id. A write creates a new version and moves the current pointer. A delete normally writes a delete marker and moves the current pointer to that marker. A list walks keys and current metadata under a prefix. A watch reports committed changes so consumers can maintain derived data without rescanning the whole bucket.

The public Object API exposes these ideas directly. The `anvil` CLI is a manual helper over the public API and does not expose every field yet. When you build production code, carry API response values such as `version_id`, `etag`, `mutation_id`, `watch_cursor`, and link `generation` instead of relying only on command output.

## Storage tenants

A storage tenant is Anvil's top-level boundary for application data. Buckets belong to a tenant, objects belong to buckets, and most tenant-facing features refer back to the same tenant: public policy scopes, relationship authorisation realms, indexes, watches, append streams, PersonalDB groups, gateways, and tenant-owned host aliases.

A tenant is not the same as a human user. A tenant might represent a customer organisation, a product environment, a workspace, or a deployment boundary. End users inside that boundary are usually subjects in the tenant's authorisation model, not separate Anvil tenants. That distinction keeps object storage, relationship checks, indexes, and audit evidence in one tenant boundary while still allowing many users inside the product.

Tenant identifiers should be stable operational names, not display labels. A UI can say `Acme Ltd`, but the storage tenant might be `acme` or an internal generated id. The tenant id is part of API routing, policy evaluation, and repair scope, so changing it should be treated as a migration, not a cosmetic rename. The bootstrap and credential flow is covered in [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/).

## Buckets

A bucket is a named container inside one tenant. Bucket names are unique within a tenant; two tenants can both have a bucket named `documents` because the tenant is part of the address and authorisation context.

Use buckets for durable boundaries, not for every small folder. A bucket is where Anvil attaches behaviour that should apply to a coherent body of data: placement region, public-read posture, gateway exposure, list and watch scope, index definitions, diagnostics, repair scope, and operational lifecycle. A document system might use a `documents` bucket for private source material and a separate `public-assets` bucket for browser-facing files. A registry-like system might use one bucket per repository class or environment rather than one bucket per package version.

Bucket creation includes a region. In the current implementation this is a bucket-level home region, not a per-object placement choice. The create path also checks mesh lifecycle state: the target region and cell must be writable according to the current placement records. If a local tutorial cannot create a bucket because the region activation workflow has not been completed, that is an operational setup issue, not an object-key modelling problem. Region, cell, node, and routing concepts are explained in [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/) and [Mesh Routing and Lifecycle](/tutorials/mesh-routing-and-lifecycle/).

## Object keys and prefixes

An object key is the stable name of an object inside a bucket. Keys often look like paths:

```text
projects/p-123/documents/d-456/original.pdf
projects/p-123/documents/d-456/extracted-text.json
releases/desktop/3.0.1/acme-desktop-linux.run
registry/packages/editor/blobs/sha256/4d967...
```

Those slashes are part of the key string. Anvil does not create implicit directories, inodes, directory permissions, or directory rename operations just because a key contains `/`. The prefix still matters because Anvil uses prefix-shaped keys for listing, watching, index selection, gateway routing, diagnostics, repair, and human inspection.

Put the scope you will list or watch near the front of the key. This key shape is strong:

```text
projects/{project_id}/documents/{document_id}/original.pdf
```

It lets a client list or watch one project's documents with `projects/{project_id}/documents/`. This shape is weaker for that workload:

```text
documents/{document_id}/projects/{project_id}/original.pdf
```

The project scope arrives too late, so a prefix query has to cover unrelated documents before application logic can filter them.

Current object-key validation is intentionally more object-store-like than filesystem-like. A key must be non-empty, no longer than 4096 characters, must not contain a NUL or control character, and must not contain a path segment that is exactly `.` or `..`. Unicode characters are accepted by the validator, but gateways, shells, and third-party tools are easier to operate when keys are URL-safe and predictable. Choose a small naming convention for your product rather than accepting arbitrary local filenames as storage keys.

## Object bodies

The object body is the bytes stored for one committed version. Anvil does not require those bytes to be JSON, text, a PDF, an image, a tarball, a model checkpoint, or any other specific format. It stores bytes and records metadata that helps readers understand what the bytes mean.

A version's body is immutable. If you upload a new PDF to the same key, Anvil creates a new version and moves the current pointer. Existing references to the previous version can still be meaningful if your application retained the `version_id` or if retention policy keeps that version available. This is why object keys should be stable names for product concepts, while version ids are evidence of a particular committed state.

Large structured information belongs in the body, not in metadata. For example, extracted text for a document can be its own object body, and typed fields can be indexed from JSON content or metadata. Metadata should remain small enough to read, list, inspect, and index without turning every metadata operation into a document download.

## Metadata

Object metadata describes the object without returning the full body. Current public API responses carry fields such as content type, content length, ETag, version id, mutation id, record hash, authorisation revision, index policy snapshot, and user metadata JSON. Application-defined metadata is useful for fields that guide routing, filtering, lifecycle, display, or diagnostics:

```json
{
  "document_type": "contract",
  "status": "signed",
  "customer_id": "acme",
  "issued_at": "2026-07-05T10:00:00Z"
}
```

Treat metadata as protected data. It can reveal customer names, workflow state, private filenames, or business identifiers even when the body is not downloaded. Anvil therefore authorises metadata reads and listing results rather than treating metadata as harmless side information. The permission vocabulary is in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and the conceptual model is in [Authorisation](/learn/authorisation/).

The native `PutObject` API can carry `content_type` and `user_metadata_json`. The current public CLI `anvil object put` uploads bytes but does not expose flags for content type or user metadata. Use the API or a client library when metadata is part of your production contract, and use the CLI mainly for smoke tests and manual inspection. The current CLI surface is documented in [Public CLI](/reference/public-cli/).

## Versions, ETags, and the current pointer

An object version is one committed state of a key. Every successful object write returns a `version_id` and an ETag. The version id identifies the committed object metadata record. The ETag is content/change evidence that clients can use for cache validation and preconditions. They are related but not interchangeable: version ids identify versions, while ETags are comparison tokens for the representation Anvil returns.

The current pointer is the per-key answer to "what should an ordinary read return?" When a caller writes a new version of `projects/p-123/contracts/main.pdf`, Anvil does not mutate the previous version's body. It commits a new version and updates the current pointer. A current `GetObject` or `HeadObject` reads whatever that pointer names at the time of the request.

This distinction is what makes compare-and-swap possible. A client can read the current version, edit a document, and then ask Anvil to write the replacement only if the current version is still the one it read. If another writer has already moved the pointer, Anvil can reject the stale write instead of silently losing work. Current CLI upload helpers do not expose version or ETag preconditions; production writers that need lost-update protection should use the API forms described in [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/) and [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/).

The current public API includes `ListObjectVersions` so callers can inspect historical versions and delete markers. The current public CLI does not provide a version-listing command, and `anvil object head` does not print the version id even though the API response contains it. If your workflow needs pinned reads, historical inspection, or audit evidence, use the API and persist the version ids your application receives.

## Delete markers

A normal delete in Anvil is represented by a delete marker. The marker is a committed version-like record that moves the current pointer into a "not found" state. Ordinary reads after the delete behave as if the object is missing, but older versions may still exist according to the storage and retention model.

This is different from a simple filesystem unlink. A filesystem delete often removes a directory entry and leaves recovery to the filesystem or backups. Anvil records the delete as part of the object history so watches, indexes, repair, and audit-style consumers can see that the key changed. A watch consumer should handle a delete event by removing or tombstoning its derived record rather than assuming the object never existed.

The API also has a version-targeted delete path. Do not confuse that with the common current delete marker. A current delete changes what ordinary reads see. A version-targeted delete is a more specific history mutation and should be used carefully because it affects reproducibility and recovery expectations.

## Links and aliases

A link is an object-like record whose value points at another object key in the same bucket. It is useful when you need a stable name that can move:

```text
releases/desktop/latest.bin -> releases/desktop/3.0.1/acme-desktop-linux.bin
sites/www -> sites/www/index.html
```

A link is not a copy. Creating `releases/desktop/latest.bin` as a link does not duplicate the target payload. Updating the link moves the alias by writing new link metadata. Deleting the link writes a delete marker for the alias; it does not delete the target object.

Links have their own generation. The generation is a compare-and-swap token for the alias, not the target object's version id. If two release jobs both try to move `latest.bin`, one can be required to present the current link generation. If another job has already moved the link, the stale update fails and the losing job must reread the descriptor before deciding what to do.

A link can be live or pinned. A live link has no target version and resolves the current version of its target key at read time. A pinned link records `target_version` and resolves that exact historical version. The API supports the target-version field; the current public CLI can create and update live links but does not expose a `--target-version` flag.

Links also carry a resolution mode. `follow` means a normal object read may resolve the link and serve the target bytes. `redirect` is modelled in the API and accepted by the CLI, but current native object reads and the current S3/static gateway do not turn redirect links into HTTP `3xx` responses; they reject redirect links with a precondition-style error. Use follow links for current static hosting and downloads unless the delivery surface you use explicitly implements redirect semantics. For practical examples, see [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/) and [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/).

By default, link creation validates that the target exists, is not a delete marker, and is a blob rather than another invalid entry. The API and CLI can also allow a dangling link deliberately. A dangling link is useful for staged deployments, but it cannot be followed successfully until the target exists. Link resolution detects loops and has a depth limit, so links should be short aliases rather than an application routing language.

## Reserved namespaces

Some object-key prefixes are owned by Anvil itself. Public object APIs and gateways reject them so tenants cannot read or forge internal system state. The current reserved prefixes are:

```text
_anvil/meta/
_anvil/index/
_anvil/authz/
_anvil/watch/
_anvil/personaldb/
_anvil/git/
_anvil/tmp/
```

The bare prefix names without the trailing slash, such as `_anvil/authz`, are also treated as reserved. The reservation only applies at the start of the object key; a user key such as `tenant-notes/_anvil/authz/example.txt` is not the system namespace.

This rule protects both confidentiality and integrity. If a tenant could read `_anvil/authz/`, it might expose relationship tuples or policy records. If a tenant could write `_anvil/index/`, it might forge derived query state. Application data should use product-owned prefixes such as `projects/`, `users/`, `registry/`, `sites/`, or `audit/` and should never depend on Anvil's internal key layout.

## Placement and routing

Placement starts at the bucket. A bucket records its home region, and Anvil writes mesh routing records so requests can find the bucket. Object writes go through the bucket's placement and lifecycle checks. In the current model exposed to tenants, you do not choose a different region for each object key with a CLI flag.

Routing is still part of the object model because every read and write must resolve to the right tenant, bucket, and region before it can touch object metadata. If a request reaches the wrong region, the configured cross-region policy decides whether the system should redirect, proxy, reject local-only, or report proxy unavailable. Some proxy behaviour is still partial, and local setup can be blocked by region activation checkpoints. Operators should treat those as mesh lifecycle concerns, not as reasons to bypass the public API or write private records by hand. See [Gateways](/learn/gateways/), [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/), and [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/) for the wider topology model.

## How this differs from a filesystem

A filesystem gives you directories, directory entries, inode-like objects, rename operations, local permissions, and often in-place mutation. Anvil gives you tenant-scoped object keys, immutable committed versions, current pointers, public API authorisation, watches, indexes, and gateway views.

That difference changes how you model data. Do not rely on implicit parent directories existing before a key can be written. Do not assume renaming a prefix is a cheap metadata operation; in an object store model, a prefix move is usually an application-level copy/link/rewrite plan. Do not treat deletion as invisible cleanup; it is a committed change that watchers and derived systems should process.

The trade-off is that object names become stable application identifiers. A document system can keep `projects/p-123/documents/d-456/original.pdf` as the current source object, store extracted text beside it, create previews under a predictable prefix, and maintain search indexes and watches from the same committed records.

## How this differs from S3

S3 is an important compatibility surface, but it is not Anvil's core model. The S3 gateway maps S3-style operations onto Anvil tenants, buckets, object keys, bodies, metadata, and public-read behaviour. That is useful for existing tools, but S3 cannot express every Anvil concept.

The native API exposes version ids, mutation ids, watch cursors, relationship-aware authorisation, typed indexes, object links, append streams, mutation batches, PersonalDB records, and mesh lifecycle state more directly. If a workflow needs CAS preconditions, pinned links, catch-up evidence, relationship checks, or precise diagnostic records, design it around the native API and use S3 as an edge protocol where it fits. See [S3-Compatible Gateway](/tutorials/s3-gateway/) and [Gateways](/learn/gateways/) for current gateway behaviour and limitations.

## Modelling examples

For project documents, keep the product owner and document id early in the key:

```text
projects/{project_id}/documents/{document_id}/original.pdf
projects/{project_id}/documents/{document_id}/metadata.json
projects/{project_id}/documents/{document_id}/extracted-text.json
projects/{project_id}/documents/{document_id}/preview/page-{page}.png
```

This gives you useful prefixes for listing one project, watching one project, rebuilding one project's derived records, or granting scoped read access.

For release artefacts, keep immutable versions and mutable channels separate:

```text
releases/{product}/{version}/{platform}/{filename}
releases/{product}/channels/latest/{platform}/{filename-link}
```

The versioned key is durable evidence. The channel key is a link that can move with a generation check. Product UI should call this "move latest" or "promote release", not "copy release", because the link is not a payload copy.

For registry-like packages, store immutable content by checksum and publish small manifests or links for names that users type:

```text
registry/{repository}/blobs/sha256/{digest}
registry/{repository}/manifests/{package}/{version}.json
registry/{repository}/channels/{package}/latest.json
```

That shape makes immutability and mutability explicit. The package gateway foundations tutorial explains how to model this today without claiming that every registry protocol adapter is implemented: [Package Gateway Foundations](/tutorials/package-gateway-foundations/).

For audit or event history, consider whether ordinary objects or append streams are the better primitive. Prefix-shaped object keys can model simple immutable records:

```text
audit/2026/07/05/000000000001.json
```

Append streams add sequence ordering and stream-specific replay semantics. The choice depends on whether you need object-addressable records, append-only ordered replay, or both. See [Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/) and [Watches and Derived Data](/learn/watches-and-derived-data/).

## Current limitations to remember

The model is broader than the current helper commands. The public API exposes object versions, version-targeted reads, user metadata, link target versions, and structured write contexts; the current public CLI exposes only a subset. In particular, current CLI helpers do not list object versions, do not print version ids from `object head`, do not set content type or user metadata on `object put`, do not expose object write precondition flags, and do not create pinned links with `target_version`.

Gateway surfaces are also narrower than the native model. Public-read buckets and S3/static delivery are useful, but they do not make the admin API public, do not bypass reserved namespaces, and do not expose every native metadata or correctness field. Redirect links are modelled but not currently served as HTTP redirects by the native object reads or current S3/static gateway.

Placement is bucket-level in the tenant-facing model, and region lifecycle must be correct before bucket and object operations can succeed. Some local hands-on flows may still need clearer activation checkpoint tooling. Document that as an operational gap rather than hiding it with private admin mutations or invented commands.

## What to take forward

The object model is the source vocabulary for Anvil. A storage tenant owns buckets. A bucket owns keys. A key has a current pointer. A current pointer names a committed version or delete marker. A version has bytes, metadata, identity, and evidence. Links are aliases, not copies. Prefixes are naming structure, not directories. Placement belongs to buckets and mesh routing. Gateways adapt protocols onto this model; they do not replace it.

If you keep those distinctions clear, the rest of Anvil becomes easier to reason about. Reads, writes, indexes, watches, authorisation, public access, S3, static hosting, repair, and mesh routing all become different views over the same durable object records.

## Example object lifecycle

Consider `s3://documents/projects/42/report.pdf`. The tenant owns the `documents` bucket. The key is `projects/42/report.pdf`. A first upload creates a version and moves the current pointer to that version. A later replacement creates another version and moves the current pointer again if the write precondition passes. A stable link such as `projects/42/current-report.pdf` can point at the current key or a pinned version, depending on whether readers should follow future replacements.

That lifecycle gives operators several observable facts. A direct object read proves current pointer resolution and byte retrieval. A `HEAD` proves metadata visibility. A prefix listing proves the directory-derived view has caught up far enough. A link read proves the link descriptor and generation. An index query proves the derived index generation and query authorisation. When those disagree, source reads are the first truth to check; derived views should be repaired from source records rather than edited directly.
