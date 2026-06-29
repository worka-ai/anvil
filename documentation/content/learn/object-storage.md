---
title: Object Storage
description: Learn object storage from first principles and how Anvil stores objects.
---

# Object Storage

**Goal:** understand buckets, keys, objects, versions, metadata, checksums, and why Anvil is an object store rather than a filesystem or SQL database.

An object store stores named blobs of bytes. The bytes might be a photo, a PDF, a JSON document, a compressed event log, a model weight file, or a source package. Each object lives inside a bucket and is addressed by a key.

```text
bucket: customer-assets
key: tenants/acme/invoices/2026/06/invoice-1007.pdf
bytes: the PDF content
metadata: content-type=application/pdf, customer=acme, period=2026-06
```

A filesystem also stores bytes, but a filesystem is optimized for local hierarchical directories, small synchronous metadata changes, and POSIX semantics. A SQL database stores rows and joins them through schemas. An object store is optimized for durable blobs, large fan-out, stable HTTP-style access, and application-defined metadata. Anvil builds on the object-store model and then adds native indexing, authorization, and watches.

## Buckets

A bucket is a named boundary. It has its own policy, object namespace, index definitions, authorization settings, encryption settings, and operational lifecycle. Use separate buckets when the data has different policy or operational behavior.

Good bucket boundaries are usually product-level or data-domain-level:

- `documents` for user-uploaded documents;
- `media` for images, audio, and video;
- `events` for append-only product timelines;
- `source-artifacts` for git packs and build inputs;
- `personaldb` for database group snapshots and derived material.

Do not create a bucket for every folder. Object keys already provide path structure inside a bucket.

## Keys

A key is a UTF-8 path-like string inside a bucket. Anvil normalizes keys so equivalent spelling does not create ambiguous objects. A predictable key lets both humans and machines infer where data belongs.

```text
tenants/acme/projects/p-123/timeline/000000042.json
tenants/acme/projects/p-123/assets/logo.png
tenants/acme/projects/p-123/source/main.pack
```

The slash is a naming convention, not a real directory. Anvil maintains a directory index so prefix listing behaves like directory traversal without requiring object payload scans.

## Object versions

When an object is written, Anvil records a new version. A version record contains the object identity, key, version id, mutation id, content hash, size, metadata, storage references, authorization revision, and timestamps. Deletes are recorded as delete markers so version history remains understandable.

Versions matter because distributed applications need a stable answer to the question: which exact bytes did this result come from? If a search result says it matched `contract.pdf` at version `v7`, a later overwrite to `v8` does not invalidate the fact that the query returned `v7`.

## Metadata

Metadata is structured information stored with an object. Some metadata is system-owned, such as content length and content hash. Some is user-defined, such as `customer=acme` or `document_type=invoice`.

Metadata exists so callers can answer questions without downloading every object:

- list invoices for June;
- filter media by language;
- find all artifacts produced by build 123;
- search only objects tagged as public knowledge-base content.

Anvil indexes metadata through bucket-scoped index definitions. That makes metadata a query surface rather than a passive label.

## Checksums and content addressing

Anvil computes BLAKE3 hashes for content and control records. A hash is a compact fingerprint of bytes. If the bytes change, the hash changes. Anvil uses hashes to verify integrity, address immutable content, validate manifests, and connect derived indexes back to source data.

This matters operationally. Repair can prove whether a segment is intact. A client can verify downloaded bytes. A derived index can prove which source manifest and cursor it was built from.

## Inline and external payloads

Small payloads can be stored inline with metadata journal records. Large payloads are stored as external content chunks referenced from metadata. This keeps small control files fast while preventing directory and metadata paths from dragging large object bytes through hot indexes.

As a user, you do not choose the physical placement for each object. You choose the bucket, key, metadata, and policy. Anvil chooses the durable internal representation.

## What you can do now

You should now be able to explain:

- why object storage is different from local files and relational rows;
- how a bucket and key identify data;
- why versions and hashes matter;
- why metadata becomes valuable only when it can be indexed.

Next, learn how key design and path indexing turn object names into fast application navigation.
