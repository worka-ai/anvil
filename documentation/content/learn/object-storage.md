---
title: Object Storage
description: Learn buckets, keys, objects, versions, metadata, and checksums from first principles.
---

# Object Storage

**What this page achieves:** you will understand what an object store is, how Anvil represents stored data, and why object storage is the base layer for the rest of Anvil.

An object store stores bytes under names. The bytes can be a PDF, image, JSON document, event frame, source package, media file, model artifact, or database snapshot. The name is split into two parts: a bucket and a key.

```text
bucket: documents
key: tenants/acme/projects/p-123/contracts/contract-42.pdf
bytes: PDF content
metadata: content-type=application/pdf, status=signed, customer=acme
```

That is the simplest form of the model: put bytes at a name, then get them back later. Anvil keeps that model but adds production-grade context around it: versions, checksums, metadata indexes, authorization, watches, and derived search inputs.

## Why not just use files?

A local filesystem is designed for one machine and POSIX-style operations: open a file handle, read and write ranges, rename directories, apply local permissions, and rely on local metadata. Distributed applications need different properties. They need stable object names, explicit versions, content hashes, large fan-out, HTTP/gRPC access, range reads, retries, and metadata that can be indexed without opening every file.

An object store does less than a filesystem in some areas and more in others. It does not pretend that `/a/b/c` is a real directory tree. It treats the slash as part of a key. That makes it easier to distribute, replicate, index, and authorize at scale.

## Buckets

A bucket is a named boundary around objects. It is not just a folder. In Anvil, a bucket can carry policy, index definitions, authorization settings, retention behavior, and operational lifecycle.

Use separate buckets when data has materially different policy or operational needs:

- `documents` for user-uploaded documents;
- `media` for images, audio, and video;
- `events` for append-only activity frames;
- `source-artifacts` for source packs, build outputs, and logs;
- `personaldb` for database group material and projections.

Do not create a bucket for every small folder. Keys already provide structure inside a bucket. A bucket should be a meaningful administrative and security boundary.

## Keys

A key is the object's name inside a bucket. Keys are usually path-like because humans and tools understand that shape:

```text
tenants/acme/projects/p-123/timeline/0000000000000042.json
tenants/acme/projects/p-123/assets/logo.png
tenants/acme/projects/p-123/source/main.pack
```

The slash does not create a real directory. It creates a prefix. Anvil maintains directory and prefix indexes so listing `tenants/acme/projects/p-123/` is fast without scanning every object in the bucket.

Good keys put ownership, security scope, and time/order near the front of the name. That lets applications list and watch focused areas without reading object bodies.

## Objects and versions

An object is not only the latest bytes. Every successful write creates a version record. A version records facts such as:

- bucket and key;
- version id and mutation id;
- content hash and size;
- user metadata;
- storage references;
- authorization revision;
- creation timestamp;
- delete marker if the write was a delete.

Versions let Anvil answer exact questions. If a search result matched `contract-42.pdf` at version `v7`, a later write to `v8` does not rewrite history. The result can still point to the exact version that was indexed.

## Metadata

Metadata is structured information attached to an object. Some metadata is system-owned: content length, content type, hash, version, and timestamps. Some metadata is application-owned: customer id, language, status, retention class, source revision, or media duration.

Metadata matters because it lets you ask questions without downloading every object:

- which invoices are from June;
- which contracts are signed;
- which images belong to this project;
- which artifacts were produced by this build;
- which documents use English text extraction.

Anvil treats metadata as a query surface. Bucket index definitions decide which fields become indexed and how queries may combine those fields with prefix, full text, vector, and authorization filters.

## Checksums and content addressing

Anvil computes hashes for object bytes and control records. A hash is a fingerprint: the same bytes produce the same hash, and different bytes produce a different hash with extremely high probability. Anvil uses hashes to verify downloads, validate manifests, identify immutable content, repair damaged state, and connect derived indexes to source data.

The practical benefit is confidence. When an index segment says it was built from a manifest, Anvil can verify that manifest and the object versions behind it. When a client downloads bytes, it can prove the bytes are the expected bytes.

## Inline and external storage

Small control payloads are cheap to store close to metadata. Large object bodies are stored as content chunks referenced by metadata. Anvil chooses the physical representation. The user-facing contract stays the same: write an object with a key and metadata, then read or query it by the supported APIs.

This split keeps hot metadata and directory operations fast without dragging large payloads into every index path.

## What you can do after this page

You should be able to describe a bucket, key, object, version, metadata record, and checksum. You should also understand why object storage is the base model Anvil extends with indexes, authorization, and watches.

Next, learn how to design keys and metadata so applications remain fast and understandable as data grows.
