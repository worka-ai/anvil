---
title: Object Storage
description: Learn buckets, keys, objects, metadata, versions, checksums, ranges, and why object storage is the base model Anvil extends.
---

# Object Storage

**What this page gives you:** a first-principles understanding of object storage. You will learn what buckets, keys, object bodies, metadata, versions, checksums, and range reads are before seeing how Anvil builds richer behavior on top.

An object store is a system that stores bytes under names. The client says, "store this content at this key," and later says, "give me the content at this key." The content might be a photo, PDF, JSON envelope, database snapshot, log file, model weight file, audio clip, or source archive. Object storage is popular because it is simple, durable, and works well for large amounts of data.

The simplest mental model is a warehouse:

```text
bucket: invoices
key: customers/acme/2026/invoice-00042.pdf
body: bytes of the PDF
metadata: customer=acme, year=2026, status=paid
```

The bucket is the warehouse. The key is the shelf label. The body is the thing stored there. Metadata is the label information that lets people find and manage it.

## Bucket

A bucket is a named container for objects. It is not only a folder. A bucket is usually where you define durable policy: who may access a family of objects, where the data may be placed, what indexes exist, what retention rules apply, and which operational expectations matter.

Create buckets around stable boundaries. Good bucket boundaries are things like product areas, tenants, datasets, artifact stores, or application domains. Avoid creating a bucket for every tiny folder; the key inside the bucket is usually the better place to express hierarchy.

## Key

A key is the name of one object inside a bucket. Keys often look like paths:

```text
tenants/acme/projects/roadmap/documents/doc-42/original.pdf
```

Anvil treats key design as important because prefixes can be listed, watched, indexed, and authorized. A key should make the object's natural home visible near the front of the name. If every key is a random identifier, the storage layer cannot list a project, watch a tenant, or shard a timeline without looking elsewhere.

A key does not have to correspond to a local filesystem path. It is an application identifier. You choose it deliberately.

## Object body

The object body is the bytes. Anvil does not require the body to be a file format. It can be:

- a binary file such as an image, archive, or model checkpoint;
- a JSON envelope describing a timeline event;
- an extracted text document;
- an audio transcript;
- a SQLite snapshot;
- a source artifact pack;
- a compressed log bundle.

The body is immutable for a particular object version. Updating an object creates a new state that can be validated, indexed, watched, and recovered.

## Metadata

Metadata is structured information about the object. It is small compared with the body and meant to answer questions without reading the full body.

For a PDF invoice, metadata might include:

```json
{
  "customer": "acme",
  "invoice_id": "inv-00042",
  "status": "paid",
  "issued_at": "2026-06-29T10:00:00Z",
  "currency": "GBP",
  "total": "4200.00"
}
```

With metadata, the application can ask for paid invoices for Acme without downloading every PDF. Anvil can also use metadata to scope full text search, vector search, watches, retention, and authorization decisions.

## Version

A version is a specific state of an object. Versions matter because distributed software has races. Two users may edit metadata at the same time. A background job may replace extracted text while an operator is restoring a bucket. A client may retry a timed-out upload.

Anvil uses versions and preconditions to avoid silent lost updates:

```text
read head -> version v7
write new metadata only if current version is still v7
```

If the current version is now `v8`, the write is rejected. The application can reload and decide how to merge. This is safer than overwriting someone else's update.

## Checksum

A checksum is a digest of bytes. It lets the system prove that the bytes stored are the bytes read later. Checksums are useful for uploads, replication, backup verification, range reads, and recovery.

When an object is large, Anvil can validate body chunks, multipart uploads, and final object hashes. Operators should treat checksum mismatches as serious durability findings, not harmless warnings.

## Range read

A range read asks for only part of an object:

```text
GET bytes 1000000..1999999 from video.mp4
```

Range reads matter for media playback, resumable downloads, partial archive inspection, and large model artifacts. They also need authorization and version checks. A caller allowed to read one object version may not be allowed to read every related object.

## What Anvil adds to ordinary object storage

Object storage gives Anvil the durable source of record. Anvil then connects that source to:

- path and directory indexes for fast listing;
- metadata indexes for structured filters;
- full text indexes for words and snippets;
- vector indexes for semantic similarity;
- relationship authorization for fine-grained visibility;
- watch streams so derived systems can catch up;
- PersonalDB witness state for local-first databases;
- source and model artifact manifests for reproducible data workflows.

Those features are valuable because they share one identity model and one mutation path. A write is not merely stored; it becomes a source event for the derived systems that make product features work.

## What you can do after this page

You should be able to explain buckets, keys, object bodies, metadata, versions, checksums, and range reads. Next, learn how to design keys and metadata so applications can list, filter, authorize, and watch data efficiently.
