---
title: CoreMeta and Blob Storage Layout
description: The RocksDB column families, inline payload policy, erasure-coded byte pipeline, and durable file rules used by Anvil's CoreStore implementation.
---

# CoreMeta and Blob Storage Layout

Anvil's storage layout is designed so an operator can answer a simple question during recovery: where is the authoritative state? The answer is CoreMeta plus the CoreStore byte pipeline. CoreMeta is the metadata plane backed by RocksDB. The byte pipeline is the blob plane backed by erasure-coded shards. Everything else is scratch, cache, export, or test infrastructure.

## What RocksDB stores

RocksDB stores CoreMeta rows. A CoreMeta key includes a table id, partition id, and tuple key. That lets one local metadata engine carry many logical tables without turning each feature into a separate database. The table id chooses the column family and the row schema, while the tuple key identifies the feature record inside that table.

The current column families are:

| Column family | Stored state |
| --- | --- |
| `cf_meta_version` | CoreMeta schema and storage metadata version records. |
| `cf_root_cache` | Root cache records and root visibility helpers. |
| `cf_transactions` | Explicit transactions, pending mutations, idempotency rows, admission certificates, and inline manifest body rows. |
| `cf_object_heads` | Current object heads, manifest CAS heads, multipart current rows, and object metadata partition manifests. |
| `cf_object_versions` | Object version metadata rows. |
| `cf_inline_payloads` | Tiny object or writer payload bodies that fit the inline policy. |
| `cf_stream_heads` | Append stream heads and stream metadata. |
| `cf_stream_records` | Stream record indexes and watch event indexes. |
| `cf_index_defs` | Index definitions and index definition state. |
| `cf_index_rows` | Index segment rows, segment locators, and derived index proof rows. |
| `cf_boundary` | Boundary schemas, boundary values, and boundary migration rows. |
| `cf_authz` | Relationship authorisation schema rows, tuple pages, and derived authz state. |
| `cf_personaldb` | PersonalDB group rows and data locator rows. |
| `cf_registry` | Registry, gateway, and git-source metadata rows. |
| `cf_mesh` | Mesh nodes, partitions, bucket locators, control records, root catalog rows, quorum profiles, bootstrap markers, repair findings, and node signing keys. |
| `cf_leases_fences` | Ownership fences, partition owners, task leases, task rows, and core fence records. |
| `cf_materialisation` | Materialisation cursors, writer segment rows, watch checkpoints, and landed byte reference rows. |
| `cf_refcounts` | Reference counts for shared payloads and manifests. |
| `cf_observability` | Observability cursors and diagnostic rows. |

RocksDB values are bounded. The encoded CoreMeta value ceiling is 64 KiB. Inline object payload eligibility defaults to 32 KiB raw input. Stream record index payloads use a smaller cap. RocksDB compression may reduce disk usage, but compression is not used to decide whether a large object is allowed into RocksDB. Eligibility is based on the raw accepted bytes and the encoded record limit.

## What the byte pipeline stores

The byte pipeline stores larger durable bytes. It is used for object bodies, multipart outputs, large stream payloads, large index segments, PersonalDB snapshots and pages, source packs, gateway blobs, registry blobs, and any future writer output that does not fit CoreMeta's inline policy.

A write through the byte pipeline follows this shape:

```text
accepted bytes
  -> staged local file
  -> content hash and logical descriptor
  -> compression policy
  -> encryption policy when configured
  -> block construction
  -> erasure coding
  -> shard placement
  -> shard fsync receipts
  -> manifest and locator rows in CoreMeta
```

The default byte profile uses four data shards and two parity shards. The read quorum is four shards. The write publish threshold is six shards for the default profile, meaning the current default expects all six shards before publication. A replicated profile also exists for low-latency shapes, but tenants should normally select named storage classes rather than direct low-level quorum parameters.

## Metadata replication is logical

Active RocksDB files are not themselves erasure-coded. Anvil does not replicate RocksDB WAL bytes, SST files, MANIFEST files, or compaction output as its consistency model. Instead, the owner sends deterministic CoreMeta row batches to the metadata replica set. Replicas persist those Anvil-level rows into their own RocksDB instances and return receipts. The owner builds commit certificates and publishes root generations only when the configured metadata quorum has persisted the required evidence.

This matters because RocksDB remains a local engine with local compaction and cache behaviour. The distributed protocol is Anvil's CoreMeta protocol, not a distributed RocksDB filesystem.

## Durable file classes

Anvil permits three broad classes of local files.

Class A files are final CoreStore state. RocksDB files belong here for metadata. Shard files belong here for blob storage. Root register files belong here for the root anchor mechanism. These files are part of the storage engine and must be backed up and restored according to the operator docs.

Class B files are bounded scratch or staging state. Landed byte files, upload temporary files, and build scratch files can exist while a request is being materialised. They must be referenced by CoreMeta pending state or be garbage-collectable. They must not become the only durable copy of committed data.

Class C files are outside the storage engine. Examples include bootstrap credential exports, test fixtures, local performance output, or operator reports. They may be useful, but they are not authoritative Anvil data.

## What this means for contributors

When adding a feature, decide which data is metadata and which data is payload. Metadata belongs in a CoreMeta table and therefore a RocksDB column family. Payload-like bytes become writer output and follow the inline-or-byte-pipeline rule. If the feature needs a specialised binary layout, define a writer segment format, publish a segment locator through CoreMeta, and make reads use the segment through the standard CoreStore readers.

Do not add a durable JSON directory, SQLite file, local index directory, or feature-specific WAL as a source of truth. If temporary files are needed for ingestion or repair, make them explicitly scratch and make recovery safe when they disappear.
