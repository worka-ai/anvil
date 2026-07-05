---
title: Objects And Metadata
description: Model application data as object bodies, keys, versions, metadata, and indexes.
---

# Objects And Metadata

**What this page gives you:** a method for turning application concepts into Anvil objects. You will learn how to decide what becomes a key, what becomes metadata, what becomes an object body, and what becomes an index.

Object modelling starts from product behaviour. Do not begin with the file picker or database table. Begin with the screens, jobs, permissions, and searches your application must support.

## Start with product questions

List the questions your product must answer:

- Which objects appear in this screen?
- Which filters can the user apply?
- Which objects are watched for live updates?
- Which fields are shown in cards or tables?
- Which objects share an authorisation boundary?
- Which background jobs process this data?
- Which objects are retained, archived, or deleted together?

The answers decide key shape, metadata fields, and index definitions.

## Choose the object body

The body is the durable payload. It can be a file, a JSON envelope, a compressed archive, a snapshot, or a derived artefact.

Good object body examples:

- original uploaded PDF;
- extracted text from a PDF;
- image thumbnail;
- model checkpoint;
- source archive;
- audit event JSON;
- PersonalDB snapshot;
- generated report bundle.

Use separate objects when lifecycle, permissions, or indexing differ. For example, an original video, extracted transcript, thumbnail, and embedding manifest may be separate objects because they are produced by different jobs and queried differently.

## Choose the key

The key should express natural scope:

```text
tenants/acme/projects/p-123/documents/doc-42/original.pdf
```

This key tells Anvil and operators where the object belongs. A document list can query the project prefix. A watch can subscribe to the prefix. Authorisation can refer to the project. Backup and diagnostics can navigate the same structure.

## Choose metadata

Metadata should contain fields needed for listing, filtering, sorting, ranking, display, retention, and authorisation context. It should not duplicate large object body content.

Example:

```json
{
  "kind": "document",
  "document_id": "doc-42",
  "title": "Master Services Agreement",
  "customer": "Acme Ltd",
  "status": "signed",
  "language": "en-GB",
  "created_by": "user-17",
  "created_at": "2026-06-29T09:30:00Z"
}
```

This metadata can power a list card without reading the PDF. It can also constrain search: text search within English signed contracts for one customer.

## Define field ownership

Metadata becomes fragile when many systems write the same fields. Document ownership:

| Field family | Typical owner |
| --- | --- |
| `title`, `status`, `tags` | Application UI or domain service |
| `content_type`, `source_id` | Ingestion service |
| `extraction_status`, `language` | Text/media extraction worker |
| `embedding_model`, `vector_status` | Embedding worker |
| `retention_class`, `delete_after` | Retention service |
| `audit_actor`, `request_id` | Audit/control layer |

Use preconditions when updating metadata from stale reads.

## Define indexes

Create indexes around stable access patterns:

| Index | Supports |
| --- | --- |
| `documents_by_project` | Project folder listing. |
| `documents_by_status` | Dashboard filters and counts. |
| `documents_by_customer` | Account views. |
| `documents_full_text` | User text search. |
| `documents_embeddings` | Semantic search. |
| `documents_source_artefacts` | Build or ingestion tracing. |

Avoid one enormous "everything" index. Smaller, purpose-built indexes are easier to reason about, rebuild, authorise, and monitor.

## Use object envelopes for records

An object body can be structured data:

```json
{
  "kind": "timeline.frame",
  "id": "0000000000000042",
  "actor": "user-17",
  "verb": "uploaded_document",
  "object": "doc-42",
  "occurred_at": "2026-06-29T09:30:00Z"
}
```

This is valid object storage. It becomes especially powerful when keys, metadata, watches, and indexes all align around the timeline.

## What you can build after this page

You should be able to design object bodies, keys, metadata, versions, and indexes from application requirements. Next, learn how to turn those indexes into product search.
