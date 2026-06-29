---
title: Objects And Metadata
description: Model application data as objects, metadata, versions, and indexes.
---

# Objects And Metadata

**What this page achieves:** you will learn how to model application data in Anvil so product screens, background jobs, and search queries can find the right objects without scanning buckets.

A common mistake is treating object storage as a dumping ground for files. That works for prototypes and fails as soon as a product needs timelines, filters, access rules, retention, audit, or search. In Anvil, object keys and metadata are part of the application model.

## Start from the product questions

Before choosing keys or metadata, list the questions your application must answer:

- Which objects appear on this screen?
- Which filters can the user apply?
- Which objects belong to one tenant, project, or folder?
- Which objects should a background job process?
- Which object changes should trigger notifications?
- Which fields are needed for authorization or retention?

The answers drive key shape and index definitions.

## Example: document object model

For a document system, a key might be:

```text
tenants/acme/projects/p-123/documents/doc-42/original.pdf
```

Metadata might be:

```json
{
  "document_id": "doc-42",
  "document_type": "contract",
  "title": "Master Services Agreement",
  "customer": "Acme Ltd",
  "status": "signed",
  "language": "en-GB",
  "created_by": "user-17",
  "created_at": "2026-06-29T09:30:00Z"
}
```

That metadata supports list cards, filters, search snippets, and audit trails without reading the PDF body.

## Versions and optimistic updates

When a user edits metadata or replaces content, use preconditions. A precondition says "apply this change only if the object is still at the version I read".

This prevents lost updates:

```text
read object head -> version v7
user edits title
write metadata with precondition version == v7
```

If another user wrote version `v8` first, Anvil rejects the stale write. The application can reload, merge, or ask the user what to do.

## Metadata update patterns

Treat metadata updates as deliberate mutations. Avoid patterns where multiple services write unrelated metadata fields without coordination. If independent services own different fields, document that ownership and use preconditions or idempotency keys.

Good field ownership examples:

- ingestion service owns `source_id`, `content_type`, and `import_batch`;
- extraction worker owns `text_extraction_status` and `language`;
- application UI owns `title`, `status`, and `tags`;
- retention service owns `retention_class` and `delete_after`.

## Index definitions

A bucket can define multiple indexes for different query families:

| Index | Supports |
| --- | --- |
| `documents_by_project` | project folder listings |
| `documents_by_status` | dashboard counts and filters |
| `documents_by_customer` | account-level views |
| `documents_full_text` | user text search |
| `documents_embeddings` | semantic search |

Do not create one enormous index for every possible query. Define indexes around stable product access patterns. Use metadata and path filters to constrain expensive search operations.

## Object envelopes for structured records

Not every object is a file. Many applications store JSON envelopes:

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

This is valid object storage. The object body is structured data, and metadata/indexes decide how it is discovered.

## What you can build after this page

You should be able to model product data as keys, bodies, metadata, versions, and indexes. Next, learn how to query that data with metadata, full text, vector, and hybrid search.
