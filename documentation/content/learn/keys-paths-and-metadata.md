---
title: Keys And Paths
description: Design object keys and metadata that make listing, indexing, authorisation, watches, and operations natural.
---

# Keys And Paths

**What this page gives you:** a practical model for designing object names and metadata. You will learn why key shape affects performance, security, search, watches, and operations.

A key is not just a string. It is the first index your application creates. Long before you define a full text index or a vector index, your key decides whether the system can efficiently list a tenant, watch a project, group timeline frames, or route authorisation decisions.

A good key answers a simple question: **where does this object belong?**

## Design from access patterns

Start by writing down the product questions:

- Which objects appear on this screen?
- Which objects are loaded together?
- Which objects are watched for live updates?
- Which objects share the same authorisation boundary?
- Which objects are archived, retained, or deleted together?
- Which objects need ordering by time or sequence?

Then design keys so those groups are visible in prefixes.

For a project timeline:

```text
tenants/acme/projects/p-123/timeline/0000000000000001.json
tenants/acme/projects/p-123/timeline/0000000000000002.json
tenants/acme/projects/p-123/timeline/0000000000000003.json
```

The prefix `tenants/acme/projects/p-123/timeline/` now means something. A UI can list it. A watch can subscribe to it. An authorisation rule can grant access to it. An operator can inspect it.

## Put broad ownership first

Put stable ownership and scope near the front of the key:

```text
tenants/{tenant}/projects/{project}/documents/{document}/original.pdf
```

This is better than:

```text
documents/{document}/tenants/{tenant}/projects/{project}/original.pdf
```

The first form lets Anvil narrow quickly to one tenant or project. The second forces broader scanning before the important scope appears.

## Use time and sequence deliberately

If humans need chronological listings, include an ordered timestamp or sequence:

```text
tenants/acme/audit/2026/06/29/14/000000018439.json
```

Use fixed-width counters or sortable timestamps when lexical order should match time order. Avoid names like `latest.json` as the only source of history. Use `latest` as a pointer if needed, but store immutable events with durable ordered keys.

## Avoid meaningless keys

These keys are technically valid but operationally weak:

```text
9c5ee86c-1db4-4f0d-8a36-bd6a7e9c5e22
files/blob-184928492.bin
uploads/tmp/final-final-v3.pdf
```

They hide ownership, purpose, and access pattern. That makes listing broad, watches noisy, authorisation harder to reason about, and operator diagnosis slower.

Random identifiers are fine as one segment of a key. They should not be the whole model.

## Metadata fills in query fields

Keys express hierarchy and coarse scope. Metadata expresses fields that need filtering, sorting, ranking, or display.

Example object metadata:

```json
{
  "kind": "document",
  "document_id": "doc-42",
  "title": "Master Services Agreement",
  "status": "signed",
  "customer": "Acme Ltd",
  "language": "en-GB",
  "created_by": "user-17",
  "created_at": "2026-06-29T09:30:00Z"
}
```

The key says the document belongs to a tenant and project. Metadata says what the document is and how product screens should filter or display it.

## Index definitions turn metadata into queries

Metadata alone is not enough. If a query must be fast, Anvil needs an index definition that says which fields are maintained for that bucket or prefix.

Example:

```json
{
  "name": "project_documents_by_status",
  "bucket": "documents",
  "prefix": "tenants/{tenant}/projects/{project}/documents/",
  "fields": ["status", "customer", "document_type", "created_at"]
}
```

Now the application can ask for signed contracts in one project without listing every object or reading every body.

## Reserved Anvil paths

Anvil owns internal paths under `_anvil/`. They are not user folders and not public object names. They contain internal metadata, index material, authorisation state, watch checkpoints, PersonalDB state, and control records.

Public APIs must not read, list, write, copy, compose, delete, or range-read these paths. If an application sees an `UnauthorizedReservedNamespace` error, the correct response is to stop and fix the caller. Do not retry through another API.

## Key design checklist

Before shipping a bucket, answer these questions:

1. What prefix represents one tenant, account, workspace, or security scope?
2. What prefix represents one product screen or timeline?
3. What prefix will background jobs watch?
4. Which metadata fields appear in filters, sort controls, snippets, or cards?
5. Which fields are controlled by users and which by background systems?
6. Which operations need optimistic preconditions?
7. Which keys or metadata fields are sensitive and must never appear in unauthorised listings or snippets?

## What you can do after this page

You should be able to design keys and metadata that make listing, watching, searching, authorisation, and operations natural. Next, learn how Anvil turns keys, metadata, text, and vectors into indexes.
