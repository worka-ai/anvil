---
title: Native API
description: Build applications against Anvil's native gRPC API with the right mental model.
---

# Native API

**What this page achieves:** you will understand when to use Anvil's native API, how requests flow through identity, authorization, storage, indexes, watches, and PersonalDB, and how to structure client code so it remains correct in production.

The native API is Anvil's full capability surface. S3 compatibility exists so existing object tools can put, get, list, and delete objects. The native API exists for applications that need Anvil-specific behavior: index definitions, metadata queries, authorization tuples, watch streams, vector search, source artifacts, and PersonalDB witnessing.

Use S3 when you need object compatibility. Use the native API when your application needs Anvil's integrated model.

## The request model

A native client does four things:

1. authenticates as an application or user-facing service;
2. receives a token with scopes and tenant context;
3. calls bucket, object, index, authz, watch, or PersonalDB services;
4. handles versioned results, request ids, and consistency responses deliberately.

A write request is not just bytes over the network. It is a mutation that can affect object state, directory indexes, metadata indexes, full text inputs, vector inputs, watch streams, and authorization-aware query visibility.

## First connection

A client profile needs an endpoint and credentials. In code, keep those concerns separate from business logic:

```text
configuration -> credential provider -> Anvil client -> repository/service layer -> application feature
```

That layering prevents feature code from scattering tokens, host strings, bucket names, and retry behavior everywhere.

A minimal flow is:

```text
load client profile
  -> exchange client credentials for a token
  -> create a bucket if needed
  -> put an object with metadata and preconditions
  -> read it back by version or latest head
```

## Buckets and objects

Create buckets around durable policy boundaries. Then write objects with stable keys and metadata. A native object write should normally include:

- bucket;
- key;
- body stream;
- content type;
- metadata;
- idempotency key for safe retries;
- precondition when overwriting known state;
- authorization context inherited from the caller token.

Idempotency matters because distributed clients retry. If a client times out after sending a write, it should be able to retry with the same idempotency key and avoid creating duplicate logical mutations.

## Metadata and index definitions

Applications should not rely on object scans for product queries. Define indexes for the questions your product asks.

For example, a document product might define:

```json
{
  "name": "documents_by_status_and_customer",
  "bucket": "documents",
  "prefix": "tenants/{tenant_id}/documents/",
  "fields": ["status", "customer", "document_type", "effective_date"]
}
```

Then feature code asks the index for signed contracts for one customer instead of listing every object and filtering in memory.

## Watches

Use watches when another part of the application needs to react to writes. A watcher receives ordered events and stores its cursor. If the process restarts, it resumes from that cursor.

Common watch consumers include:

- UI timeline broadcasters;
- metadata denormalizers;
- text extraction workers;
- vector embedding workers;
- PersonalDB projection builders;
- audit export jobs.

A watch consumer should be idempotent. It may see retries or resume after interrupted work. Store the processed cursor only after the derived work is durable.

## Authorization-aware application design

Do not treat authorization as a final UI filter. Build application calls so authorization is part of every storage and search request.

Good pattern:

```text
call Anvil search with caller identity and required consistency
  -> receive only authorized results
  -> render result list
```

Weak pattern:

```text
call broad search as admin
  -> filter results in application memory
  -> hope snippets, counts, and timing did not leak data
```

The weak pattern is unsafe. It can leak object existence through counts, snippets, facets, or side effects.

## Error handling

Handle errors by category:

| Category | Meaning | Application response |
| --- | --- | --- |
| Authentication | Caller has no valid identity. | Refresh credentials or ask user to sign in. |
| Authorization | Caller is known but not permitted. | Show access denied or hide the action. |
| Preconditions | Expected version or idempotency state did not match. | Reload, merge, or retry deliberately. |
| Index readiness | Requested consistency is not yet available. | Wait, show loading, or choose weaker consistency if product semantics allow it. |
| Validation | Request shape or metadata is invalid. | Fix caller code; do not retry blindly. |

## What you can build after this page

You should be able to design a native client layer that uses tokens, buckets, object writes, metadata, watches, and authorization correctly. Next, read the S3 compatibility guide if you need existing S3 tools, then the object metadata and search guides for product-specific queries.
