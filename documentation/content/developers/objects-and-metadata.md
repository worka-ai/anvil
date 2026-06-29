---
title: Objects And Metadata
description: Build reliable object workflows with predictable keys, metadata, idempotency, and preconditions.
---

# Objects And Metadata

**Goal:** design a production object workflow that is retry-safe, listable, searchable, and clear to operate.

A production object workflow has more than an upload call. It needs a key naming scheme, metadata rules, retry behavior, update preconditions, and a plan for how objects appear in user interfaces.

## Choose the key format first

Write the key format down before writing code. A common pattern is:

```text
tenants/{tenant_id}/{domain}/{entity_id}/{kind}/{sequence_or_name}
```

Examples:

```text
tenants/acme/projects/p-123/assets/logo.png
tenants/acme/projects/p-123/timeline/0000000000000042.json
tenants/acme/projects/p-123/source/rev-8f31/repo.pack
```

This gives you clean prefix listing and watch scoping.

## Attach metadata deliberately

Metadata should contain fields that users filter by, operators inspect, or indexes need.

```json
{
  "tenant": "acme",
  "project": "p-123",
  "kind": "timeline-frame",
  "schema": "forge-frame-v1",
  "created_by": "agent:codex",
  "content_language": "en-GB"
}
```

Do not put large documents into metadata. Store the document as the object body. Metadata is for compact queryable descriptors.

## Use preconditions for updates

If two clients can update the same key, use preconditions. Read the object's current ETag or version id, then write only if it still matches. If the precondition fails, reload and merge deliberately.

This protects users from accidental overwrites caused by retries, stale forms, or concurrent workers.

## Use idempotency keys

For native API mutations, provide an idempotency key for one logical operation. If the request times out and the client retries with the same key, Anvil can return the original result rather than applying the mutation twice.

Good idempotency keys are stable for the operation and unique across different operations:

```text
upload:tenant-acme:project-p-123:asset-logo:revision-8f31
```

## Watch the prefix

If a UI needs to update when objects change, do not poll broad listings. Subscribe to a watch stream for the relevant prefix and checkpoint the cursor. The watch event tells your UI or service which object changed and where to resume after reconnecting.

## What good looks like

A well-designed workflow has:

- predictable keys;
- compact metadata;
- explicit index definitions for queryable fields;
- update preconditions for shared keys;
- idempotency keys for retries;
- prefix watches for live updates;
- authorization checks before data is shown.
