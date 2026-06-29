---
title: Keys, Paths, And Metadata
description: Design object keys and metadata that are easy to query, authorize, watch, and operate.
---

# Keys, Paths, And Metadata

**What this page achieves:** you will learn how to design object names and metadata so Anvil can list, filter, secure, and watch data efficiently.

A key is not just a storage detail. It is the first index your application creates. If keys encode ownership and time clearly, your application can list and watch focused prefixes. If keys hide those facts in random names, every downstream system has to inspect metadata or object bodies to understand what the object means.

## Prefixes are query boundaries

A prefix is the beginning of a key. In this key:

```text
tenants/acme/projects/p-123/timeline/0000000000000042.json
```

useful prefixes include:

```text
tenants/acme/
tenants/acme/projects/
tenants/acme/projects/p-123/
tenants/acme/projects/p-123/timeline/
```

A user interface can list the timeline prefix. A background processor can watch it. An authorization rule can grant access to it. An operator can inspect it. That is why prefix design matters.

## A good key tells the truth early

Put stable, high-value boundaries early in the key:

```text
tenants/{tenant_id}/workspaces/{workspace_id}/runs/{run_id}/frames/{frame_id}.json
tenants/{tenant_id}/workspaces/{workspace_id}/assets/{sha256}/preview.png
tenants/{tenant_id}/workspaces/{workspace_id}/source/{revision}/repo.pack
```

These keys answer basic questions before the object is opened:

- which tenant owns it;
- which workspace or project it belongs to;
- whether it is a timeline frame, asset, or source artifact;
- whether lexical ordering matches business ordering.

Compare that with weak keys:

```text
frames/{frame_id}-{tenant_id}-{workspace_id}.json
assets/random/{uuid}.png
uploads/latest.json
```

Those names hide ownership, make listing broad, make watches noisy, and increase the chance that authorization must depend on slower metadata inspection.

## Metadata complements paths

Paths answer hierarchical questions. Metadata answers property questions. Use both.

A contract object might use this key:

```text
tenants/acme/projects/p-123/contracts/2026/contract-42.pdf
```

and this metadata:

```json
{
  "document_type": "contract",
  "customer": "Acme Ltd",
  "status": "signed",
  "language": "en-GB",
  "effective_date": "2026-06-01",
  "renewal_owner": "legal"
}
```

The key is good for listing contracts under one project. Metadata is good for finding signed contracts across many projects or filtering by renewal owner.

## Index definitions make metadata useful

Metadata by itself is just labels. An index definition tells Anvil which labels are queryable and how they should be maintained. A metadata index can answer:

- `status = signed`;
- `customer = Acme Ltd AND document_type = contract`;
- `effective_date >= 2026-01-01` within a prefix;
- `language = en-GB` before full text search.

Index definitions matter because production queries cannot scan every object. The index is maintained from object writes and watch streams, so query time can use a prepared structure instead of a bucket-wide scan.

## Reserved internal paths

Anvil owns paths under `_anvil/`. These are not customer folders and not public object names. They store internal metadata, index segments, authorization state, watches, PersonalDB material, and control records.

Public object APIs reject reads and writes to reserved paths before normal authorization. This is deliberate. If a caller could probe `_anvil/authz/` or `_anvil/indexes/`, the caller could learn sensitive implementation and relationship facts. Structured APIs expose safe views after authorization checks.

The rule is simple: **application data never uses `_anvil/`, and client code never bypasses structured APIs to inspect it.**

## Timeline pattern

Append-only timelines are common. Use fixed-width sortable ids:

```text
tenants/acme/workspaces/ws-1/runs/run-1/frames/0000000000000001.json
tenants/acme/workspaces/ws-1/runs/run-1/frames/0000000000000002.json
tenants/acme/workspaces/ws-1/runs/run-1/frames/0000000000000003.json
```

Fixed width keeps lexical order equal to chronological order. That means listing the prefix returns frames in the intended sequence without extra sorting logic.

## Checklist for key design

Before choosing a key shape, answer these questions:

1. What is the earliest ownership boundary?
2. What prefix will the UI list most often?
3. What prefix will background jobs watch?
4. Which part of the key should be lexically sortable?
5. Which facts should be metadata rather than path segments?
6. Which authorization relationships need to line up with prefixes?

## What you can do after this page

You should be able to design keys that make listing, filtering, watching, and authorization natural. Next, learn how Anvil turns paths, metadata, text, and embeddings into indexes and search.
