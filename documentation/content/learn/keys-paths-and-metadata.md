---
title: Keys, Paths, And Metadata
description: Learn how predictable object paths and metadata create fast application queries.
---

# Keys, Paths, And Metadata

**Goal:** design object keys and metadata that remain understandable, fast to list, and easy to secure as a system grows.

Anvil lets you store any object under any valid key, but key design is an application architecture decision. A key is the first index most people create, whether they realize it or not. If keys are predictable, operators can inspect data, applications can list efficiently, and derived systems can watch focused prefixes.

## Prefixes are query boundaries

A prefix is the beginning of a key. In this key:

```text
tenants/acme/projects/p-123/timeline/000000042.json
```

useful prefixes include:

```text
tenants/acme/
tenants/acme/projects/
tenants/acme/projects/p-123/
tenants/acme/projects/p-123/timeline/
```

When a client lists `tenants/acme/projects/p-123/timeline/`, Anvil answers from its directory index. It does not scan every payload in the bucket. That distinction is the difference between a UI that opens instantly and a UI that degrades as data grows.

## Good key design

A good key places high-cardinality or security-critical boundaries early enough that list, watch, and authorization scopes stay focused.

Good:

```text
tenants/{tenant_id}/workspaces/{workspace_id}/runs/{run_id}/frames/{frame_id}.json
tenants/{tenant_id}/workspaces/{workspace_id}/assets/{sha256}/preview.png
tenants/{tenant_id}/workspaces/{workspace_id}/source/{revision}/repo.pack
```

Weak:

```text
frames/{frame_id}-{tenant_id}-{workspace_id}.json
assets/random/{uuid}.png
uploads/latest.json
```

The weak examples hide the domain boundary. They force downstream code to inspect metadata or object bodies before it knows what the object belongs to.

## Metadata complements paths

Path structure answers hierarchical questions. Metadata answers property questions. Use both.

A document key might be:

```text
tenants/acme/documents/2026/06/contract-42.pdf
```

Metadata might include:

```json
{
  "document_type": "contract",
  "customer": "Acme Ltd",
  "language": "en-GB",
  "status": "signed",
  "retention_class": "legal"
}
```

The path is good for listing the June documents. Metadata is good for finding signed contracts across months.

## Index definitions

An index definition tells Anvil which object fields should become queryable. An index can cover path prefixes, metadata fields, extracted text, embedding vectors, or combinations of those inputs.

A metadata index definition answers questions such as:

- find objects where `status = signed`;
- find objects where `customer = Acme Ltd` and `document_type = contract`;
- list objects modified after a timestamp within a prefix.

The important idea is that an index is maintained from writes and watches. Querying an index is not the same as scanning the bucket.

## Reserved internal paths

Anvil owns key prefixes under `_anvil/`. They store internal metadata, index files, authorization records, PersonalDB logs, and watch data. Public object APIs reject reads and writes to these prefixes. They are not hidden customer folders; they are a hard security boundary.

If a caller tries to list, read, write, copy, or delete a reserved key, Anvil rejects the operation before checking whether such an object exists. This prevents information leaks through guessed internal paths.

## Pattern for application timelines

Many applications need append-only timelines. Use sortable keys:

```text
tenants/acme/workspaces/ws-1/runs/run-1/frames/0000000000000001.json
tenants/acme/workspaces/ws-1/runs/run-1/frames/0000000000000002.json
tenants/acme/workspaces/ws-1/runs/run-1/frames/0000000000000003.json
```

Fixed-width numbers keep lexical order equal to chronological order. Anvil can list the prefix and return frames in the intended sequence.

## What you can do now

You should now be able to design keys that encode ownership, scope, time, and type without forcing object-body scans. Next, learn how Anvil turns metadata, text, and embeddings into search.
