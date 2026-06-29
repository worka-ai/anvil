---
title: Native API
description: Build applications against Anvil's native API with a production mental model.
---

# Native API

**What this page gives you:** a developer's model for using Anvil's full capability surface. You will learn when to use the native API, how requests flow, and how to structure application code so storage, search, authorisation, watches, and PersonalDB remain coherent.

The native API is Anvil's complete interface. S3 compatibility is useful for existing object tools, but S3 cannot express Anvil-specific operations such as index creation, authorisation schemas, watch subscriptions, vector search, PersonalDB groups, source manifests, and structured repair diagnostics.

Use S3-compatible clients when the job is moving object bytes. Use the native API when the application needs Anvil's integrated model.

## Request lifecycle

A native request normally passes through these stages:

```text
client configuration
  -> credential provider
  -> token exchange or token refresh
  -> request with identity, tenant, scope, and idempotency
  -> authentication
  -> authorisation
  -> validation and preconditions
  -> durable mutation or read
  -> derived events and indexes
  -> response with version, cursor, request id, or diagnostic reason
```

That lifecycle matters because a storage request is rarely "just a file operation". A write may affect object state, metadata indexes, full text inputs, vector inputs, watches, source manifests, PersonalDB projections, and authorisation-visible query results.

## Client layering

Keep client code layered:

```text
configuration
  -> credentials
  -> Anvil client
  -> application repository/service
  -> UI or business workflow
```

Do not scatter endpoint strings, credentials, bucket names, retry behaviour, and metadata conventions across product code. A repository layer can own Anvil-specific concerns and present application-level operations such as `upload_contract`, `search_documents`, or `submit_local_changeset`.

## Idempotency and retries

Distributed clients retry. A timeout does not tell the client whether the server committed the write. For retryable mutations, include an idempotency key. Retrying the same logical operation with the same key should return the same logical result rather than creating duplicate mutations.

Use idempotency for:

- object puts where duplicate writes would be harmful;
- metadata updates from background jobs;
- PersonalDB commit submissions;
- source artefact ingestion;
- administrative operations triggered by automation.

## Preconditions

Use preconditions when updating known state. If a user edited metadata based on version `v7`, the write should say it applies only to `v7`. If the object is now `v8`, the server rejects the stale update and the application can reload or merge.

Preconditions prevent silent lost updates. They also make failures meaningful: the application knows it saw stale data rather than a generic write error.

## Buckets and object writes

A production object write should normally include:

- bucket;
- key;
- body stream;
- content type;
- metadata;
- idempotency key;
- optional precondition;
- caller identity and authorisation context;
- expected checksum when the client can provide it.

Choose keys and metadata from the application model, not from whichever upload widget happened to send the file.

## Index definitions and queries

Define indexes for the queries the product actually needs. A document application might have:

- documents by project and creation time;
- documents by status and customer;
- full text over extracted text and title;
- vectors for semantic search;
- source artefacts by build id;
- PersonalDB projections by assignee or due date.

Query indexes through the native API with caller identity. Do not fetch broad result sets with administrative credentials and filter locally.

## Watches

Use watches when another part of the application needs to react to change. A watcher stores its cursor after derived work is durable.

Common watchers include:

- UI timeline broadcasters;
- text extraction workers;
- embedding workers;
- metadata denormalizers;
- audit export jobs;
- PersonalDB projection builders;
- cache invalidators.

A watcher must be idempotent. It may process the same event after a crash or retry. Write derived outputs so repeating work is safe.

## Authorisation-aware application design

Good pattern:

```text
call Anvil with the end-user identity and required action
  -> receive only authorised objects, rows, snippets, and counts
  -> render the result
```

Weak pattern:

```text
call broad query as admin
  -> filter in application memory
  -> hope counts, snippets, timings, and facets did not leak data
```

The weak pattern is unsafe. Authorisation must protect direct reads, listings, metadata filters, full text snippets, vector neighbours, watches, and PersonalDB projections.

## Error handling

Handle errors by category:

| Category | Meaning | Developer response |
| --- | --- | --- |
| Authentication | No valid identity. | Refresh credentials or ask the user to sign in. |
| Authorisation | Identity exists but lacks permission. | Hide action, request access, or show access denied. |
| Precondition | Version, ETag, or expected state changed. | Reload, merge, or retry deliberately. |
| Idempotency | Same key was reused inconsistently. | Fix caller logic; do not generate a random retry key. |
| Index readiness | Required derived state is not current. | Show loading, wait, or choose weaker consistency only if safe. |
| Validation | Request shape is wrong. | Fix code; do not retry blindly. |
| Reserved namespace | Caller touched `_anvil/`. | Stop and fix the caller. |

## What you can build after this page

You should be able to design an Anvil client layer that uses credentials, idempotency, preconditions, objects, metadata, indexes, watches, authorisation, and PersonalDB deliberately. Next, use the S3 compatibility guide for existing object tools or the object metadata guide for product modelling.
