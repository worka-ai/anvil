---
title: Native API
description: Use Anvil's native API when you need the full storage, index, authz, watch, and PersonalDB surface.
---

# Native API

**Goal:** choose the native API when you need Anvil's complete capabilities, understand the request model, and make a first authenticated call.

The native API is Anvil's primary interface. It exposes object operations, bucket operations, index management, search, watch streams, authorization tuple APIs, PersonalDB witness APIs, source artifact APIs, and administrative diagnostics. The S3-compatible gateway exists for object tools; the native API is where the full product lives.

## When to use it

Use the native API when you need any of the following:

- create or update index definitions;
- run metadata, full text, vector, or hybrid search;
- stream object, authz, index, or PersonalDB watches;
- write or check relationship tuples;
- create PersonalDB groups or commit changesets;
- inspect source artifacts or model ingestion state;
- request idempotent object mutations with Anvil-specific metadata.

If you only need PUT, GET, HEAD, DELETE, and LIST from existing object tooling, use the S3-compatible gateway. If your app depends on Anvil concepts, use the native API directly.

## Authentication flow

A tenant administrator creates an application and grants it policy. The application receives a client id and client secret. The client exchanges those credentials for a bearer token. Every native API request sends that token in gRPC metadata:

```text
authorization: Bearer <token>
```

The token proves the caller. Anvil still evaluates scopes, relationship tuples, reserved namespace rules, bucket policy, and operation-specific checks before returning data.

## First workflow

1. Ask an administrator to create a tenant application.
2. Ask for permissions such as `bucket:create`, `object:write`, `object:read`, and `object:list` scoped to your bucket.
3. Configure the CLI or client with the Anvil endpoint and credentials.
4. Request a token.
5. Create a bucket.
6. Put an object.
7. Head the object to confirm metadata.
8. List the prefix.

The CLI performs these steps for you, but understanding the flow helps when writing service code.

## Idempotency

Production clients retry. Anvil APIs that mutate state accept idempotency keys where appropriate. An idempotency key lets a client safely retry a request after a timeout without creating duplicate mutations.

Use a stable idempotency key for one logical operation. Do not reuse the same key for a different operation.

## Errors

Native errors include structured status, request id, and operation-specific details. Treat authentication errors, authorization errors, reserved namespace errors, precondition failures, and not-found errors differently in user interfaces. A forbidden object should not be shown as "storage is down".

## What you should implement first

For a service integration, start with:

- token acquisition and refresh;
- bucket creation or bucket discovery;
- object PUT/GET/HEAD/LIST;
- clear handling for forbidden, not found, conflict, and precondition failures;
- request ids in logs.

Then add indexes, watches, and PersonalDB when the application flow requires them.
