---
title: Security Errors
description: Interpret Anvil authentication, authorization, namespace, caveat, precondition, and index-readiness errors.
---

# Security Errors

**What this page achieves:** you will know what common security errors mean, what to do next, and which errors must not be bypassed.

Security errors are part of the product contract. They prevent data exposure and preserve consistency. Do not treat them as generic transient failures.

## Common errors

| Error | Meaning | Correct response |
| --- | --- | --- |
| `Unauthenticated` | No valid identity was presented. | Configure credentials, refresh the token, or ask the user to sign in. |
| `PermissionDenied` | Identity is valid but lacks the required action or relationship. | Grant the missing scope/tuple or hide the operation. |
| `UnauthorizedReservedNamespace` | Caller attempted to access Anvil-owned internal paths through public APIs. | Stop the operation. Use structured admin/native APIs. |
| `PreconditionFailed` | Version, ETag, idempotency, or conditional state did not match. | Reload current state and retry deliberately. |
| `InvalidCaveatHash` | Tuple write referenced an undefined or invalid caveat. | Fix the caveat definition/reference before retrying. |
| `IndexNotReady` | Query requested a consistency level the index has not reached. | Wait, show loading, or choose weaker consistency only if product semantics allow it. |
| `InvalidTokenScope` | Token scope syntax or resource pattern is malformed. | Fix the credential or grant configuration. |
| `StaleFence` | A write attempted to use stale partition or generation authority. | Refresh routing/lease state and retry only through the correct owner. |

## Reserved namespace errors are hard failures

Reserved namespace failures are not hints to retry with another method. Public APIs must not read, list, write, copy, compose, delete, or range-read `_anvil/` paths.

Applications should log the request id and stop. Operators should investigate why a caller attempted internal paths.

## Permission errors and user experience

A forbidden response does not always mean the UI should show a loud error. If an item should be invisible, hide it. If a user explicitly tried an action they cannot perform, explain that they lack access. Avoid echoing sensitive object names to untrusted callers.

## Index readiness is not authorization failure

`IndexNotReady` means the requested consistency point is not available yet. A UI can show a loader. A workflow gate can wait. A background job can retry. Do not turn it into a broad admin query that bypasses consistency and authorization.

## What you can do after this page

You should be able to classify common security and consistency errors and respond without weakening Anvil's protections.
