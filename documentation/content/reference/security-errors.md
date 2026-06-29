---
title: Security Errors
description: Interpret Anvil authentication, authorization, namespace, caveat, precondition, and index-readiness errors.
---

# Security Errors

**What this page gives you:** a reference for common security and consistency errors. You will know what they mean and which ones must never be bypassed.

Security errors protect data exposure and consistency. Treat them as part of the system contract, not generic transient failures.

| Error | Meaning | Correct response |
| --- | --- | --- |
| `Unauthenticated` | No valid identity was presented. | Configure credentials, refresh token, or sign in. |
| `PermissionDenied` | Identity is valid but lacks permission. | Grant the missing scope/tuple or hide the operation. |
| `UnauthorizedReservedNamespace` | Caller attempted public access to Anvil-owned internal paths. | Stop. Use structured native/admin APIs if insight is needed. |
| `PreconditionFailed` | Version, ETag, idempotency, or expected state did not match. | Reload and retry deliberately. |
| `InvalidCaveatHash` | Tuple write referenced an undefined or modified caveat. | Fix the caveat definition/reference. |
| `IndexNotReady` | Requested consistency point is not available. | Wait, show loading, or choose weaker consistency only if safe. |
| `InvalidTokenScope` | Scope syntax or resource pattern is malformed. | Fix credential or grant configuration. |
| `StaleFence` | Write attempted through stale partition or generation authority. | Refresh routing or lease state and retry through the owner. |

## Reserved namespace failures

Reserved namespace failures are hard failures. Public APIs must not read, list, write, copy, compose, delete, or range-read `_anvil/` paths. Applications should log the request id and fix the caller.

## Permission failures and UX

A forbidden response does not always mean a loud error. If an item should be invisible, hide it. If a user explicitly attempted an action they cannot perform, explain that they lack access. Do not reveal sensitive object names in the error body.

## Index readiness

`IndexNotReady` is not an authorization failure. It means the requested derived state has not reached the required cursor or generation. A UI can show a loader; a workflow gate can wait; a background job can retry.

## What you can do after this page

You should be able to classify common security and consistency errors without weakening Anvil's protections.
