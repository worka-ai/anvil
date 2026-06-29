---
title: Security Errors
description: Interpret common Anvil security and authorization errors.
---

# Security Errors

**Goal:** understand security failures without weakening protections or leaking data.

Security errors are intentionally precise for authorized operators and intentionally conservative toward public callers.

## Common errors

| Error | Meaning | Typical fix |
| --- | --- | --- |
| `Unauthenticated` | No valid identity was presented. | Configure credentials or refresh token. |
| `PermissionDenied` | Identity is known but lacks the required action/resource relationship. | Grant the missing scope or relationship tuple. |
| `UnauthorizedReservedNamespace` | Caller attempted to access `_anvil/` internal paths through public APIs. | Use structured admin/native APIs instead of raw object paths. |
| `PreconditionFailed` | ETag, version, date, or idempotency precondition did not match. | Reload current state and retry deliberately. |
| `InvalidCaveatHash` | Tuple write referenced an undefined or invalid caveat. | Register or correct the caveat definition. |
| `IndexNotReady` | Query requested a consistency level the index has not reached. | Wait, show loading, or choose a weaker consistency mode if acceptable. |

## Do not paper over forbidden results

A forbidden response is not a storage outage. User interfaces should show an access message only when appropriate. Services should log the request id and operation but avoid echoing sensitive object names to untrusted callers.

## Reserved namespace handling

Reserved namespace errors are hard failures. Do not retry with different headers, do not probe for existence, and do not create exceptions in S3 clients. Public object APIs never expose raw internal namespace bytes.
