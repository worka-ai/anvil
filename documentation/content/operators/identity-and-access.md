---
title: Identity And Access
description: Operate tenants, applications, scopes, relationship tuples, and reserved namespaces safely.
---

# Identity And Access

**Goal:** run Anvil access control with least privilege, clear boundaries, and auditable relationship changes.

Anvil access control starts with tenant applications and token scopes, then extends into relationship authorization. Operators should understand both because production systems usually use both.

## Tenant applications

Create one application per service, integration, or deployment unit. Do not share one all-powerful application across unrelated systems.

Recommended pattern:

- one ingestion app for data import;
- one user-facing API app for product reads/writes;
- one background processing app for derived work;
- one audit app for read-only compliance export.

Grant each only the actions and resource patterns it needs.

## Scope grants

A scope grant combines action and resource pattern. Examples:

```text
bucket:create|*
object:write|documents/tenants/acme/*
object:read|documents/tenants/acme/*
index:read|documents/*
personaldb:commit|workspace-db/*
```

Review broad grants regularly. `*|*` is for tightly controlled break-glass administration, not normal application traffic.

## Relationship tuples

Use tuples for user and resource relationships. Store relationships in the authorization system rather than duplicating access lists in object metadata. Metadata can describe an object, but authorization tuples decide access.

Example tuple language:

```text
workspace:ws-1#owner@user:amy
workspace:ws-1#member@group:design#member
document:doc-42#viewer@workspace:ws-1#member
```

## Reserved namespace policy

Anvil internal paths under `_anvil/` are not public objects. Operators must not create tooling that bypasses this rule through S3 or normal object APIs. Use structured admin APIs for diagnostics.

Public operations against reserved paths fail with `UnauthorizedReservedNamespace`. That includes GET, HEAD, LIST, range reads, PUT, COPY, COMPOSE, DELETE, PATCH, and conditional updates.

## Audit practice

Record:

- who created or reset application credentials;
- which policy grants changed;
- which namespace schema revisions were published;
- tuple write request ids;
- derived authorization index lag;
- denied requests and their request ids.

Authorization problems are easier to resolve when request ids connect application logs to Anvil logs.
