---
title: Identity And Access
description: Operate tenants, applications, scopes, relationship tuples, schemas, and reserved namespaces.
---

# Identity And Access

**What this page achieves:** you will know how to operate Anvil access control with least privilege, relationship authorization, auditability, and safe reserved namespace handling.

Access control in Anvil protects every data exposure path: object reads, listings, search, vector results, watch streams, PersonalDB state, source artifacts, and administrative APIs. Operators need to understand both coarse token scopes and fine-grained relationship authorization.

## Tenants and applications

A tenant is an administrative boundary. An application is a credentialed caller inside that boundary. Create separate applications for separate responsibilities.

Recommended pattern:

| Application | Typical permissions |
| --- | --- |
| Ingestion service | Write specific buckets and metadata fields. |
| User API service | Read/write product prefixes on behalf of users. |
| Search worker | Read source objects and write derived index material through internal paths only via Anvil. |
| Audit exporter | Read selected metadata and immutable logs. |
| Break-glass admin | Broad rights, tightly controlled and monitored. |

Avoid sharing one powerful application across unrelated services. When something goes wrong, you need to know which component had which authority.

## Scope grants

A scope grant is an action-resource pattern:

```text
bucket:create|*
object:write|documents/tenants/acme/*
object:read|documents/tenants/acme/*
index:read|documents/*
personaldb:commit|groups/acme-main
```

Use the smallest scope that supports the job. Broad grants such as `*|*` are for controlled administration, not normal application traffic.

Review scopes regularly. Remove grants when services are retired. Rotate application credentials after suspected exposure.

## Relationship authorization operations

Relationship authorization is controlled through namespace schemas and tuple writes. Treat schema changes as production changes because they alter how permissions are computed.

A safe workflow is:

1. Review the schema change.
2. Validate tuple rewrite behavior in a staging tenant.
3. Publish the schema revision.
4. Monitor derived authorization index lag.
5. Apply tuple writes with idempotency keys.
6. Verify representative permission checks.
7. Keep request ids for audit.

Example tuple facts:

```text
workspace:ws-1#owner@user:amy
workspace:ws-1#member@group:design#member
document:doc-42#viewer@workspace:ws-1#member
```

## Caveats

Caveats add conditions to tuples. Operators must control caveat definitions and hashes. A tuple referencing an unknown or invalid caveat must be rejected.

Examples of caveat use:

- access expires at a timestamp;
- access applies only to a specific device posture;
- access is valid only while a workflow state remains active.

Do not allow arbitrary callers to define caveat code as part of tuple writes. Caveats are policy material.

## Reserved namespaces

Anvil internal paths under `_anvil/` are not public object paths. Public APIs reject attempts to read, list, write, copy, compose, delete, range-read, or conditionally mutate them.

This is a hard security boundary. Operators should not build tools that bypass it. Use structured diagnostics and admin APIs to inspect internal state.

## Audit requirements

Record and retain:

- application credential creation and rotation;
- scope grant changes;
- namespace schema publications;
- tuple write request ids;
- caveat definition changes;
- denied authorization checks;
- reserved namespace rejection counts;
- derived authorization index lag;
- break-glass administrative sessions.

Audit logs are most useful when request ids connect client logs, Anvil service logs, and durable mutation records.

## What you can do after this page

You should be able to operate Anvil identity and access without relying on broad credentials or unsafe search filtering. Next, learn how to monitor and recover indexes.
