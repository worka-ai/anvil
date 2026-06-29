---
title: Identity And Access
description: Operate Anvil authentication, scopes, relationship authorisation schemas, tuples, caveats, and reserved namespace protections.
---

# Identity And Access

**What this page gives you:** an operator's model for credentials, token scopes, relationship authorisation, caveats, and safe access audits.

Anvil access control protects every data exposure path: object reads, listings, metadata filters, full text search, vector results, watch streams, PersonalDB state, source artefacts, and administrative APIs. Operators must manage both coarse API credentials and fine-grained relationship policy.

## Identity layers

Anvil distinguishes several layers:

1. **Credential material** proves a caller can request a token.
2. **Token scopes** bound what broad API families and resources the caller may use.
3. **Relationship tuples** define fine-grained product permissions.
4. **Caveats** add conditional policy such as time or context.
5. **Reserved namespace rules** protect internal paths regardless of normal object grants.

All layers matter. Broad credentials with weak relationship policy are unsafe. Strong relationship policy with leaked admin credentials is also unsafe.

## Tenant and application credentials

Create separate application credentials for separate jobs. A backup tool, ingestion worker, public API backend, and admin automation should not share one credential.

For each credential, document:

- owner;
- purpose;
- allowed buckets and prefixes;
- allowed API families;
- rotation process;
- emergency revocation process;
- expected request volume.

## Authorisation namespaces

An authorisation namespace has two operational meanings that must not be confused.

A tuple namespace label is data inside a tuple, such as `document` in `document:doc-42#viewer@user:amy`. It identifies the protected-object family used for a permission check. It does not create a storage bucket, does not create an object, and does not give the caller authority to define policy.

A namespace schema definition is privileged policy. It defines which relations may exist for a tuple namespace label, which subject kinds may appear on those relations, which usersets may be referenced, which caveats are valid, and which computed relations are valid permission checks.

Treat namespace definitions like production policy code. A namespace change can alter reads, writes, search visibility, watch delivery, and projection output for every object of that type. Do not expose namespace definition writes as ordinary tenant self-service unless that tenant has been explicitly delegated policy-administration authority.

The admin CLI and bootstrap path are for privileged setup: creating tenants, creating applications and secrets, granting coarse token scopes, registering namespace and caveat definitions, and seeding the first owner/admin relationships. Ordinary tenant users should not redefine namespaces, mint scopes, or bypass tuple typing through object writes.

After bootstrap, scoped tenant applications may write allowed relationship tuples, check permissions, and watch authorisation changes only within their grants. They cannot create tenant boundaries, grant themselves broader authority, redefine namespace schemas or caveats, write invalid tuple shapes, or read `_anvil/` internal paths.

## Relationship schemas

A relationship schema defines object types, relations, and permissions. Review schemas like code. A small schema change can grant broad access.

Good review questions:

- Which relation grants read access?
- Which relation grants write access?
- Does parent inheritance match product expectations?
- Are groups expanded correctly?
- Are caveats required where time-bound access exists?
- Can a user indirectly become an owner through an unexpected path?

## Tuple operations

Tuple writes are security mutations. They should be audited with request ids, actor identity, object, relation, subject, caveat reference, and source reason.

Avoid broad tuple imports without validation. A malformed import can make data invisible or overexposed.

## Caveat operations

Caveats must be defined before use and referenced by verified hash. This prevents a caller from claiming a condition name while changing its body.

If Anvil reports an invalid caveat hash, stop the operation and fix the policy definition or caller. Do not bypass caveat checks to unblock a workflow.

## Reserved namespace enforcement

Paths under `_anvil/` are Anvil-owned. Public APIs must not read, list, write, copy, compose, delete, or range-read those paths. Operators should not grant exceptions through user-facing policy.

Administrative insight should come from structured admin or native APIs, not by exposing internal object paths.

## Access audit checklist

Regularly verify:

- stale application credentials are removed;
- broad scopes are justified;
- tuple imports have source records;
- caveat hashes match deployed definitions;
- reserved namespace attempts are logged and investigated;
- search and watch APIs enforce the same access model as direct object reads;
- PersonalDB group access matches application policy.

## What you can do after this page

You should be able to operate credentials, scopes, relationship schemas, tuples, caveats, and reserved namespace protections without treating authorisation as an application afterthought.
