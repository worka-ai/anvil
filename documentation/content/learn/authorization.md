---
title: Authorization
description: Learn authentication, authorization, Zanzibar concepts, and how Anvil protects data.
---

# Authorization

**Goal:** understand the difference between identity and permission, then learn the relationship model Anvil uses to protect objects, indexes, and PersonalDB rows.

Authentication answers: who is calling? Authorization answers: what may that caller do? A valid token proves identity, but it does not automatically grant access to every object.

Anvil uses two complementary authorization layers:

1. token scopes for coarse application permissions such as `object:write` on a resource pattern;
2. Zanzibar-style relationship authorization for fine-grained object, row, and group relationships.

## Token scopes

An application receives credentials. It exchanges those credentials for a bearer token. The token contains scopes such as:

```text
bucket:create|*
object:write|documents/*
object:read|documents/tenants/acme/*
```

Scopes are simple and fast. They are useful for service-level rights and initial administration. They are not expressive enough to model rich sharing such as "members of the legal group can view contracts for projects they are assigned to unless the contract has an active hold caveat".

## Relationship authorization

A relationship tuple states that a subject has a relation to an object.

```text
document:contract-42#viewer@user:amy
group:legal#member@user:amy
document:contract-42#viewer@group:legal#member
```

Read those as:

- Amy is a viewer of contract 42;
- Amy is a member of the legal group;
- members of legal are viewers of contract 42.

A Zanzibar-style system evaluates these relationships through namespace schemas. A namespace schema defines object types, relations, computed usersets, and tuple-to-userset rewrites.

## Why derived indexes exist

Naively answering relationship questions can require graph traversal. At small scale that is fine. At production scale, repeated graph expansion becomes expensive.

Anvil maintains derived userset indexes from tuple writes and namespace schemas. These indexes precompute common relationship paths so permission checks are fast. A check can require a specific consistency level. If the derived index has not processed the required revision, Anvil either waits or falls back to direct tuple expansion according to the requested semantics.

## Caveats

A caveat is a condition attached to a tuple. For example, access might be valid only until a timestamp or only from a trusted network. Anvil stores caveat hashes and validates caveat references before accepting tuple writes. This prevents a caller from smuggling undefined authorization logic into the tuple log.

## Authorization and search

Search must not leak data. Anvil applies authorization filtering before returning results. A user should not learn that a secret document exists because a full text or vector result hinted at it.

This rule applies to:

- object reads;
- object writes;
- prefix listing;
- metadata queries;
- full text search;
- vector search;
- PersonalDB group opens and commits;
- projection reads;
- internal administrative operations.

## Reserved namespaces are not authorization policy

Reserved paths under `_anvil/` are blocked before normal object authorization. Even an otherwise privileged public object caller cannot read or write those raw paths through object APIs. Structured admin APIs expose safe views after their own authorization checks.

## What you can do now

You should now be able to explain why Anvil uses both scopes and relationship tuples, why derived authorization indexes matter, and why search must be authorization-aware at query time.
