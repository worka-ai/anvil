---
title: Authorization
description: Learn identity, scopes, Zanzibar-style relationship authorization, caveats, and safe search.
---

# Authorization

**What this page achieves:** you will understand the difference between authentication and authorization, why simple scopes are not enough for sharing, and how Anvil protects reads, writes, listings, search, and PersonalDB.

Authentication answers: **who is calling?** Authorization answers: **what may that caller do?** A token can prove identity without granting permission to every object.

Anvil uses two complementary layers:

1. token scopes for coarse service permissions;
2. Zanzibar-style relationship tuples for fine-grained product permissions.

## Token scopes

A scope is an action-resource grant. Examples:

```text
bucket:create|*
object:write|documents/tenants/acme/*
object:read|documents/tenants/acme/projects/p-123/*
index:read|documents/tenants/acme/*
personaldb:commit|groups/acme-main
```

Scopes are fast and simple. They are useful for service accounts, ingestion jobs, administrative tools, and coarse boundaries. They are not expressive enough for rich sharing rules such as "members of the legal group may view contracts for projects they are assigned to unless a hold caveat is active".

## Relationship tuples

A relationship tuple states that a subject has a relation to an object:

```text
document:contract-42#viewer@user:amy
group:legal#member@user:amy
document:contract-42#viewer@group:legal#member
```

Read them as:

- Amy is directly a viewer of contract 42;
- Amy is a member of the legal group;
- members of the legal group are viewers of contract 42.

A namespace schema defines object types, relations, computed usersets, and tuple-to-userset rewrites. The schema tells Anvil what relationship paths are valid and how to evaluate them.

## Why this is called Zanzibar-style authorization

Zanzibar is a relationship-based authorization model popularized for large-scale systems. The key idea is that permissions are computed from relationship facts rather than copied into every object. Instead of writing a list of users into every document, you write relationship tuples and define how those relationships imply access.

This matters because sharing is usually graph-shaped. Users belong to groups. Groups belong to organizations. Documents belong to folders. Folders belong to projects. Projects have members. A relationship model can express that structure without duplicating permission lists everywhere.

## Derived authorization indexes

Direct graph traversal can be expensive. Anvil maintains derived userset indexes from tuple writes and namespace schemas. These indexes precompute common relationship paths so checks stay fast.

A permission check is not just a boolean. It is a boolean at a consistency point. Anvil can evaluate whether the derived authorization index has processed the revision needed for the requested operation. That is how search and listings avoid exposing stale or unauthorized results.

## Caveats

A caveat is a condition attached to a tuple. A relationship might be valid only until a timestamp, only for a region, only when a device posture is trusted, or only while a workflow state remains active.

Anvil validates caveat hashes and references before accepting tuple writes. That prevents a caller from inventing undefined authorization logic during a mutation. Caveat definitions are controlled schema material, not arbitrary code smuggled through object metadata.

## Authorization and every read surface

Authorization must protect more than `GET object`. An attacker can learn sensitive information from search counts, prefix listings, object existence checks, vector neighbors, snippets, metadata filters, and database projection reads.

Anvil applies authorization to:

- object reads, writes, deletes, copies, and conditional updates;
- prefix listing and metadata queries;
- full text search;
- vector and hybrid search;
- source artifact access;
- watch subscriptions;
- PersonalDB group opens, commits, snapshots, and projections;
- structured administrative APIs.

## Reserved namespaces

Paths under `_anvil/` are denied before normal object authorization. They are Anvil-owned internal state. Public object APIs cannot read, list, write, copy, compose, patch, range-read, or delete them.

This is not a normal permission rule. It is a hard boundary. Safe information from internal state is exposed only through structured APIs that perform their own authorization checks.

## What you can do after this page

You should be able to explain scopes, relationship tuples, schemas, caveats, derived authorization indexes, and why authorization must be part of search. Next, learn how watch streams keep indexes and projections current.
