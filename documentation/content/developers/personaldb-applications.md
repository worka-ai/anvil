---
title: PersonalDB Applications
description: Build local-first applications that use Anvil as the PersonalDB witness and projection host.
---

# PersonalDB Applications

**What this page achieves:** you will learn how an application uses PersonalDB through Anvil to coordinate SQLite changesets, commits, snapshots, and projections.

A local-first application gives each device a local database so the UI stays fast and can work with unreliable connectivity. The server-side challenge is accepting, ordering, validating, and distributing changes. Anvil's PersonalDB APIs provide that witness role.

## Application shape

A typical application has:

- a local SQLite database;
- a PersonalDB group id;
- a schema version;
- a local commit queue;
- a sync worker;
- a projection reader for server-derived views;
- authorization rules for who may open, commit, and read group data.

The local database remains useful without a network. The witness decides which changes become part of the shared history.

## Opening a group

Opening a group gives the client the current head, schema information, snapshot options, and authorization context. A new device usually starts by downloading a snapshot at a known head, then applying later commits.

Conceptual flow:

```text
open group
  -> receive current head and schema
  -> download snapshot if local database is missing or too old
  -> apply commits after snapshot head
  -> begin normal sync loop
```

## Submitting commits

A commit request includes a base head, SQLite changeset, author identity, idempotency key, and schema information. Anvil verifies that the caller may commit, that the changeset is valid for the group, and that ordering rules are satisfied.

If accepted, Anvil records the commit and returns a certificate. The client can store that certificate with local sync state.

If rejected, the client should inspect the reason. Common causes are stale base head, authorization failure, schema mismatch, invalid changeset, or conflict policy rejection.

## Snapshots

Snapshots compact history. A new device should not have to replay every commit from the beginning of time. Anvil stores snapshots as durable objects with hashes and source heads.

A snapshot is valid only if it proves which commit head it represents. Projection builders and clients should not trust anonymous database files without source proof.

## Projections

A projection is a server-side derived view over accepted commits. Use projections when the application needs queryable server-visible summaries, feeds, or indexes without exposing raw group data directly.

Examples:

- open tasks by assignee;
- calendar events by month;
- unread message counts;
- audit rows by actor;
- media references by object key.

Projection reads are authorization-checked and tied to source cursors.

## Offline and retry behavior

The client should queue local changes while offline. When connectivity returns, it submits commits with idempotency keys. If a request times out, retry with the same key. If the base head is stale, fetch the latest accepted commits, apply or merge them locally, then submit a new changeset.

Do not invent a separate server-side database path for the same data. That creates two sources of truth. Use PersonalDB commits and projections.

## What you can build after this page

You should be able to design a local-first sync loop where Anvil witnesses accepted SQLite changes, stores snapshots, and serves authorized projections. Next, learn how to store source and model artifacts with the same object/index/auth model.
