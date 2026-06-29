---
title: PersonalDB Applications
description: Build local-first applications that use Anvil as the PersonalDB witness and projection host.
---

# PersonalDB Applications

**What this page gives you:** a developer-oriented model for local-first applications using PersonalDB through Anvil. You will learn how to structure client sync, commits, snapshots, projections, and authorisation.

A local-first application reads and writes a local SQLite database. The UI stays fast because ordinary views query local state. The application still needs shared truth: accepted changes, current heads, snapshots for new devices, projections for server-side queries, and authorisation rules.

Anvil acts as the PersonalDB witness. It validates and records commits, signs accepted heads, stores snapshots, maintains projections, and authorizes every operation.

## Application components

A typical application has:

- a local SQLite database;
- a PersonalDB group id;
- schema version metadata;
- a local commit queue;
- a sync worker;
- a conflict handling strategy;
- a snapshot downloader;
- projection readers for server-visible views;
- authorisation rules for group open, commit, snapshot, and projection reads.

Do not create a separate server database for the same shared state. That creates two sources of truth. Use PersonalDB commits and projections.

## Opening a group

A device opens a group to learn the current head and synchronization options:

```text
open group
  -> verify caller may open
  -> receive schema information and current head
  -> download snapshot if local database is missing or too old
  -> apply commits after snapshot head
  -> start normal sync loop
```

The local database is trusted only after it is tied to a known accepted head.

## Submitting a commit

A commit request should include:

- group id;
- base head;
- schema version;
- SQLite changeset;
- author identity;
- idempotency key;
- client timestamp or logical clock when required;
- optional conflict policy inputs.

Anvil validates the request, records the commit if accepted, and returns a certificate. Store that certificate with local sync state.

## Handling rejection

Common rejections:

| Rejection | Meaning | Client response |
| --- | --- | --- |
| Stale base head | Other commits were accepted first. | Fetch commits, merge/apply, submit a new changeset. |
| Schema mismatch | Client schema is not compatible. | Run migration or block until upgraded. |
| Authorisation denied | Caller may not open or commit. | Stop sync and surface access issue. |
| Invalid changeset | Changeset cannot be applied safely. | Fix client bug or conflict handling. |
| Idempotency mismatch | Same key reused for different content. | Fix retry logic. |

## Projections

Use projections when the server needs queryable derived views. For example, a task application may keep each device local but expose a server projection of open tasks by assignee. Projection builders consume accepted commits and write authorised queryable views.

Projection reads should include caller identity. A projection row is still data exposure.

## What you can build after this page

You should be able to build a local-first sync loop where Anvil witnesses SQLite changesets, returns commit certificates, stores snapshots, and serves authorised projections.
