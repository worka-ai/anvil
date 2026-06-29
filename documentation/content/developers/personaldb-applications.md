---
title: PersonalDB Applications
description: Build applications that use Anvil as the PersonalDB witness and object archive.
---

# PersonalDB Applications

**Goal:** understand how an application uses PersonalDB through Anvil without treating SQLite files as ordinary blobs.

A PersonalDB-backed app keeps a local database for fast user interaction and submits changesets to Anvil for witnessing. Anvil verifies, orders, stores, certifies, snapshots, projects, and emits watches for those changes.

## Create a group

A database group is created with a registered schema. The schema defines tables and constraints. Anvil stores the schema internally and uses it to validate future changesets.

```text
CreatePersonalDbGroup(
  database_id = "workspace-acme",
  schema_sql = "CREATE TABLE tasks (...);",
  policy = StrictWitnessed
)
```

`StrictWitnessed` means commits are not accepted unless Anvil verifies and signs them.

## Commit a mutation

A client submits:

- database id;
- base head;
- idempotency key;
- actor identity;
- SQLite changeset bytes;
- authorization context.

Anvil checks that the base head is valid, validates changeset semantics, evaluates row/resource authorization, appends the log, signs a commit certificate, updates row metadata, and emits group watch events.

## Recover a replica

A recovering replica asks for the latest snapshot and then catches up from the log after the snapshot head. This avoids replaying the full history every time.

## Use projections for limited views

A projection is a derived database group. Use it when a client should see only part of the source data. Examples:

- a support view of ticket summaries without private billing details;
- a mobile read model with denormalized rows;
- an analytics view containing only aggregated, authorized facts.

Projection definitions are declarative and sealed by Anvil. They are not arbitrary client-provided SQL scripts.

## Do not store raw database files as app state

You can store SQLite snapshots as objects, but the application state path should go through PersonalDB APIs. Raw object uploads do not give Anvil row metadata, commit certificates, authorization effects, or projection watches.

## What to log

For each commit, log:

- database id;
- actor;
- idempotency key;
- base head;
- returned head;
- commit certificate id;
- request id;
- any rejected row/resource operation.

Those fields make client sync problems diagnosable.
