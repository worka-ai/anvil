---
title: PersonalDB
description: Learn why Anvil includes a PersonalDB witness and how SQLite changes become replicated application state.
---

# PersonalDB

**Goal:** understand PersonalDB at a conceptual level: database groups, SQLite changesets, witnesses, commit certificates, snapshots, projections, and row-level authorization.

PersonalDB is a replicated application data model built around SQLite-compatible changesets. A client can maintain a local SQLite database, produce changesets, and submit those changes to a witness. Anvil is that witness.

## Why an object store includes a database witness

Applications often need both blobs and structured local state. A project management app might store uploaded files as objects but store tasks, comments, and preferences in a local database. If the local database sync path is separate from object storage, the application must coordinate two durability and authorization systems.

Anvil integrates the witness path so object data, database changesets, row metadata, authorization, projections, snapshots, and watches live under one consistency model.

## Database groups

A PersonalDB database group is one logical replicated database. It has:

- a registered schema;
- a canonical changeset log;
- a committed head;
- commit certificates;
- snapshots;
- row metadata indexes;
- optional projection groups;
- watch streams.

A group is the unit of witness ordering. Commits for one group are routed to the current owner of that group partition so sequence numbers and heads remain consistent.

## Changesets

A changeset describes row-level inserts, updates, and deletes. The client submits a verified mutation envelope containing the changeset, base head, actor identity, idempotency key, and requested authorization context.

Anvil validates the request, checks authorization, applies SQLite changeset semantics through the PersonalDB changeset module, derives row effects, appends the canonical log record, signs a commit certificate, updates row metadata, and emits watches.

## Commit certificates

A commit certificate is Anvil's signed statement that a changeset was accepted at a specific database head. It gives clients and downstream systems a durable proof of ordering.

Certificates are important because distributed clients may reconnect, retry, or receive events out of order. The certificate anchors the commit in the witnessed log.

## Snapshots

A snapshot is a compressed SQLite database image for a database group at a known head. Snapshots let new replicas and recovering clients avoid replaying the entire log from the beginning. The snapshot manifest records which log segment range and head produced the snapshot.

## Projections

A projection group is a derived database group built from authorized rows and columns of one or more source groups. For example, a customer-support projection might include ticket summaries and customer names but exclude private billing fields.

Projection definitions are declarative. They are not arbitrary executable SQL supplied by untrusted clients. Anvil validates and seals projection definitions before building them.

## Row metadata and authorization

Anvil maintains row metadata for PersonalDB groups. Row metadata records ownership, resource identity, updated columns, and authorization-relevant relationships. That lets Anvil evaluate row-level authorization and build projections without trusting application servers to interpret changesets correctly.

## What you can do now

You should now be able to explain why Anvil acts as a PersonalDB witness, what a database group is, why commit certificates matter, and how projections and watches fit into the model.
