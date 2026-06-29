---
title: PersonalDB
description: Learn PersonalDB, local-first SQLite changesets, witness commits, snapshots, projections, and authorisation.
---

# PersonalDB

**What this page gives you:** a first-principles explanation of local-first data and the role Anvil plays as a PersonalDB witness. You will learn what changesets, heads, certificates, snapshots, and projections mean.

A local-first application stores useful data on the user's device. The UI can work quickly because it reads a local database. It can continue through weak network connections because local writes are queued. The hard part is coordination: when devices reconnect, which changes are accepted, in what order, and how does every device learn the result?

PersonalDB is the protocol and data model for coordinating local SQLite databases. Anvil provides the server-side witness.

## Local-first problem

Imagine a notes app. A laptop and phone both have a local SQLite database. The user edits a note on the laptop while offline. The phone edits the same note. Later both reconnect.

The system needs to answer:

- Which changes were made?
- Which shared history did each device start from?
- Are the changes authorised?
- Are they valid for the schema?
- Do they conflict?
- What certificate proves a change was accepted?
- What snapshot should a new device download?
- What server-side projection can power search or dashboards?

A simple upload endpoint is not enough. The server must witness ordered changes.

## Changeset

A SQLite changeset records row-level changes: inserts, updates, and deletes. It is more precise than "upload the entire database". The client can submit the changes since its last accepted head.

A PersonalDB commit includes:

- group id;
- base head;
- schema version;
- author identity;
- idempotency key;
- SQLite changeset bytes;
- optional metadata and conflict policy inputs.

The base head says what shared history the client believed it was extending.

## Witness

A witness is the authority that accepts or rejects commits. Anvil verifies that the caller may commit, that the changeset applies to the expected base or conflict policy, that schema rules hold, and that the operation is idempotent.

If accepted, Anvil records the commit and returns a certificate. The certificate proves the commit became part of the accepted history at a specific head.

If rejected, the client must handle the reason: stale base, schema mismatch, authorisation failure, invalid changeset, conflict policy rejection, or duplicate idempotency result.

## Snapshot

A snapshot is a compact database image at a known head. Without snapshots, a new device might need to replay every commit since the group was created. With snapshots, it downloads a database at head `H`, verifies it, then applies commits after `H`.

Snapshots are objects with hashes and source proof. They are not anonymous files. A snapshot must prove which commit head it represents.

## Projection

A projection is a server-side derived view over accepted commits. It exists when a service needs queryable server-visible data without asking every client to upload a separate database.

Examples:

- tasks by assignee;
- calendar events by month;
- unread counts;
- audit rows by actor;
- search records for notes;
- media references by object key.

Projection builders consume PersonalDB watch events and checkpoint cursors. Projection reads are authorisation-checked like object and search reads.

## How Anvil integrates PersonalDB

PersonalDB needs many features Anvil already owns:

- durable object storage for snapshots and changesets;
- metadata and path indexes for group state;
- watch streams for commits and projection builders;
- authorisation for group open, commit, snapshot, and projection reads;
- manifests and hashes for proof;
- backup and recovery for full durable state.

That is why PersonalDB belongs inside Anvil rather than as a detached side service.

## What you can do after this page

You should be able to explain local-first data, SQLite changesets, witnessed commits, commit certificates, snapshots, projections, and how Anvil makes PersonalDB part of the same storage and authorisation model.
