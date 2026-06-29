---
title: PersonalDB
description: Learn PersonalDB, local-first SQLite changesets, witnesses, snapshots, projections, and authorization.
---

# PersonalDB

**What this page achieves:** you will understand why PersonalDB exists, how Anvil witnesses changes, and how object storage, authorization, watches, and projections support local-first applications.

A local-first application keeps a useful database on the user's device. The application can read quickly, work offline, and later sync changes. SQLite is a strong fit for this because it is embedded, reliable, and widely understood.

The hard part is not storing a local database file. The hard part is coordinating accepted changes across devices and services. Which changes were committed? In what order? Who was allowed to make them? Which projections should update? Which snapshot should a new device download?

PersonalDB gives that coordination protocol a durable server-side witness. Anvil provides that witness path.

## The problem PersonalDB solves

Imagine two devices editing the same project database. Each device can produce SQLite changesets. A server must decide whether each changeset is valid, authorized, and ordered. It must also make the accepted history available to other devices.

Without a witness, every client has to trust that every other client is honest and current. With a witness, accepted commits receive certificates and become part of a durable group history.

## Core nouns

| Concept | Meaning |
| --- | --- |
| Group | A synchronization domain for one PersonalDB database or related database set. |
| Commit | A proposed changeset submitted to the group. |
| Witness | The service that validates, orders, records, and signs accepted commits. |
| Snapshot | A compact database state at a known commit head. |
| Projection | A derived queryable view built from accepted commits. |
| Certificate | Proof that a commit was accepted with a specific head, policy, and hash. |

## Commit flow

A simplified PersonalDB commit flow is:

```text
client prepares SQLite changeset
  -> client submits changeset, base head, identity, and idempotency key
  -> Anvil verifies authorization and group policy
  -> Anvil validates the changeset against expected schema and head rules
  -> Anvil records the commit in the group log
  -> Anvil returns a signed commit certificate
  -> watchers update snapshots and projections
```

The certificate is important. It gives clients proof that the witness accepted the commit. Other systems can reference the certificate rather than trusting an informal response.

## Why Anvil is involved

Anvil already owns durable objects, metadata, authorization, watches, and derived indexes. PersonalDB needs all of those:

- commit logs and snapshots are durable objects;
- group state and row metadata need indexes;
- authorization decides who may open, commit, read, or project;
- watch streams notify clients and projection builders;
- repair can rebuild projections from accepted commits;
- source hashes and manifests make snapshots verifiable.

PersonalDB is not a separate side channel. It is integrated into the same storage and authorization model as objects and search.

## Projections

A projection is a derived view over accepted database commits. For example, a task application might project open tasks by assignee, due date, and priority. A projection should not depend on unaccepted local writes. It should be built from the witness log and carry a source cursor.

Projection reads are authorization-checked. A caller should not learn about private rows because a projection happened to include them.

## Conflict boundaries

PersonalDB does not remove the need for application-level conflict design. It gives the application a reliable place to validate, order, and distribute changes. Applications still choose schemas, merge rules, constraints, and user experience for conflicting edits.

The witness makes those decisions enforceable and auditable.

## What you can do after this page

You should be able to explain why local-first applications need a witness, what a PersonalDB commit certificate proves, and how Anvil stores and indexes PersonalDB state. You are now ready to move from concepts to developer guides.
