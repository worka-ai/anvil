---
title: PersonalDB
description: Understand PersonalDB as local SQLite plus Anvil-witnessed changesets, heads, certificates, projections, watches, snapshots, and repair.
---

# PersonalDB

PersonalDB is for applications that want SQLite at the edge but need shared, witnessed history. The application still owns a local SQLite database on each device or worker. Reads, writes, indexes, transactions, and offline behaviour are local SQLite concerns. Anvil's job is different: it witnesses SQLite changesets, records an accepted log chain, advances group heads, stores commit certificates, emits watches, builds snapshots and projections, and enforces tenant and row-level authorisation around those mutations.

This makes PersonalDB unlike ordinary object storage. An object write replaces bytes at a key and moves a current pointer. A PersonalDB commit appends a row-level changeset to an ordered group history if, and only if, the request extends the current head and passes witness validation. It is also unlike a SQL server. Anvil is not a remote endpoint for arbitrary `SELECT`, `INSERT`, or `UPDATE` statements. Clients query their local SQLite files. Anvil stores and verifies the replicated history that lets those local files catch up safely.

Read this page with [CoreStore](/learn/corestore/), [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Authorisation](/learn/authorisation/), [PersonalDB Tutorial](/tutorials/personaldb/), [Watches Tutorial](/tutorials/watches/), [Repair and Diagnostics](/tutorials/repair-and-diagnostics/), [PersonalDB Operations](/operators/personaldb-operations/), [Public CLI](/reference/public-cli/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## The local SQLite side

A local-first application keeps its working state close to the user. A notes app might store `notes`, `folders`, and `attachments` tables in a SQLite file on a laptop. The UI can read and write those tables with normal SQLite transactions even when the network is unavailable. That is the point: the user's main interaction is not blocked on a remote database round trip.

The synchronisation problem starts after the local transaction. When the device reconnects, it needs to answer questions that a raw database upload cannot answer safely:

```text
Which accepted head did this local change start from?
Which rows changed, and how?
Does this changeset match the registered schema?
Is the authenticated principal allowed to make those row effects?
Did another replica already advance the group head?
What proof can other replicas store before applying the change?
Can an old replica replay from its head, or must it restore a snapshot?
```

PersonalDB uses SQLite session changesets for that boundary. A changeset is a compact record of row inserts, updates, and deletes, not a copy of the whole database file. Anvil decodes those changesets, validates their table effects against the registered schema, and stores the accepted bytes as part of a witnessed log.

## Groups and replicas

A **PersonalDB group** is one replicated SQLite history inside an Anvil storage tenant. The public API names it with a `database_id`. The group manifest records the schema hash, genesis hash, consistency policy, object layout version, active membership and policy epochs, row-index generation, projection generation, manifest hash, and manifest signature. At creation, Anvil writes a committed head at log index `0` whose log hash is the genesis hash.

A **replica** is a device or process with a local SQLite copy of that group. It should persist at least three pieces of sync state next to its SQLite file: the last applied log index, the last applied log hash, and the last processed watch cursor. A replica that loses those checkpoints cannot safely guess where it is; it must ask Anvil through catch-up or restore from a snapshot when the service tells it to.

The group is tenant-scoped. The request tenant must match the authenticated token tenant. A database id is treated as a safe identifier, not as a path; the current service rejects empty ids and ids containing `/` or `..`.

## Heads and the log chain

A PersonalDB head is the witness's statement of the latest accepted position. It contains a `log_index`, a `log_hash`, schema hash, policy epoch, membership epoch, row-index generation, segment path, update time, head hash, and signature. Every accepted commit advances the head by exactly one log index.

The log itself is a hash chain. A committed log record stores the previous log hash, the changeset payload hash, the verified mutation-envelope hash, the commit certificate hash, references to the changeset payload and certificate, and its own entry hash. Catch-up and repair use that chain to detect missing segments, non-contiguous indexes, mismatched previous hashes, missing payloads, and invalid certificates.

This is why PersonalDB does not branch silently. If a replica submits a changeset based on an old or different head, the service rejects it with a failed precondition instead of creating a second history. The safe client response is to catch up, rebase or merge locally according to the application protocol, and submit a new changeset from the current head.

## What the witness validates

Anvil is the witness for public PersonalDB commits. A submit request includes tenant id, database id, principal, session token, request id, idempotency key, base log index and hash, client log epoch, membership epoch, policy epoch, leader replica id, voter acknowledgements, changeset payload hash, changeset bytes, and optional debug metadata. The current API shape is deliberately explicit because the witness must reject spoofed or stale state.

The service binds the request to the authenticated caller. The request tenant must match the bearer token tenant. The request principal must match the authenticated subject. The request `session_token` must match the authenticated bearer token. This prevents a caller from putting another principal in the request body and having Anvil witness the change as that user.

The service then checks the commit evidence. Epochs must be non-zero and match the active group. The base log index and hash must match the committed head. The changeset payload hash must be the BLAKE3 hash of the exact changeset bytes. The changeset must be non-empty, below the configured size limit, decodable as a SQLite changeset, and limited to tables registered by the group's schema SQL. The current default maximum changeset size is 16 MiB, with a hard implementation cap of 128 MiB.

Before publishing, Anvil also acquires a PersonalDB partition write fence and rereads the committed head. That protects the commit path from a stale writer publishing after ownership has moved or the head has changed during handoff. This is the same general correctness family as the fence and CAS patterns explained in [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/), but applied to PersonalDB group history.

## Commit certificates

A successful witness decision returns more than `OK`. It returns a commit certificate. The certificate records the tenant, database id, log index, previous log hash, new entry hash, changeset payload hash, verified mutation-envelope hash, client log epoch, membership epoch, policy epoch, leader replica id, voter-acknowledgement hash, authorisation revision, witness identity, witness time, certificate hash, and witness signature.

Replicas should treat that certificate as durable evidence for the log entry they apply. If a client crashes after applying the changeset but before storing the certificate and head, it has lost important proof. A robust client stores the applied log index, log hash, and certificate evidence in the same recovery discipline as the local SQLite transaction that applied the changeset.

Certificates are not a conflict resolver by themselves. They prove that Anvil accepted one changeset at one position in the group history. Application code still decides how to transform local edits when a replica discovers that another commit already advanced the head.

## Authorisation and row effects

PersonalDB authorisation has two layers. Group-level actions decide who can create, read, commit to, or watch a group. The current public policy actions are `personaldb:create`, `personaldb:read`, `personaldb:commit`, and `personaldb:watch` on resources shaped like `tenant-<tenant_id>/<database_id>`. If a public policy scope does not allow a read, commit, or watch, the service can also check relationship authorisation on the `personaldb` kind with relations such as `reader`, `committer`, and `watcher`.

Commits also create row effects. Anvil derives a verified mutation envelope from the decoded SQLite changeset. Inserts require `personaldb:insert`, updates require `personaldb:update`, and deletes require `personaldb:delete` on a derived row resource. In the current implementation the source row binding is derived from the table name and primary-key hash: the resource type is the table name, and the row resource id is shaped from the table name plus primary-key hash. The public resource string checked for the row effect is `tenant-<tenant_id>/<database_id>/<resource_type>/<resource_id>`.

That means `personaldb:commit` on the group is not automatically permission to mutate every row. A caller must also be authorised for the row-level effects produced by the changeset. Projection definitions can introduce their own resource bindings and permission-aware filters for derived groups, but the same principle holds: the source history and derived views must not leak or mutate rows that the principal is not authorised to affect.

Authorisation revisions are included in certificates and watch events. That gives downstream consumers evidence about which permission view was used when the commit or projection event was produced. If your application writes relationship tuples and then commits PersonalDB rows that depend on those tuples, design the workflow so the relevant revisions are observable and stored.

## Catch-up and snapshots

Catch-up is the normal recovery path for a replica. The replica sends the log index and log hash it already has. If that position is on the chain, Anvil returns later log records, changeset bytes, certificates, certificate JSON, the current committed head, and a `has_more` flag. The replica applies returned changesets in log order to its local SQLite file, verifies and stores the certificate evidence it needs, and advances its local head only after each apply is durable.

If the supplied position is not on the chain, the service returns `snapshot_required=true` with a reason such as `divergent_replica`. If the committed head is missing, the reason is `missing_committed_head`. A snapshot-required response is not a normal empty result. It says the replica cannot safely replay from the state it claimed.

Snapshots are compact recovery points. The current snapshot builder can reconstruct a SQLite database by applying committed changesets, compress it with zstd, write a snapshot object and manifest, and publish a snapshots head. Snapshot creation is threshold-based: by default Anvil considers a new snapshot after 1024 committed entries or 64 MiB of committed changeset payload since the latest snapshot, unless the deployment overrides the PersonalDB snapshot thresholds.

The important exposure gap is restore. The API can tell catch-up callers that a snapshot is required and can return the snapshots head metadata, and the implementation stores snapshot manifests and compressed SQLite bytes internally. The current public CLI does not provide a snapshot download or restore command, and the public PersonalDbService does not expose a dedicated snapshot-fetch RPC. Production clients need an explicit API/client path for snapshot restore before they can rely on snapshots as a complete hands-on workflow.

## Watches and checkpoints

A PersonalDB watch tells consumers that the group history has moved. Group watch events carry a split `cursor_low` and `cursor_high`, event type, log index, log hash, changeset payload hash, certificate hash, committed head hash, authorisation revision, emitted time, and a watch envelope. The event is a signal and a checkpoint, not the full replay payload. Consumers that need to apply data should call catch-up from their stored log head.

The current group watch API first returns stored events after the requested cursor, then tails live events. The public CLI exposes this through both `anvil personaldb watch` and `anvil watch personaldb`. The CLI prints the cursor pair and event type, which is enough for manual tailing but not enough for a full synchroniser.

A consumer should store its last processed watch cursor only after its own side effect is durable. A projection monitor might store the cursor after updating a status record. A replica might store it after catch-up has applied and checkpointed the corresponding log head. If a live watcher falls behind the retained broadcast window, the service reports data loss; the correct recovery is to reopen catch-up from the durable log head and then restart the watch from a safe cursor.

Projection watches exist in the public API as `WatchPersonalDbProjection`. They report source log position, projection log position, definition hash, authorisation revision, and the projection watch cursor. The current public CLI does not expose a projection-watch command, so projection-specific workers should use the API directly.

## Projections are derived PersonalDB groups

A projection is not a magic SQL view inside Anvil. It is a derived PersonalDB group whose commits are built from source-group commits. A projection definition names source database ids, a target database id, a target actor or scope, table mappings, column mappings, row filters, resource bindings, and a writeback policy. The definition is canonicalised, hashed, and stored through CoreStore.

When a source commit is accepted, the projection builder looks for definitions that use that source. It decodes the source changeset, applies table and column mappings, evaluates row filters, checks relationship-authorisation conditions where the definition asks for them, builds a projection changeset, and commits that changeset into the target group with an internal projection-builder actor. The target group then has its own head, catch-up behaviour, watch events, certificates, and repair story.

This is useful when a server needs a smaller or permission-shaped SQLite dataset for sync. For example, a source group might contain all notes in a workspace, while a projection group contains only not-deleted notes visible to one service actor. The projection is derived state. If it lags or becomes inconsistent, the source commits and projection definition are the evidence needed to diagnose or rebuild it.

Writeback is supported only for projection definitions whose writeback policy allows mapped columns. Definitions with `deny` reject direct writeback. Writeback translates a changeset submitted to the projection group back into a source-group changeset, then witnesses that source commit. This should be treated as an API/client feature today because the current CLI submit helper does not expose the fields needed for a production submit loop.

## Repair and operational evidence

PersonalDB repair is conservative. The public repair path can assess a group's manifest, committed head, log segments, changeset payload references, and commit certificates. If it finds a problem, it records a repair finding for review. It does not silently invent missing commits, rewrite user history, or decide application conflicts.

The current public CLI surface is `anvil repair run personal-db <database_id>`, with findings read through the repair findings commands. Operators should pair that with [PersonalDB Operations](/operators/personaldb-operations/) and [Repair and Diagnostics](/tutorials/repair-and-diagnostics/). A clean repair result proves the checked log chain is internally consistent up to the committed head; it does not prove every client replica is current, every projection has caught up, or every row-level permission decision was what the product intended.

Operationally, watch the same source-versus-derived split that appears elsewhere in Anvil. The group log, changeset payloads, certificates, manifests, and heads are source evidence. Projection groups, row indexes, snapshots, and external caches are derived or recovery state. Derived state should be explainable from source records and should have lag, cursor, or finding evidence when it is not current.

## Current public surfaces and gaps

The current public API exposes group create/read, projection create/read, changeset submit, catch-up, group watch, and projection watch. The public CLI exposes `anvil personaldb group create`, `group read`, `projection create`, `projection read`, `changeset submit`, `catch-up`, and `watch`; the generic `anvil watch personaldb` command tails the same group watch surface.

There are important gaps to design around. The CLI `changeset submit` command is not a complete production submit helper today: it generates request and idempotency fields itself, sends an empty session token, and cannot provide voter acknowledgements, while the service requires the session token to match the authenticated bearer token and requires at least one voter acknowledgement. The CLI `group read` prints only a compact manifest line, not the full committed head. The CLI `catch-up` prints counts and booleans, not replayable changeset payloads and certificates. There is no public CLI projection-watch helper. Snapshot restore and snapshot download are not exposed as a complete public workflow. There is also no standalone, full PersonalDB projection-definition reference page comparable to the index JSON reference; today you must rely on the tutorial, proto, and source for the exact projection JSON shape.

Those gaps do not make PersonalDB conceptual only. They mean production application code should use the PersonalDbService API or a real client library rather than shell commands for synchronisation. Keep the CLI for manual inspection and smoke tests. Keep Anvil as the witness and durable history layer. Keep SQLite as the client database. Keep row effects, watches, projections, snapshots, and repair tied back to the committed log chain.

## What to take forward

PersonalDB is a local-first replication primitive, not an object bucket and not a hosted SQL database. A client edits SQLite locally, submits SQLite changesets, stores commit certificates, catches up from a known head, and tails watches for new history. Anvil validates and witnesses the ordered group log, enforces group and row-level authorisation, builds snapshots and projections, and records enough evidence for repair. The safest designs treat every replica, projection, and repair tool as a consumer of the same committed chain rather than as a second source of truth.

## PersonalDB versus object storage

Use ordinary objects when the source of truth is a file, document, model, package, or media payload. Use PersonalDB when the source of truth is an ordered sequence of SQLite changes accepted from local replicas. Storing a SQLite database file as an object can preserve bytes, but it does not provide commit witnessing, catch-up, projection records, or row-level authorisation evidence.

A PersonalDB incident should therefore be debugged as a log and witness problem: which replica submitted the changeset, what base log index it claimed, which commit certificate was returned, which snapshot generation is current, and which projection cursor has caught up.
