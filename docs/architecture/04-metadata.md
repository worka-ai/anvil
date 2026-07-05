---
title: CoreStore Metadata And Derived State
description: How Anvil represents metadata, indexes, watches, repair evidence, and derived state without a separate metadata database.
tags: [architecture, deep-dive, metadata, indexing, corestore]
---

# CoreStore Metadata And Derived State

Anvil metadata is not a separate database. It is a set of CoreStore records that describe buckets, object versions, object links, task state, authorisation state, index definitions, index generations, PersonalDB groups, source artefacts, gateway records, and operator diagnostics.

This matters because metadata is as critical as payload bytes. If a payload is durable but the object head, access record, or index generation is not, the system cannot recover correctly. CoreStore keeps those facts in the same durability and recovery model as the bytes they describe.

## Source records and derived records

Anvil separates source facts from derived views.

A source fact is the committed record that defines truth:

- an object metadata frame saying version `v12` was written;
- a bucket mutation frame saying a bucket was created;
- an authz tuple saying `document:42#viewer@user:amy` exists;
- a PersonalDB commit certificate;
- a gateway upload session finalisation;
- a mesh lifecycle control record.

A derived view is a maintained structure that makes reads fast:

- directory and path index segments;
- typed field and range index segments;
- full text posting segments;
- vector segment graphs;
- derived userset indexes;
- PersonalDB projection indexes;
- routing projections;
- diagnostics and lag records.

Derived views are rebuilt from source records. They are never the only copy of committed truth.

## Object metadata

Object metadata is written as stream records and refs:

```text
object write
  -> object metadata stream append
  -> current object CoreRef update
  -> object watch cursor
  -> index build task
```

The stream gives replay and audit. The current object ref gives fast head lookup. Sealed directory segments give fast listing. Watch cursors let indexes and clients catch up without scanning whole buckets.

Lists and heads use these maintained structures, but they must still respect reserved namespace rules and authorisation.

## Index metadata

Index definitions are source records. Index generations are published only after their segment objects and source cursor proof are durable. That creates a clean sequence:

```text
index definition committed
  -> worker reads source watch cursor
  -> worker writes segment CoreObjects
  -> worker writes generation head CoreRef
  -> query planner may use the generation
```

If a build fails, the old valid generation remains available. If a generation exists but the source cursor proof is missing, repair can detect and rebuild it.

## Authorisation metadata

Anvil uses one relationship-authorisation engine for system and tenant realms. Tuple logs, namespace schemas, caveat hashes, derived userset indexes, and authorisation lag records are CoreStore state.

Public callers cannot read or write reserved authz paths as objects. They interact through structured APIs: write tuples, check permissions, list authorised objects where permitted, and watch authorised changes. Query planners use the same authorisation state before exposing object keys, snippets, vector hits, or diagnostics.

## Repair metadata

Repair findings are records, not ad-hoc log lines. A repair finding describes the subject, evidence, severity, proposed action, and whether a rebuild is safe. Repair routines may rebuild derived state from source records, but they must not synthesize committed object, tuple, or PersonalDB truth.

## Operational metadata

Operator-facing state uses the same approach:

- admin audit events are CoreStreams;
- region, cell, node, host alias, and link lifecycle records are CoreStore-backed control records;
- routing projections are derived from lifecycle/control streams;
- diagnostics are durable and filterable;
- page cursors bind filters, revisions, and generations.

This keeps recovery and auditability consistent across the public data plane and the admin plane.

## Reserved namespace

`_anvil/` is not a user folder. It is the reserved namespace for Anvil-owned internal records. Public native and S3-compatible APIs must reject reads, writes, deletes, copies, lists, ranges, watches, multipart operations, append stream operations, and manifest operations that target reserved paths.

Administrators should use admin and native APIs for introspection. They should not expose internal paths through bucket grants.
