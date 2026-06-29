---
title: Overview
description: A progressive learning path for Anvil, starting from first principles.
---

# Overview

**What this page achieves:** by the end you should know what Anvil is, why it exists, and how to read the rest of the documentation in an order that builds real understanding.

Anvil is a storage platform for applications that need durable objects, fast discovery, permission-aware search, real-time change streams, and local-first database coordination in one place. It starts from the familiar idea of an object store: clients put bytes under names and later fetch them back. Then it adds the pieces application teams usually attach around that store later: metadata indexes, full text search, vector search, relationship authorization, watch streams, source artifact handling, and PersonalDB witnessing.

The reason those features belong together is correctness. If an application writes a document to one system, indexes it in another, stores permissions somewhere else, streams notifications through a fourth service, and syncs local database state through a fifth service, the application is now responsible for keeping all those systems consistent. That is usually where bugs appear: stale search results, leaked private object names, projections that miss events, or local databases that disagree about which write won.

Anvil gives those concerns one mutation path. A write becomes durable object state, version metadata, authorization context, watch events, and index input. A search result is filtered through the same authorization model used by direct reads. A PersonalDB commit is witnessed beside the object and index machinery that can project it into queryable data.

## The mental model

Start with six concepts. Every later page expands one of them.

| Concept | Plain meaning | Anvil meaning |
| --- | --- | --- |
| Object | Named bytes plus facts about those bytes. | The durable unit stored in a bucket under a key, with versions, metadata, hashes, and policy. |
| Bucket | A boundary around related objects. | A namespace with its own policies, index definitions, retention rules, and operational controls. |
| Key | The name of an object. | A path-like identifier that can be designed for fast listing, watches, and authorization boundaries. |
| Index | A shortcut for answering questions quickly. | A derived structure for path listings, metadata filters, full text search, vector search, source artifacts, authorization, and PersonalDB projections. |
| Authorization tuple | A statement about who relates to what. | A Zanzibar-style relationship fact such as `document:123#viewer@user:amy`. |
| Watch | A stream of durable changes. | The mechanism that keeps derived systems current without rescanning entire buckets. |

A useful way to think about Anvil is: **objects are the source of record, indexes are maintained views, authorization is evaluated before exposure, and watches connect source changes to every derived view.**

## A simple example

Imagine a product that stores contracts for many customers. The first requirement sounds easy: upload a PDF and let people download it. A basic object store can do that. The next requirements appear quickly:

- list all contracts for one customer and project;
- filter contracts by status, expiry date, and renewal owner;
- search inside extracted text;
- find contracts semantically similar to a clause description;
- prevent users from discovering documents they cannot access;
- show a live activity timeline when a contract changes;
- keep a local-first application database synchronized with accepted commits.

With separate systems, every one of those requirements adds glue code. With Anvil, the object key, metadata, index definitions, authorization tuples, watch cursors, and PersonalDB witness records are all part of one storage architecture.

## What Anvil is not

Anvil is not a traditional filesystem. It does not expose POSIX directory semantics. It uses buckets and keys because large distributed systems need stable object identity, immutable versions, checksums, range reads, and prefix listing rather than local file handles.

Anvil is not a relational database replacement. It does not try to replace joins, SQL transactions, or relational modeling. It stores and indexes application objects and can witness PersonalDB changesets. Use a relational database when relational transactions are the right abstraction. Use Anvil when durable objects, derived indexes, authorization-aware search, and local-first data coordination are the core problem.

Anvil is not a search engine bolted onto storage. Search in Anvil is tied to object versions, metadata, authorization revisions, and watch-driven maintenance. That is why a query result can be traced back to the object and source cursor that produced it.

## How to read this guide

Read the Learn section in order if you are new to any concept:

1. **Object Storage** explains buckets, keys, objects, versions, checksums, and metadata.
2. **Keys, Paths, And Metadata** shows how good key design makes applications easier to build and operate.
3. **Indexes And Search** teaches indexing, full text search, vector search, and hybrid ranking.
4. **Authorization** introduces authentication, authorization, scopes, Zanzibar-style tuples, caveats, and safe search.
5. **Watches And Derived Data** explains how indexes and projections stay current.
6. **PersonalDB** explains local-first SQLite changesets, witnesses, projections, and conflict boundaries.

After that, use the Developer guides to build applications and the Operator guides to run Anvil in production. The Reference section exists for exact settings, commands, packages, and errors after the mental model is clear.

## What you can do after this page

You should be able to explain Anvil in one sentence: **Anvil is an object storage platform where indexing, search, authorization, watches, and PersonalDB witnessing are first-class parts of the write and read path.**

Next, learn the object-store model from first principles.
