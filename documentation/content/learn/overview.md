---
title: Learn Anvil
description: A progressive introduction to Anvil for readers new to object storage, indexing, search, authorization, watches, and PersonalDB.
---

# Learn Anvil

**What this page gives you:** a map of Anvil's ideas before the rest of the documentation goes deeper. You can start here without knowing what an object store, index, vector search, watch stream, or Zanzibar-style authorization system is.

Anvil is a storage platform for software teams that need more than a place to put files. It begins with a simple promise: a client can put bytes under a name and read those bytes later. That is object storage. Many systems stop there. Anvil keeps that foundation and adds the capabilities application teams normally have to assemble around it: metadata indexes, full text search, vector search, relationship authorization, change streams, source and model artifact handling, and PersonalDB witnessing for local-first applications.

The reason those features belong together is not convenience alone. They all depend on the same facts. A search result should point at a real object version. A vector neighbor should not reveal data the caller cannot read. A directory listing should move forward when a write commits. A projection should prove which source events it consumed. An authorization decision should protect objects, search snippets, watch streams, and database projections, not only direct downloads. When these systems are separate, application code becomes the place where correctness is assembled. Anvil makes that correctness part of the storage layer.

## The story in one example

Imagine a product that stores project documents. The first feature is upload and download. A user uploads `contract.pdf`; another user opens it later. An ordinary object store can do that.

The second feature is a project timeline. The application needs to list recent files for one project. Now key design and metadata matter.

The third feature is filtering. Users want signed contracts for one customer. Now metadata indexes matter.

The fourth feature is search. Users type `payment terms` and expect relevant documents with snippets. Now full text indexing matters.

The fifth feature is semantic search. Users ask for documents similar to a policy draft. Now vector search matters.

The sixth feature is sharing. Some project files are private, some are shared with groups, and some are visible only while a legal hold is active. Now relationship authorization and caveats matter.

The seventh feature is live UI. A browser tab should update when another user uploads or edits metadata. Now watches and cursors matter.

The eighth feature is offline work. A desktop or mobile client should edit a local SQLite database and later sync. Now PersonalDB witnessing, snapshots, projections, and commit certificates matter.

Anvil is designed so these features are not separate islands. The same object identity, version, metadata, authorization model, and durable mutation stream flow through them.

## Core vocabulary

| Term | First meaning | Anvil meaning |
| --- | --- | --- |
| Bucket | A named container. | A policy and placement boundary for related objects. |
| Key | A name for one object. | A path-like identifier designed for listing, watches, authorization, and index scope. |
| Object | Stored bytes. | Bytes plus metadata, version, checksums, authorization context, and derived index inputs. |
| Metadata | Labels about an object. | Queryable fields that let applications find objects without reading every body. |
| Version | A particular state of an object. | The unit used for preconditions, consistency, indexes, and recovery. |
| Index | A shortcut for a query. | A maintained derived structure for paths, metadata, text, vectors, authz, source artifacts, or PersonalDB projections. |
| Watch | A stream of changes. | A durable ordered feed with cursors so derived systems can catch up without rescanning. |
| Tuple | A relationship fact. | A Zanzibar-style statement such as `document:123#viewer@user:amy`. |
| PersonalDB | Local-first database coordination. | A server witness for SQLite changesets, commit certificates, snapshots, and projections. |

A useful mental model is: **objects are source facts, indexes are maintained views, authorization gates every exposure, and watches connect durable changes to derived state.**

## What Anvil is not

Anvil is not a relational database replacement. Use a relational database when SQL joins, multi-row transactions, and relational constraints are the right abstraction. Anvil stores objects and derived views over objects; it can witness PersonalDB changesets, but it does not ask every application to model ordinary relational data as object blobs.

Anvil is not just an S3 endpoint. S3 compatibility is important because it lets existing tools move bytes. The native Anvil model includes features S3 does not express: index definitions, authorization schemas, watches, vector search, PersonalDB groups, source artifact manifests, and structured diagnostics.

Anvil is not a search engine bolted onto storage. Search is tied to object versions, metadata, source cursors, authorization revisions, and index readiness. That connection is what makes search safe enough for product data.

## How to read these docs

The Learn section is a progressive course. Each page introduces concepts before using Anvil-specific vocabulary.

1. **Object Storage** explains buckets, keys, objects, metadata, versions, and checksums.
2. **Keys And Paths** teaches how names and metadata become the first application index.
3. **Indexes And Search** introduces directory indexes, metadata indexes, full text search, vector search, and hybrid ranking.
4. **Authorization** explains authentication, authorization, scopes, relationship tuples, caveats, and safe search.
5. **Watches And Derived Data** explains cursors, lag, generations, replay, and repair.
6. **PersonalDB** explains local-first SQLite changesets, witnesses, snapshots, and projections.

After that, read the Developer guides to build applications and the Operator guides to run Anvil in production.

## The expert model you are building toward

By the end of the documentation, you should be able to reason about an Anvil deployment from both sides:

- as a developer, you can design keys, metadata, indexes, authorization relationships, S3-compatible imports, native API calls, and PersonalDB sync loops;
- as an operator, you can deploy nodes, configure identity, monitor index lag, diagnose authorization failures, rebuild derived indexes, back up durable state, and release server and client artifacts.

If you remember one sentence from this page, use this one: **Anvil is an object storage platform where search, indexing, authorization, watches, source artifacts, and local-first database witnessing are first-class parts of the storage system rather than separate application glue.**
