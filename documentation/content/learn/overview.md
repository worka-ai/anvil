---
title: Learn Anvil
description: A first-principles introduction to Anvil's object-native storage model, API planes, CoreStore foundation, derived views, authorisation, gateways, and operation.
---

# Learn Anvil

This Learn section is the conceptual book for Anvil. The tutorials show you how to perform operations. Learn explains why those operations are shaped the way they are, what state they create, and how to reason about correctness when objects, indexes, watches, gateways, regions, and authorisation all meet.

You do not need to arrive as an object-store expert. This book assumes you may be learning object keys, control planes, data planes, indexes, watches, Zanzibar-style relationships, regions, cells, nodes, and CoreStore for the first time. The aim is to give junior-to-mid developers and operators a shared language before they copy a command, design a schema, grant a permission, expose a gateway, or repair a derived index.

If you want hands-on steps, start with [Tutorials Overview](/tutorials/overview/). If you need implementation-level storage details for review or contribution, use [Architecture Overview](/architecture/overview/) and [Release Architecture Status](/architecture/release-status/). If you need exact command syntax, use [Public CLI](/reference/public-cli/) and [Admin CLI](/reference/admin-cli/). If you need permission strings, use [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/). This page is the map for the concepts those references assume.

## What Anvil is

Anvil is an API-first, object-native storage system for applications that need more than "put this file somewhere". It stores tenant-scoped objects and versions, records metadata, emits watches, builds query and search indexes, enforces authorisation, exposes gateway protocols, participates in PersonalDB witnessing, and gives operators topology and repair surfaces.

The important phrase is **object-native**. Anvil starts with durable application objects: bytes, metadata, keys, versions, and current pointers. Around those objects it provides the features applications usually bolt on separately: listing, links, compare-and-swap, append histories, typed queries, full-text search, vector search, relationship checks, public/static delivery, and operational diagnostics. Those features are meant to share one storage and authorisation model rather than becoming a collection of unrelated side databases.

Anvil is not just S3. S3 compatibility is useful because existing tools know how to upload and download objects, but S3 is a gateway over Anvil's model, not the model itself. The native API expresses things S3 does not: relationship-aware queries, typed index definitions, watch cursors, append stream records, task leases, fenced mutation batches, object links, PersonalDB groups, and mesh lifecycle operations. When a gateway cannot express a correctness requirement, the native API is the source of truth.

Anvil is also not a relational database with object-shaped rows. It can store JSON, index fields, and query derived views, but it is not designed around SQL joins or multi-table constraints. If your central problem is highly normalised ad hoc relational querying, Anvil is probably not the only store you need. If your central problem is durable objects plus safe metadata, search, watches, authorisation, event history, gateway delivery, and regional operation, Anvil is the system these pages explain.

## Why API-first matters

API-first does not mean "there is a CLI for everything". It means the public API and the private admin API are the product contract. Production services should carry structured request and response values: version ids, ETags, idempotency keys, zookies, watch cursors, stream ids, lease fence tokens, page tokens, audit ids, and repair finding ids. Those values let a service retry safely, reject stale writes, resume from a known point, prove a derived view caught up, and explain what happened during an incident.

The CLIs exist to make those APIs visible by hand. `anvil` is a helper for tenant/public API work: buckets, objects, authz tuples, indexes, watches, streams, leases, host aliases, public policy, and tenant diagnostics. `anvil-admin` is a helper for private operator work: tenant bootstrap, first credentials, policy handover, regions, cells, nodes, routing, admin audit, and system repairs. Neither CLI should become a back door in application design.

This split is one of Anvil's core safety properties. Tenant applications should not need the admin API to publish a document, write an object, create an index, or manage tenant-owned aliases. Operators should not use tenant object APIs to mutate mesh topology or system routing. When a workflow seems to require crossing that boundary, pause and identify which API plane really owns the operation.

## Public and admin planes

Anvil has a **public plane** and an **admin plane**.

The public plane is where tenant applications work. A tenant principal authenticates, receives a bearer token, and calls APIs for objects, buckets, tenant app credentials, public policy grants, relationship authorisation, indexes, watches, append streams, leases, PersonalDB, S3/static delivery, and tenant-owned host aliases. Public policy scopes and relationship tuples decide what that tenant principal can do or see.

The admin plane is where operators manage the system boundary. It creates tenants before tenant credentials exist, provisions the first tenant app, grants initial handover scopes, manages regions and nodes, inspects routing records, runs system diagnostics and repair, and reads admin audit. It is private by design. Exposing S3, static hosting, or the public API is not a reason to expose the admin API.

A developer can read the book as a guide to designing service calls. An operator can read it as a guide to understanding state, routing, lifecycle, and repair. Both readers need the same mental model because an incident rarely respects team boundaries: a missing search hit could be an object visibility issue, an index lag issue, a tuple issue, a region placement issue, or a repair issue.

## Source records and derived views

A storage system becomes hard to debug when it forgets which data is source and which data is derived. Anvil keeps that distinction visible.

Source records are the things the system treats as durable truth for a feature: object versions and current pointers, bucket and tenant records, append stream records, relationship tuples and schemas, PersonalDB commits and heads, mesh lifecycle and routing control records. These records are written intentionally and should be protected by authorisation, validation, idempotency, and preconditions.

Derived views are maintained from source records: directory indexes, path indexes, metadata and typed JSON indexes, full-text segments, vector segments, hybrid rankings, derived authorisation userset indexes, routing projections, diagnostics, repair findings, and application-maintained projections. Derived views can lag, fail to build, need repair, or be rebuilt. They should make reads faster or easier, but they should not become a second source of truth.

This is why watches and cursors appear throughout the book. A watch cursor gives a consumer a point in the source history. A derived query that can prove it has caught up to that cursor is stronger than a query that merely returns rows. When catch-up is not implemented for a query path yet, the documentation should say so, and applications should treat results as eventually derived rather than instantly fresh.

## CoreStore underneath

CoreStore is Anvil's unified durable substrate. You do not need to understand the Rust implementation to read this book, but you should understand why CoreStore exists. Without a shared substrate, object bodies might have one recovery story, indexes another, authz another, gateway state another, and mesh lifecycle another. Repair and security would then have to understand every feature's private storage rules.

CoreStore gives Anvil common building blocks: immutable blobs, refs, streams, transactions, fences, watches, and root catalog state. Feature code still has schemas and record formats, but durable Anvil state is meant to become CoreStore-backed records, blobs, refs, or streams. [CoreStore](/learn/corestore/) is the chapter that explains those building blocks in more detail.

The practical consequence is simple: if a feature is durable Anvil state, it should be recoverable, watchable, repairable, and authorisable through the same broad model. Gateways and indexes should not smuggle in a separate source of truth.

## A small vocabulary before the rest of the book

A **tenant** is the top-level storage and authorisation boundary. A tenant may represent a customer, environment, workspace, or product boundary. It is not the same as an end user.

A **bucket** is a named container inside a tenant. Use buckets for durable operational boundaries such as policy, placement, gateway exposure, indexing strategy, and recovery scope.

An **object key** is the stable name of an object inside a bucket. Keys often look like paths, but they are not filesystem paths. Prefixes matter because they support listing, watching, gateway routing, and index selection.

An **object version** is one committed state of an object. Ordinary reads follow the current pointer. Safe writers use version, ETag, manifest, or lease-fence preconditions to avoid overwriting somebody else's work.

A **watch** is an ordered feed of committed changes. Consumers store checkpoints and resume from cursors rather than rescanning everything.

An **index** is a derived acceleration structure. Anvil supports path, metadata-filter, typed JSON, full-text, vector, and hybrid index families. Index results must respect visibility, because object keys and scores can be sensitive.

A **relationship tuple** is an authorisation fact such as "user-17 is a viewer of document X". Public policy scopes decide who may call the API; relationship tuples decide product-level access between subjects and objects.

A **gateway** translates an outside protocol into the Anvil model. S3 and static hosting are gateway surfaces. Package gateway work is currently documented as foundations and modelling guidance, not as a claim that every registry protocol is implemented today.

A **region**, **cell**, and **node** describe placement and operation. Regions are routing and placement boundaries; cells are capacity or failure boundaries inside regions; nodes are Anvil processes with advertised capabilities.

A **PersonalDB group** is a local-first SQLite-oriented data group whose changesets, commits, heads, projections, and watches Anvil can witness and store. It is related to object storage but has a different correctness model.

## How to read the Learn book

Read [Object Model](/learn/object-model/) first. It teaches tenants, buckets, keys, bodies, metadata, versions, links, and reserved prefixes. Almost every later concept refers back to this model.

Then read [CoreStore](/learn/corestore/) to understand why durable state flows through one substrate, and [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/) to understand placement and topology. These pages prepare you for the practical setup and mesh tutorials: [Run Anvil Locally](/tutorials/setup-local-anvil/), [Bootstrap Administration](/tutorials/admin-bootstrap/), [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/), and [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/).

Next, read [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/) and [Reads, Listing, and Links](/learn/reads-listing-and-links/). Together they explain current pointers, pinned versions, compare-and-swap, idempotency, list visibility, and aliases. The matching tutorials are [Buckets and Objects](/tutorials/buckets-and-objects/), [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/), and [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/).

After that, read [Watches and Derived Data](/learn/watches-and-derived-data/) and [Indexes and Query](/learn/indexes-and-query/). These chapters teach how Anvil avoids broad rescans and how derived query structures are built, queried, filtered, diagnosed, and repaired. The practical sequence is [Watches](/tutorials/watches/), [Path, Metadata, and Typed Query Indexes](/tutorials/indexes-path-metadata-and-typed-query/), [Full-Text Search](/tutorials/full-text-search/), [Vector Search](/tutorials/vector-search/), and [Hybrid Search](/tutorials/hybrid-search/).

Then read [Authorisation](/learn/authorisation/). Do not postpone it until the end. Every read, list, query, gateway response, and repair view can expose information. The corresponding tutorial, [Authorisation](/tutorials/authorisation/), separates public policy scopes from relationship authorisation and introduces schemas, tuples, usersets, checks, and zookies.

Read [Gateways](/learn/gateways/) when you want Anvil to speak another protocol. Pair it with [Public Access](/tutorials/public-access/), [S3-Compatible Gateway](/tutorials/s3-gateway/), [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/), and [Package Gateway Foundations](/tutorials/package-gateway-foundations/). The lesson is always the same: a gateway adapts a protocol; it does not replace Anvil's storage, routing, or authorisation model.

Read [PersonalDB](/learn/personaldb/) if your application needs local-first SQLite replication, witnessing, changesets, projections, or recovery. Pair it with [PersonalDB](/tutorials/personaldb/) when you want the current CLI/API surfaces and gaps.

Finally, read [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/) and [Choosing the Right Primitive](/learn/choosing-the-right-primitive/). These chapters help operators reason about placement, lifecycle, routing, draining, proxying, and operational trade-offs, and they help developers decide whether an object, append stream, index, lease, PersonalDB group, or gateway is the right primitive for a product problem.

## Current honesty

The Learn book describes the model Anvil is built around and points out current limitations where they matter. Some hands-on tutorial flows are affected by implementation or documentation gaps: region activation needs a friendlier checkpoint workflow, some CLI helpers do less than the API or require broader helper access, mutation batches and some fenced writes are API-only, and full-text/vector/hybrid catch-up does not yet provide the same proof as metadata-backed and typed JSON paths. Package gateway pages are foundations unless a specific protocol adapter is implemented.

This honesty is intentional. A book-quality storage guide should not make a junior developer believe every derived view is instantly fresh, every CLI helper is production-complete, or every gateway is equivalent to the native API. It should teach the shape of the system, the current contract, and the evidence you need when something fails.

## What to take forward

Anvil gives you a way to build object-backed applications where bytes, metadata, versions, access, search, events, and operation belong to one explainable model. The public API is where tenants and applications work. The admin API is where operators manage the system boundary. Source records are protected durable truth. Derived views are useful, rebuildable, and sometimes lagging. Gateways are edges. CoreStore is the substrate underneath.

If you keep those distinctions clear, the rest of Learn becomes much easier: every chapter is a deeper explanation of one part of the same system.

## Reading the model as a sequence

A useful way to read the Learn book is to follow one document through the system. First the object model names the document as a bucket/key/version. The write chapter explains the mutation context that made the version visible. The read chapter explains how current pointers, range reads, listings, and links find it again. The authorisation chapter explains which app or product subject can see it. The index chapter explains how a derived search hit is built from the object source record. The watch chapter explains how builders and applications catch up without rescanning. The gateway chapter explains how S3 or static HTTP maps back to the same object. The topology chapters explain which region, cell, and node serve the request.

If a concept seems abstract, ask two questions: where is the source record, and which plane is allowed to mutate it? Those answers usually determine the correct API, CLI, failure mode, and repair path.
