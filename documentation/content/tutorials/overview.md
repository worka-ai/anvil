---
title: Tutorials Overview
description: Read the Anvil tutorials as a guided path from local bootstrap to a complete object-backed application.
---

# Tutorials Overview

Think of this section as the opening chapter of a practical book. The tutorials do not start with search, gateways, or repair because those features only make sense once you understand the Anvil model underneath them: a private operator plane creates the system boundary, a public tenant plane owns application data, and durable object-native records are the source from which indexes, watches, streams, aliases, and diagnostics are derived.

Anvil is API-first. Production services should call the public gRPC APIs, generated clients, or deliberately wrapped application clients. The `anvil` CLI is a manual helper over tenant/public APIs. The private admin CLI is a manual helper over the private admin API. The examples use those CLIs because they make the request shape visible, but the CLIs are not the product architecture and they are not a substitute for carrying structured API values such as version ids, zookies, watch cursors, stream ids, lease fence tokens, repair findings, and idempotency keys in your own code.

If you are new to Anvil, read the pages in order. If you already operate Anvil, use this overview to find the chapter that matches the part of the system you are changing. If you are building an application on a hosted Anvil deployment, you can often start at [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/) or [Buckets and Objects](/tutorials/buckets-and-objects/), but you should still understand why tenant work stays on the public API and topology work stays on the admin API.

For the conceptual background, keep [Learn: Overview](/learn/overview/), [Object Model](/learn/object-model/), [Authorisation](/learn/authorisation/), [Indexes and Query](/learn/indexes-and-query/), [Watches and Derived Data](/learn/watches-and-derived-data/), [Gateways](/learn/gateways/), and [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/) nearby. For exact command and permission reference, use [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and [Index Definitions and Query JSON](/reference/index-definitions-and-query-json/).

## What you are building towards

The path ends with a small document system, but the document domain is only a teaching device. Along the way you learn the pattern Anvil expects for many application families: create a tenant boundary, use separate app credentials, write canonical objects, keep protected metadata small, version writes safely, grant relationship-based access, derive query and search indexes, process watches instead of rescanning, record event history in append streams, coordinate workers with leases and fences, expose public copies deliberately, and diagnose or repair derived state without pretending it is the source of truth.

A developer should read each tutorial asking: what API would my service call, what state is canonical, what values must I persist for retry and concurrency, and what does the CLI example prove or fail to prove? An operator should read each tutorial asking: which plane owns this operation, which principal is authorised, what evidence is produced, what can safely be repaired, and what must stay private?

Those questions matter because many examples have intentionally narrow authority. A permission denied result is often the correct lesson, not a broken tutorial.

## Before you begin

Use a scratch directory for local files, generated JSON, downloaded objects, and temporary credentials. The repository should not become your tutorial workspace.

```bash
mkdir -p /tmp/anvil-tutorial
cd /tmp/anvil-tutorial
```

Most tenant examples assume an `acme` public CLI profile and bearer token created by the early tutorials. Many operator examples assume a local `anvil-local` container and an admin bearer token passed with `docker exec`. If you start in the middle and a command cannot authenticate, go back to the setup, bootstrap, and tenant handover chapters rather than widening permissions blindly.

## The first part: bring up a safe boundary

Start with [Run Anvil Locally](/tutorials/setup-local-anvil/). It teaches what the local process exposes, which ports are public versus private, where storage lives, and why a local instance is still a real Anvil deployment rather than an in-memory demo. This is where developers learn how to get a server to talk to, and operators learn which environment variables and listeners deserve attention.

[Bootstrap Administration](/tutorials/admin-bootstrap/) comes next because the first credential is not tenant-owned yet. It teaches how the private admin API gets its initial system administrator and why bootstrap credentials should be treated differently from tenant app secrets. The lesson is not "use admin for everything"; it is "use admin only until there is a tenant credential to hand over".

[Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/) introduces the topology vocabulary. Even a local single-process setup has a region, cell, node, capabilities, and lifecycle state. That vocabulary explains later placement failures, cross-region routing decisions, and repair diagnostics. You do not need a production mesh to benefit from learning the shape early.

[Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/) is the handover chapter. Operators create the storage tenant and first tenant app through the private admin API. From there, tenant-side app creation, token minting, and narrow public policy grants move to the public API. This page is the foundation for least privilege throughout the rest of the book.

## The second part: model documents as objects

[Buckets and Objects](/tutorials/buckets-and-objects/) teaches the first tenant-owned data operation. A bucket is an operational boundary inside one tenant; an object key is a stable name inside that bucket; an object body is the canonical bytes for one current version. The page also explains a current local gap: the public CLI upload helper discovers bucket ids through `ListBuckets`, which is not yet ideal for a least-privilege write-only path.

[Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/) separates three places data can live: the object body, protected object metadata, and derived typed index fields. That distinction prevents a common design mistake: copying business state into metadata or projection objects just to make queries easy. The body remains canonical; metadata stays compact and operational; indexes are rebuildable views.

[Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/) teaches the concurrency model. Object writes create versions and move a current pointer. API callers can use version or ETag preconditions so stale edits fail instead of silently overwriting newer work. The same page introduces object links as symlink-like aliases inside a bucket: useful for stable names, latest artefacts, and static-site roots, but not copies of the target payload.

## The third part: put access decisions in the right layer

[Authorisation](/tutorials/authorisation/) separates public policy scopes from relationship authorisation. Public policy scopes decide whether a tenant app may call an API operation such as `object:write`, `index:create`, or `authz:tuple_write`. Relationship tuples decide whether an application subject is an owner, viewer, member, or other relation on a product object. The page also introduces schemas, schema bindings, usersets, checks, revisions, and zookies at a tutorial level.

[Public Access](/tutorials/public-access/) explains public-read as a deliberate data-plane policy, not an authorisation bypass. Public means anyone who can reach the public surface can read matching data. It does not make the admin API public, does not grant writes, and should normally be used with dedicated public buckets or carefully reviewed content.

This part is where developers should be especially careful. A working read can be allowed by a token scope, a relationship tuple, or public-read policy. Those are different reasons. Keep them visible in logs and application decisions.

## The fourth part: derive views without losing the source

[Watches](/tutorials/watches/) teaches how consumers avoid rescanning. Watches emit ordered changes with cursors. A worker stores the last cursor only after its own side effects are durable, then resumes from that cursor after restart. This is the bridge between object writes and derived maintenance.

[Path, Metadata, and Typed Query Indexes](/tutorials/indexes-path-metadata-and-typed-query/) teaches the first query indexes. It explains `selector_json`, `extractor_json`, `build_policy_json`, and query JSON in a flow rather than as unexplained blobs. It also introduces query visibility: an index reader still may not see every object when the index uses `inherit_object`.

[Full-Text Search](/tutorials/full-text-search/) adds token-based search over selected text fields. It explains tokenisation, phrase queries, path and metadata narrowing, and the current direct query limit: `query_text` is not a boolean expression language.

[Vector Search](/tutorials/vector-search/) adds embedding-based similarity. It teaches dimensions, metrics, normalisation, extractor choices, HNSW at a high level, and the difference between caller-supplied vectors and provider-generated vectors. It also says plainly that deterministic/test embeddings are not production-quality embeddings.

[Hybrid Search](/tutorials/hybrid-search/) combines lexical and vector signals in one authorised index. It explains when hybrid ranking is useful, what current fixed scoring weights do, where metadata and path filters fit, and why query vectors still have to come from the same model space as the indexed vectors.

For all search pages, remember that indexes are derived data. An object write can commit before a segment is built. Some query paths support watch-cursor catch-up today; full-text, vector, and hybrid catch-up remains a current limitation that you should treat as operational lag rather than proven freshness.

## The fifth part: record history and coordinate workers

[Append Streams and Audit Logs](/tutorials/append-streams-and-audit-logs/) teaches ordered event records. Objects are good for current state; append streams are good for histories, audits, replays, and worker inputs. This page explains stream identity, record sequence, idempotency support in the API, reading, tailing, segment sealing, and why sealing is not logical stream closure.

[Task Leases and Fenced Mutations](/tutorials/task-leases-and-fenced-mutations/) teaches worker ownership. A task lease tells workers who currently owns a unit of work. A fence token lets the API reject stale workers. The CLI exposes lease lifecycle commands, while correctness-sensitive fenced data mutations currently require direct API use through mutation batches and write preconditions.

[PersonalDB](/tutorials/personaldb/) explains how Anvil participates in PersonalDB groups, replicas, commits, heads, witnessing, projections, watches, and recovery. Read it if your application uses SQLite-shaped local state that needs witnessed replication rather than ordinary object replacement. It is intentionally separate from object storage because the correctness model is different.

## The sixth part: expose protocol surfaces deliberately

[S3-Compatible Gateway](/tutorials/s3-gateway/) shows S3 compatibility as a gateway over the Anvil core model. It maps common object operations to buckets and keys, but it does not turn S3 into the core security model. Credentials, bucket policy, object visibility, versioning limits, metadata behaviour, and public-read semantics still come from Anvil.

[Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/) shows object-backed static delivery through host routing and tenant-owned host aliases. It ties together public-read policy, `PUBLIC_REGION_BASE_DOMAIN`, custom host records, object links, latest-file aliases, dangling links, and follow-versus-metadata behaviour. It also repeats an important operational rule: exposing S3 or static hosting does not require exposing the private admin API.

[Package Gateway Foundations](/tutorials/package-gateway-foundations/) is a planning tutorial, not a claim that Docker, npm, PyPI, Maven, or Rust registry gateways are already implemented. It shows how to model package artefacts today with objects, immutable version keys, mutable channel links, checksums, metadata, indexes, public access, and S3/native API publishing while keeping future registry adapters honest.

## The seventh part: operate the system you created

[Mesh Routing and Lifecycle](/tutorials/mesh-routing-and-lifecycle/) returns to the operator plane. It explains regions, cells, nodes, placement, bucket home regions, routing records, cross-region redirect/proxy/reject policy, lifecycle states, host routing, draining, and current activation-checkpoint gaps. Developers should read it to understand why a placement or routing precondition can fail; operators should read it before changing topology.

[Repair and Diagnostics](/tutorials/repair-and-diagnostics/) teaches safe triage. Diagnostics are read-only evidence. Repairs are explicit actions that may write findings or rebuild derived state. The page separates tenant/public repairs from private admin repairs and explains what repair does not prove: it does not make source data correct, bypass authorisation, or guarantee every downstream consumer has caught up.

## The final chapter: assemble the pieces

[End-to-End Document System](/tutorials/end-to-end-document-system/) is the capstone. It does not introduce a new primitive. Instead, it shows how the previous chapters fit together for a realistic document workflow: tenant apps, canonical documents, metadata, relationship access, versioned writes, links, indexes, search, watches, append streams, leases, public copies, diagnostics, and repair.

If you can explain each part of that document system, you understand the shape of Anvil application design: source records first, derived views second, API values carried explicitly, and operations split cleanly between tenant and operator planes.

## Current gaps to keep in mind

Several pages intentionally call out implementation or documentation gaps instead of pretending the happy path always runs locally. Region activation currently depends on an activation-checkpoint workflow that is not yet exposed as a friendly operator command. Some public CLI helpers are broader or thinner than the API: object uploads cannot yet set all metadata fields or CAS preconditions, object head does not print version ids, repair finding pagination is limited, and mutation batches are API-only. Full-text, vector, and hybrid search have current catch-up limitations. Package gateways beyond S3 are foundations rather than implemented protocol adapters.

Those gaps do not weaken the learning path. They are part of the learning path. Anvil documentation should teach what the system does today, what an API-primary application should persist and check, and where an operator should stop rather than inventing an unsupported command.

## How to know you are ready to move on

After each tutorial, ask four questions. What durable state changed? Which principal was authorised to change it? Which API plane owned the operation? What evidence would I use to debug or reverse it later?

If you can answer those questions, continue. If you cannot, reread the page before adding more permissions or moving to the next feature. Anvil is designed so that storage, authorisation, indexes, watches, and repair evidence form one explainable system. The tutorials are written to teach that system in order.
