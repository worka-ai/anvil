---
title: CoreStore: one storage layer for objects, search, permissions, and watches
slug: /blog/corestore-unified-storage/
description: Anvil now persists objects, metadata, indexes, authorisation, PersonalDB, mesh control, and gateway records through one CoreStore architecture.
---

# CoreStore: one storage layer for objects, search, permissions, and watches

Object storage starts with a simple promise: put bytes somewhere durable and read them later. Production systems need more than that. They need lists, metadata filters, full text search, vector search, relationship permissions, live change streams, package records, audit trails, and database sync witnesses. If each feature brings its own durable storage path, the product becomes a collection of small databases that happen to sit next to an object store.

This release changes Anvil's centre of gravity. CoreStore is now the internal durability boundary for every authoritative feature record. Objects, metadata, indexes, authorisation, PersonalDB, mesh lifecycle, task leases, gateway records, and audit records all map onto the same primitives.

## The design in one sentence

Anvil stores immutable bytes as `CoreObject`s, ordered facts as `CoreStream`s, and mutable heads as `CoreRef`s.

That sounds small, but it is the main scale decision in the release.

- A payload, index segment, source pack, package blob, or PersonalDB snapshot is immutable bytes, so it is a `CoreObject`.
- A bucket mutation, object version event, authz tuple, task queue entry, append-stream record, or audit event is ordered history, so it is a `CoreStream`.
- A current object head, index generation, PersonalDB head, gateway tag, or ownership state is a mutable pointer, so it is a `CoreRef`.

Feature-specific encodings still exist. A vector segment is not the same bytes as a full-text posting segment. The difference is that both are stored and recovered through the same CoreStore rules.

## Why this is better

The practical benefit is that durability, replication, fencing, watches, and repair are no longer reinvented per feature.

When a client writes an object, Anvil stores the body as a CoreObject and publishes the metadata and current-object head through a mutation batch. When an indexer builds a generation, it writes segment CoreObjects and publishes the generation head only after source cursor proof is durable. When a worker updates protected state, it carries a fence token and the batch rejects stale ownership before visible state changes. When a query returns results, it intersects index candidates with authorisation before exposing keys or snippets.

The same pattern applies across the system. That gives operators one recovery model and gives developers a safer product surface.

## What changed internally

The release moves durable state for these feature families onto CoreStore:

| Feature family | CoreStore mapping |
| --- | --- |
| Object payloads | Immutable CoreObjects and quorum-readable manifests. |
| Object metadata | CoreStream journals, current-object CoreRefs, and sealed directory segments. |
| Buckets | CoreStream bucket journals and control refs. |
| Append streams | CoreStream entries and sealed segment CoreObjects. |
| Task leases | CoreRefs, fences, and stream records. |
| Authorisation | Namespace schemas, tuple logs, derived userset indexes, and lag watches. |
| Search indexes | Typed field, full-text, vector, and hybrid segment CoreObjects. |
| PersonalDB | Changeset payloads, commit certificates, snapshots, projections, and watch records. |
| Mesh lifecycle | Region, cell, node, routing, host alias, link, and drain control records. |
| Gateway records | Mounts, credentials, repositories, blobs, tags, upload sessions, challenges, and audits. |
| Audit and repair | Durable streams and findings instead of ad-hoc local state. |

The implementation follows RFC 0006: CoreStore is the only durable persistence substrate, production indexes are materialised, authorisation participates in query planning, and gateway protocols do not define the storage model.

## Query results are permission-aware

Search systems often retrieve a large candidate set and then filter it down in application code. That is a disclosure risk. It can leak counts, timings, page shapes, or object identifiers even when the final list looks correct.

Anvil's query planner now treats authorisation as part of the plan. Path, typed field, full-text, vector, and hybrid queries carry source IDs and authorisation labels. The planner intersects candidate sets with permission sets before returning keys, snippets, scores, or page tokens. Page tokens are bound to the query shape, principal context, authz revision, mesh context, and index generations.

This is what makes search safe enough for protected product data.

## Gateway-neutral by design

S3 compatibility is useful, but S3 is only one protocol. Anvil's internal model now treats gateways as protocol adapters over Anvil resources. A gateway mount maps a host or route into an Anvil tenant, authz scope, bucket, and repository prefix. Gateway credentials identify a principal and gateway kind. Gateway uploads, blobs, tags, and audits are CoreStore records.

The public gateway surface in this release is S3-compatible object access. The same CoreStore gateway foundation covers static host aliases, object links, container registry records, Rust crate registry records, npm package records, PyPI records, and Maven records as they are exposed through protocol handlers.

## How operators should think about the release

`STORAGE_PATH` is durable CoreStore state. It is not a cache. It contains the roots, manifests, shards, refs, streams, sealed segments, and feature records needed to recover the system.

`ADMIN_LISTEN_ADDR` is the internal administrative plane. Keep it on a private network. The `admin` CLI talks to this listener for tenant creation, application provisioning, policy grants and revokes, region and node lifecycle, repair, diagnostics, audit listing, and secret key rotation. It does not write directly into the storage directory and it does not need the server's secret encryption key.

`API_LISTEN_ADDR` is the public data plane for native API and S3-compatible traffic. Public object access is still subject to authentication, policy, relationship authorisation, reserved namespace denial, and gateway rules.

## How to try it

Create a tenant and app through the admin API:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"

admin --host http://127.0.0.1:50052 tenant create \
  --name acme \
  --home-region eu-west-1 \
  --audit-reason "create acme tenant"

admin --host http://127.0.0.1:50052 app create \
  --tenant-id acme \
  --app-name docs-writer \
  --audit-reason "create docs writer app"

admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id acme \
  --app-name docs-writer \
  --action object:write \
  --resource 'documents/*' \
  --audit-reason "allow document uploads"
```

Then configure the application CLI and write objects normally:

```bash
anvil-cli static-config \
  --name acme \
  --host http://127.0.0.1:50051 \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default

anvil-cli bucket create documents eu-west-1
anvil-cli object put ./contract.txt s3://documents/contracts/contract-42.txt
anvil-cli object head s3://documents/contracts/contract-42.txt
```

The user-facing commands are ordinary. The important change is that the bytes, metadata, watches, indexes, permissions, and recovery evidence now share one coherent storage architecture.
