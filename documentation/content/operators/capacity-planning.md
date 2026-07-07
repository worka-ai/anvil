---
title: Capacity Planning
description: Plan Anvil capacity across tenants, buckets, objects, CoreStore, indexes, watches, PersonalDB, gateways, and operational headroom.
---

# Capacity Planning

Capacity planning for Anvil is not only a disk-sizing exercise. A deployment can run out of comfortable operating room because it has too many small object metadata records, too many watch consumers, a vector index that cannot fit its working set, a public gateway that saturates network egress, a PersonalDB projection backlog, or a repair that has no spare CPU in which to rebuild derived state.

The useful question is not "how many terabytes do we have?" but "which part of the system becomes the bottleneck for this tenant mix, and what evidence will show it before users feel it?" Anvil stores source records in CoreStore, builds derived views such as indexes and projections, exposes those records through native and gateway APIs, and routes work across regions, cells, and nodes. Each layer has its own capacity shape.

Read this chapter with [Production Model](/operators/production-model/), [Topology Planning](/operators/topology-planning/), [CoreStore Operations](/operators/corestore-operations/), [Index Operations](/operators/index-operations/), [Watch and Derived Maintenance](/operators/watch-and-derived-maintenance/), [PersonalDB Operations](/operators/personaldb-operations/), [Gateway Operations](/operators/gateway-operations/), [Backup and Recovery](/operators/backup-and-recovery/), [Observability](/operators/observability/), [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/), [CoreStore](/learn/corestore/), [Indexes and Query](/learn/indexes-and-query/), and [Watches and Derived Data](/learn/watches-and-derived-data/).

## Start with units, not hardware

Hardware is the last line of the capacity plan. The first line is the set of units the product will create and how quickly they change. Two deployments with the same stored bytes can have completely different operating profiles: one may store a few multi-gigabyte media files, while another stores billions of tiny JSON records with metadata, watches, indexes, and public gateway reads.

A good estimate includes these units:

| Unit | What it pressures |
| --- | --- |
| Storage tenants and apps | Credential envelopes, public policy records, relationship authz realms, audit volume, and administrative handover. |
| Buckets | Placement/routing records, bucket policy, object listing state, index definitions, gateway mounts, and public-read decisions. |
| Object keys and versions | Metadata journals, current-pointer movement, delete markers, path listings, object watches, and backup deltas. |
| Object body bytes | CoreStore blob shards, manifests, disk bandwidth, range reads, upload staging, backup size, and restore time. |
| User metadata and typed fields | Metadata filter indexes, typed JSON extraction, diagnostics, high-cardinality term spaces, and query ordering. |
| Append records | Stream growth, sequence-order checks, segment sealing, replay cost, checkpoint lag, and audit export pressure. |
| Watch consumers | Retained event windows, checkpoint storage, replay bursts, live tailing, and backpressure. |
| Index definitions and source scope | Build work, rebuild windows, diagnostics, segment count, query latency, and authorisation filtering. |
| Full-text tokens and positions | Segment size, tokenizer cost, phrase-query support, scoring work, and rebuild time. |
| Vectors, dimensions, and chunks | Embedding cost, HNSW memory, segment size, nearest-neighbour latency, and model-version separation. |
| PersonalDB groups and commits | Witness latency, changeset payload storage, heads, certificates, snapshots, projection lag, and catch-up cost. |
| Gateway traffic | Public listener capacity, SigV4 verification, host routing, TLS/reverse-proxy behaviour, range reads, cacheability, and egress. |
| Repair and rebuild jobs | Spare CPU, memory, disk, and I/O required when derived state must be rebuilt while production continues. |

The table is not a formula. It is a checklist for building the formula that fits your product. For each row, estimate steady-state count, peak write rate, peak read rate, retention period, rebuild tolerance, and the failure mode if the unit grows faster than expected.

## Regions, cells, and nodes are capacity boundaries

A mesh is the whole cooperating Anvil deployment. A region is a placement and routing boundary that usually maps to a data centre, cloud region, sovereignty boundary, or latency promise. A cell is smaller: typically a rack, failure domain, storage pool, Kubernetes node pool, or other capacity slice an operator can drain and reason about. A node is one Anvil server process, with background work running inside that process rather than in a separate worker-node product.

Capacity planning should follow those boundaries. Region sizing answers where tenant and bucket data should live. Cell sizing answers what happens when a rack-like boundary drains or fails. Node sizing answers whether one process has enough CPU, memory, disk, and network for the capabilities it advertises: object, index, PersonalDB, gateway, admin, or an all-in-one combination.

The cell boundary matters because failure and fullness are usually correlated. If all vector-heavy buckets, public gateways, or PersonalDB groups land in the same cell, the region may look large on paper while one rack is overloaded. If a cell has no spare capacity elsewhere to drain into, maintenance becomes an outage plan. Keep enough headroom outside each cell to absorb the work you expect to move during maintenance or incident response.

High-volume traffic should prefer regional endpoints after a bucket's home region is known. Generic routing, redirects, and supported proxy paths are useful fallbacks, but they should not be the hot path for every read. If every request crosses region boundaries, you have bought more latency, more egress, and less clear capacity evidence.

## CoreStore capacity is source-record capacity

CoreStore is the recovery boundary. Objects, refs, streams, transactions, tenant records, authz records, indexes, PersonalDB commits, gateway records, diagnostics, repair findings, lifecycle records, and audit evidence ultimately become CoreStore-backed state. Capacity planning therefore starts with the durable storage path and the fact that source records cannot be treated as disposable cache.

The current local backend has useful local integrity machinery. Blob bytes use a `4+2` shard profile in the local storage path, and control records such as manifests, refs, streams, transactions, and directories use local control replicas with quorum semantics. That affects disk space, file count, write amplification, and read repair behaviour. It is not proof that a node can lose its whole volume and reconstruct from other regions. Plan backup, restore, and disk monitoring as if each node volume is operationally significant.

Small records can be harder on capacity than large records. A tiny object may store only a few bytes of body, but it still creates object metadata, a version, a current-pointer update, watch events, path-listing work, optional index entries, and audit or diagnostic evidence. Many small records also increase inode pressure and backup catalogue size. Large objects stress different resources: upload staging, shard writes, manifests, checksums, range reads, network egress, and restore time.

Do not plan only for the current state. Plan for mutation history. Object metadata journals, append streams, authz tuple logs, PersonalDB commit logs, and watch records grow because they are evidence. Compaction and segment sealing can make reads and listings efficient, but they are maintenance of history, not permission to ignore growth. If compaction falls behind, the capacity symptom may be slower listing or growing backup deltas before it is a hard disk-full event.

## Tenants, buckets, and authorisation scale

A storage tenant is an Anvil boundary for buckets, apps, policy, authz, audit, and tenant-owned data. It is not necessarily the same thing as one end-user account in a SaaS product. A product with many customer organisations might use one tenant per organisation, one tenant per environment, or another model that matches isolation and delegation needs. A product with many individual users does not automatically need one Anvil storage tenant per user.

Tenant and app counts create control-plane and authorisation capacity. Every app credential can need encrypted envelope storage, rotation, audit, and policy grants. Public policy scopes are simple and fast to reason about, but broad grants can create visibility and audit problems. Relationship authorisation adds schemas, tuples, revisions, usersets, checks, and derived lag. That is powerful, but it has its own write rate and repair story.

Avoid designing for an unproven billion-tenant shape just because the product has billion-user ambitions. Anvil's model is meant to be partitioned by tenant, bucket, region, cell, prefix, and index scope, but the current repository does not prove that a single deployment has run with billions of storage tenants. Plan directionally: split storage tenants where isolation needs it, split buckets where read/list/index scope needs it, place tenants across regions deliberately, and measure the actual tenant creation, policy, authz, and audit rates your deployment can sustain.

Bucket count has a different profile from tenant count. A bucket carries placement, public-read policy, listing state, index definitions, gateway routes, and repair scope. Too few buckets can force coarse authorisation and oversized indexes. Too many buckets can make placement, diagnostics, and policy management noisy. Choose buckets around lifecycle and access boundaries: private documents, public assets, package artefacts, audit exports, media originals, and search corpora often deserve separate buckets.

## Objects, small files, and large files

For billions of small objects, the first bottleneck is usually metadata and derived maintenance rather than raw bytes. Prefix layout matters because listing and watches often work by bucket and prefix. A flat key space with no useful prefix structure makes operational triage harder. A prefix per tenant, project, date, content family, or lifecycle state can keep watches and repairs narrow, but prefixes should reflect real product boundaries rather than an arbitrary hash that nobody can explain during an incident.

Small objects also make index design visible. A typed JSON index over every tiny object may be appropriate for a task dashboard, but it multiplies extraction and diagnostics by object count. A metadata-filter index can be cheaper for exact tags. A path index is useful for navigation but does not answer typed predicates. Full-text and vector indexes over tiny snippets can work well if query value justifies the build cost; they are wasteful if the product mostly does exact lookups by id.

Large media, backups, model artefacts, and package blobs stress body-path capacity. Plan for concurrent uploads, multipart state, range reads, object checksums, manifest reads, public egress, and restore bandwidth. Large files may have low object count but high egress cost through S3/static gateways. They also make repair and backup windows longer. A bucket with multi-terabyte media needs enough spare disk and network for an emergency copy or restore, not only enough space for today's bytes.

Versioning changes the calculation for both small and large files. Anvil object writes create versions and move a current pointer. A mutable key with frequent overwrites can accumulate many versions and delete markers. If your product has a "latest" name, consider using object links for the mutable name and immutable versioned keys for payloads. Links are not copies, so they avoid duplicating body bytes, but they still create link metadata and generation history.

## Index and search capacity

Indexes are derived state, but they are often the most visible capacity consumer. A successful object write proves source durability. It does not prove an index builder has caught up, a full-text segment was published, a vector was embedded, or a hybrid query can see the new object. Capacity planning must budget for index build and query separately.

Path and metadata-filter indexes are usually about navigation and exact selection. They grow with object count, key length, metadata value size, and update rate. Typed JSON indexes add extractor cost and term/order structures. High-cardinality typed fields such as document ids or message ids can be useful, but they produce large term spaces and may not help broad dashboard queries. Low-cardinality fields such as `state`, `kind`, or `region` can be efficient filters, but they are only useful when the data shape is consistent.

Full-text indexes grow with token volume, retained positions, selected fields, and body size. Phrase queries need positions, so phrase-capable indexes cost more than simple token search. Full-text query syntax today is not a complete boolean language; do not reserve capacity for a feature shape you do not actually expose, and do not promise operators that a query planner will optimise unsupported boolean expressions.

Vector indexes have a different cost centre. Count vectors, not just objects. One document can produce many chunks and therefore many vectors. Memory and build time depend on vector dimension, metric, normalisation, HNSW parameters, segment count, and filter selectivity. A 1,536-dimensional embedding corpus has a very different footprint from a 384-dimensional corpus. Mixing embeddings from different models or normalisation rules because the dimensions happen to match creates poor results and hard-to-debug capacity waste.

Hybrid search combines full-text and vector material under one definition. Capacity is therefore closer to the sum of both paths than to a cheap query-time trick. Current direct hybrid scoring uses the implementation's fixed recipe rather than custom operator-defined weights, and current full-text/vector/hybrid direct queries do not expose the same meaningful source-cursor catch-up evidence that typed and metadata-backed queries can. Plan user experience and alerting around derived lag.

Production vector search also needs a production embedding source. Caller-supplied vectors move embedding CPU and model serving outside Anvil. Provider-generated vectors require a configured provider and enough throughput for build and rebuild. The deterministic or test-only embedding path is not a production semantic model and may be disabled unless explicitly configured.

## Watches, append streams, and derived consumers

Watches avoid rescanning source records, but they are not free. Each source stream needs retained history, event envelopes, consumers that can replay, and checkpoints that mean something. Capacity is driven by event rate, checkpoint discipline, retained window, poisoned events, consumer backpressure, and rebuild strategy.

A watch consumer should checkpoint only after its derived output is durable. That makes restarts safe, but it also means backlogs are real: if output is slow, the checkpoint lags. Plan where checkpoints live, how long consumers can be offline, how much replay is acceptable after a restart, and when the product switches from incremental replay to rebuild or repair.

Append streams are ordered histories for audit, event sourcing, export ledgers, and similar workflows. Their capacity shape is sequence rate, record payload size, retention, segment sealing, readers, and replay windows. Segment sealing is storage maintenance, not logical stream closure. A sealed segment can reduce read overhead or package history, but it does not mean the application stream is finished unless the feature has a separate closed state.

Audit exports and external side effects need extra headroom. A SIEM exporter, webhook mirror, package catalogue builder, or static-site inventory should not advance its checkpoint before the downstream output is durable. If the downstream system slows down, the Anvil source stream may be healthy while the derived consumer is behind. That is a capacity incident in the consumer, not proof that source writes should stop being recorded.

## PersonalDB capacity

PersonalDB capacity is log-chain capacity, not remote SQL capacity. Applications own local SQLite files. Anvil witnesses changesets, validates heads and authorisation, stores commit evidence, emits watches, supports catch-up, and maintains snapshots and projections. Operators therefore size for group count, replica count, commit rate, changeset byte volume, snapshot thresholds, projection lag, watch lag, and repair evidence.

A PersonalDB-heavy deployment with many small groups may stress group manifests, heads, watch fan-out, and audit. A deployment with fewer high-write groups may stress witness latency, changeset storage, catch-up, snapshot creation, and projection builders. Changeset size limits matter operationally: current documentation describes a default maximum changeset size of 16 MiB with a hard implementation cap of 128 MiB. Large changesets can reduce commit frequency but make retries, catch-up, and snapshots more expensive.

Projections are derived PersonalDB groups. They need the same source-versus-derived thinking as indexes. If a projection is behind, users may see stale derived data while the source group is healthy. The current API exposes projection-watch concepts, but the CLI surface is compact and incomplete for some PersonalDB workflows. Capacity plans should therefore include application/API-level metrics for source head, projection head, snapshot creation, commit rejection reasons, and catch-up latency.

## Gateway and public access capacity

Gateways translate external protocols into Anvil's native model. They do not create a separate capacity universe. S3 traffic still becomes object reads, writes, listings, version reads, multipart state, public policy checks, relationship checks, and CoreStore access. Static hosting still becomes host routing, public-read checks, object links, object reads, and egress. Package-style delivery today is a modelling pattern over objects, links, metadata, checksums, indexes, public access, and S3/native API rather than a complete registry protocol surface.

Gateway-heavy deployments often run out of network, connection, reverse-proxy, or signature-verification headroom before they run out of disk. S3 Signature Version 4 verification, range reads, multipart uploads, and list operations have different costs. Static hosting adds host routing, custom-domain verification, cache behaviour, and public-read exposure. Public-read buckets should be deliberately sized and audited because public means anyone who can reach the public surface may read matching data.

Reverse proxies are part of capacity. If TLS terminates before Anvil, host and scheme forwarding must be trusted only from configured proxy ranges, and S3 signatures must be computed against the effective host the client used. A proxy misconfiguration can look like bad credentials, wrong-region routing, or missing objects. Leave enough observability and request-id capacity to diagnose that edge under load.

## Repair, rebuild, and upgrade headroom

A steady-state cluster at 75 or 80 percent resource usage may still be under-provisioned. Anvil needs headroom for rebuilds, repairs, compaction, backup snapshots, restore drills, topology drains, release migrations, and incident diagnostics. Derived systems are deliberately repairable from source records, but repair consumes the same CPUs, disks, and networks as production traffic unless you isolate it through topology and scheduling.

Plan for at least three non-steady-state events:

| Event | Capacity implication |
| --- | --- |
| One cell drains for maintenance | Other cells in the region need enough spare object, gateway, index, and PersonalDB capacity to carry the affected load. |
| A large index rebuild runs | Builders need extra CPU, memory, disk, and source-read bandwidth while queries may still be served from older generations. |
| A restore drill or backup snapshot runs | Storage and network need enough room to copy source records and enough operator time to prove restored source and derived paths. |

Do not let repair be the first time you learn an index cannot rebuild within the product's recovery window. Test rebuilds against representative data, not only a tiny tutorial bucket. Record elapsed time, peak memory, disk growth, read throughput, and whether user-visible queries degraded.

## Example capacity shapes

A **billions-of-small-objects** deployment is dominated by metadata, path listing, watches, diagnostics, and narrow indexes. The body bytes may be modest. Use prefix design to keep watch and repair scopes narrow, split buckets by lifecycle or access boundary, avoid whole-bucket full-text/vector indexes unless the product truly needs them, and test listing and typed-query lag with realistic object counts. Treat claims about billions of tenants or objects as goals to validate in your deployment, not as proven limits from the current repository.

A **large-media** deployment is dominated by object bytes, upload staging, shard writes, manifests, range reads, public egress, backup size, and restore time. It may have simple metadata and few indexes, but it needs strong network and disk planning. Keep immutable originals separate from public derivatives, use links for mutable names, plan CDN/cache behaviour if used, and make sure backups include the CoreStore manifests as well as payload shards.

A **search-heavy private corpus** is dominated by extraction, full-text/vector/hybrid segment builds, authorisation-aware query filtering, and rebuild windows. Keep selectors focused, use `inherit_object` where search results must respect object visibility, avoid mixing embedding models, and plan for index lag. Typed JSON can handle structured dashboards while full-text and vector search handle discovery; do not force one index family to solve every query.

A **PersonalDB-heavy local-first product** is dominated by commit witness latency, group heads, changeset payloads, snapshots, projection lag, watch fan-out, and row-authorisation checks. Capacity evidence should compare source group heads with projection heads and replica checkpoints. The operator does not query users' local SQLite files to size Anvil; the operator sizes the witness, log, catch-up, projection, snapshot, and repair paths.

A **gateway-heavy delivery system** is dominated by public edge traffic, S3/static request patterns, host routing, public-read audit, egress, and cache behaviour. It may also have package-like immutable artefacts and mutable `latest` links. Keep package artefacts as immutable objects with checksums, move channels through generation-checked links, and use typed indexes for catalogues. Do not describe Docker, npm, PyPI, Maven, or similar registry compatibility as available unless the current deployed release exposes that protocol.

## Current public surfaces and gaps

The current repository gives operators useful primitives but not a magic capacity number. The local CoreStore backend uses local shard and control-replica machinery inside one storage path. That is useful implementation evidence, but it is not a proven distributed erasure-coded storage fabric with automatic multi-region disaster recovery. Region activation, drain completion, and cross-region proxying also have current surface gaps that affect how much capacity you can safely move during maintenance.

Index operations expose diagnostics and repair, but not every index family has the same freshness evidence. Metadata-backed and typed JSON queries can use meaningful catch-up fields; direct full-text, vector, and hybrid queries currently have weaker catch-up semantics. Index read scopes are also coarser than ideal in some places, so bucket and tenant design still matter for security and capacity.

PersonalDB has rich API concepts for groups, commits, catch-up, projections, watches, snapshots, and repair, but some CLI helpers are compact or incomplete for production synchronisation. Package gateways are foundational rather than complete registry protocol surfaces. Observability concepts and signal names exist, but dashboards and export wiring are deployment work. Billion-tenant or billion-object targets should be validated with representative load tests before being promised to customers.

A practical capacity plan is therefore iterative. Design tenant, bucket, region, cell, node, index, watch, and gateway boundaries. Measure the units above under representative load. Keep repair and drain headroom. Run restore and rebuild drills. Treat derived lag as a first-class signal. Then revise the topology before the next growth step, not after the cell, index, or gateway is already saturated.
