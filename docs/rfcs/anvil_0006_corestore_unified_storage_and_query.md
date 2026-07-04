# RFC ANVIL-0006: CoreStore Unified Storage, Authorisation-Aware Query, And Gateway Foundation

## Status

Draft.

## Date

2026-07-04.

## Normative Language

The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, `MAY`, and `OPTIONAL` in this document are normative. They are to be interpreted as described in RFC 2119.

## 1. Abstract

Anvil is a distributed object storage system with native indexing, append streams, object links, authorisation, package and protocol gateways, and operational control records. These features MUST be implemented on top of one durable storage substrate rather than each feature inventing its own local persistence, journal, replication, and recovery path.

This RFC defines CoreStore, the single durable persistence layer for Anvil. CoreStore stores every durable byte Anvil owns: user objects, object metadata, bucket metadata, append-stream records, index segments, authorisation schemas and tuples, task leases, mesh routing records, object links, package gateway state, embedded database snapshots and changesets, repair findings, and operational audit records.

CoreStore provides four core abstractions:

1. erasure-coded immutable blobs;
2. ordered durable streams;
3. compare-and-swap refs over immutable state;
4. watch cursors over committed changes.

All higher-level Anvil features MUST be expressed as typed records, streams, indexes, refs, and manifests stored through CoreStore. Local files MAY be used for staging, cache, download buffering, upload buffering, temporary compaction work, or repair scratch space. Local files MUST NOT be the canonical durable source of truth for any Anvil feature.

This RFC also defines the production indexing model. Indexes are materialised, watch-driven, generationed CoreStore data structures. Typed field indexes, full-text indexes, vector indexes, path indexes, package indexes, append sequence indexes, and authorisation indexes all emit or consume the same canonical `SourceId`. Composite and hybrid queries are planned over those primitive indexes. Authorisation is a first-class query predicate, not a final afterthought over an unbounded result set.

## 2. Goals

An implementation conforming to this RFC MUST:

1. store all durable Anvil state through CoreStore;
2. prevent feature-specific local journals from becoming authoritative storage;
3. erasure-code durable blobs, sealed stream segments, index segments, metadata segments, authorisation segments, and control records through the same storage substrate;
4. use local disk only as staging, cache, WAL-for-retry, or scratch space unless explicitly marked as non-durable;
5. expose object, metadata, stream, index, authorisation, lease, route, gateway, and package features as layers above CoreStore;
6. make every durable mutation observable through a CoreStore watch cursor;
7. make every derived structure rebuildable from canonical CoreStore data and watch streams;
8. require every materialised index to publish its source cursor, generation, and definition hash;
9. support typed field/range indexes over object bodies, object metadata, object system fields, append record bodies, append record metadata, append record system fields, and other explicitly defined sources;
10. support production vector indexing using configured embedding providers and model metadata, not deterministic placeholder embeddings;
11. support full-text indexing using configured text extractors and analysers;
12. support composite queries across path, typed field, full-text, vector, append, package, and authorisation indexes;
13. apply authorisation constraints early in query planning and index traversal;
14. require final authorisation verification before returning every object, record, or gateway result;
15. implement one Zanzibar-style authorisation engine for both Anvil's internal system model and tenant-defined realms;
16. protect Anvil's internal authorisation realm, control objects, routing objects, node records, credential records, and reserved namespaces from tenant mutation and tenant read access;
17. model principals, groups, service accounts, credentials, resources, and relationships directly, rather than exposing a storage API shaped only by any one gateway protocol;
18. support S3-compatible access as one gateway over the Anvil model, not as the model itself;
19. provide a gateway foundation for container registries, package registries, static sites, and public downloads without adding separate storage engines;
20. define conformance tests that prove no durable feature bypasses CoreStore.

## 3. Non-Goals

This RFC does not require:

1. TLS termination inside Anvil;
2. a separate worker-node class;
3. a separate storage implementation per feature;
4. a synchronous global transaction for every operation in every region;
5. exposing CoreStore internals as a general user-facing API;
6. making every query strongly synchronous with the latest write by default;
7. making public bucket access anonymous at the internal authorisation layer;
8. a domain-specific mail, build, package, workflow, database, or queue API;
9. permanent stream closure semantics as part of segment sealing;
10. user control over Anvil's internal system authorisation realm.

## 4. Design Constraints

CoreStore exists because Anvil's features must share one correctness model.

The implementation MUST satisfy these constraints:

1. **One durability path.** A feature that needs durable state MUST use CoreStore. It MUST NOT introduce a separate authoritative journal, index file, or metadata file with its own replication semantics.
2. **Immutable data, mutable refs.** Large or historical data SHOULD be stored as immutable blobs or sealed stream segments. Mutable state MUST be expressed as compare-and-swap refs that point at immutable records or manifests.
3. **Fenced ownership.** Partition ownership, task leases, background maintenance, and mutable index generation updates MUST use fence tokens. Stale owners MUST NOT be able to commit.
4. **Watch-driven derivation.** Derived indexes and projections MUST advance from source watch cursors. A rescan MAY repair or bootstrap a derived structure, but normal freshness MUST come from watch processing.
5. **Generationed query results.** Query page tokens MUST bind source generation, index generation, authorisation revision, predicate hash, order hash, and cursor position.
6. **Authorisation as data.** Authorisation schemas, bindings, tuples, caveat definitions, derived userset indexes, and authorisation lag checkpoints MUST be CoreStore records.
7. **Gateway neutrality.** Gateway-specific credentials, tags, manifests, package metadata, and protocol quirks MUST map into CoreStore records and Anvil resources. They MUST NOT define the global permission model.
8. **Explicit internal namespaces.** Reserved internal records MUST be inaccessible through public user APIs. Only Anvil server code holding an internal server-minted authority MAY mutate them.

## 5. Core Terminology

### 5.1 Mesh

A mesh is the complete Anvil deployment sharing a routing directory and administrative trust domain. A mesh contains one or more regions.

### 5.2 Region

A region is a placement, routing, and failure-domain boundary. Buckets have a home region unless an explicit replication policy says otherwise. Requests MAY enter through any region, but writes MUST be routed to the owning region for the target bucket or control resource unless the resource is declared globally replicated.

### 5.3 Cell

A cell is an operational subset of a region. A region MAY contain one cell or many cells. Cells own partitions and placement groups. Cells are not a separate user-facing namespace.

### 5.4 Node

A node is one Anvil process with a stable node identity, storage capacity, network addresses, capabilities, lifecycle state, heartbeat state, and placement weight.

All nodes are equal. A node MAY be selected to perform background work, but that selection is an in-process responsibility. It MUST NOT imply a dedicated worker process or a different node type.

### 5.5 Tenant

A tenant is an Anvil account-level boundary. Tenant identifiers MUST be unique within the mesh. Tenant names used in URLs MUST resolve to tenant identifiers through the mesh routing directory.

### 5.6 Bucket

A bucket is a tenant-scoped object namespace. Bucket names MUST be unique only inside one tenant.

The durable identity of a bucket is:

```text
BucketId = mesh_id + tenant_id + bucket_name
```

An implementation MAY assign an opaque bucket UUID, but public routing semantics MUST remain tenant-scoped.

### 5.7 Principal

A principal is an actor that can authenticate to Anvil or be the subject of an authorisation relationship. A principal MAY represent a human user, service account, internal Anvil subsystem, gateway credential, device, application, or group.

### 5.8 Credential

A credential is an authentication mechanism bound to a principal. Examples include native API tokens, S3 access keys, registry bearer tokens, package-publisher tokens, and internal server tokens.

Credentials MUST NOT be authorisation rules. Credentials identify a principal; the authorisation engine decides what the principal may do.

### 5.9 Resource

A resource is an object over which Anvil can evaluate relationships. Buckets, object prefixes, object versions, streams, indexes, links, package repositories, package versions, credentials, principals, tenants, regions, nodes, and authz realms are resources.

### 5.10 Authz Realm

An authz realm is a named authorisation scope with a bound schema, tuple log, derived indexes, and revisions. Anvil has reserved system realms. Tenants MAY create tenant-owned realms where permitted by the Anvil system realm.

### 5.11 CoreStore

CoreStore is Anvil's only durable persistence substrate. CoreStore stores immutable blobs, ordered streams, compare-and-swap refs, watch records, and derived materialised index segments.

### 5.12 CoreObject

A CoreObject is an immutable byte sequence stored through an erasure-coded placement plan. CoreObjects are content-addressed by hash and described by a manifest.

### 5.13 CoreStream

A CoreStream is an ordered sequence of records in one partition. Streams are append-only. Records become immutable after commit. Streams are physically stored as sealed CoreSegments.

### 5.14 CoreSegment

A CoreSegment is an immutable CoreObject containing a bounded range of stream records, index rows, posting lists, vector blocks, authz derived rows, or packed small records.

### 5.15 CoreRef

A CoreRef is a named mutable pointer to an immutable CoreObject or CoreSegment generation. CoreRefs are updated only by compare-and-swap.

### 5.16 Fence Token

A fence token proves ownership of a partition, task lease, stream-writer role, index-generation writer role, or other exclusive mutable responsibility. A stale fence token MUST be rejected before any protected write is applied.

### 5.17 SourceId

A SourceId is the canonical identity emitted by every primitive index. It identifies the Anvil record that matched an index query.

SourceIds allow path, typed field, full-text, vector, append, package, and authorisation indexes to intersect results without lossy string matching.

## 6. Architectural Overview

The required architecture is:

```text
+---------------------------------------------------------------+
| Public APIs and gateways                                      |
| Native gRPC, S3, static site, container registry, packages     |
+-----------------------------+---------------------------------+
                              |
+-----------------------------v---------------------------------+
| Domain services                                                |
| Objects, buckets, links, streams, leases, authz, indexes,      |
| package repositories, embedded database support, mesh routing  |
+-----------------------------+---------------------------------+
                              |
+-----------------------------v---------------------------------+
| CoreStore APIs                                                 |
| put_blob, get_blob, append_stream, read_stream, cas_ref,       |
| watch, acquire_fence, release_fence, materialise_index_segment |
+-----------------------------+---------------------------------+
                              |
+-----------------------------v---------------------------------+
| Placement and durability                                       |
| erasure coding, shard placement, quorum acknowledgement,       |
| repair, compaction, region replication policy                  |
+-----------------------------+---------------------------------+
                              |
+-----------------------------v---------------------------------+
| Node local media                                               |
| shard files, staging files, cache files, scratch files          |
+---------------------------------------------------------------+
```

The boundary is strict:

1. APIs and domain services MAY define feature-specific record schemas.
2. APIs and domain services MUST NOT define feature-specific durable storage engines.
3. CoreStore owns durable placement, erasure coding, shard repair, stream segment storage, ref compare-and-swap, and watch emission.
4. Node local media stores CoreStore shards, staging files, caches, and scratch files only.


## 6A. CoreStore Root, Partition, And Bootstrap Model

### 6A.1 Bootstrap Rule

CoreStore cannot depend on a higher-level Anvil service to find its own roots. A conforming implementation MUST therefore implement a minimal CoreStore bootstrap layer. The bootstrap layer is part of CoreStore itself, not a feature-specific journal.

The bootstrap layer contains only:

1. node identity and local shard inventory;
2. the latest known signed mesh root catalog records;
3. the placement set for root partitions;
4. the cryptographic hashes needed to verify root catalog records;
5. replay checkpoints for CoreStore root partitions.

The bootstrap layer MUST NOT contain user objects, bucket metadata, authz tuples, index rows, package metadata, append records, or feature-specific state.

### 6A.2 Root Catalog

The root catalog is the durable entry point into a mesh. It MUST be stored as CoreStore root records replicated across the root placement set. A node-local copy MAY exist as a cache, but the authoritative root catalog is the highest valid generation accepted by the root placement quorum.

Required root catalog shape:

```json
{
  "schema": "anvil.core.root_catalog.v1",
  "mesh_id": "mesh_01...",
  "generation": 42,
  "previous_hash": "sha256:...",
  "root_partitions": [
    {
      "partition_id": "core.root.refs.0",
      "owner_node_id": "node_01...",
      "fence": 1007,
      "placement_group": "root-pg-0",
      "embedded_head_segment_manifest": {
        "schema": "anvil.core.object_manifest.v1",
        "mesh_id": "mesh_01...",
        "region_id": "eu-west-1",
        "object_hash": "sha256:...",
        "logical_size": 65536,
        "encoding": {
          "profile_id": "root_replicated_v1",
          "data_shards": 1,
          "parity_shards": 0,
          "stripe_size": 65536,
          "encryption": "aead_xchacha20poly1305_v1"
        },
        "placements": [
          {
            "shard_index": 0,
            "node_id": "node_01...",
            "shard_hash": "sha256:...",
            "stored_size": 65536,
            "generation": 88
          }
        ],
        "created_at": "2026-07-04T12:00:00Z",
        "mutation_id": "mut_01..."
      }
    }
  ],
  "placement_catalog_ref": "core.ref:/system/placement/current",
  "stream_directory_ref": "core.ref:/system/streams/current",
  "ref_directory_ref": "core.ref:/system/refs/current",
  "authz_system_realm_ref": "core.ref:/system/authz/realm/current",
  "created_at": "2026-07-04T12:00:00Z",
  "signed_by": "node_01...",
  "signature": "base64url..."
}
```

A root catalog update MUST be accepted only if:

1. its generation is greater than the currently accepted generation;
2. its `previous_hash` matches the accepted prior catalog unless this is genesis;
3. the signer holds the root partition fence;
4. a root placement quorum stores and verifies the record;
5. each `embedded_head_segment_manifest` is a complete CoreObject manifest sufficient to locate and verify its shards after a cold start;
6. the referenced root partition segments are readable.

### 6A.3 Manifest Lookup

CoreObject manifests are ordinary CoreStore records after bootstrap. To break the apparent circular dependency, root partition segment manifests are embedded in the root catalog as complete CoreObject manifests, including placement entries. Those root segments contain the stream and ref directories needed to locate all other manifests.

The lookup sequence is:

```text
node start
  -> load local bootstrap cache
  -> contact root placement peers
  -> select highest valid root catalog generation with quorum
  -> read root partition segments named by root catalog
  -> reconstruct stream/ref/placement directories
  -> locate requested CoreObject manifest by logical name or content hash
  -> read normal CoreObject shards
```

A node MUST NOT accept a local bootstrap cache as authoritative without root quorum verification unless explicitly started in a documented disaster-recovery mode that cannot serve public traffic.

### 6A.4 Recovery Enumeration

Every node MUST be able to enumerate local CoreStore shards by content hash, placement group, generation, and owning CoreObject hash. Recovery MUST compare local shard inventory with the root catalog and placement catalog. Unknown shards MAY be quarantined. Missing shards MUST be scheduled for repair when enough other shards exist.

### 6A.5 Partition Model

Every mutable CoreStore object belongs to exactly one partition. Mutable objects include streams, refs, fence records, index generation heads, root catalog records, and package tag heads.

Partition identity MUST be derived from logical name, resource scope, and family. The mapping MUST be deterministic and recorded in the placement catalog.

A partition has:

1. partition id;
2. placement group;
3. owner node id;
4. fence generation;
5. sequence allocator state;
6. current head CoreRef or stream segment;
7. lifecycle state.

### 6A.6 Linearizability Boundary

CoreStore MUST provide linearizable writes within one partition. Stream sequence assignment and CoreRef compare-and-swap are linearizable inside the owning partition.

A multi-resource mutation batch MUST either:

1. map all visible state changes to one transaction partition; or
2. be rejected as a cross-partition atomic mutation.

Cross-partition workflows MAY be implemented as higher-level sagas, but they MUST NOT be presented as atomic CoreStore mutation batches.

### 6A.6A Mutation Partition Co-Location

Every public mutation MUST declare or derive one `MutationPartitionId`. For object and stream operations inside a bucket, the default mutation partition is:

```text
mutation-partition = hash(mesh_id, anvil_storage_tenant_id, tenant_id, bucket_name)
```

The following bucket-scoped records MUST be co-located in that mutation partition:

1. current-object refs for objects in the bucket;
2. object metadata stream records for the bucket;
3. object watch stream records for the bucket;
4. object link refs for links in the bucket;
5. append stream heads for streams in the bucket;
6. resource-scoped task leases protecting objects or streams in the bucket;
7. package tag heads for repositories materialised in the bucket;
8. upload-session refs for uploads targeting the bucket.

An index generation head is not part of ordinary object-write atomicity. Indexers consume watch events and publish index generations in their own index partitions.

A mutation that names resources with different `MutationPartitionId` values MUST fail before visible writes unless a separate RFC defines a cross-partition transaction protocol. This RFC intentionally does not define cross-partition atomic commit.


### 6A.7 Split-Brain Rejection

A partition owner MUST hold a valid fence issued through the partition ownership record. A write from an old owner, an old fence, or a node outside the current placement epoch MUST be rejected before it becomes visible.

If two nodes claim ownership, readers and writers MUST use the highest valid ownership generation committed by quorum. The lower generation MUST be treated as stale.


### 6A.8 Quorum Contract

Each partition placement group MUST declare a quorum profile:

```json
{
  "schema": "anvil.core.quorum_profile.v1",
  "placement_group": "pg_01...",
  "replica_count": 5,
  "write_quorum": 3,
  "read_quorum": 3,
  "fence_quorum": 3,
  "epoch": 17
}
```

For partition metadata, fence records, CoreRef heads, stream heads, and transaction commit records, a write is committed only after `write_quorum` replicas durably acknowledge the same record hash in the same epoch.

A read that determines current ownership, current CoreRef value, stream head, or transaction state MUST consult `read_quorum` replicas or a cache proven fresh by a watch cursor at least as new as the required read generation.

The quorum profile MUST satisfy intersection between read and write quorums for the active epoch. A new epoch MUST NOT become active until its epoch-change record is committed by the prior epoch or by documented disaster-recovery authority that cannot serve public traffic until completed.

### 6A.9 Partition Ownership Transfer

Ownership transfer MUST be a generationed state machine:

```text
unowned -> claiming -> owned -> draining -> unowned
owned   -> transferring -> owned
owned   -> offline -> claiming
```

A node acquiring ownership MUST:

1. read the current partition ownership record from quorum;
2. prove the prior owner lease is expired, released, drained, or administratively fenced off;
3. commit a new ownership record with incremented fence generation;
4. start assigning stream sequences only after the new ownership record is committed;
5. reject all writes using prior fence generations.

A node releasing ownership MUST stop accepting new writes before committing release. In-flight writes using the old fence MUST either commit before release or fail as stale after release.


## 7. CoreStore API Contract

### 7.1 Required Operations

A conforming implementation MUST provide these logical operations to Anvil services:

```rust
trait CoreStore {
    async fn put_blob(&self, input: PutBlob) -> Result<CoreObjectRef>;
    async fn get_blob(&self, input: GetBlob) -> Result<ByteStream>;
    async fn append_stream(&self, input: AppendStreamRecord) -> Result<StreamAppendReceipt>;
    async fn read_stream(&self, input: ReadStream) -> Result<Vec<StreamRecord>>;
    async fn seal_stream_segment(&self, input: SealStreamSegment) -> Result<CoreSegmentRef>;
    async fn compare_and_swap_ref(&self, input: CompareAndSwapRef) -> Result<CasRefReceipt>;
    async fn watch(&self, input: WatchRequest) -> Result<WatchStream>;
    async fn acquire_fence(&self, input: AcquireFence) -> Result<FencedPermit>;
    async fn release_fence(&self, input: ReleaseFence) -> Result<()>;
}
```

The Rust shape above is illustrative. The semantics are normative.

### 7.2 PutBlob

`PutBlob` stores immutable bytes. It MUST:

1. compute a content hash over canonical bytes;
2. split bytes into stripes according to the selected erasure coding profile;
3. place shards on selected nodes according to the placement plan;
4. verify enough shard writes to meet the durability profile;
5. write or update the CoreObject manifest through CoreStore's ref/stream mechanism;
6. return only after the object is durable according to policy.

`PutBlob` MAY deduplicate by content hash. Deduplication MUST NOT bypass authorisation checks on higher-level resources that point at the blob.

### 7.3 GetBlob

`GetBlob` reads immutable bytes by CoreObject reference. It MUST:

1. locate the manifest;
2. select available shards;
3. reconstruct bytes if required;
4. verify shard hashes and content hash;
5. return an error if integrity verification fails.

### 7.4 AppendStreamRecord

`AppendStreamRecord` appends one immutable record to a stream. It MUST:

1. validate stream identity;
2. validate any required fence token;
3. assign a monotonic sequence inside the stream partition;
4. include an idempotency key when supplied;
5. write the record into the current open segment or create a new segment;
6. emit a watch event with a durable cursor;
7. return the assigned sequence and cursor.

If the idempotency key was previously committed with the same payload hash, the operation MUST return the original receipt. If the same idempotency key was committed with a different payload hash, the operation MUST fail before appending.

### 7.5 SealStreamSegment

`SealStreamSegment` finalises the current physical segment. It MUST NOT close the logical stream.

After sealing:

1. the segment MUST be immutable;
2. the segment MUST be stored as a CoreObject;
3. the stream MAY accept later records in a new segment;
4. readers MUST be able to read across sealed segment boundaries.

### 7.6 CompareAndSwapRef

`CompareAndSwapRef` updates a CoreRef only if all preconditions hold.

Supported preconditions MUST include:

1. expected current generation;
2. expected current target hash;
3. absent ref;
4. present ref;
5. active fence token;
6. authorisation revision or zookie where the write depends on an authz decision;
7. source watch cursor where the write is a derived update.

A failed precondition MUST leave the ref unchanged.

### 7.7 Watch

`Watch` returns committed changes from a watchable source. Watch cursors MUST be stable, monotonic inside their stream, and safe to persist by consumers.



A watch event MUST use this envelope:

```json
{
  "schema": "anvil.core.watch_event.v1",
  "stream_id": "watch.object.tenant_01.releases.03",
  "partition_id": "part_01...",
  "sequence": 1844,
  "cursor": "watch_01...",
  "previous_event_hash": "sha256:...",
  "event_hash": "sha256:...",
  "event_type": "object_current_updated",
  "source_id": { "schema": "anvil.query.source_id.v1" },
  "transaction_id": "txn_01...",
  "record_generation": 44,
  "source_cursor_vector": {},
  "created_at": "2026-07-04T12:00:00Z"
}
```

Within one watch stream, `sequence` MUST be strictly increasing and gap-free. `previous_event_hash` MUST chain to the prior event in that stream. A consumer that observes a gap or hash mismatch MUST stop applying derived state and enter catch-up or repair.

Watch replay MUST be idempotent. Applying the same sequence and event hash more than once MUST have no additional effect. Applying the same sequence with a different hash MUST fail with a divergence error.

There is no implicit total order across independent watch streams. A derived subsystem that consumes multiple streams MUST persist a cursor vector containing the latest applied cursor per stream.

Watch retention and compaction MUST preserve the ability for configured consumers to resume from their persisted cursor. If a cursor has expired, Anvil MUST return a structured `WatchCursorExpired` error and require the consumer to rebuild from a snapshot or repair source.

Watchable sources MUST include:

1. object metadata changes;
2. object payload version changes;
3. bucket changes;
4. append stream records;
5. CoreRef changes;
6. authz tuple changes;
7. authz schema binding changes;
8. index generation changes;
9. mesh routing changes;
10. node and region lifecycle changes;
11. credential changes;
12. package registry metadata changes.

### 7.8 AcquireFence

`AcquireFence` grants exclusive mutable ownership for a resource and returns a fence token. It MUST:

1. derive the requesting principal from authentication context;
2. reject caller-supplied owner identity as authority;
3. store the fence state through CoreStore;
4. increment the fence generation on every successful ownership change;
5. reject stale checkpoint, commit, release, or mutation attempts;
6. enforce maximum TTL policy.


## 7A. Transaction And Visibility Model

### 7A.1 Mutation Transaction Record

Every visible multi-step mutation MUST produce a transaction record. The transaction record is the visibility boundary. Staged blobs, prepared stream records, prepared metadata records, and prepared index updates are not visible until the transaction commit record is visible.

Required shape:

```json
{
  "schema": "anvil.core.transaction.v1",
  "transaction_id": "txn_01...",
  "scope_partition": "part_01...",
  "state": "committed",
  "preconditions_hash": "sha256:...",
  "operations_hash": "sha256:...",
  "prepared_refs": ["core:sha256:..."],
  "visible_updates": [
    {
      "kind": "core_ref_update",
      "ref_name": "tenant/tenant_01/bucket/releases/object/current/app.pkg",
      "new_generation": 44
    },
    {
      "kind": "stream_append",
      "stream_id": "object_metadata:tenant_01:releases",
      "visible_sequence": 9001,
      "prepared_record_hash": "sha256:..."
    }
  ],
  "committed_at": "2026-07-04T12:00:00Z",
  "committed_by_principal": "principal_01..."
}
```

Allowed transaction states are:

```text
prepared | committed | aborted
```

Only `committed` transactions affect visible state.

### 7A.2 Write Sequence

A visible object write MUST follow this sequence or an equivalent one with identical semantics:

```text
validate request
  -> authenticate principal
  -> check authorisation
  -> stage payload bytes locally
  -> CoreStore.put_blob(payload) as unreferenced immutable data
  -> prepare metadata record referencing payload
  -> prepare current-ref update
  -> check transaction preconditions
  -> append transaction commit record in transaction partition
  -> publish visible ref/stream heads derived from committed transaction
  -> emit watch event referencing transaction id
  -> acknowledge success
```

If the final transaction commit fails, staged blobs and prepared records MUST remain invisible and MAY be garbage collected. No reader may observe partial state.

### 7A.3 Stream Visibility

A stream record prepared as part of a transaction MUST NOT be visible to stream readers until its transaction is committed.

Prepared stream records MUST NOT consume visible stream sequence numbers. The transaction commit step assigns visible sequence numbers from the stream owner while holding the stream partition fence. Aborted prepared records therefore cannot create gaps in visible sequence numbers.

Stream readers MUST either:

1. read only committed records ordered by visible sequence; or
2. return prepared records only to internal recovery code explicitly operating in repair mode.

Physical segment offsets MAY contain abandoned prepared bytes. Visible stream iteration MUST ignore them because they have no committed visible sequence.

### 7A.4 Atomicity Scope

Atomic mutation batches are atomic at the visible CoreStore state layer for one transaction partition. The implementation MAY write immutable blobs before commit because unreferenced immutable blobs are not visible state.

A mutation batch that cannot be assigned to one transaction partition MUST fail with `CrossPartitionAtomicMutationUnsupported` before writing visible records.

### 7A.5 Recovery

Recovery MUST scan prepared and committed transaction records. A prepared transaction with no committed record MUST be completed or aborted according to idempotency and precondition state. A committed transaction MUST be replayed idempotently until all derived visible heads and watch events match the transaction record.

## 8. Core Data Formats

### 8.1 Encoding Rules

CoreStore control records MUST use canonical JSON unless this RFC specifies a binary frame. Canonical JSON means:

1. UTF-8 encoding;
2. deterministic object key ordering;
3. no insignificant whitespace in stored hash input;
4. integers encoded as decimal JSON numbers where the value range is exact;
5. binary data encoded as base64url without padding;
6. timestamps encoded as RFC3339 UTC strings with nanosecond precision where needed.

Binary segment frames MUST use little-endian integers unless otherwise stated.

### 8.2 Logical CoreStore Namespaces

CoreStore records are addressed by logical names. Logical names are not public object keys and MUST NOT be exposed as bucket contents.

Logical name grammar:

```text
logical-name     = mesh "/" scope "/" family "/" resource
mesh             = "mesh:" id
scope            = system-scope / tenant-scope / bucket-scope / region-scope
system-scope     = "system"
tenant-scope     = "tenant:" id
bucket-scope     = "tenant:" id "/bucket:" name
region-scope     = "region:" name
family           = "object" / "bucket" / "stream" / "ref" / "index" /
                   "authz" / "lease" / "mesh" / "node" / "region" /
                   "gateway" / "package" / "database" / "audit" / "repair"
resource         = 1*(ALPHA / DIGIT / "." / "_" / "-" / "/" / ":" / "~")
id               = 1*(ALPHA / DIGIT / "_" / "-")
name             = 1*(ALPHA / DIGIT / "." / "_" / "-")
```

Examples:

```text
mesh:prod/system/authz/realm:system/schema/current
mesh:prod/tenant:tenant_01/bucket:releases/object/current/apps/app.pkg
mesh:prod/tenant:tenant_01/bucket:events/index/typed/due-work/generation/current
mesh:prod/region:eu-west-1/mesh/routing/bucket-home/tenant_01/releases
```

Logical names MUST be validated before use. Implementations MAY map logical names to internal shard paths, but the mapping MUST be an implementation detail owned by CoreStore.

### 8.3 CoreObject Manifest

A CoreObject manifest describes an immutable blob and its shard placement.

Required JSON shape:

```json
{
  "schema": "anvil.core.object_manifest.v1",
  "mesh_id": "mesh_01...",
  "region_id": "eu-west-1",
  "object_hash": "sha256:...",
  "logical_size": 12345,
  "encoding": {
    "profile_id": "rs_4_2_v1",
    "data_shards": 4,
    "parity_shards": 2,
    "stripe_size": 4194304,
    "encryption": "aead_xchacha20poly1305_v1"
  },
  "placements": [
    {
      "shard_index": 0,
      "node_id": "node_01...",
      "shard_hash": "sha256:...",
      "stored_size": 4194304,
      "generation": 88
    }
  ],
  "created_at": "2026-07-04T12:00:00Z",
  "mutation_id": "mut_01..."
}
```

Validation rules:

1. `schema` MUST equal `anvil.core.object_manifest.v1`.
2. `object_hash` MUST be the hash of the logical plaintext bytes before erasure coding and encryption.
3. `placements` MUST include at least the configured minimum shard count.
4. Each `shard_hash` MUST verify the stored shard bytes.
5. The manifest itself MUST be stored through CoreStore as a control object or stream record.

### 8.4 CoreStream Segment Frame

A CoreStream segment is a binary frame stored as a CoreObject.

Frame layout:

```text
segment        = magic version header_len header_json record_count records trailer
magic          = %x41.4E.53.45.47.30.30.31 ; "ANSEG001"
version        = uint16-le                       ; 1
header_len     = uint32-le
header_json    = *OCTET                         ; canonical JSON, header_len bytes
record_count   = uint64-le
records        = *record
record         = record_header_len record_header_json payload_len payload crc32c
record_header_len = uint32-le
record_header_json = *OCTET                     ; canonical JSON
payload_len    = uint64-le
payload        = *OCTET
crc32c         = uint32-le                       ; header+payload checksum
trailer        = trailer_len trailer_json segment_hash
trailer_len    = uint32-le
trailer_json   = *OCTET                         ; canonical JSON
segment_hash   = 32OCTET                        ; SHA-256 over all previous bytes
```

The segment header JSON MUST include:

```json
{
  "schema": "anvil.core.stream_segment_header.v1",
  "stream_id": "stream_01...",
  "partition_id": "part_01...",
  "segment_id": "seg_01...",
  "first_sequence": 1,
  "last_sequence": 2000,
  "source_family": "object_metadata",
  "created_at": "2026-07-04T12:00:00Z",
  "sealed_at": "2026-07-04T12:03:00Z"
}
```

Each record header JSON MUST include:

```json
{
  "schema": "anvil.core.stream_record_header.v1",
  "stream_id": "stream_01...",
  "sequence": 1,
  "record_kind": "object.put",
  "payload_hash": "sha256:...",
  "payload_content_type": "application/json",
  "mutation_id": "mut_01...",
  "idempotency_key_hash": "sha256:...",
  "created_at": "2026-07-04T12:00:00Z"
}
```

### 8.5 CoreRef Record

A CoreRef record stores one compare-and-swap update.

Required JSON shape:

```json
{
  "schema": "anvil.core.ref_update.v1",
  "ref_name": "tenant/tenant_01/bucket/releases/object/latest.exe/current",
  "previous_generation": 41,
  "new_generation": 42,
  "previous_target": "sha256:...",
  "new_target": "sha256:...",
  "preconditions": {
    "expected_generation": 41,
    "fence_token": "fence_01...",
    "authz_revision": "azr_01..."
  },
  "mutation_id": "mut_01...",
  "committed_at": "2026-07-04T12:00:00Z"
}
```

The CoreRef update stream MUST be append-only. The current value MAY be cached, but recovery MUST be possible from the ref update stream and checkpointed snapshots stored through CoreStore.

### 8.6 SourceId

`SourceId` is the canonical identity used by all primitive indexes and query plans. It MUST include enough information to uniquely identify the matched source record and to run a final authorisation check.

SourceId JSON shape:

```json
{
  "schema": "anvil.query.source_id.v1",
  "mesh_id": "mesh_01...",
  "anvil_storage_tenant_id": "storage_tenant_01...",
  "authz_scope": {
    "anvil_storage_tenant_id": "storage_tenant_01...",
    "authz_realm_id": "realm_01..."
  },
  "kind": "object_current",
  "resource_namespace": "anvil_object",
  "resource_id": "tenant_01/releases/apps/app-1.pkg",
  "bucket": {
    "tenant_id": "tenant_01...",
    "bucket_name": "releases",
    "object_key": "apps/app-1.pkg",
    "object_version": "v_01..."
  },
  "append": null,
  "package": null,
  "control": null,
  "tombstone": false,
  "generation": 44
}
```

For append records, `append` MUST contain stream key and sequence. For packages, `package` MUST contain gateway, repository, package name, package version, file name, digest, and tag where applicable. For mesh control records, `control` MUST contain control family and record key.

Allowed `kind` values MUST include:

```text
object_current
object_version
append_record
authz_resource
package_repository
package_version
package_file
package_tag
git_object
personal_database_record
mesh_control_record
```

Canonical binary encoding MUST be:

```text
source-id-binary = version kind mesh-id storage-tenant-id authz-realm-id
                   resource-namespace resource-id generation tombstone variant-bytes
version          = uint16-le ; 1
kind             = uint16-le ; enum value
mesh-id          = len-bytes
storage-tenant-id = len-bytes
authz-realm-id   = len-bytes
resource-namespace = len-bytes
resource-id      = len-bytes
generation       = uint64-le
tombstone        = uint8 ; 0 or 1
variant-bytes    = len-bytes ; canonical JSON for kind-specific fields
len-bytes        = uint32-le *OCTET
```

SourceId sort order MUST be lexicographic over this tuple:

```text
(mesh_id, anvil_storage_tenant_id, authz_realm_id, kind, resource_namespace, resource_id, generation, tombstone, variant-bytes)
```

Tombstones MUST be represented with `tombstone = true`. A materialised index generation for `object_current` MUST suppress older live entries when a later tombstone for the same resource is present at or before the generation cursor. Index repair MUST treat tombstones as source records, not as missing data.

Every primitive index result MUST include a SourceId.

### 8.7 Page Token

Query page tokens MUST be opaque to clients and MUST be authenticated by Anvil. The decoded token MUST bind:

```json
{
  "schema": "anvil.query.page_token.v1",
  "mesh_id": "mesh_01...",
  "anvil_storage_tenant_id": "storage_tenant_01...",
  "authz_scope": {
    "anvil_storage_tenant_id": "storage_tenant_01...",
    "authz_realm_id": "realm_01..."
  },
  "tenant_id": "tenant_01...",
  "bucket_name": "releases",
  "caller_principal_hash": "sha256:...",
  "query_hash": "sha256:...",
  "predicate_hash": "sha256:...",
  "order_hash": "sha256:...",
  "index_inputs": [
    {
      "index_id": "idx_due_fields",
      "definition_hash": "sha256:...",
      "generation": 17
    },
    {
      "index_id": "idx_body_text",
      "definition_hash": "sha256:...",
      "generation": 9
    },
    {
      "index_id": "idx_body_vector",
      "definition_hash": "sha256:...",
      "generation": 12
    },
    {
      "index_id": "authz_visible_objects",
      "definition_hash": "sha256:...",
      "generation": 55
    }
  ],
  "authz_revision": "azr_01...",
  "last_source_id": { "schema": "anvil.query.source_id.v1" },
  "last_sort_tuple": ["2026-07-04T12:00:00Z", 100, "item_01..."],
  "expires_at": "2026-07-04T12:15:00Z"
}
```

A page token MUST be rejected if the query shape, predicate, order, any `{index_id, definition_hash, generation}` input, acceleration view generation, or authorisation revision is incompatible with the current request.

### 8.8 Gateway Credential Record

Gateway credentials MUST be stored as CoreStore records and MUST point to principals.

Required JSON shape:

```json
{
  "schema": "anvil.gateway.credential.v1",
  "credential_id": "cred_01...",
  "tenant_id": "tenant_01...",
  "subject_principal": "principal_01...",
  "gateway": "s3",
  "credential_kind": "access_key",
  "public_identifier_hash": "sha256:...",
  "secret_hash": "argon2id:...",
  "state": "active",
  "scopes": [
    {
      "resource": "anvil_bucket:tenant_01/releases",
      "relation": "writer"
    }
  ],
  "created_at": "2026-07-04T12:00:00Z",
  "expires_at": null,
  "rotated_from": null,
  "revoked_at": null
}
```

Gateway credentials MUST NOT contain plaintext secrets after creation. Credential verification MUST authenticate the credential, resolve `subject_principal`, and then perform normal authorisation checks.

## 9. Durability, Placement, And Local Media

### 9.1 Local Staging

A node MAY write incoming data to local staging before CoreStore commit. Staging files MUST be labelled as non-authoritative. If the node crashes before CoreStore commit, recovery MAY retry or discard staging data according to mutation idempotency rules.

A write MUST NOT be acknowledged as durable until CoreStore has committed the required CoreObject, CoreStream record, or CoreRef update.

### 9.2 Erasure Coding

CoreStore MUST support erasure-coded placement profiles. A profile defines:

```json
{
  "schema": "anvil.core.erasure_profile.v1",
  "profile_id": "rs_4_2_v1",
  "data_shards": 4,
  "parity_shards": 2,
  "minimum_read_shards": 4,
  "minimum_write_ack_shards": 6,
  "stripe_size": 4194304,
  "placement_scope": "region",
  "repair_priority": "normal"
}
```

The default profile SHOULD tolerate at least two node or shard losses inside the placement scope. The exact default profile MAY vary by deployment class, but it MUST be explicit in control records and manifests.

### 9.3 What Is Erasure Coded

The following MUST be stored through CoreStore and therefore MUST receive the configured durability treatment:

1. user object payloads;
2. packed small-object segments;
3. object metadata segments;
4. bucket metadata segments;
5. object link records;
6. append stream segments;
7. full-text index segments;
8. vector index segments;
9. typed field/range index segments;
10. path index segments;
11. authz tuple log segments;
12. authz schema records;
13. authz derived index segments;
14. task lease and fence records;
15. mesh routing records;
16. node and region lifecycle records;
17. package registry manifests, tags, indexes, and blobs;
18. embedded database snapshots and changesets;
19. repair findings and audit records.

### 9.4 Small Objects

Small objects MUST NOT create one inefficient durable file per user object when a pack segment is more efficient. CoreStore SHOULD pack small objects and metadata records into CoreSegments.

A small-object pack segment MUST:

1. be immutable after sealing;
2. include per-entry offsets, lengths, hashes, and SourceIds;
3. be stored as a CoreObject;
4. be referenced by object metadata;
5. support repair and integrity verification like any other CoreObject.

### 9.5 Large Objects

Large objects MUST be chunked into CoreObjects. The object metadata MUST reference a payload manifest containing ordered chunk references.

A large-object read MUST verify chunk hashes and object-level hash. Range reads SHOULD avoid reconstructing unrelated chunks where the erasure profile permits it.

### 9.6 Cache

A node MAY cache reconstructed blobs, shard reads, index blocks, authz posting lists, and package metadata. Cache entries MUST be invalidated or versioned by CoreObject hash, CoreRef generation, index generation, or authz revision.

Cache loss MUST NOT lose durable data.

## 10. Mapping Anvil Features Onto CoreStore

### 10.1 Objects

Object writes produce:

1. one payload CoreObject or one packed small-object entry;
2. one object metadata stream record;
3. one current-object CoreRef update;
4. one watch event;
5. zero or more index-source events for derived indexers.

Object reads consult the current-object CoreRef, load metadata, then load payload bytes from CoreStore.

### 10.2 Object Metadata

Object metadata MUST be a canonical CoreStore record. It MUST include:

```json
{
  "schema": "anvil.object.metadata.v1",
  "tenant_id": "tenant_01...",
  "bucket_name": "releases",
  "object_key": "latest.exe",
  "version": "v_01...",
  "payload_ref": "core:sha256:...",
  "content_type": "application/octet-stream",
  "user_metadata": {},
  "created_at": "2026-07-04T12:00:00Z",
  "created_by_principal": "principal_01...",
  "mutation_id": "mut_01..."
}
```

Metadata updates MUST be versioned. Current metadata MAY be cached but MUST be recoverable from CoreStore records.

### 10.3 Buckets

Bucket creation, deletion, policy changes, home-region changes, lifecycle changes, and replication-policy changes MUST be CoreStore control records. Bucket listing MUST be served from a materialised bucket index or a CoreStore stream checkpoint, not by scanning local directories.

### 10.4 Object Links

An object link is a metadata record that points to a target object key and optional version. Links MUST be stored as CoreStore records and updated through CoreRef compare-and-swap.

Many links MAY point to the same target. Updating a link MUST NOT copy target payload bytes.

### 10.5 Append Streams

Append stream records MUST use CoreStream semantics. Append payload bytes MAY be embedded in stream records when small or referenced as CoreObjects when large.

Segment sealing rotates physical storage and MUST NOT close the logical stream.

### 10.6 Task Leases And Fences

Task lease state MUST be represented as CoreRef and CoreStream records. A lease mutation MUST derive owner identity from the authenticated principal. A request field MAY name a task; it MUST NOT establish owner identity.

Lease-protected object mutations, stream appends, and index commits MUST include the required fence token as a write precondition.

### 10.7 Embedded Database Support

Embedded database snapshots, changesets, projection segments, row metadata indexes, and witness records MUST be CoreStore records. Changesets SHOULD be append stream records when ordered history matters. Snapshots SHOULD be immutable CoreObjects referenced by database-head CoreRefs.

A database witness operation that decides mutation validity MUST be protected by CoreRef compare-and-swap and fence semantics where concurrent writers are possible.

### 10.8 Source Repositories

Source repository packs, commits, trees, blobs, refs, derived file indexes, and source watch cursors MUST be CoreStore records. Repository object storage MAY reuse content-addressed CoreObjects. Branch and tag heads MUST be CoreRefs.

### 10.9 Package And Registry Gateways

Package gateway data MUST be CoreStore data. This includes:

1. repository metadata;
2. package metadata;
3. package version metadata;
4. package files;
5. package signatures and checksums;
6. tags and dist-tags;
7. container manifests;
8. container blobs;
9. upload sessions;
10. publish audit records;
11. gateway credentials;
12. credential revocation records.

Gateway credentials MUST authenticate principals. Gateway-specific roles MUST map to authorisation relationships over Anvil resources.

## 11. Indexing Architecture

### 11.1 Index Definition

Every index MUST have a durable definition record:

```json
{
  "schema": "anvil.index.definition.v1",
  "index_id": "idx_01...",
  "tenant_id": "tenant_01...",
  "bucket_name": "events",
  "kind": "typed_field",
  "source": {
    "kind": "object_current",
    "prefix": "queue/"
  },
  "extractors": [
    {
      "field": "queue_name",
      "type": "string",
      "source": "body_json_pointer",
      "pointer": "/state/queue_name"
    },
    {
      "field": "available_at",
      "type": "timestamp",
      "source": "body_json_pointer",
      "pointer": "/state/available_at"
    }
  ],
  "orderings": [
    { "field": "available_at", "direction": "asc" },
    { "field": "priority", "direction": "desc" },
    { "field": "item_id", "direction": "asc" }
  ],
  "authz": {
    "resource_kind": "object",
    "required_relation": "read"
  },
  "definition_hash": "sha256:...",
  "created_at": "2026-07-04T12:00:00Z"
}
```

The `definition_hash` MUST be computed over canonical definition JSON excluding mutable runtime status fields.

### 11.2 Index Sources

Indexes MUST support these source families:

1. current object metadata and payload;
2. object version metadata and payload;
3. append stream records;
4. embedded database row records;
5. source repository objects;
6. package registry records;
7. mesh control records;
8. authz tuple and schema records.

A source family MUST define how to produce SourceIds, watch events, and extractor input bytes.

### 11.3 Extractors

An extractor converts source data into typed index fields or index documents.

Required extractor kinds:

```text
object_key
object_content_type
object_created_at
object_user_metadata_pointer
object_body_utf8
object_body_json_pointer
append_stream_key
append_sequence
append_created_at
append_content_type
append_user_metadata_pointer
append_payload_utf8
append_payload_json_pointer
package_name
package_version
gateway_tag
media_transcript
source_repository_path
source_repository_blob_text
embedded_database_table
embedded_database_column
```

An extractor MUST fail loudly for invalid input. It MUST NOT invent missing values unless the index definition explicitly configures a default.

### 11.4 Materialisation

Production indexes MUST be materialised. A query MUST NOT scan all objects or all append records in a bucket for normal operation.

An indexer MUST:

1. read source watch events from a persisted cursor;
2. fetch source records from CoreStore;
3. extract values according to the index definition;
4. write immutable index segments through CoreStore;
5. update the index generation CoreRef through compare-and-swap;
6. publish lag diagnostics.

A bootstrap scan MAY create generation 1 for a new index. After bootstrap, maintenance MUST be watch-driven.

### 11.5 Path Index

The path index maps key prefixes to SourceIds. It MUST be materialised for buckets above the configured small-bucket threshold. It MUST support ordered listing, delimiter grouping, pagination, and prefix filtering.

### 11.6 Typed Field And Range Index

Typed field indexes MUST support:

1. equality;
2. set membership;
3. range predicates;
4. existence;
5. null checks;
6. typed ordering;
7. stable pagination;
8. compound keys;
9. sparse fields;
10. index lag reporting.

Supported field types MUST include:

```text
string
bytes
bool
int64
uint64
float64
decimal
timestamp
uuid
source_id
```

Comparison semantics MUST be defined by field type. String comparison MUST declare collation. Timestamp comparison MUST normalise to UTC.

### 11.7 Full-Text Index

Full-text indexes MUST be materialised as CoreStore segments. A full-text index definition MUST specify:

1. source family;
2. text extractors;
3. language or analyser;
4. tokenisation rules;
5. normalisation rules;
6. stop-word policy;
7. stemming policy where used;
8. stored fields;
9. ranking model;
10. generation policy.

The index MUST emit SourceIds and scores. The query planner MUST be able to intersect full-text results with typed filters, path filters, vector candidates, and authorisation result sets.

### 11.8 Vector Index

Vector indexes MUST use a production embedding provider or explicit caller-supplied vectors. Deterministic pseudo-embeddings MUST NOT be used in production modes.

A vector index definition MUST specify:

```json
{
  "schema": "anvil.index.vector_definition.v1",
  "source": { "kind": "object_current", "prefix": "docs/" },
  "extractor": {
    "kind": "object_body_utf8"
  },
  "embedding": {
    "provider": "configured_provider_name",
    "model": "text-embedding-model",
    "dimension": 1536,
    "modality": "text",
    "normalisation": "unit_l2",
    "chunking": {
      "strategy": "token_window",
      "max_tokens": 800,
      "overlap_tokens": 80
    }
  },
  "ann": {
    "algorithm": "hnsw",
    "metric": "cosine",
    "m": 32,
    "ef_construction": 200,
    "ef_search_default": 128
  }
}
```

Vector index records MUST include embedding provenance:

1. provider name;
2. model name;
3. model version if available;
4. dimension;
5. modality;
6. normalisation;
7. chunking configuration hash;
8. extractor definition hash.

Changing any of these fields MUST create a new index generation. Old generations MAY remain queryable until deleted by lifecycle policy.

### 11.9 Filtered Vector Search

Vector indexes MUST support permission-aware and predicate-aware filtering.

A vector segment MUST support one of these strategies:

1. pre-filtered ANN traversal using an allowed SourceId bitset or label filter;
2. exact vector scan over a bounded allowed candidate set when the filter is highly selective;
3. bounded ANN over-fetch with explicit diagnostics when neither of the above can satisfy the requested limit efficiently.

Unbounded over-fetch followed by final authorisation filtering MUST NOT be the primary strategy for protected queries.

### 11.10 Authorisation Index

The authorisation engine MUST maintain materialised indexes that allow efficient permission-aware query planning.

Required indexes:

1. `(subject, relation, resource_namespace) -> resource SourceIds or resource ids`;
2. `(resource, relation) -> subjects and usersets`;
3. `(tuple key) -> current tuple state`;
4. `(schema binding) -> current schema revision`;
5. `(userset dependency) -> affected derived entries`.

Computed usersets and tuple-to-userset rewrites MUST be reflected in derived indexes. The indexes MUST publish their authz revision and watch cursor.

### 11.11 Package Indexes

Package and registry gateways MUST use materialised package indexes. Required package index families:

1. repository by tenant and gateway;
2. package by repository and name;
3. version by package and semantic version or gateway-specific version;
4. tag or dist-tag by package;
5. blob by digest;
6. manifest by digest;
7. package file by checksum;
8. publisher audit by principal and time.

Package indexes MUST emit SourceIds and MUST participate in authorisation-aware query planning.

### 11.12 Index Segment Format

Every materialised index segment MUST be a CoreObject. The segment header MUST describe the index definition hash, source cursor range, source family, field schema, generation, and row count.

Typed field segment header:

```json
{
  "schema": "anvil.index.typed_segment_header.v1",
  "index_id": "idx_01...",
  "definition_hash": "sha256:...",
  "generation": 17,
  "source_family": "object_current",
  "first_source_cursor": "watch_01...",
  "last_source_cursor": "watch_02...",
  "fields": [
    { "name": "state", "type": "string", "collation": "bytewise" },
    { "name": "available_at", "type": "timestamp", "collation": "utc" },
    { "name": "priority", "type": "int64", "collation": "numeric" }
  ],
  "row_count": 100000,
  "min_key": ["pending", "2026-07-04T00:00:00Z", 0],
  "max_key": ["retry", "2026-07-04T23:59:59Z", 1000]
}
```

Typed field rows MUST be sorted by the segment key. A row MUST contain typed key values, SourceId, object or record version where applicable, and optional stored fields declared by the definition.

Full-text segment header:

```json
{
  "schema": "anvil.index.full_text_segment_header.v1",
  "index_id": "idx_01...",
  "definition_hash": "sha256:...",
  "generation": 9,
  "analyser_hash": "sha256:...",
  "term_count": 90000,
  "posting_count": 500000,
  "source_cursor": "watch_02..."
}
```

Vector segment header:

```json
{
  "schema": "anvil.index.vector_segment_header.v1",
  "index_id": "idx_01...",
  "definition_hash": "sha256:...",
  "generation": 12,
  "dimension": 1536,
  "metric": "cosine",
  "algorithm": "hnsw",
  "embedding_provenance_hash": "sha256:...",
  "vector_count": 250000,
  "source_cursor": "watch_02..."
}
```

An index segment MUST NOT depend on local absolute paths. It MUST be readable on any node that can read its CoreObject and the referenced definitions.



### 11.12A Typed Field Row Encoding

Typed field rows MUST use a deterministic binary encoding so segments can be searched without reparsing JSON.

```text
typed-row        = key-count key-values source-id value-flags stored-fields row-hash
key-count        = uint16-le
key-values       = *typed-value
typed-value      = type-tag null-flag encoded-value
type-tag         = uint8
null-flag        = uint8 ; 0 present, 1 null, 2 missing
encoded-value    = bytes ordered according to field type
source-id        = source-id-binary
value-flags      = uint32-le ; bit 0 tombstone, bit 1 deleted-current
stored-fields    = len-bytes
row-hash         = 32OCTET ; SHA-256 over preceding row fields
```

Typed values MUST be encoded for lexicographic byte ordering:

```text
null        = %x00
missing     = %x01
bool false  = %x10
bool true   = %x11
int64       = %x20 8OCTET ; big-endian after xor with 0x8000000000000000
uint64      = %x21 8OCTET ; big-endian
float64     = %x22 8OCTET ; IEEE sortable transform, NaN rejected unless configured
string      = %x30 utf8-bytes %x00 ; invalid UTF-8 rejected
bytes       = %x31 escaped-bytes terminator
decimal     = %x32 sortable-i128 ; precision/scale declared by index definition
terminator  = %x00.00
timestamp   = %x40 int64 ; UTC nanoseconds since Unix epoch, sortable int64 encoding
uuid        = %x50 16OCTET
source-id   = %x60 source-id-binary
```

`escaped-bytes` MUST encode each input `0x00` byte as `0x00 0xff`. The field terminator MUST be `0x00 0x00`. No other byte is escaped. This rule applies to both UTF-8 string bytes and raw byte fields.

`decimal` fields MUST declare `precision` and `scale` in the index definition. Decimal values MUST be normalised to that scale and encoded as a signed 128-bit integer coefficient. The coefficient MUST be transformed for lexicographic order by xor with `0x80000000000000000000000000000000` and then encoded big-endian. Values outside declared precision/scale MUST fail extraction.

`float64` ordering MUST use the standard sortable transform: for negative values invert all bits; for non-negative values set the sign bit. NaN MUST be rejected unless the index definition explicitly maps NaN to null.


Descending order MUST be implemented by inverting encoded key bytes for that field inside the segment key. Segment metadata MUST record whether a field is ascending or descending.

For `object_current`, a later row for the same logical resource supersedes earlier rows when its generation is greater. Tombstone rows MUST suppress prior rows at or below the tombstone generation.

### 11.12B Full-Text Posting Encoding

Full-text segments MUST store a sorted term dictionary followed by compressed posting blocks. The term dictionary maps analyser-normalised term bytes to posting-block offsets. Terms MUST be sorted lexicographically by UTF-8 bytes after analyser normalisation.

```text
posting = source-id field-id term-frequency position-count positions score-features
field-id = uint16-le
term-frequency = uint32-le
position-count = uint32-le
positions = *uint32-le
score-features = len-bytes
```

Full-text segment layout:

```text
fts-segment      = header term-dictionary posting-blocks stored-fields trailer
term-dictionary  = term-count *term-entry
term-count       = uint64-le
term-entry       = term-len term-bytes doc-frequency first-block-offset
term-len         = uint32-le
term-bytes       = *OCTET
doc-frequency    = uint64-le
first-block-offset = uint64-le
posting-blocks   = *posting-block
posting-block    = block-len codec posting-count postings block-crc32c
codec            = uint16-le ; 0 = none_v1
posting-count    = uint32-le
postings         = *posting
```

`none_v1` posting blocks MUST store postings directly in ascending SourceId sort order using the `posting` layout above. Later codecs MAY be added only by naming a new codec id and defining its byte format. Posting blocks MUST be independently checksummed. Deletion is handled by generation and tombstone SourceIds. A searcher MUST ignore postings older than the visible current generation when a tombstone or newer current row exists.

### 11.12C Vector Block Encoding

Vector segments MUST store vector blocks and a SourceId table. Each vector entry MUST bind:

1. SourceId;
2. vector ordinal;
3. embedding provenance hash;
4. source generation;
5. optional chunk ordinal;
6. optional segment-local authz/filter labels.

Vector numeric encoding MUST be `f32` little-endian unless the vector definition declares another numeric format. All vectors in one segment MUST have the definition dimension.

Vector segment layout:

```text
vector-segment   = header source-table vector-blocks ann-blocks trailer
source-table     = source-count *source-entry
source-count     = uint64-le
source-entry     = source-id-binary generation chunk-ordinal label-count labels
chunk-ordinal    = uint32-le
label-count      = uint32-le
labels           = *uint64-le
vector-blocks    = *vector-block
vector-block     = block-len vector-count raw-f32-vectors block-crc32c
ann-blocks       = ann-kind ann-len ann-bytes ann-crc32c
ann-kind         = uint16-le ; 1 = hnsw_v1
```

For `hnsw_v1`, `ann-bytes` MUST contain level count, node count, neighbour lists by level, and entry point ordinal in a deterministic binary format defined by the implementation and recorded by `ann_format_hash`. Approximate-nearest-neighbour graph or tree data MUST be stored inside the vector segment CoreObject or referenced by CoreObject hash from the segment header. It MUST NOT depend on local absolute paths.

### 11.12D Segment Merge And Compaction

Index compaction MUST create a new generation. It MUST:

1. read one or more prior index generations;
2. apply tombstones and superseding current rows;
3. preserve SourceId ordering;
4. write new immutable segments through CoreStore;
5. publish the new generation with CoreRef compare-and-swap;
6. keep prior generations readable while page tokens can reference them;
7. delete prior generations only after retention policy proves no live token or consumer can require them.

Compaction MUST NOT mutate existing index segments in place.

### 11.12E Degraded Plan Bounds

A degraded index plan MUST declare:

1. maximum source records scanned;
2. maximum vector candidates read;
3. maximum postings read;
4. maximum permission entries expanded;
5. whether results may be incomplete;
6. whether ranking may be approximate.

If a plan cannot satisfy configured bounds, Anvil MUST return `QueryPlanRequiresIndex` or `QueryPlanExceedsBounds` instead of scanning unbounded data.


### 11.13 Index Statistics

Index statistics are required for composite planning. Each index generation MUST publish statistics sufficient for bounded planning:

1. total SourceIds;
2. per-field cardinality estimate;
3. per-field min and max where ordered;
4. null or missing count;
5. segment count;
6. segment key ranges;
7. index byte size;
8. last source cursor;
9. lag diagnostics;
10. degraded or failed extraction counts.

Statistics MUST be stored through CoreStore and versioned with the index generation.

## 12. Composite Query Planning

### 12.1 QuerySpec

Composite queries MUST be represented as a query plan over primitive indexes.

Example QuerySpec:

```json
{
  "schema": "anvil.query.spec.v1",
  "scope": {
    "mesh_id": "mesh_01...",
    "anvil_storage_tenant_id": "storage_tenant_01...",
    "authz_scope": {
      "anvil_storage_tenant_id": "storage_tenant_01...",
      "authz_realm_id": "realm_01..."
    },
    "tenant_id": "tenant_01...",
    "bucket_name": "events"
  },
  "source_kind": "object_current",
  "where": {
    "all": [
      { "path_prefix": "queue/outbound/" },
      { "field": "state", "op": "in", "value": ["pending", "retry"] },
      { "field": "available_at", "op": "lte", "value": "2026-07-04T12:00:00Z" },
      { "full_text": { "query": "delivery failure" } },
      { "vector": { "field": "body_embedding", "near": "query_vector_01", "k": 200 } },
      { "can": { "relation": "read" } }
    ]
  },
  "order_by": [
    { "field": "available_at", "direction": "asc" },
    { "field": "priority", "direction": "desc" },
    { "field": "item_id", "direction": "asc" }
  ],
  "limit": 100,
  "consistency": {
    "min_source_cursor": "watch_01...",
    "min_authz_revision": "azr_01...",
    "allow_stale_index": false
  }
}
```

### 12.2 Planning Rules

The query planner MUST:

1. normalise all predicates into a canonical query hash;
2. select primitive indexes by source kind and predicate coverage;
3. estimate candidate cardinality using index statistics;
4. include authorisation as a required predicate for protected resources;
5. choose an intersection order that minimises work;
6. push filters into primitive indexes where supported;
7. avoid scanning unbounded buckets or streams;
8. bind page tokens to selected index generations;
9. perform final authorisation checks on returned SourceIds;
10. return diagnostics when a query uses a degraded plan.

### 12.3 Hybrid Search

Hybrid search MUST be a query-planner capability, not a special two-index feature. A hybrid query MAY combine:

1. vector similarity;
2. full-text ranking;
3. typed filters;
4. path filters;
5. package metadata filters;
6. append stream filters;
7. recency boosts;
8. authorisation filters.

The ranking expression MUST be explicit in the query or index definition. Example:

```json
{
  "rank": {
    "sum": [
      { "weight": 0.65, "score": "vector.cosine" },
      { "weight": 0.25, "score": "full_text.bm25" },
      { "weight": 0.10, "score": "recency.decay" }
    ]
  }
}
```

### 12.4 Permission-Aware Filtering

For protected resources, the planner MUST obtain an authorisation candidate set before returning results. The authorisation candidate set MAY be exact or generation-bound with a final exact check, but it MUST be used early enough to avoid querying or filtering billions of unauthorised records.

The planner MUST fail closed if it cannot push an authorisation predicate into at least one bounded primitive index path. A degraded plan MAY over-fetch only when all of these are true:

1. the over-fetch limit is bounded by configured policy;
2. the maximum scanned candidate count is returned in diagnostics;
3. the caller requested or accepted degraded results;
4. final authorisation checks still run;
5. the plan cannot disclose unauthorised object existence through counts, scores, or diagnostics.

Default production policy MUST reject unbounded over-fetch.

The required flow is:

```text
client query
  -> authenticate principal
  -> resolve authz scope and required relation
  -> obtain authz revision R
  -> obtain materialised permission set or bounded permission iterator for R
  -> plan primitive index reads with permission set as a filter
  -> intersect source candidate sets
  -> fetch candidate records
  -> final CheckPermission at revision R or stronger
  -> return page token bound to R and index generations
```

A query implementation that performs unbounded index search and then drops unauthorised records at the end is non-conforming.

### 12.5 Query Consistency

Query consistency options MUST include:

1. best-effort current generation;
2. require source watch cursor caught up to a supplied cursor;
3. require authz revision at least a supplied revision;
4. require exact index generation;
5. fail rather than serve stale index results.

When a required cursor or revision is not available, Anvil MUST return a structured lag error containing current cursor, requested cursor, current generation, and index id where safe to disclose.

### 12.6 Composite Acceleration Views

Primitive index intersection is sufficient for ad hoc queries, but common high-volume query shapes MUST be eligible for materialised acceleration views.

An acceleration view is a derived index whose definition references other primitive index definitions and an authorisation relation. It stores pre-composed SourceId sets, sorted result keys, score components, or permission-filtered candidate blocks.

Example definition:

```json
{
  "schema": "anvil.query.acceleration_view.v1",
  "view_id": "view_01...",
  "source_kind": "object_current",
  "inputs": [
    { "index_id": "idx_due_fields", "generation_policy": "latest" },
    { "index_id": "idx_body_text", "generation_policy": "latest" },
    { "authz_relation": "reader", "generation_policy": "latest" }
  ],
  "materialised_order": [
    { "field": "available_at", "direction": "asc" },
    { "field": "priority", "direction": "desc" }
  ],
  "refresh": {
    "mode": "watch_driven",
    "max_lag_ms": 5000
  }
}
```

Acceleration views MUST be optional. Correctness MUST NOT depend on a manually maintained application projection. If no acceleration view exists, the query planner MUST still plan against primitive materialised indexes.

Acceleration views MUST be maintained from watch streams and MUST publish generation, source cursors, input index generations, authz revision, and lag diagnostics.

### 12.7 Permission Set Representation

Permission sets used by the query planner MUST be represented in a form that primitive indexes can consume efficiently.

Allowed representations include:

1. sorted SourceId ranges;
2. compressed SourceId bitmaps scoped to an index generation;
3. bounded iterators over resource ids;
4. segment-local allow bitsets;
5. exact small sets for highly selective permissions.

The representation MUST declare authz scope, authz revision, resource namespace, source kind, and generation. Primitive indexes MUST reject permission filters with incompatible authz scope, source kind, generation, or namespace.

Permission iterators MUST have explicit upper bounds or segment-local bounds. If the planner cannot prove a finite bound before scanning, it MUST reject the query or require a materialised acceleration view.

## 13. Authorisation Model

### 13.1 One Engine

Anvil MUST implement one Zanzibar-style authorisation engine. The same evaluator, schema model, tuple log, schema binding model, caveat evaluation model, revision model, watch model, and derived index model MUST be used for:

1. Anvil's reserved system resources;
2. tenant resources;
3. user-defined authz realms;
4. gateway credentials;
5. package repositories;
6. object resources;
7. stream resources;
8. index resources;
9. mesh administration resources.

The implementation MUST NOT create a second authorisation system for internal resources.

### 13.1A AuthzScope

Every authorisation operation MUST name an `AuthzScope`:

```rust
pub struct AuthzScope {
    pub anvil_storage_tenant_id: AnvilStorageTenantId,
    pub authz_realm_id: AuthzRealmId,
}
```

`AnvilStorageTenantId` is Anvil's storage-level isolation boundary. It is not the same thing as any tenant, account, organisation, customer, workspace, or actor stored by a system using Anvil.

`AuthzRealmId` is the ReBAC isolation boundary inside an Anvil storage tenant. A single Anvil storage tenant MAY contain many authz realms.

Every tuple write, tuple read, schema bind, check, list, watch, query authorisation predicate, page token, and derived authz index MUST carry or bind the full `AuthzScope`. Backend code MUST NOT infer authz realm isolation from a token, client object, bucket name, in-memory cache key, or caller convention.


### 13.2 Reserved System Realm

Anvil MUST have a reserved system authz realm. The system realm stores Anvil's own schema, tuples, and derived indexes.

The system realm MUST:

1. be created during cluster bootstrap;
2. be stored in CoreStore;
3. be readable only by authorised Anvil administrative principals;
4. be mutable only through administrative APIs that require system-level permissions;
5. reject all public API attempts to read or write its raw tuple, schema, derived index, and watch paths;
6. reject tenant-owned credentials attempting to mutate system schema or system tuples;
7. emit audit records for every mutation.

### 13.3 Tenant Realms

A tenant MAY create tenant-owned authz realms if the system realm grants that tenant principal the required relation. Tenant-owned realms MUST use the same schema and evaluator semantics as the system realm.

A tenant realm owner MAY:

1. put a schema;
2. bind a schema revision to a realm;
3. write tuples in that realm;
4. read tuples where authorised;
5. run checks where authorised;
6. list objects or subjects where authorised;
7. watch tuple changes where authorised.

A tenant realm owner MUST NOT mutate Anvil system resources unless separately authorised in the system realm.

### 13.4 Built-In System Namespaces

The system schema MUST include these namespaces or equivalent names with identical semantics:

```text
anvil_tenant
anvil_principal
anvil_group
anvil_service_account
anvil_credential
anvil_bucket
anvil_object
anvil_object_prefix
anvil_object_link
anvil_stream
anvil_index
anvil_authz_realm
anvil_authz_schema
anvil_region
anvil_cell
anvil_node
anvil_gateway
anvil_package_repository
anvil_package
anvil_package_version
anvil_registry_blob
anvil_mesh
```

Required relations MUST include:

```text
owner
admin
member
reader
writer
publisher
link_admin
index_reader
index_admin
stream_reader
stream_writer
schema_reader
schema_admin
credential_admin
gateway_user
gateway_admin
node_admin
region_admin
mesh_admin
public_reader
```

The implementation MAY add more relations. It MUST document every built-in relation.

### 13.5 Relation Rule Semantics

Anvil authz schemas MUST support these relation rule forms:

```text
inherit(relation)
computed(tuple_relation, target_relation)
tuple_to_userset(tuple_relation, target_relation)
caveated(rule, caveat_name)
union(rule...)
intersection(rule...)
difference(base_rule, excluded_rule)
```

A direct tuple for a relation MUST grant that relation. The rules above define additional ways to derive the relation.

Minimum system namespace semantics:

```json
{
  "namespace": "anvil_bucket",
  "relations": {
    "reader": [
      { "inherit": "owner" },
      { "inherit": "admin" },
      { "inherit": "writer" },
      { "computed": { "tuple_relation": "parent_tenant", "target_relation": "member" } },
      { "tuple_to_userset": { "tuple_relation": "public_reader", "target_relation": "member" } }
    ],
    "writer": [
      { "inherit": "owner" },
      { "inherit": "admin" }
    ],
    "index_reader": [
      { "inherit": "owner" },
      { "inherit": "admin" },
      { "inherit": "reader" }
    ],
    "link_admin": [
      { "inherit": "owner" },
      { "inherit": "admin" }
    ]
  }
}
```

The JSON above is illustrative of the required semantics. The wire format MAY be protobuf, JSON, or another typed representation, but the evaluator MUST implement these rule forms consistently for system and tenant realms.

### 13.6 Public Access

Public access MUST be represented as ordinary authorisation data. A public bucket or public prefix grants a relation to a public principal such as `principal:public:anyone` in the relevant realm.

External requests that arrive without user credentials MAY be mapped to the public principal by the gateway. Internally, the query, object read, and listing paths MUST still run an authorisation check for the public principal.

### 13.7 Reserved CoreStore Paths

Reserved CoreStore namespaces MUST NOT be directly accessible through public object APIs, S3 APIs, package APIs, or tenant authz APIs.

Reserved prefixes include:

```text
_core/
_system/
_authz/system/
_mesh/
_nodes/
_regions/
_credentials/
_internal/
```

For reserved namespaces:

1. unauthorised `GET`, `HEAD`, `LIST`, `PUT`, `DELETE`, `COPY`, `APPEND`, `WATCH`, and `QUERY` MUST fail;
2. failures MUST be hard authorisation failures, not silent empty results, unless the public API specifically requires non-disclosure semantics;
3. server-internal access MUST require an internal server-minted authority;
4. audit records MUST be emitted for rejected mutation attempts where safe.

## 14. User-Facing Security And Administration Model

### 14.1 Principals Instead Of Applications-As-Policy

The user-facing model SHOULD expose principals and relationships directly.

Operators and tenant admins create:

1. users;
2. service accounts;
3. groups;
4. credentials for principals;
5. buckets;
6. indexes;
7. streams;
8. package repositories;
9. authorisation realms;
10. grants between principals and resources.

Gateway terms such as S3 access key, registry token, package publisher token, or static-site token are credential types, not the underlying permission model.

### 14.2 Example Administrative Flow

A representative flow:

```text
anvil tenant create acme
anvil principal create user alice --tenant acme
anvil principal create service-account deploy-bot --tenant acme
anvil bucket create releases --tenant acme --region eu-west-1
anvil grant anvil_bucket:acme/releases writer service-account:acme/deploy-bot
anvil grant anvil_bucket:acme/releases reader principal:public:anyone
anvil credential issue --subject service-account:acme/deploy-bot --gateway s3
anvil credential issue --subject service-account:acme/deploy-bot --gateway docker
```

The exact CLI syntax MAY evolve, but the concepts MUST remain principal, credential, resource, and relationship.

## 15. Gateway Foundation

### 15.1 Gateway Rule

A gateway maps an external protocol into Anvil resources, credentials, objects, streams, indexes, and authorisation checks. It MUST NOT bypass CoreStore or the authorisation engine.



Every gateway MUST define:

1. route mount and host matching rules;
2. external identifier normalisation rules;
3. Anvil resource identity mapping;
4. credential challenge and verification flow;
5. upload session lifecycle;
6. digest and checksum verification rules;
7. tag or alias update semantics;
8. revocation propagation requirements;
9. reserved internal resource paths;
10. audit record shapes.

### 15.1A Identifier Normalisation

Gateway identifiers MUST be normalised before they become Anvil resource ids. Normalisation MUST define case sensitivity, Unicode normalisation, path traversal rejection, separator handling, maximum length, and reserved names.

A gateway MUST reject identifiers that would map outside the target tenant, bucket, repository, package, prefix, or reserved namespace.

### 15.1B Upload Sessions

Gateway upload sessions MUST be CoreStore records. An upload session MUST include:

```json
{
  "schema": "anvil.gateway.upload_session.v1",
  "session_id": "upload_01...",
  "gateway": "docker",
  "tenant_id": "tenant_01...",
  "repository": "containers/api",
  "subject_principal": "principal_01...",
  "state": "open",
  "expected_digest": null,
  "received_bytes": 0,
  "staged_parts": [],
  "created_at": "2026-07-04T12:00:00Z",
  "expires_at": "2026-07-04T13:00:00Z"
}
```

Upload finalisation MUST verify digest or checksum before publishing visible package, blob, or object state. Expired sessions MUST be garbage collected without exposing partial data.

Upload session states are:

```text
open -> receiving -> finalising -> committed
open -> aborted
receiving -> receiving
receiving -> aborted
receiving -> expired
finalising -> committed
finalising -> aborted
committed -> terminal
aborted -> terminal
expired -> terminal
```

Allowed operations:

1. `start`: creates `open` or returns the existing session for the same idempotency key and target;
2. `append_part`: moves `open` to `receiving` or `receiving` to `receiving` and appends staged part references;
3. `finalise`: moves `receiving` to `finalising`, verifies digest/checksum, writes visible package/blob/object state, then moves to `committed`;
4. `abort`: moves `open` or `receiving` to `aborted`;
5. `expire`: moves stale `open` or `receiving` sessions to `expired`.

Every state transition MUST be a CoreRef compare-and-swap on the upload session generation. Concurrent finalisation MUST produce exactly one winner. A losing finaliser MUST read the committed result if the digest matches or fail with a conflict if it does not.

Upload parts are staged immutable CoreObjects or local staging records. They MUST NOT become visible package/blob/object data until the `committed` transition publishes the visible resource transaction.

Each appended part MUST have this identity:

```json
{
  "schema": "anvil.gateway.upload_part.v1",
  "session_id": "upload_01...",
  "part_id": "part_000001",
  "offset": 0,
  "length": 1048576,
  "payload_hash": "sha256:...",
  "idempotency_key_hash": "sha256:...",
  "core_object_ref": "core:sha256:..."
}
```

Part rules:

1. `part_id` MUST be unique inside a session;
2. `offset` and `length` MUST define a non-overlapping byte range;
3. contiguous protocols MUST require the next part offset to equal current committed received length;
4. multipart protocols MAY accept out-of-order parts only if the protocol supplies stable part numbers and finalisation defines the order;
5. retrying the same `append_part` idempotency key with the same `part_id`, `offset`, `length`, and `payload_hash` MUST return the original part receipt;
6. retrying the same `append_part` idempotency key with different bytes or range MUST fail;
7. concurrently appending two different parts at the same offset MUST produce exactly one winner;
8. a losing concurrent append MAY retry after reading the latest session generation;
9. finalisation MUST verify there are no missing required byte ranges or duplicate final part ordinals.



### 15.1C Token Challenge And Revocation

Gateways that use challenge/response authentication MUST map challenges to Anvil credentials and principals. A successful challenge only authenticates the principal. Every action still requires an authorisation check.

Credential revocation MUST take effect for new gateway requests no later than the configured credential cache TTL. The TTL MUST be documented and bounded. Administrative revocation MAY force immediate cache invalidation through watch events.


### 15.2 S3-Compatible Gateway

The S3-compatible gateway maps:

```text
S3 access key      -> Anvil credential -> principal
S3 bucket          -> Anvil tenant bucket
S3 object key      -> Anvil object key
S3 object metadata -> Anvil object metadata
S3 public read     -> public principal relation
S3 list            -> path index + authz filter
```

S3 gateway writes MUST create normal Anvil object records through CoreStore. S3 gateway reads MUST use normal Anvil object and authorisation paths.

### 15.3 Static Site Gateway

A static site gateway maps a host alias and path to a tenant bucket, optional prefix, object key, and object link resolution. Static site listing MUST be disabled unless explicitly enabled by authorisation and gateway configuration.

### 15.4 Container Registry Gateway

A container registry gateway MUST map registry concepts as follows:

```text
registry namespace/repository -> anvil_package_repository
manifest digest               -> anvil_package_version or anvil registry manifest resource
blob digest                   -> anvil_registry_blob and CoreObject payload
manifest tag                  -> object link or package tag CoreRef
upload session                -> CoreStore stream/ref guarded by credential and lease
```

A tag update MUST be a CoreRef compare-and-swap operation. Blob upload finalisation MUST verify digest before commit. Pull and push permissions MUST be authorisation relations over repository, manifest, and blob resources.

### 15.5 Rust Crate Registry Gateway

A Rust crate registry gateway MUST store crate files as CoreObjects and crate index entries as package index records. Crate ownership, publish permission, yank permission, and read permission MUST be authorisation relations.

### 15.6 npm Gateway

An npm gateway MUST store package metadata documents, tarballs, dist-tags, and publish audit records through CoreStore.

Minimum support MUST include:

1. package metadata read;
2. tarball read;
3. package publish;
4. version publish;
5. dist-tag update;
6. token authentication mapped to Anvil principals;
7. package ownership and publish grants through authorisation relations.

Broader compatibility MAY add search endpoints, audit endpoints, provenance, two-factor workflow hooks, deprecation metadata, access-level flags, and organisation scopes. These extensions MUST still map into CoreStore and authorisation resources.

### 15.7 PyPI Gateway

A PyPI gateway MUST store project metadata, release files, simple-index documents, attestations, and publish audit records through CoreStore. Upload, yank, owner, maintainer, and read permissions MUST be authorisation relations.

### 15.8 Maven Gateway

A Maven gateway MUST store group/artifact/version metadata, POM files, checksums, signatures, and package files through CoreStore. Publish and read permissions MUST be authorisation relations over repository or package resources.

### 15.8A Gateway Mount Resolution

Every gateway request MUST resolve through a `GatewayMount` record before route-specific handling.

Required shape:

```json
{
  "schema": "anvil.gateway.mount.v1",
  "mount_id": "gw_01...",
  "gateway": "docker",
  "hosts": ["registry.acme.eu-west-1.anvil-storage.com"],
  "path_prefixes": ["/"],
  "mesh_id": "mesh_01...",
  "region": "eu-west-1",
  "anvil_storage_tenant_id": "storage_tenant_01...",
  "authz_scope": {
    "anvil_storage_tenant_id": "storage_tenant_01...",
    "authz_realm_id": "realm_01..."
  },
  "tenant_id": "tenant_01...",
  "registry_instance_id": "registry_01...",
  "default_bucket": "packages",
  "repository_prefix": "",
  "state": "active",
  "generation": 12
}
```

Resolution order MUST be:

1. exact host alias mount;
2. virtual-host regional mount;
3. path-style regional mount;
4. reject if no active mount matches.

The mount supplies `mesh_id`, `anvil_storage_tenant_id`, `authz_scope`, `tenant_id`, gateway kind, registry instance id, and default bucket. Route parsing MUST NOT infer authz scope from package names, object keys, or caller-supplied headers.

S3 path-style regional requests MUST use this shape:

```text
https://<region>.anvil-storage.com/<tenant>/<bucket>/<object-key>
```

Package and registry path-style regional requests MUST use this shape:

```text
https://<region>.anvil-storage.com/<tenant>/_gateway/<gateway>/<registry-instance>/<protocol-path>
```

Virtual-host package and registry requests MUST use a host alias or this shape:

```text
https://<registry-instance>.<tenant>.<region>.anvil-storage.com/<protocol-path>
```

### 15.9 Required Gateway Route Mounts

A conforming implementation that enables the named gateway MUST implement these route mounts or exact protocol equivalents:

```text
S3:
  /<bucket>/<key>                         -> object read/write/list/delete
  virtual host <bucket>.<tenant>.<region> -> object read/write/list/delete

Container registry:
  /v2/                                    -> registry API root
  /v2/<name>/blobs/uploads/              -> start upload session
  /v2/<name>/blobs/uploads/<uuid>        -> patch/finalise upload session
  /v2/<name>/blobs/<digest>              -> blob read
  /v2/<name>/manifests/<reference>       -> manifest read/write
  /v2/<name>/tags/list                   -> tag listing

Rust crate registry:
  /api/v1/crates/new                     -> publish crate
  /api/v1/crates/<crate>/<version>/yank  -> yank version
  /api/v1/crates/<crate>/<version>/unyank -> unyank version
  /crates/<crate>/<version>/download     -> crate file read
  /index/*                               -> sparse index read

npm:
  /<package>                             -> package metadata read/write
  /<package>/-/<tarball>                 -> tarball read/write
  /-/package/<package>/dist-tags/<tag>   -> dist-tag read/write
  /-/whoami                              -> token validation

PyPI:
  /simple/<project>/                     -> simple index read
  /packages/<path>                       -> distribution file read
  /legacy/                               -> upload endpoint
  /pypi/<project>/json                   -> project metadata read

Maven:
  /<group-path>/<artifact>/<version>/<file> -> artifact read/write
  /<group-path>/<artifact>/maven-metadata.xml -> metadata read/write
```

### 15.10 Gateway Identity Mapping

Gateway identities MUST map to Anvil resources as follows:

```text
container name         -> anvil_package_repository(gateway=docker, repository=name)
container manifest ref -> anvil_package_version(reference or digest)
container blob digest  -> anvil_registry_blob(digest)
crate name             -> anvil_package(gateway=cargo, name)
crate version          -> anvil_package_version(gateway=cargo, name, version)
npm package            -> anvil_package(gateway=npm, scope, name)
npm dist-tag           -> package tag CoreRef
PyPI project           -> anvil_package(gateway=pypi, normalised project name)
PyPI file              -> anvil_package_version file by checksum
Maven coordinate       -> anvil_package(gateway=maven, group, artifact)
Maven version/file     -> anvil_package_version file
```

Normalised identifiers MUST be stored in package indexes. Original display names MAY be stored as metadata but MUST NOT be used as authority keys.

### 15.11 Gateway Challenge Flows

Container registry bearer-token flow MUST work as:

```text
client requests protected route
  -> gateway returns WWW-Authenticate challenge naming service and scope
client requests token with gateway credential
  -> Anvil authenticates credential and resolves principal
  -> Anvil checks requested scope against authorisation relations
  -> Anvil returns bounded token for principal, repository, actions, expiry
client retries protected route with token
  -> gateway validates token and performs normal authorisation check again
```

npm, PyPI, Maven, and Cargo token flows MUST authenticate a credential, resolve a principal, and check the route action against Anvil authorisation relations. A valid token MUST NOT bypass route-level authorisation.

### 15.12 Gateway Audit Shapes

Gateway audit records MUST include:

```json
{
  "schema": "anvil.gateway.audit.v1",
  "gateway": "docker",
  "operation": "manifest_put",
  "tenant_id": "tenant_01...",
  "repository": "containers/api",
  "package": null,
  "version_or_reference": "latest",
  "digest": "sha256:...",
  "subject_principal": "principal_01...",
  "credential_id": "cred_01...",
  "request_id": "req_01...",
  "result": "success",
  "created_at": "2026-07-04T12:00:00Z"
}
```

Audit records MUST be append stream records stored through CoreStore.

### 15.13 Gateway Reserved Resources

Gateways MUST reserve internal repository names and paths required by the protocol implementation. Reserved names MUST be rejected during package, repository, object, and tag creation.

Minimum reserved names:

```text
_anvil
_core
_system
_authz
_credentials
..
.
```

A gateway MUST reject percent-encoded or Unicode-normalised forms that resolve to reserved names after decoding and normalisation.


## 16. Multi-Region And Mesh Semantics

### 16.1 Tenant And Bucket Routing

Tenant identity MUST resolve through the mesh routing directory. Buckets belong to a tenant and have a home region. Requests received outside the home region MUST be redirected or proxied according to policy.

### 16.2 Region-Local Durability

By default, object payloads and bucket-local metadata are erasure-coded inside the bucket home region. A bucket replication policy MAY request additional region copies. Each region copy MUST itself be stored through CoreStore and MUST have independent repair state.

### 16.3 Globally Replicated Control Records

Mesh-level records SHOULD be globally replicated. These include:

1. tenant directory;
2. region directory;
3. node directory;
4. bucket routing directory;
5. gateway host alias directory;
6. system authz realm records;
7. credential issuer records;
8. package gateway routing records.

Global records MUST still use CoreStore; they are not an exception to the unified storage rule.

### 16.4 Cross-Region Proxy

Cross-region proxying MUST preserve caller identity, request id, authz context, required relation, and consistency requirements. The receiving region MUST perform its own authorisation check. A proxy token proves the request came from a trusted Anvil node; it MUST NOT replace end-user authorisation.

### 16.5 Region Drain

Region drain MUST follow explicit bucket dispositions. This RFC does not make region drain invent bucket migration or writable-primary promotion.

Allowed dispositions are:

```text
block_until_empty
remain_proxy_only
read_only_until_removed
delete_after_retention
```

If a separate bucket movement or replication-cutover feature is implemented, its migration plan, copy progress, cutover ref, and audit events MUST be CoreStore records. A bucket MUST NOT acknowledge writes in a new home region until that feature has committed a generation-checked cutover ref.

Without such an explicit bucket movement feature, draining a region MUST NOT make another region writable primary for buckets currently owned by the draining region.

## 17. Mutation, CAS, And Lease Fencing

### 17.1 Mutation Batch

A mutation batch groups protected changes. It MAY include:

1. put object;
2. delete object;
3. patch JSON object;
4. append stream record;
5. update object link;
6. checkpoint task lease;
7. commit task lease;
8. update package tag;
9. update index generation ref.

A mutation batch MUST declare preconditions. Preconditions MUST be checked before any protected mutation is made visible.

### 17.2 Atomicity

A mutation batch MUST be atomic at Anvil's visible state layer. Either all included visible state changes commit, or none do. Implementation MAY stage immutable blobs before the visible commit. Unreferenced staged blobs MAY be garbage collected.

### 17.3 Fence Preconditions

A lease-fenced mutation MUST include:

```json
{
  "lease_fence": {
    "task_id": "task_01...",
    "fence_token": "fence_01...",
    "required_owner_principal": "principal_01..."
  }
}
```

The server MUST derive the authenticated principal independently and compare it with the lease owner. The caller MUST NOT be able to impersonate ownership by setting request fields.

### 17.4 Required Stale-Owner Tests

Conformance MUST include tests proving:

1. caller cannot set owner identity by request field;
2. active lease cannot be checkpointed by another authenticated principal;
3. same owner string from another token fails;
4. expired lease can be taken over and increments fence;
5. stale fence cannot checkpoint;
6. stale fence cannot commit;
7. stale fence cannot mutate an object;
8. stale fence cannot append a protected stream record;
9. two concurrent acquires produce exactly one winner;
10. TTL above server cap is rejected or clamped according to policy;
11. token from tenant A cannot acquire tenant B lease;
12. force release requires lease administration permission.

## 18. Watch-Driven Derived Maintenance

### 18.1 Watch Cursor Discipline

Every derived subsystem MUST persist its source cursor through CoreStore. A derived subsystem includes indexes, authz derived usersets, package indexes, path indexes, embedded database projections, search indexes, vector indexes, routing materialisations, and repair summaries.

### 18.2 Lag Reporting

Each derived subsystem MUST report:

1. source cursor processed;
2. latest known source cursor;
3. lag count where computable;
4. lag time where computable;
5. current generation;
6. last error;
7. last successful commit time.

### 18.3 Repair

Repair MAY rescan canonical CoreStore data to rebuild a derived structure. Repair output MUST be written as a new generation and swapped into place with CoreRef compare-and-swap.

Repair MUST NOT mutate source records to fit a derived index.

A repair job MUST produce a repair manifest:

```json
{
  "schema": "anvil.repair.manifest.v1",
  "repair_id": "repair_01...",
  "target_family": "typed_field_index",
  "target_id": "idx_01...",
  "source_start_cursor": "watch_01...",
  "source_end_cursor": "watch_02...",
  "old_generation": 16,
  "new_generation": 17,
  "findings_ref": "core:sha256:...",
  "promoted": false,
  "created_at": "2026-07-04T12:00:00Z"
}
```

Promotion of repair output MUST be a fenced CoreRef compare-and-swap. Failed or abandoned repair output MUST remain invisible and be eligible for garbage collection.

## 19. Operational Model

### 19.1 Admin Port

Administrative APIs MUST be served on a separate administrative listener. Public listeners MUST NOT expose administrative APIs. Administrative APIs MUST still authenticate and authorise every request.

### 19.2 Internal Server Authority

An internal server authority is a credential minted for Anvil server-to-server operation. It MAY access reserved CoreStore namespaces only for operations required by Anvil itself. It MUST be scoped, auditable, and rotatable.

### 19.3 Observability

CoreStore MUST expose metrics for:

1. shard write latency;
2. shard read latency;
3. erasure reconstruction count;
4. repair backlog;
5. stream append latency;
6. CoreRef CAS conflicts;
7. fence acquisition conflicts;
8. index lag by index id;
9. authz derived lag by realm;
10. query degraded-plan count;
11. filtered vector fallback count;
12. local staging bytes;
13. cache hit ratio;
14. cross-region proxy latency;
15. package gateway upload finalisation latency.

### 19.4 Audit

The following MUST emit audit records:

1. tenant create/delete;
2. principal create/delete;
3. credential issue/revoke/rotate;
4. grant/revoke;
5. authz schema put/bind;
6. bucket create/delete;
7. object public access change;
8. gateway repository create/delete;
9. package publish/yank/tag change;
10. node add/drain/remove;
11. region add/drain/remove;
12. internal reserved namespace access denial;
13. forced lease release;
14. repair generation promotion.

Audit records MUST be append stream records stored through CoreStore.

## 20. Failure And Recovery

### 20.1 Node Crash Before Commit

If a node crashes before CoreStore commit, recovery MAY discard staging data. Idempotent retry MUST either complete the original mutation or return the original committed receipt.

### 20.2 Node Crash After Shard Writes Before Ref Commit

Unreferenced shard data MAY exist after a crash. It MUST NOT be visible without a committed CoreRef, stream record, or manifest reference. Garbage collection MAY remove it after retention policy.

### 20.3 Stale Partition Owner

A stale partition owner MUST fail all fenced writes. The failure MUST occur before the write becomes visible.

### 20.4 Indexer Crash

An indexer crash MUST leave the current generation readable. On restart, the indexer resumes from its persisted cursor or rebuilds a new generation. It MUST NOT publish a partially built generation.

### 20.5 Region Unavailable

If a bucket home region is unavailable, requests MUST follow the bucket's replication and failover policy. If no policy permits failover writes, writes MUST fail rather than silently creating divergent state.

## 21. Conformance Requirements

### 21.0 Root And Atomicity Tests

Conformance MUST prove:

1. a new node can recover the root catalog from root placement quorum without local feature journals;
2. a stale root catalog is rejected;
3. a corrupted root catalog signature is rejected;
4. CoreObject manifest lookup works after process restart;
5. shard inventory repair finds missing shards;
6. object write failure before transaction commit leaves no visible object;
7. object write failure after transaction commit replays to the same visible state;
8. a cross-partition atomic batch is rejected before visible writes;
9. CoreRef CAS is linearizable inside one partition under concurrent writers;
10. stream sequence assignment is gap-free inside one partition.

### 21.1 No Durable Bypass Test

The implementation MUST include a conformance test that enumerates durable feature families and verifies each persists through CoreStore APIs.

Feature families MUST include:

```text
object_payload
object_metadata
bucket_metadata
object_link
append_stream
task_lease
authz_schema
authz_tuple
authz_derived_index
path_index
typed_field_index
full_text_index
vector_index
package_repository
package_blob
mesh_route
node_lifecycle
region_lifecycle
embedded_database_snapshot
embedded_database_changeset
audit_record
```

### 21.2 Local Storage Guard Test

The implementation MUST include tests or static checks proving feature code does not write authoritative local files outside CoreStore-owned staging/cache/shard/scratch directories.

### 21.3 Query Planner Tests

Conformance MUST prove:

1. typed field indexes answer equality/range/order queries without bucket scan;
2. full-text results intersect with typed filters;
3. vector results intersect with typed filters;
4. vector results intersect with authz filters before final result fetch;
5. path filters intersect with authz filters;
6. page tokens reject changed predicates;
7. page tokens reject changed order;
8. page tokens reject incompatible index generations;
9. stale authz revision is detected where a stronger revision was required;
10. final authorisation checks run for every returned result;
11. vector search rejects unbounded over-fetch;
12. filtered vector search uses permission filters before final fetch;
13. tombstones suppress stale object_current entries;
14. index compaction preserves page-token readable generations until expiry;
15. acceleration views cannot return results outside their bound authz revision.

### 21.4 Embedding Provider Tests

Production-mode vector tests MUST use a configured embedding provider abstraction. Test mode MAY use a deterministic test provider only when the provider is explicitly named as `test_only` and cannot be selected in production configuration.

### 21.5 Authz Tests

Conformance MUST prove:

1. tenant admin cannot mutate system realm without system permission;
2. tenant admin can manage a tenant-owned realm when granted;
3. public access maps to public principal checks;
4. reserved namespaces reject public reads;
5. reserved namespaces reject public writes;
6. ListObjects and query APIs are permission-aware;
7. computed usersets update derived indexes from watch events;
8. tuple-to-userset rewrites affect query planning;
9. schema binding changes publish a new authz revision;
10. page tokens bind authz revision;
11. page tokens bind full AuthzScope;
12. tenant realm tuples cannot affect system realm decisions;
13. system realm tuples cannot be read through tenant APIs;
14. permission-set iterators reject incompatible authz scope;
15. index diagnostics do not leak protected object existence.

### 21.6 Gateway Tests

Conformance MUST prove gateway writes and reads use normal Anvil resources:

1. S3 PUT creates CoreStore object data and metadata;
2. S3 LIST uses path index and authz filtering;
3. static site read resolves host alias and object link;
4. container tag update is CoreRef-backed;
5. package publish writes package index records;
6. gateway credentials map to principals;
7. revoking a credential prevents gateway access without changing object data;
8. gateway identifier normalisation rejects traversal and reserved names;
9. upload finalisation rejects digest mismatch;
10. package tag update is atomic under concurrent publishers;
11. credential revocation invalidates gateway cache within configured TTL.

### 21.7 Watch And Repair Tests

Conformance MUST prove:

1. watch events are digest chained;
2. watch gap detection stops derived application;
3. replaying the same event is idempotent;
4. replaying the same sequence with different hash fails;
5. expired cursors require repair or rebuild;
6. multi-source derived indexes persist cursor vectors;
7. repair output is invisible before promotion;
8. repair promotion is fenced and generation-checked;
9. shard repair reconstructs missing shards from erasure-coded peers;
10. local cache deletion does not lose committed state.

### 21.8 Multi-Region Tests

Conformance MUST prove:

1. remote-region reads preserve caller identity through proxy;
2. receiving region performs its own authorisation check;
3. proxy token alone cannot authorise user data access;
4. region drain does not promote a writable primary without an explicit movement feature;
5. global control records recover from a node restart;
6. stale bucket locator generation cannot commit a write.

## 22. Implementation Checklist

A complete implementation MUST deliver:

1. CoreStore API and internal service boundary;
2. erasure-coded CoreObject write/read path;
3. CoreStream segment format and append/read/seal path;
4. CoreRef compare-and-swap path;
5. fence acquisition and validation path;
6. watch cursor path for every source family;
7. object service rewritten to use CoreStore only;
8. bucket metadata rewritten to use CoreStore only;
9. append streams rewritten to use CoreStore only;
10. task leases rewritten to use CoreStore only;
11. authz schema, tuple, and derived indexes rewritten to use CoreStore only;
12. path index materialisation;
13. typed field/range index materialisation;
14. full-text index materialisation;
15. production vector embedding provider abstraction;
16. vector index materialisation with filtered search;
17. composite query planner;
18. authz permission-set integration in the query planner;
19. package gateway data model over CoreStore;
20. admin port enforcement;
21. reserved namespace hard denial;
22. conformance tests listed in this RFC.

## 23. Security Requirements

### 23.1 No Caller-Supplied Authority

Caller-supplied fields MUST NOT establish principal identity, lease ownership, system authority, region ownership, node ownership, or credential ownership. Those identities MUST come from authenticated context and server-side records.

### 23.2 Reserved Namespace Denial

Reserved namespace access MUST fail closed. If Anvil cannot prove the caller has the required internal or administrative authority, it MUST deny.

### 23.3 Query Disclosure

Query APIs MUST NOT leak unauthorised object keys, package names, stream keys, index names, or authz tuple contents. When non-disclosure is required, the API MAY return not found instead of permission denied.

### 23.4 Gateway Credentials

Gateway credentials MUST be scoped to principals and gateway type. A credential valid for one gateway MUST NOT automatically authenticate to another gateway unless explicitly configured.

### 23.5 Index Side Channels

Index counts, lag diagnostics, and degraded-plan diagnostics MUST be scoped by authorisation. A caller MUST NOT be able to infer the existence of protected objects from index metadata unless authorised.

## 24. Compatibility With Existing Public Features

This RFC preserves Anvil's public product shape:

1. objects remain addressable by tenant, bucket, and key;
2. S3-compatible access remains a gateway;
3. append streams remain append-only logs;
4. typed queries remain available;
5. full-text and vector search remain available;
6. object links remain symlink-like metadata;
7. package gateways can be added without new storage engines;
8. administrative APIs remain separate from public APIs.

The internal storage architecture is stricter: every persistent record belongs to CoreStore.

## 25. Example End-To-End Flows

### 25.1 Object Write

```text
Client PUT object
  -> authenticate principal
  -> check writer relation on bucket/prefix
  -> stage upload locally
  -> CoreStore.put_blob(payload) as unreferenced immutable data
  -> prepare object metadata record
  -> prepare current-object ref update
  -> CoreStore commit transaction with metadata append + ref update
  -> emit object watch cursor referencing transaction id
  -> return object version and mutation id
```

### 25.2 Protected Queue-Like Claim And Update

```text
Worker asks for due work
  -> planner obtains authz permission set for worker principal
  -> planner intersects typed field index with authz set
  -> returns due SourceIds
Worker acquires task lease
  -> CoreStore.acquire_fence(task_id, authenticated principal)
Worker updates canonical object and appends attempt event
  -> MutationBatch preconditions include object version and lease fence
  -> server validates authenticated owner and active fence
  -> CoreStore.put_blob(new object JSON) as unreferenced immutable data
  -> prepare attempt append, current-object ref update, and lease-state ref update
  -> CoreStore commit transaction atomically in the transaction partition
  -> watch streams expose only committed records
  -> return committed receipt
```

### 25.3 Static Site Latest Link

```text
Publisher uploads app-v3.0.1.exe
  -> object write stores payload through CoreStore
Publisher updates latest.exe link
  -> check link_admin relation
  -> CoreStore.compare_and_swap_ref(link/latest.exe, target app-v3.0.1.exe)
Reader requests /latest.exe through custom host
  -> resolve host alias
  -> map to tenant bucket and key
  -> resolve object link
  -> check public_reader or reader relation
  -> stream target object bytes
```

### 25.4 Authz-Aware Hybrid Search

```text
Client searches documents
  -> authenticate principal
  -> resolve required relation: object read
  -> load authz visible SourceId iterator at revision R
  -> run typed field index for filters
  -> run full-text index for query terms
  -> run vector index with authz/filter bitset
  -> merge scores according to ranking expression
  -> final CheckPermission for returned SourceIds at R
  -> return results and page token bound to R and index generations
```

## 26. Consistency Rules

A conforming implementation MUST maintain these invariants:

1. no visible object version points at a missing CoreObject;
2. no index generation points at a missing CoreSegment;
3. no CoreRef update can be committed with a stale fence;
4. no query result is returned without final authorisation verification;
5. no page token can be reused under a different query shape;
6. no package tag points at a missing package version;
7. no object link points across tenant boundaries;
8. no tenant credential mutates system realm records unless the system realm authorises it;
9. no derived index generation is published before its source cursor is durable;
10. no local cache or staging file is required to recover committed state.

## 27. Open Implementation Decisions That Are Not Architecture Decisions

The following are implementation choices, not architecture choices. Implementors MAY choose them without changing this RFC:

1. exact erasure coding parameters per deployment profile;
2. exact on-disk shard filename format;
3. exact cache eviction policy;
4. exact ANN implementation for vector segments, provided filtered search semantics hold;
5. exact text analyser implementations, provided definitions are durable and generationed;
6. exact binary encoding library for CoreStream segment JSON canonicalisation;
7. exact CLI command spelling, provided principal/credential/resource/relationship concepts remain intact.

The following are architecture decisions and MUST NOT be changed without a new RFC:

1. CoreStore is the only durable persistence substrate;
2. production indexes are materialised;
3. authorisation is one engine for system and tenant realms;
4. authorisation participates in query planning;
5. gateway protocols do not define the storage or permission model;
6. local files are not authoritative feature journals.
