# RFC ANVIL-0003: Mesh Routing, Object Links, And Administrative Lifecycle

## Status

Draft.

## Date

2026-07-02.

## Normative Language

The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, `MAY`, and `OPTIONAL` in this document are normative. They are to be interpreted as described in RFC 2119.

## 1. Abstract

Anvil will operate as a mesh of regional Anvil clusters. Tenants are globally unique. Buckets are scoped to a tenant and are not globally unique. A bucket is placed in one region at a time unless an explicit replication policy says otherwise. A request received by any Anvil edge or node is routed by tenant and bucket identity to the region that owns the bucket.

This RFC defines:

1. the mesh routing model for globally unique tenants and tenant-scoped buckets;
2. the required URL forms, including `bucket.tenant.<region>.anvil-storage.com` for CDN and static-site use cases;
3. object links, which are symlink-like metadata entries such as `latest.exe` pointing at an immutable object such as `my-app-v3.0.1.exe`;
4. the administrative plane required to manage nodes, regions, memberships, routing, and links;
5. node and region lifecycle state machines, including explicit add, activate, drain, remove, and recovery operations;
6. the role of `libp2p` as cluster transport, discovery, and invalidation, but not as the durable source of truth for membership.

TLS termination is out of scope for this RFC. Anvil MUST assume TLS is terminated by a fronting system such as a load balancer, reverse proxy, CDN, ingress controller, or platform gateway. If Anvil later terminates TLS itself, that work MUST be specified in a separate RFC.

## 2. Goals

An implementation conforming to this RFC MUST:

1. treat tenant names as globally unique across the Anvil mesh;
2. treat bucket names as unique only within a tenant;
3. support routing by tenant, bucket, and region without requiring globally unique bucket names;
4. support virtual-host access for `bucket.tenant.<region>.anvil-storage.com`;
5. support path-style and region-style access for tooling that cannot use virtual hosts;
6. support custom host aliases for CDN and static-site serving;
7. support symlink-like object links where many link paths can point at the same object;
8. update object links atomically without copying target object bytes;
9. keep node and region membership in durable Anvil control records;
10. use stable node identities across restarts;
11. use `libp2p` for peer transport, peer discovery, gossip, and invalidation only;
12. expose node, region, and mesh administration only through the administrative plane;
13. make the `admin` CLI a first-class release asset and the primary operator workflow for bootstrap, node lifecycle, region lifecycle, and membership changes;
14. preserve authorisation checks for all public, administrative, and internal routing operations;
15. provide conformance tests for routing, links, node lifecycle, region lifecycle, and administrative listener isolation.

## 3. Non-Goals

This RFC does not require:

1. globally unique bucket names;
2. Anvil-managed TLS certificates;
3. Anvil terminating TLS;
4. a separate database or external coordination service;
5. dedicated worker node classes;
6. automatic cross-region object replication for all buckets;
7. object links that create new object payload copies;
8. public access to administrative APIs;
9. using `libp2p` gossip as the durable membership store;
10. a global synchronous transaction across all regions for every object write.

## Dependency

RFC ANVIL-0004 defines the authz realm and persistent schema model that MUST be implemented before this RFC relies on Anvil-backed Zanzibar decisions for mesh administration, node lifecycle, region lifecycle, object links, or routing authorisation.

## 4. Core Terminology

### 4.1 Mesh

An Anvil mesh is the complete deployment spanning all regions that share a tenant namespace and routing directory. A mesh has a globally unique `mesh_id`.

### 4.2 Region

A region is a named placement and routing boundary, for example `eu-west-1` or `us-east-1`. A region contains one or more cells. Region names MUST be unique inside a mesh.

### 4.3 Cell

A cell is an operationally isolated subset of a region. A cell contains Anvil nodes and owns partitions, background work, and routing targets. A region MAY contain a single cell, but the data model MUST support many cells per region.

### 4.4 Node

A node is one Anvil process with a stable `node_id`, a persisted cluster keypair, public API address metadata, cluster transport address metadata, capabilities, lifecycle state, and heartbeat state.

All Anvil nodes are equal. A node MAY be selected to run background work, but this is an in-process virtual responsibility, not a separate worker-node class.

### 4.5 Tenant

A tenant is the top-level identity and authorisation boundary. Tenant names MUST be globally unique across the mesh.

### 4.6 Bucket

A bucket is an object namespace owned by a tenant. A bucket name MUST be unique within its tenant and MUST NOT be required to be globally unique.

The durable bucket identity is:

```text
BucketIdentity = tenant_id + bucket_name
```

An implementation MAY also assign an opaque `bucket_id`, but `bucket_id` MUST NOT replace tenant-scoped bucket names in public routing semantics.

### 4.7 Object

An object is a durable payload addressed by an object key inside a tenant bucket.

### 4.8 Object Link

An object link is a metadata entry at an object key that points to another object key and optional target version inside the same bucket. It is symlink-like: the link has its own key, metadata, generation, authorisation checks, and watch events, but it does not duplicate the target object payload. Cross-bucket and cross-tenant links are not part of this RFC.

Many object links MAY point to the same target object.

## 5. URL And Host Routing Model

### 5.1 Required URL Forms

Anvil MUST support these URL forms for object reads and writes where the operation is otherwise authorised.

#### 5.1.1 Path-Style Regional URL

```text
https://<region>.anvil-storage.com/<tenant>/<bucket>/<object-key>
```

Example:

```text
https://eu-west-1.anvil-storage.com/acme/releases/my-app-v3.0.1.exe
```

The host supplies the preferred region. The path supplies tenant, bucket, and object key.

#### 5.1.2 Bucket-Path Regional URL

```text
https://<region>.anvil-storage.com/<tenant>/<bucket>
https://<region>.anvil-storage.com/<tenant>/<bucket>/<object-key>
```

This is the canonical form for programmatic clients that do not support virtual-host addressing.

#### 5.1.3 Virtual-Host Regional URL

```text
https://<bucket>.<tenant>.<region>.anvil-storage.com/<object-key>
```

Example:

```text
https://releases.acme.eu-west-1.anvil-storage.com/latest.exe
```

The host supplies bucket, tenant, and region. The request path supplies only the object key.

This form exists so that a customer-controlled hostname such as `cdn.customer-domain.com` can be configured as a CNAME to the Anvil virtual-host endpoint and serve `/file/path` naturally.

#### 5.1.4 Custom Host Alias URL

```text
https://<customer-host>/<object-key>
```

A custom host alias maps an externally supplied `Host` header to a tenant bucket and optional key prefix. The mapping is stored in Anvil's host-alias directory.

Example mapping:

```json
{
  "schema": "anvil.mesh.host_alias.v1",
  "hostname": "cdn.customer-domain.com",
  "tenant_id": "tenant_acme",
  "bucket_name": "releases",
  "region": "eu-west-1",
  "prefix": "public/",
  "state": "active",
  "generation": 17
}
```

A request for:

```text
https://cdn.customer-domain.com/latest.exe
```

resolves to:

```text
tenant=tenant_acme
bucket=releases
region=eu-west-1
key=public/latest.exe
```

### 5.2 Host Grammar

The virtual-host form MUST be parsed with this grammar after lowercasing the host and removing any trailing dot:

```abnf
lower-alpha      = %x61-7A
digit            = %x30-39
label-char       = lower-alpha / digit / "-"
label-end        = lower-alpha / digit
label            = lower-alpha / (lower-alpha *61(label-char) label-end)
region-name      = label *("-" label)
tenant-name      = label *("-" label)
bucket-name      = label *("-" label)
base-domain      = "anvil-storage" "." "com"
virtual-host     = bucket-name "." tenant-name "." region-name "." base-domain
```

A label MUST be 1 to 63 octets after IDNA processing. The complete host MUST be no more than 253 octets. Anvil MUST normalise internationalised hostnames to ASCII punycode before matching.

If tenant or bucket naming rules later admit dots, the virtual-host form MUST use an escaping or encoded-host scheme before dotted tenant and bucket names are enabled. This RFC requires the virtual-host form to reject dotted tenant and bucket names.

Object keys in URL paths MUST be percent-decoded exactly once after host routing. The decoded key MUST reject null bytes, control characters, path traversal segments, and absolute path forms. The decoded key MAY contain `/` as an object-key separator.

### 5.3 TLS Caveat

Anvil MUST NOT depend on terminating TLS for correctness. TLS certificates, wildcard coverage, custom-domain certificates, SNI routing, and certificate renewal are the responsibility of the fronting system.

Anvil MUST receive enough trusted forwarding metadata from the fronting system to determine:

1. original host;
2. original scheme;
3. client IP where policy requires it;
4. request id or tracing context where available.

If a deployment cannot provide trusted forwarding metadata, Anvil MUST use the raw request authority and connection metadata available to it.

Trusted forwarding metadata MUST be accepted only from configured trusted proxy source ranges. Required supported headers:

```text
Forwarded
X-Forwarded-Host
X-Forwarded-Proto
X-Forwarded-For
X-Request-Id
```

If the remote peer is not trusted, Anvil MUST ignore forwarded headers. If forwarded host metadata conflicts with the raw authority and the remote peer is trusted, the forwarded host wins. If multiple forwarded host values are present, Anvil MUST use the first value added by the trusted edge according to deployment configuration and reject ambiguous chains when it cannot determine that value.

### 5.4 Routing Decision

For every request that contains tenant and bucket identity, Anvil MUST resolve a `BucketLocator` before executing the operation.

If the bucket is owned by the local region, the node MAY serve the request locally.

If the bucket is owned by a different region, the node MUST do one of:

1. return a permanent or temporary redirect with the target region endpoint;
2. proxy the request to the owning region;
3. reject the request if the operation is not proxyable and redirect is disabled.

The routing decision MUST be deterministic from the current bucket locator, request method, request size, client capability, and deployment policy.

Allowed routing policies:

```text
redirect_preferred | proxy_preferred | proxy_required | local_only
```

`redirect_preferred` returns redirects for safe methods and protocol-compatible writes. It MAY proxy methods that cannot be safely redirected.

`proxy_preferred` proxies when the remote region is healthy and falls back to redirect when policy allows.

`proxy_required` rejects remote-region requests if proxying is unavailable.

`local_only` rejects remote-region requests and is intended for deployments without inter-region proxying.

Default proxyable methods:

```text
GET | HEAD | OPTIONS | PUT | POST | DELETE | PATCH
```

A deployment MAY disable proxying for large uploads and return a redirect instead. For ordinary HTTP redirects Anvil MUST use `307` or `308` for methods with request bodies so the method and body are preserved. For S3-compatible wrong-region responses, Anvil MUST use the protocol-compatible status and `x-amz-bucket-region` header.

## 6. Mesh Directory Records

The mesh directory is the durable routing source of truth. It contains tenant locators, bucket locators, region descriptors, node descriptors, cell descriptors, and host aliases.

The mesh directory MUST be maintained in Anvil-owned control records. Implementations MUST NOT depend on scanning all object metadata to route a request.

### 6.1 Partitioned Directory Layout

Directory records MUST be partitioned by stable hash prefix to avoid per-tenant or per-bucket filesystem fanout bottlenecks.

The partition prefix is:

```text
partition = first 4 lowercase hex characters of blake3(canonical-key)
```

Required internal layout:

```text
_anvil/control/v1/mesh/
  tenants/{partition}/{tenant_id}.json
  tenant-names/{partition}/{tenant_name}.json
  buckets/{partition}/{tenant_id}/{bucket_name}.json
  regions/{region}.json
  cells/{region}/{cell_id}.json
  nodes/{region}/{cell_id}/{node_id}.json
  host-aliases/{partition}/{hostname}.json
```

Public object APIs MUST NOT read, list, write, copy, patch, or delete `_anvil/control/*` paths. These records are accessible only through structured administrative APIs after authorisation.

### 6.2 Tenant Name Index

Every tenant name used in URLs MUST resolve through a tenant-name index before a bucket lookup occurs. The canonical tenant name is lowercase ASCII. Tenant-name records are stored at:

```text
_anvil/control/v1/mesh/tenant-names/{partition}/{tenant_name}.json
```

The tenant-name record MUST have this JSON shape:

```json
{
  "schema": "anvil.mesh.tenant_name.v1",
  "mesh_id": "mesh_01",
  "tenant_name": "acme",
  "tenant_id": "tenant_acme",
  "status": "active",
  "idempotency_key": "req-123",
  "reservation_expires_at": null,
  "created_at": "2026-07-02T00:00:00Z",
  "updated_at": "2026-07-02T00:00:00Z",
  "generation": 1
}
```

Tenant creation MUST use an ordered reservation protocol, not an unspecified multi-record transaction:

1. canonicalise `tenant_name`;
2. generate `tenant_id`;
3. create the tenant-name record with `status = reserved` using compare-and-swap create against the tenant-name key;
4. create the tenant locator using compare-and-swap create against the tenant-id key;
5. update the tenant-name record to `status = active` with an expected generation check.

Allowed tenant-name `status` values:

```text
reserved | active | tombstoned
```

URL routing MUST only resolve tenant-name records in `active` state. A `reserved` record MUST behave as not found for public routing.

If the tenant-name record already exists and points at a different `tenant_id`, tenant creation MUST fail with `TenantNameAlreadyExists` unless the existing record is an expired `reserved` record that recovery has tombstoned.

If a retry supplies the same idempotency key and the existing tenant-name record points at the same `tenant_id`, the create operation MUST continue the ordered protocol from the first incomplete step and return the final tenant response.

A reserved tenant-name record MUST include `reservation_expires_at` and `idempotency_key`. Recovery MUST complete the reservation when the matching tenant locator exists, or tombstone the reservation when it expired and no matching tenant locator exists.

The tenant-name record is the only authority for URL tenant-name resolution. Implementations MUST NOT scan tenant locators to resolve a tenant name.

### 6.3 Tenant Locator

A tenant locator MUST have this JSON shape:

```json
{
  "schema": "anvil.mesh.tenant_locator.v1",
  "mesh_id": "mesh_01",
  "tenant_id": "tenant_acme",
  "tenant_name": "acme",
  "home_region": "eu-west-1",
  "status": "active",
  "profile_revision": 52,
  "created_at": "2026-07-02T00:00:00Z",
  "updated_at": "2026-07-02T00:00:00Z",
  "generation": 52
}
```

Allowed `status` values:

```text
creating | active | suspended | deleting | deleted
```

Tenant names MUST be globally unique. Creating a tenant MUST use a compare-and-swap write against the tenant-name index so that exactly one concurrent create can win.

### 6.4 Bucket Locator

A bucket locator MUST have this JSON shape:

```json
{
  "schema": "anvil.mesh.bucket_locator.v1",
  "mesh_id": "mesh_01",
  "tenant_id": "tenant_acme",
  "bucket_name": "releases",
  "bucket_id": "bucket_01HY...",
  "home_region": "eu-west-1",
  "home_cell": "cell_a",
  "status": "active",
  "placement_policy": "regional-primary",
  "object_prefix": "objects/tenant_acme/releases/",
  "created_at": "2026-07-02T00:00:00Z",
  "updated_at": "2026-07-02T00:00:00Z",
  "generation": 19
}
```

Allowed `status` values:

```text
creating | active | read_only | moving | draining | deleted
```

The tuple `(tenant_id, bucket_name)` MUST be unique. `bucket_name` alone MUST NOT be globally unique.

### 6.5 Host Alias Descriptor

A host alias maps a complete hostname to a tenant bucket and optional prefix. It MUST have this JSON shape:

```json
{
  "schema": "anvil.mesh.host_alias.v1",
  "hostname": "cdn.customer-domain.com",
  "tenant_id": "tenant_acme",
  "bucket_name": "releases",
  "region": "eu-west-1",
  "prefix": "public/",
  "state": "active",
  "created_at": "2026-07-02T00:00:00Z",
  "updated_at": "2026-07-02T00:00:00Z",
  "generation": 4
}
```

Allowed `state` values:

```text
pending_verification | active | suspended | deleted
```

Anvil MUST NOT define TLS ownership or certificate issuance in this record. Hostname verification MAY be implemented by a fronting system and reflected into this record by an administrative API.

Host alias rules:

1. hostnames MUST be lowercased and stored without a trailing dot;
2. hostnames MUST be matched exactly after normalisation; wildcard custom host aliases are not part of this RFC;
3. ports MUST be stripped before host-alias lookup;
4. host aliases MUST NOT overlap native Anvil hostnames under `*.anvil-storage.com`;
5. prefix joining MUST normalise exactly one slash between prefix and request path;
6. prefix joining MUST reject path traversal after percent decoding;
7. if a host alias and native Anvil virtual host both appear to match, native Anvil virtual-host routing wins and the host alias MUST be rejected at creation time.

### 6.6 Control Partitions, Replication, And Checkpoints

The mesh directory MUST be implemented as partitioned control streams. Each control stream has exactly one fenced writer at a time and many readers. A control-stream mutation MUST be accepted only when the writer presents the current fence token for that control partition.

Control streams are the replication source. Each stream is stored as an append-only Anvil-owned control log under:

```text
_anvil/control/v1/streams/{stream_family}/{partition}.anlog
```

A materialised JSON record under `_anvil/control/v1/mesh/...` is a projection of the latest committed control-stream mutation. If the projection and stream disagree, the stream wins and repair MUST rebuild the projection.

Control stream files MUST use length-delimited frames. Multi-byte integers are unsigned big-endian. The frame header is:

```text
magic            = %x41.4E.56.43.54.4C.31.00 ; "ANVCTL1\0"
version          = 2OCTET                    ; value 1
header_len       = 4OCTET
payload_len      = 8OCTET
header_crc32     = 4OCTET
payload_crc32    = 4OCTET
header_json      = header_len OCTET          ; UTF-8 JSON control mutation header
payload_json     = payload_len OCTET         ; UTF-8 JSON record payload or tombstone
control_frame    = magic version header_len payload_len header_crc32 payload_crc32 header_json payload_json
```

The `header_json` MUST contain the control mutation envelope. The `payload_json` MUST contain the full new record for `create` and `upsert`, and a tombstone descriptor for `delete` and `tombstone`. A reader MUST verify both CRC values before applying a frame. A partial final frame MUST be ignored until repair truncates or completes it.

A node acquires writer authority for a control partition by acquiring an `ownership_fence` with `resource_kind = control_partition` and `resource_id = {stream_family}/{partition}`. The returned fence is the only write authority for that stream.

Control partition id:

```text
control_partition = first 4 lowercase hex characters of blake3(record-family + ":" + canonical-record-key)
```

Required control stream families:

```text
tenant_name
tenant_locator
bucket_locator
host_alias
region_descriptor
cell_descriptor
node_descriptor
ownership_fence
```

A control mutation envelope MUST have this JSON shape when serialised for audit, watch, repair, or replication diagnostics:

```json
{
  "schema": "anvil.mesh.control_mutation.v1",
  "mesh_id": "mesh_01",
  "stream_family": "bucket_locator",
  "partition": "0a7f",
  "sequence": 1844,
  "record_key": "tenant_acme/releases",
  "operation": "upsert",
  "expected_generation": 18,
  "new_generation": 19,
  "writer_node_id": "node_01J0...",
  "writer_fence": 44,
  "idempotency_key": "req-123",
  "record_digest": "blake3:...",
  "created_at": "2026-07-02T00:00:00Z"
}
```

Allowed `operation` values:

```text
create | upsert | delete | tombstone
```

`heartbeat` and `checkpoint` MUST NOT be encoded as control mutation operations. Node heartbeat is represented by node descriptor updates where needed. Consumer checkpoints are separate checkpoint records.

Each region MUST maintain a checkpoint for every control stream it consumes. A checkpoint record MUST have this JSON shape:

```json
{
  "schema": "anvil.mesh.control_checkpoint.v1",
  "mesh_id": "mesh_01",
  "region": "eu-west-1",
  "stream_family": "bucket_locator",
  "partition": "0a7f",
  "last_sequence": 1844,
  "last_digest": "blake3:...",
  "updated_at": "2026-07-02T00:00:00Z"
}
```

A region MAY become active only after it has consumed all required control partitions to the activation checkpoint supplied by the administrative operation. The activation checkpoint MUST include the sequence and digest for each required partition.

The activation checkpoint file used by `admin region activate --activation-checkpoint <file>` MUST be UTF-8 JSON with this shape:

```json
{
  "schema": "anvil.mesh.activation_checkpoint.v1",
  "mesh_id": "mesh_01",
  "region": "eu-west-1",
  "created_at": "2026-07-02T00:00:00Z",
  "required_streams": [
    {
      "stream_family": "tenant_name",
      "partition": "0a7f",
      "sequence": 1844,
      "digest": "blake3:..."
    }
  ]
}
```

A region activation request MUST fail with `ActivationCheckpointNotReached` until the region has materialised every listed stream position with the listed digest.

If two regions attempt to write the same control record concurrently, exactly one mutation can commit because every mutation MUST include both an expected record generation and a current partition fence. The loser MUST return `ControlConflict` with the current generation and current owner metadata.

Replication recovery MUST be idempotent. Replaying the same control mutation with the same sequence and digest MUST be a no-op. Replaying the same sequence with a different digest MUST fail with `ControlStreamDivergence` and require repair.

## 7. Object Links

### 7.1 Model

An object link is an object-directory entry whose kind is `link`. It is addressed by its own object key and points at a target object key and optional target version.

Object links are not named by a separate alias field. The link key is the object key at which the link entry is stored.

Example:

```text
bucket: releases
object key: my-app-v3.0.1.exe
link key: latest.exe
```

The `latest.exe` link points to `my-app-v3.0.1.exe`. Tomorrow, the same link key may point to `my-app-v3.0.2.exe` without copying either payload.

Many link keys MAY point at the same target object.

### 7.2 Link Descriptor

The link descriptor MUST have this JSON shape when exposed through administrative or diagnostic APIs:

```json
{
  "schema": "anvil.object_link.v1",
  "tenant_id": "tenant_acme",
  "bucket_name": "releases",
  "link_key": "latest.exe",
  "target_key": "my-app-v3.0.1.exe",
  "target_version": "01J0...",
  "resolution": "follow",
  "created_at": "2026-07-02T00:00:00Z",
  "updated_at": "2026-07-02T00:00:00Z",
  "created_by": "principal:publisher",
  "generation": 12
}
```

Allowed `resolution` values:

```text
follow | redirect
```

`follow` means Anvil resolves the link internally and serves the target object.

`redirect` means Anvil returns an HTTP redirect to the target object's canonical URL where the protocol supports redirects.

### 7.3 Link Target Scope

This RFC requires links to target objects in the same bucket. A link descriptor MUST NOT name another tenant or another bucket.

Cross-bucket and cross-tenant links are intentionally out of scope because they make cache invalidation, authorisation, lifecycle, and region-drain semantics materially more complex. A later RFC MAY add them with explicit target-bucket routing and authorisation rules.

### 7.4 Link Creation And Update

Creating or updating a link MUST be an atomic metadata mutation. It MUST NOT copy the target object's payload.

A link mutation request MUST include:

1. tenant id;
2. bucket name;
3. link key;
4. target key;
5. optional target version;
6. expected link generation, or an explicit create-only flag;
7. idempotency key;
8. authenticated principal.

The target key MUST resolve to a blob object or retained object version in the same bucket at mutation time unless the request explicitly sets `allow_dangling = true`. The default is `allow_dangling = false`.

If the expected generation does not match the current link generation, Anvil MUST reject the mutation with a compare-and-swap conflict.

### 7.5 Link Resolution

For `GET` and `HEAD`, Anvil MUST resolve links by default unless the request explicitly asks for link metadata. Native APIs MUST expose link metadata through `ReadObjectLink`. HTTP object APIs MUST expose link metadata when the request includes `x-anvil-link-mode: metadata`. S3-compatible APIs MAY expose link metadata through a vendor extension, but ordinary S3 `GET` and `HEAD` MUST follow links by default.

For `PUT`, `DELETE`, and metadata mutation, Anvil MUST operate on the link entry itself when the target key is the link key. It MUST NOT silently mutate the target object through a link.

If `target_version` is absent, link resolution uses the current live version of the target key at read time. If `target_version` is present, link resolution uses exactly that retained version. If the target or target version no longer exists, reads MUST fail with `DanglingObjectLink`.

Link resolution MUST enforce authorisation on both:

1. the link key; and
2. the target object.

If either check fails, the operation MUST fail. The error returned to unauthorised callers MUST NOT reveal whether the link, target, or both caused the denial.

### 7.6 Object Directory Entry

The object directory MUST represent links explicitly. A directory entry MUST have a `kind` enum rather than inferring links from metadata strings. Required kinds:

```text
blob | link | delete_marker
```

A link directory entry MUST include:

```json
{
  "schema": "anvil.object_directory_entry.v1",
  "tenant_id": "tenant_acme",
  "bucket_name": "releases",
  "key": "latest.exe",
  "kind": "link",
  "generation": 12,
  "link_target_key": "my-app-v3.0.1.exe",
  "link_target_version": "01J0...",
  "link_resolution": "follow",
  "metadata_digest": "blake3:...",
  "updated_at": "2026-07-02T00:00:00Z"
}
```

A blob directory entry MUST NOT contain link target fields.

### 7.7 Link Loops And Chains

Implementations MAY support link-to-link chains, but MUST detect loops and MUST enforce a maximum resolution depth. The default maximum depth MUST be 8.

If the maximum depth is exceeded or a loop is detected, Anvil MUST return a structured link-resolution error.

### 7.8 Link Watch Events

Creating, updating, or deleting a link MUST emit object watch events for the link key. It MUST NOT emit a target object mutation event unless the target object itself changed.

The watch event payload MUST identify the event as a link mutation and include the new link generation.

### 7.9 Cache Semantics

When serving a link with `resolution = follow`, the response validators MUST include link generation and target object identity. The ETag for a followed link MUST change when either:

1. the link target changes; or
2. the target object version served by the link changes.

This avoids stale caches continuing to serve an old target after `latest.exe` is moved.

### 7.10 HTTP Link Details

When `x-anvil-link-mode: metadata` is supplied, HTTP object APIs MUST return the link descriptor JSON with content type `application/vnd.anvil.object-link+json`.

When serving a followed link, Anvil SHOULD include these headers where the protocol permits them:

```text
x-anvil-object-kind: link
x-anvil-link-key: <link-key>
x-anvil-link-generation: <generation>
x-anvil-link-target-version: <target-version-or-empty>
```

For `resolution = redirect`, ordinary HTTP APIs MUST use `307` for non-GET methods and MAY use `302` or `307` for `GET` and `HEAD` according to deployment policy. S3-compatible APIs MUST use protocol-compatible redirects and headers.

A pinned `target_version` MUST be encoded in canonical URLs using the existing Anvil object-version query parameter or version header. If no object-version mechanism exists for the API surface, redirect mode MUST reject pinned links with `InvalidArgument` rather than emitting an ambiguous URL.

## 8. Configuration

An implementation MUST expose explicit configuration for public, administrative, and cluster planes. Required configuration fields:

```text
api_listen_addr
admin_listen_addr
cluster_listen_addr
public_api_addr
public_region_base_domain
public_cluster_addrs
region
cell_id
node_id_path
cluster_keypair_path
bootstrap_addrs
cluster_secret
mesh_id
```

`api_listen_addr` is the public data-plane listener.

`admin_listen_addr` is the administrative listener. Its default SHOULD be loopback-only. A production deployment that exposes it on a routable interface MUST do so deliberately.

`cluster_listen_addr` is the node-to-node cluster transport listener.

`public_api_addr` is the externally reachable address advertised for this node when direct node routing is required.

`public_region_base_domain` is the region host suffix, for example `eu-west-1.anvil-storage.com`.

`public_cluster_addrs` are the externally reachable `libp2p` addresses advertised to other nodes. An implementation MUST advertise configured public cluster addresses when present; it MUST NOT rely only on local listen addresses in NAT or container deployments.

`node_id_path` stores the stable node id. `cluster_keypair_path` stores the persisted `libp2p` keypair.

`bootstrap_addrs` are initial peer addresses used to join the cluster transport. Bootstrap addresses are discovery hints only. They MUST NOT grant membership authority.

`cluster_secret` MAY authenticate cluster gossip messages. It MUST NOT replace durable node registration or administrative authorisation.

`mesh_id`, `region`, and `cell_id` bind the process to its mesh placement.

## 9. Administrative Plane

### 9.1 Listener Separation

Anvil MUST expose a separate administrative listener. Administrative APIs MUST NOT be registered on the public listener.

The public listener MAY expose object APIs, S3-compatible APIs, registry gateway APIs, and public token flows.

The administrative listener MUST expose tenant, application, bucket, link, host-alias, node, region, cell, repair, diagnostics, and authorisation administration.

Every administrative route MUST require authentication and an Anvil administrative authorisation check.

### 9.2 AnvilAdmin Namespace

Anvil MUST define an Anvil-owned administrative namespace named:

```text
anvil_admin
```

The root object is:

```text
anvil_admin:cluster:{mesh_id}
```

Required relations:

```text
owner
operator
security_admin
tenant_admin
bucket_admin
index_admin
registry_admin
node_admin
region_admin
routing_admin
repair_admin
auditor
```

Required permissions:

```text
manage_admins        = owner
manage_cluster       = owner | operator
manage_tenants       = owner | tenant_admin
manage_apps          = owner | tenant_admin
manage_buckets       = owner | bucket_admin | operator
manage_indexes       = owner | index_admin | operator
manage_registries    = owner | registry_admin
manage_registry_auth = owner | registry_admin | security_admin
manage_authz_schema  = owner | security_admin
manage_authz_tuples  = owner | security_admin
manage_nodes         = owner | node_admin | operator
manage_regions       = owner | region_admin | operator
manage_routing       = owner | routing_admin | operator
manage_host_aliases  = owner | routing_admin | bucket_admin
manage_links         = owner | bucket_admin | operator
run_repair           = owner | repair_admin | operator
view_diagnostics     = owner | operator | auditor
view_audit_log       = owner | auditor
```

Capabilities MUST be represented as typed Rust values at API boundaries. Handlers MUST NOT pass raw capability strings around after request parsing.

### 9.3 Required Administrative APIs

The administrative API MUST provide operations equivalent to this service shape:

```protobuf
service AdminService {
  rpc CreateTenant(CreateTenantRequest) returns (TenantResponse);
  rpc CreateApplication(CreateApplicationRequest) returns (ApplicationSecretResponse);
  rpc RotateApplicationSecret(RotateApplicationSecretRequest) returns (ApplicationSecretResponse);

  rpc CreateBucketAdmin(CreateBucketAdminRequest) returns (BucketAdminResponse);
  rpc SetBucketPublicAccessAdmin(SetBucketPublicAccessAdminRequest) returns (BucketAdminResponse);

  rpc CreateObjectLink(CreateObjectLinkRequest) returns (ObjectLinkResponse);
  rpc UpdateObjectLink(UpdateObjectLinkRequest) returns (ObjectLinkResponse);
  rpc DeleteObjectLink(DeleteObjectLinkRequest) returns (AdminMutationResponse);
  rpc ReadObjectLink(ReadObjectLinkRequest) returns (ObjectLinkResponse);
  rpc ListObjectLinks(ListObjectLinksRequest) returns (ListObjectLinksResponse);

  rpc CreateHostAlias(CreateHostAliasRequest) returns (HostAliasResponse);
  rpc ActivateHostAlias(ActivateHostAliasRequest) returns (HostAliasResponse);
  rpc SuspendHostAlias(SuspendHostAliasRequest) returns (HostAliasResponse);
  rpc DeleteHostAlias(DeleteHostAliasRequest) returns (AdminMutationResponse);
  rpc ListHostAliases(ListHostAliasesRequest) returns (ListHostAliasesResponse);

  rpc CreateRegion(CreateRegionRequest) returns (RegionResponse);
  rpc ActivateRegion(ActivateRegionRequest) returns (RegionResponse);
  rpc SetRegionReadOnly(SetRegionReadOnlyRequest) returns (RegionResponse);
  rpc DrainRegion(DrainRegionRequest) returns (DrainOperationResponse);
  rpc RemoveRegion(RemoveRegionRequest) returns (AdminMutationResponse);
  rpc ListRegions(ListRegionsRequest) returns (ListRegionsResponse);

  rpc RegisterCell(RegisterCellRequest) returns (CellResponse);
  rpc ActivateCell(ActivateCellRequest) returns (CellResponse);
  rpc DrainCell(DrainCellRequest) returns (DrainOperationResponse);
  rpc RemoveCell(RemoveCellRequest) returns (AdminMutationResponse);
  rpc ListCells(ListCellsRequest) returns (ListCellsResponse);

  rpc RegisterNode(RegisterNodeRequest) returns (NodeResponse);
  rpc ActivateNode(ActivateNodeRequest) returns (NodeResponse);
  rpc DrainNode(DrainNodeRequest) returns (DrainOperationResponse);
  rpc ForceOfflineNode(ForceOfflineNodeRequest) returns (NodeResponse);
  rpc RemoveNode(RemoveNodeRequest) returns (AdminMutationResponse);
  rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);

  rpc ListRoutingRecords(ListRoutingRecordsRequest) returns (ListRoutingRecordsResponse);
  rpc RepairRoutingRecord(RepairRoutingRecordRequest) returns (AdminMutationResponse);

  rpc RunRepair(RunRepairRequest) returns (RepairTaskResponse);
  rpc ListDiagnostics(ListDiagnosticsRequest) returns (DiagnosticsResponse);
  rpc ListAuditEvents(ListAuditEventsRequest) returns (AuditEventsResponse);
}
```

Every mutating admin request MUST carry or derive:

1. request id;
2. authenticated principal;
3. required `AnvilAdminCapability`;
4. idempotency key;
5. audit reason;
6. expected generation where the operation updates an existing lifecycle record.

### 9.4 Administrative Request And Response Contract

The administrative API MUST support gRPC. It MAY also support HTTP+JSON, but HTTP+JSON MUST preserve the same request fields, response fields, error codes, idempotency semantics, and authorisation checks.

Administrative authentication MUST produce a typed principal before route handling. The principal MAY come from a bearer token, mTLS identity, or another configured authenticator, but the route handler MUST receive a structured value equivalent to:

```protobuf
message AdminPrincipal {
  string principal_id = 1;
  string tenant_id = 2;
  repeated string authenticated_methods = 3;
}
```

Administrative requests MUST include metadata equivalent to:

```protobuf
message AdminRequestContext {
  string request_id = 1;
  string idempotency_key = 2;
  string audit_reason = 3;
  uint64 expected_generation = 4;
}
```

Mutations that create a new record MUST set `expected_generation = 0`. Mutations that update an existing record MUST set `expected_generation` to the generation observed by the caller unless the specific method is documented as create-only.

Common responses:

```protobuf
message AdminMutationResponse {
  string request_id = 1;
  string resource_id = 2;
  uint64 generation = 3;
  string audit_event_id = 4;
  bool idempotent_replay = 5;
}

message AdminError {
  string request_id = 1;
  string code = 2;
  string message = 3;
  string resource_id = 4;
  uint64 current_generation = 5;
}
```

Required admin error codes:

```text
Unauthenticated
PermissionDenied
InvalidArgument
NotFound
AlreadyExists
GenerationConflict
ControlConflict
ControlStreamDivergence
LifecycleTransitionDenied
DrainInProgress
DrainBlocked
NodeUnhealthy
RegionUnhealthy
DanglingObjectLink
LinkLoop
RoutingRecordUnavailable
```

Required key request shapes:

```protobuf
message RegisterNodeRequest {
  AdminRequestContext context = 1;
  string node_id = 2;
  string region = 3;
  string cell_id = 4;
  string libp2p_peer_id = 5;
  repeated string public_cluster_addrs = 6;
  string public_api_addr = 7;
  repeated NodeCapability capabilities = 8;
}

message DrainNodeRequest {
  AdminRequestContext context = 1;
  string node_id = 2;
  uint64 graceful_timeout_ms = 3;
  bool force_after_timeout = 4;
}

message CreateRegionRequest {
  AdminRequestContext context = 1;
  string region = 2;
  string public_base_url = 3;
  string virtual_host_suffix = 4;
  uint32 placement_weight = 5;
}

message DrainRegionRequest {
  AdminRequestContext context = 1;
  string region = 2;
  RegionDrainDisposition default_disposition = 3;
  repeated BucketDrainOverride bucket_overrides = 4;
}

message CreateObjectLinkRequest {
  AdminRequestContext context = 1;
  string tenant_id = 2;
  string bucket_name = 3;
  string link_key = 4;
  string target_key = 5;
  string target_version = 6;
  ObjectLinkResolution resolution = 7;
  bool allow_dangling = 8;
}

message UpdateObjectLinkRequest {
  AdminRequestContext context = 1;
  string tenant_id = 2;
  string bucket_name = 3;
  string link_key = 4;
  string target_key = 5;
  string target_version = 6;
  ObjectLinkResolution resolution = 7;
  bool allow_dangling = 8;
}

message CreateHostAliasRequest {
  AdminRequestContext context = 1;
  string hostname = 2;
  string tenant_id = 3;
  string bucket_name = 4;
  string region = 5;
  string prefix = 6;
}
```

`NodeCapability`, `ObjectLinkResolution`, and `RegionDrainDisposition` MUST be enums. They MUST NOT be accepted as unchecked strings after request parsing.

Required enum values:

```protobuf
enum NodeCapability {
  NODE_CAPABILITY_UNSPECIFIED = 0;
  NODE_CAPABILITY_OBJECT = 1;
  NODE_CAPABILITY_INDEX = 2;
  NODE_CAPABILITY_PERSONALDB = 3;
  NODE_CAPABILITY_GATEWAY = 4;
  NODE_CAPABILITY_ADMIN = 5;
}

enum ObjectLinkResolution {
  OBJECT_LINK_RESOLUTION_UNSPECIFIED = 0;
  OBJECT_LINK_RESOLUTION_FOLLOW = 1;
  OBJECT_LINK_RESOLUTION_REDIRECT = 2;
}

enum RegionDrainDisposition {
  REGION_DRAIN_DISPOSITION_UNSPECIFIED = 0;
  REGION_DRAIN_DISPOSITION_BLOCK_UNTIL_EMPTY = 1;
  REGION_DRAIN_DISPOSITION_REMAIN_PROXY_ONLY = 2;
  REGION_DRAIN_DISPOSITION_READ_ONLY_UNTIL_REMOVED = 3;
  REGION_DRAIN_DISPOSITION_DELETE_AFTER_RETENTION = 4;
}

enum LifecycleState {
  LIFECYCLE_STATE_UNSPECIFIED = 0;
  LIFECYCLE_STATE_JOINING = 1;
  LIFECYCLE_STATE_ACTIVE = 2;
  LIFECYCLE_STATE_READ_ONLY = 3;
  LIFECYCLE_STATE_DRAINING = 4;
  LIFECYCLE_STATE_DRAINED = 5;
  LIFECYCLE_STATE_DRAINED_WITH_EXCEPTIONS = 6;
  LIFECYCLE_STATE_OFFLINE = 7;
  LIFECYCLE_STATE_REMOVED = 8;
}
```

List requests MUST use cursor pagination:

```protobuf
message PageRequest {
  string cursor = 1;
  uint32 limit = 2;
}

message PageResponse {
  string next_cursor = 1;
  bool has_more = 2;
}
```

A cursor MUST bind the filter fields, sort order, caller principal, and authorisation revision used to create it. Reusing a cursor with different filters MUST fail with `InvalidArgument`.

Resource responses MUST include full descriptor payloads and current generation. Minimum response shapes:

```protobuf
message NodeResponse { string request_id = 1; NodeDescriptor node = 2; string audit_event_id = 3; }
message RegionResponse { string request_id = 1; RegionDescriptor region = 2; string audit_event_id = 3; }
message CellResponse { string request_id = 1; CellDescriptor cell = 2; string audit_event_id = 3; }
message ObjectLinkResponse { string request_id = 1; ObjectLinkDescriptor link = 2; string audit_event_id = 3; }
message HostAliasResponse { string request_id = 1; HostAliasDescriptor host_alias = 2; string audit_event_id = 3; }
message ListNodesResponse { PageResponse page = 1; repeated NodeDescriptor nodes = 2; }
message ListRegionsResponse { PageResponse page = 1; repeated RegionDescriptor regions = 2; }
message ListObjectLinksResponse { PageResponse page = 1; repeated ObjectLinkDescriptor links = 2; }
message ListHostAliasesResponse { PageResponse page = 1; repeated HostAliasDescriptor host_aliases = 2; }
```

The descriptor messages MUST mirror the JSON descriptor schemas in this RFC field-for-field.

### 9.5 Admin CLI As First-Class Asset

The `admin` CLI MUST be built, packaged, tested, and released as a first-class build asset with every Anvil server release.

The CLI MUST support:

1. local bootstrap against local Anvil storage where no admin listener exists yet;
2. remote administration against the admin listener;
3. explicit public/admin endpoint selection;
4. machine-readable JSON output for automation;
5. human-readable table output for operators;
6. idempotency keys for mutating commands;
7. audit reason flags for mutating commands.

Required command families and minimum command shapes:

```text
admin bootstrap init --mesh-id <mesh> --owner-principal <principal> --audit-reason <reason>
admin tenant create --name <tenant-name> --home-region <region> --idempotency-key <key> --audit-reason <reason>
admin app create --tenant <tenant-name> --app-name <name> --audit-reason <reason>
admin bucket create --tenant <tenant-name> --bucket <bucket> --region <region> --audit-reason <reason>
admin bucket public-access set --tenant <tenant-name> --bucket <bucket> --allow true|false --expected-generation <n> --audit-reason <reason>

admin link create --tenant <tenant-name> --bucket <bucket> --link <key> --target <key> [--target-version <version>] --resolution follow|redirect --idempotency-key <key> --audit-reason <reason>
admin link update --tenant <tenant-name> --bucket <bucket> --link <key> --target <key> [--target-version <version>] --expected-generation <n> --audit-reason <reason>
admin link delete --tenant <tenant-name> --bucket <bucket> --link <key> --expected-generation <n> --audit-reason <reason>
admin link read --tenant <tenant-name> --bucket <bucket> --link <key> --output json|table

admin host-alias create --hostname <host> --tenant <tenant-name> --bucket <bucket> --region <region> [--prefix <prefix>] --audit-reason <reason>
admin host-alias activate --hostname <host> --expected-generation <n> --audit-reason <reason>
admin host-alias suspend --hostname <host> --expected-generation <n> --audit-reason <reason>
admin host-alias delete --hostname <host> --expected-generation <n> --audit-reason <reason>

admin region create --region <region> --base-url <url> --virtual-host-suffix <suffix> --placement-weight <n> --audit-reason <reason>
admin region activate --region <region> --activation-checkpoint <file> --expected-generation <n> --audit-reason <reason>
admin region read-only --region <region> --expected-generation <n> --audit-reason <reason>
admin region drain --region <region> --default-disposition <disposition> --expected-generation <n> --audit-reason <reason>
admin region remove --region <region> --expected-generation <n> --audit-reason <reason>
admin region list --output json|table

admin cell register --region <region> --cell <cell> --placement-weight <n> --audit-reason <reason>
admin cell activate --region <region> --cell <cell> --expected-generation <n> --audit-reason <reason>
admin cell drain --region <region> --cell <cell> --expected-generation <n> --audit-reason <reason>
admin cell remove --region <region> --cell <cell> --expected-generation <n> --audit-reason <reason>

admin node register --node-id <node> --region <region> --cell <cell> --peer-id <peer> --api-addr <addr> --cluster-addr <multiaddr> --capability <cap>... --audit-reason <reason>
admin node activate --node-id <node> --expected-generation <n> --audit-reason <reason>
admin node drain --node-id <node> --graceful-timeout-ms <ms> --expected-generation <n> --audit-reason <reason>
admin node force-offline --node-id <node> --expected-generation <n> --audit-reason <reason>
admin node remove --node-id <node> --expected-generation <n> --audit-reason <reason>
admin node list --region <region> --output json|table

admin routing list --tenant <tenant-name> --bucket <bucket> --output json|table
admin routing repair --record <record-id> --expected-generation <n> --audit-reason <reason>
admin authz ...
admin repair ...
admin diagnostics ...
admin audit ...
```

All mutating commands MUST require `--audit-reason`. All mutating commands MUST either accept an explicit `--idempotency-key` or generate one and print it in JSON output. Update and delete commands MUST require `--expected-generation` unless the command is explicitly create-only.

Every CLI command with `--output json` MUST return this envelope:

```json
{
  "schema": "anvil.admin_cli.output.v1",
  "request_id": "req-123",
  "ok": true,
  "resource_type": "node",
  "resource": {},
  "generation": 4,
  "audit_event_id": "audit_01J0...",
  "idempotency_key": "req-123",
  "error": null
}
```

On failure, `ok` MUST be `false`, `resource` MUST be `null`, and `error` MUST contain the `AdminError` fields.

The server Docker image MUST include the admin CLI, and release artifacts MUST publish the CLI as a standalone binary where the release process supports standalone binaries.

## 10. Node Registry And Lifecycle

### 10.1 Stable Node Identity

Each node MUST have a stable `node_id`. The `node_id` MUST survive process restarts. It MUST NOT be derived only from a listening address, region name, or ephemeral `libp2p` peer id.

Each node MUST have a persisted cluster keypair used for `libp2p` identity. A node MUST NOT generate a new cluster keypair on every process start after it has been registered.

### 10.2 Node Descriptor

A node descriptor MUST have this JSON shape:

```json
{
  "schema": "anvil.mesh.node.v1",
  "mesh_id": "mesh_01",
  "node_id": "node_01J0...",
  "region": "eu-west-1",
  "cell_id": "cell_a",
  "libp2p_peer_id": "12D3KooW...",
  "public_api_addr": "https://node-1.eu-west-1.internal:50051",
  "public_cluster_addrs": ["/dns4/node-1/tcp/7443/quic-v1"],
  "capabilities": ["object", "index", "personaldb", "gateway", "admin"],
  "state": "joining",
  "drain": null,
  "last_heartbeat_at": "2026-07-02T00:00:00Z",
  "created_at": "2026-07-02T00:00:00Z",
  "updated_at": "2026-07-02T00:00:00Z",
  "generation": 3
}
```

Allowed `state` values:

```text
joining | active | draining | drained | offline | removed
```

### 10.3 Node Registration

A node MUST be registered before it can become active. Registration MUST create a node descriptor in `joining` state.

Registration MAY be initiated by:

1. an admin CLI command that creates a node descriptor and admission token; or
2. a node self-registration request signed by a bootstrap credential accepted only on the admin listener.

Registration MUST verify that:

1. the region exists;
2. the cell exists;
3. the node id is unique;
4. the advertised capabilities are valid enum values;
5. the caller has `manage_nodes` authority;
6. the request is idempotent or generation-checked.

### 10.4 Node Activation

A `joining` node becomes `active` only after:

1. it has connected to the cluster transport;
2. it has emitted a valid heartbeat;
3. its public API address passes health checks;
4. its capability set has been validated;
5. its region and cell are active;
6. an authorised admin or automatic policy activates it.

Placement MUST NOT assign new ownership to a node until it is active.

### 10.5 Node Drain

Draining a node is a controlled lifecycle transition from `active` to `draining` to `drained`.

When a node enters `draining`, Anvil MUST:

1. stop assigning new partition ownership to the node;
2. stop assigning new background tasks to the node;
3. stop routing new proxy requests to the node unless no other safe route exists;
4. ask the node to checkpoint active work;
5. transfer partition ownership using fenced ownership records;
6. wait for in-flight writes to complete, fail, or time out;
7. emit audit events for the drain start and completion.

A drained node MUST NOT own partitions, leases, background tasks, or routing targets.

### 10.6 Node Removal

A node MAY be removed only when it is `drained`, `offline`, or explicitly force-removed by an administrator with `manage_nodes` authority.

Force removal MUST:

1. increment affected ownership generations;
2. invalidate stale leases and fences held by the removed node;
3. emit an audit event;
4. trigger repair checks for partitions last owned by the removed node.

### 10.7 Node Lifecycle State Machine

Allowed transitions:

```text
joining  -> active
joining  -> removed
active   -> draining
active   -> offline
draining -> drained
draining -> offline
drained  -> active
drained  -> removed
offline  -> active
offline  -> draining
offline  -> removed
removed  -> terminal
```

All other transitions MUST be rejected.

### 10.8 `libp2p` Role

Anvil MAY use `libp2p` for:

1. node-to-node transport;
2. peer discovery;
3. peer heartbeat distribution;
4. metadata invalidation;
5. watch fanout hints;
6. repair coordination hints.

Anvil MUST NOT use `libp2p` gossip as the durable source of truth for:

1. whether a node is a member;
2. whether a node is active;
3. whether a node may own partitions;
4. whether a node may receive new tasks;
5. whether a node is drained;
6. whether a region exists.

Those decisions MUST be made from durable administrative records and fenced ownership state.

Required cluster topics:

```text
anvil.mesh.membership.v1
anvil.mesh.invalidation.v1
anvil.mesh.watch-hint.v1
```

A cluster gossip message MUST have this authenticated envelope before any topic-specific payload is applied:

```json
{
  "schema": "anvil.cluster.message.v1",
  "mesh_id": "mesh_01",
  "node_id": "node_01J0...",
  "libp2p_peer_id": "12D3KooW...",
  "topic": "anvil.mesh.invalidation.v1",
  "sequence": 8821,
  "timestamp_ms": 1783000000000,
  "payload_digest": "blake3:...",
  "signature": "base64:..."
}
```

The signature MUST be verified with the registered node identity or configured cluster message secret before the payload is used. Messages outside the configured clock-skew window MUST be ignored. The default clock-skew window is 60000 ms.

A valid gossip message MAY trigger cache invalidation or eager watch catch-up. It MUST NOT by itself create, activate, drain, remove, or resurrect a node, cell, region, bucket, or tenant.

### 10.9 Ownership Fence Record

Any partition, stream, task queue, watch checkpoint, index shard, PersonalDB group, or bucket-primary role that can be owned by a node MUST use a fenced ownership record. The record MUST have this JSON shape when exposed for diagnostics or repair:

```json
{
  "schema": "anvil.mesh.ownership_fence.v1",
  "resource_kind": "bucket_primary",
  "resource_id": "tenant_acme/releases",
  "owner_node_id": "node_01J0...",
  "owner_region": "eu-west-1",
  "owner_cell": "cell_a",
  "fence": 44,
  "state": "active",
  "lease_expires_at": "2026-07-02T00:01:00Z",
  "last_heartbeat_at": "2026-07-02T00:00:30Z",
  "generation": 44
}
```

Allowed `resource_kind` values MUST be typed enums and MUST include at least:

```text
control_partition | bucket_primary | object_partition | index_partition | personaldb_group | task_queue | watch_partition
```

Allowed fence `state` values:

```text
active | transferring | draining | expired | released
```

A write path protected by a fence MUST include the current `owner_node_id` and `fence`. If either value is stale, the write MUST fail with `StaleFence`.

### 10.10 Fence Protocol

The ownership service MUST provide these operations internally. The administrative API MAY expose diagnostic wrappers, but public APIs MUST NOT expose them directly.

```protobuf
service OwnershipService {
  rpc AcquireOwnership(AcquireOwnershipRequest) returns (OwnershipFenceResponse);
  rpc RenewOwnership(RenewOwnershipRequest) returns (OwnershipFenceResponse);
  rpc TransferOwnership(TransferOwnershipRequest) returns (OwnershipFenceResponse);
  rpc ReleaseOwnership(ReleaseOwnershipRequest) returns (OwnershipFenceResponse);
  rpc ForceExpireOwnership(ForceExpireOwnershipRequest) returns (OwnershipFenceResponse);
}

message OwnershipResource {
  OwnershipResourceKind resource_kind = 1;
  string resource_id = 2;
}

message AcquireOwnershipRequest {
  string request_id = 1;
  string idempotency_key = 2;
  string authenticated_node_id = 3;
  OwnershipResource resource = 4;
  uint64 requested_lease_ms = 5;
}

message RenewOwnershipRequest {
  string request_id = 1;
  string authenticated_node_id = 2;
  OwnershipResource resource = 3;
  uint64 current_fence = 4;
  uint64 requested_lease_ms = 5;
}

message TransferOwnershipRequest {
  string request_id = 1;
  string idempotency_key = 2;
  string authenticated_node_id = 3;
  OwnershipResource resource = 4;
  uint64 current_fence = 5;
  string new_owner_node_id = 6;
}

message ReleaseOwnershipRequest {
  string request_id = 1;
  string idempotency_key = 2;
  string authenticated_node_id = 3;
  OwnershipResource resource = 4;
  uint64 current_fence = 5;
  bool administrative_force = 6;
}

message ForceExpireOwnershipRequest {
  string request_id = 1;
  string idempotency_key = 2;
  string admin_principal_id = 3;
  OwnershipResource resource = 4;
  string reason = 5;
}

message OwnershipFenceResponse {
  string request_id = 1;
  OwnershipResource resource = 2;
  string owner_node_id = 3;
  string owner_region = 4;
  string owner_cell = 5;
  uint64 fence = 6;
  OwnershipFenceState state = 7;
  string lease_expires_at = 8;
  uint64 generation = 9;
  bool idempotent_replay = 10;
}

enum OwnershipResourceKind {
  OWNERSHIP_RESOURCE_KIND_UNSPECIFIED = 0;
  OWNERSHIP_RESOURCE_KIND_CONTROL_PARTITION = 1;
  OWNERSHIP_RESOURCE_KIND_BUCKET_PRIMARY = 2;
  OWNERSHIP_RESOURCE_KIND_OBJECT_PARTITION = 3;
  OWNERSHIP_RESOURCE_KIND_INDEX_PARTITION = 4;
  OWNERSHIP_RESOURCE_KIND_PERSONALDB_GROUP = 5;
  OWNERSHIP_RESOURCE_KIND_TASK_QUEUE = 6;
  OWNERSHIP_RESOURCE_KIND_WATCH_PARTITION = 7;
}

enum OwnershipFenceState {
  OWNERSHIP_FENCE_STATE_UNSPECIFIED = 0;
  OWNERSHIP_FENCE_STATE_ACTIVE = 1;
  OWNERSHIP_FENCE_STATE_TRANSFERRING = 2;
  OWNERSHIP_FENCE_STATE_DRAINING = 3;
  OWNERSHIP_FENCE_STATE_EXPIRED = 4;
  OWNERSHIP_FENCE_STATE_RELEASED = 5;
}
```

`AcquireOwnership` MUST succeed only when the resource has no active unexpired owner, is already owned by the same authenticated node, or is in `expired` or `released` state. A successful acquire MUST increment the fence unless it is an idempotent replay by the same owner for the same idempotency key.

`RenewOwnership` MUST succeed only for the authenticated node that currently owns the resource and presents the current fence. It MUST NOT accept an owner id supplied only in the request body.

`TransferOwnership` MUST require the current owner fence, mark the record `transferring`, then commit the new owner with `fence + 1`. A transfer MUST be idempotent by idempotency key.

`ReleaseOwnership` MUST require the current owner and fence unless the caller has administrative force-release authority.

`ForceExpireOwnership` MUST require administrative force-release authority and MUST increment the fence so stale owners cannot commit after expiry.

Failover after heartbeat expiry MUST be implemented as `ForceExpireOwnership` followed by `AcquireOwnership` by the replacement owner.

### 10.11 Heartbeat And Lease Bounds

Node heartbeat defaults:

```text
heartbeat_interval_ms = 5000
heartbeat_missed_deadline_ms = 20000
node_offline_after_ms = 60000
max_ownership_lease_ms = 120000
```

A deployment MAY lower these values by policy. A deployment MUST NOT raise `max_ownership_lease_ms` above the server compiled maximum unless a new binary explicitly changes that maximum.

Ownership renewal MUST be authenticated as the registered node principal. A caller MUST NOT be able to claim ownership by sending an arbitrary `owner_node_id` in the request body.

## 11. Region And Cell Registry

### 11.1 Region Descriptor

A region descriptor MUST have this JSON shape:

```json
{
  "schema": "anvil.mesh.region.v1",
  "mesh_id": "mesh_01",
  "region": "eu-west-1",
  "state": "active",
  "public_base_url": "https://eu-west-1.anvil-storage.com",
  "virtual_host_suffix": "eu-west-1.anvil-storage.com",
  "placement_weight": 100,
  "default_cell": "cell_a",
  "created_at": "2026-07-02T00:00:00Z",
  "updated_at": "2026-07-02T00:00:00Z",
  "generation": 9
}
```

Allowed `state` values:

```text
joining | active | read_only | draining | drained | drained_with_exceptions | offline | removed
```

### 11.2 Cell Descriptor

A cell descriptor MUST have this JSON shape:

```json
{
  "schema": "anvil.mesh.cell.v1",
  "mesh_id": "mesh_01",
  "region": "eu-west-1",
  "cell_id": "cell_a",
  "state": "active",
  "placement_weight": 100,
  "created_at": "2026-07-02T00:00:00Z",
  "updated_at": "2026-07-02T00:00:00Z",
  "generation": 5
}
```

Allowed `state` values are the same as region state values.

### 11.3 Adding A Region

Creating a region MUST:

1. create a region descriptor in `joining` state;
2. define its public base URL;
3. define its virtual-host suffix;
4. register at least one cell or require a subsequent cell registration before activation;
5. verify the caller has `manage_regions` authority;
6. emit an audit event.

A region MAY become `active` only when:

1. it has at least one active cell;
2. at least one active node exists in that cell;
3. routing health checks pass;
4. mesh directory replication has reached the required checkpoint for the region;
5. an authorised admin or automatic policy activates it.

### 11.4 Draining A Region

Draining a region MUST:

1. mark the region `draining`;
2. stop new bucket placement in the region;
3. stop creating new host aliases that target the region;
4. decide a disposition for every active bucket in the region;
5. drain all cells in the region or leave them active only for an explicit `remain_proxy_only` or `read_only_until_removed` exception;
6. update bucket locators using generation-checked mutations;
7. drain regional background work;
8. emit audit events for the drain start, each bucket disposition, and drain completion.

Allowed bucket dispositions during region drain:

```text
block_until_empty | remain_proxy_only | read_only_until_removed | delete_after_retention
```

Disposition semantics:

1. `block_until_empty`: the drain cannot complete until no bucket locator names the region as primary. This is the default.
2. `remain_proxy_only`: the bucket remains physically in the region, but no new writes are accepted unless the region is later reactivated. Other regions may proxy reads to it. The region cannot be fully removed while this disposition remains active.
3. `read_only_until_removed`: the bucket becomes read-only and is retained until an explicit later removal.
4. `delete_after_retention`: the bucket is marked for deletion after the configured retention period and cannot receive new writes.

This RFC does not define cross-region bucket migration or replication cutover. A region drain MUST NOT invent migration behaviour. If an operator wants a bucket writable in another region, a separate bucket movement or replication feature must be specified and implemented before that disposition is available.

A bucket drain exception record MUST be written for every bucket that uses `remain_proxy_only` or `read_only_until_removed`:

```json
{
  "schema": "anvil.mesh.bucket_drain_exception.v1",
  "tenant_id": "tenant_acme",
  "bucket_name": "releases",
  "region": "eu-west-1",
  "disposition": "remain_proxy_only",
  "reason": "customer-approved delayed migration",
  "expires_at": "2026-08-02T00:00:00Z",
  "generation": 1
}
```

A region MUST NOT reach `drained` while any active bucket locator still has that region as its writable primary. A region MAY reach `drained_with_exceptions` only if every remaining bucket has a valid exception record and the region continues to serve only the explicitly allowed operation class.

### 11.5 Region Lifecycle State Machine

Allowed transitions:

```text
joining   -> active
joining   -> removed
active    -> read_only
active    -> draining
active    -> offline
read_only -> active
read_only -> draining
draining  -> drained
draining  -> drained_with_exceptions
draining  -> offline
drained   -> active
drained_with_exceptions -> active
drained_with_exceptions -> draining
drained   -> removed
offline   -> active
offline   -> draining
offline   -> removed
removed   -> terminal
```

All other transitions MUST be rejected.

## 12. Placement And Routing

### 12.1 Placement Inputs

Bucket placement MUST consider:

1. requested region where supplied;
2. tenant policy;
3. region lifecycle state;
4. cell lifecycle state;
5. node lifecycle state;
6. capacity and placement weight;
7. bucket class or workload hints;
8. compliance constraints where configured.

Placement MUST NOT select a region, cell, or node in `draining`, `drained`, `offline`, or `removed` state for new writable ownership.

### 12.2 Bucket Creation

Bucket creation MUST:

1. resolve tenant by globally unique tenant name or tenant id;
2. reject duplicate `(tenant_id, bucket_name)`;
3. select an active region and cell;
4. create the regional bucket metadata;
5. create the bucket locator;
6. update the tenant bucket index;
7. emit a watch event;
8. return the canonical region endpoint and virtual-host endpoint.

If bucket creation fails after regional metadata is created but before the bucket locator is committed, recovery MUST either complete the locator write or roll back the regional metadata. The recovery path MUST be idempotent.

### 12.3 Request Routing Sequence

```text
client
  -> public listener
  -> parse host/path
  -> resolve tenant locator
  -> resolve bucket locator
  -> if local region: execute operation
  -> if remote region and proxy allowed: proxy to remote region
  -> if remote region and redirect allowed: return redirect
  -> otherwise: return routing error
```

### 12.4 Redirect Requirements

A redirect response MUST include the target regional endpoint. For S3-compatible requests, redirects MUST include the bucket region using the protocol's expected header.

Redirects MUST NOT leak private tenant or bucket metadata beyond what the client already supplied unless the caller is authorised to view that metadata.

### 12.5 Proxy Requirements

Proxying MUST preserve:

1. authenticated principal;
2. request id;
3. authorisation context;
4. idempotency key;
5. conditional headers;
6. range headers;
7. checksum headers;
8. tracing metadata.

A proxy MUST NOT mint broader authority than the original request has.

Inter-region proxying MUST use Anvil's internal gRPC proxy service over the cluster network. It MUST NOT use public HTTP object routes unless explicitly configured as an emergency fallback. The internal proxy request MUST carry the original authenticated principal, request id, idempotency key, method, canonical host/path, selected bucket locator generation, and authorisation context.

The internal proxy service MUST have this streaming shape or an exactly equivalent wire contract:

```protobuf
service InternalProxyService {
  rpc ProxyObject(stream ProxyRequestChunk) returns (stream ProxyResponseChunk);
}

message ProxyRequestHeader {
  string request_id = 1;
  string idempotency_key = 2;
  string principal_id = 3;
  string tenant_id = 4;
  string bucket_name = 5;
  string object_key = 6;
  string method = 7;
  string canonical_host = 8;
  string canonical_path = 9;
  uint64 bucket_locator_generation = 10;
  repeated Header headers = 11;
  bytes authz_context = 12;
}

message ProxyRequestChunk {
  oneof part {
    ProxyRequestHeader header = 1;
    bytes body = 2;
  }
}

message ProxyResponseHeader {
  string request_id = 1;
  uint32 status = 2;
  repeated Header headers = 3;
  repeated Header trailers = 4;
  bool committed = 5;
}

message ProxyResponseChunk {
  oneof part {
    ProxyResponseHeader header = 1;
    bytes body = 2;
  }
}

message Header { string name = 1; bytes value = 2; }
```

The first request chunk MUST be `ProxyRequestHeader`. The first response chunk MUST be `ProxyResponseHeader`. Body chunks MUST preserve order. Backpressure MUST use HTTP/2 or QUIC stream flow control from the underlying transport.

Retries are allowed for `GET`, `HEAD`, and `OPTIONS`. Mutating requests may be retried only when an idempotency key is present and the previous proxy response either did not contain a response header or contained `committed = false`.

`libp2p` streams MAY carry the internal gRPC transport where supported, but the proxy protocol is still the Anvil internal proxy service. Gossip messages MUST NOT carry object payloads.

## 13. Authorisation Requirements

### 13.1 Public Requests

Public object and registry requests MUST be authorised against the tenant, bucket, object, registry, or link being accessed.

Anonymous reads MAY be allowed only when the target policy explicitly permits anonymous read.

Anonymous requests MUST NOT create, update, delete, or administer tenants, buckets, links, host aliases, nodes, cells, regions, registry configuration, or authorisation tuples.

### 13.2 Link Authorisation

A followed link requires permission to read both the link entry and the target object.

A link update requires permission to write the link key and permission to read the target object. 
### 13.3 Routing Authorisation

Routing lookup MAY occur before object authorisation because routing is needed to find the authority owner. Routing lookup MUST return only the minimum information needed to route the request. Full locator details MUST be returned only through authorised administrative APIs.

### 13.4 Administrative Authorisation

Every administrative API MUST check `anvil_admin` capabilities. No admin route may rely only on network location, shared secret, or CLI origin.

## 14. Watch And Cache Invalidation

### 14.1 Required Watch Event Families

Anvil MUST emit watch events for:

1. tenant locator changes;
2. tenant-name index changes;
3. bucket locator changes;
4. region descriptor changes;
5. cell descriptor changes;
6. node descriptor changes;
7. host alias changes;
8. object link changes;
9. object payload or metadata changes;
10. ownership fence changes.

Every watch event MUST use this envelope shape when exposed through native APIs, diagnostics, or replication:

```json
{
  "schema": "anvil.watch.event.v1",
  "stream": "mesh.bucket_locator.0a7f",
  "sequence": 1844,
  "event_id": "evt_01J0...",
  "event_type": "bucket_locator_updated",
  "scope": {
    "tenant_id": "tenant_acme",
    "bucket_name": "releases",
    "region": null,
    "cell_id": null,
    "node_id": null
  },
  "record_key": "tenant_acme/releases",
  "record_generation": 19,
  "record_digest": "blake3:...",
  "writer_node_id": "node_01J0...",
  "writer_fence": 44,
  "created_at": "2026-07-02T00:00:00Z"
}
```

The `scope` object is polymorphic. Events that are not bucket scoped MUST set irrelevant fields to `null` and set the relevant region, cell, node, or tenant fields.

Within one stream, watch sequence numbers MUST be strictly increasing and gap-free. A watch reader that observes a gap MUST stop applying derived state and enter repair/catch-up.

Watch checkpoints MUST be stored per reader, stream, and partition. A checkpoint MUST include the last applied sequence and digest. Applying an already applied event MUST be idempotent. Applying the same sequence with a different digest MUST fail with `ControlStreamDivergence`.

### 14.2 Routing Cache Invalidation

Nodes MAY cache tenant, bucket, host-alias, node, region, and cell records. Caches MUST be invalidated by watch events or bounded by a short TTL.

A stale routing cache MUST NOT allow writes to a drained region, drained cell, drained node, or stale bucket primary. Mutating operations MUST check generation/fence records at the ownership boundary before committing.

### 14.3 Link Cache Invalidation

Object link updates MUST invalidate cached responses for the link key. If Anvil emits CDN purge hints to a fronting system, the purge hint MUST target the link key, not every target object that link has ever pointed at.

## 15. Failure Handling

### 15.1 Node Failure

If a node misses heartbeat deadlines, it MAY be marked `offline` by authorised control logic. Marking a node offline MUST increment or invalidate ownership fences before another node commits work for the same partition.

### 15.2 Node Recovery

An offline node MAY return to `active` only after:

1. it proves the same stable node identity;
2. it refreshes its cluster transport connection;
3. it passes health checks;
4. any stale leases it held are reconciled;
5. the node descriptor transition is generation-checked.

### 15.3 Region Failure

If a region is unreachable, other regions MAY continue to accept requests for buckets they own. For buckets owned by the unreachable region, other regions MUST follow configured policy:

```text
redirect_only | proxy_if_available | unavailable
```

A region failure MUST NOT cause another region to become writable primary for a bucket. Writable-primary movement and read-replica promotion are not defined by this RFC.

### 15.4 Stale Owner Rejection

Every write path that depends on node, cell, region, or bucket ownership MUST reject stale owners using generation or fence checks. This includes object writes, multipart commits, object link updates, watch checkpoints, index maintenance, PersonalDB commits, and background task commits.

## 16. Sequences

### 16.1 Node Registration And Activation

```text
operator/admin CLI
  -> AdminService.RegisterNode(node_id, region, cell, addresses, capabilities)
  -> check anvil_admin#manage_nodes
  -> write node descriptor state=joining generation=N
  -> return admission material

node process
  -> start with stable node_id and persisted libp2p key
  -> join cluster transport
  -> emit heartbeat

operator/admin CLI or auto policy
  -> AdminService.ActivateNode(node_id, expected_generation=N)
  -> health check node API and cluster transport
  -> verify region active and cell active
  -> write node descriptor state=active generation=N+1
  -> placement may assign work
```

### 16.2 Node Drain

```text
operator/admin CLI
  -> AdminService.DrainNode(node_id, expected_generation=N)
  -> check anvil_admin#manage_nodes
  -> write node descriptor state=draining generation=N+1
  -> placement excludes node
  -> background scheduler stops new work for node
  -> ownership manager transfers fenced partitions
  -> active work checkpoints or times out
  -> write node descriptor state=drained generation=N+2
  -> emit audit event
```

### 16.3 Virtual-Host GET With Link Resolution

```text
client GET https://releases.acme.eu-west-1.anvil-storage.com/latest.exe
  -> parse host: bucket=releases tenant=acme region=eu-west-1
  -> resolve tenant locator for acme
  -> resolve bucket locator for tenant_acme/releases
  -> local region matches eu-west-1
  -> resolve object entry latest.exe
  -> entry kind is link
  -> check read on link latest.exe
  -> resolve target my-app-v3.0.1.exe
  -> check read on target
  -> serve target bytes with validators including link generation and target version
```

### 16.4 Moving A Link

```text
publisher/admin
  -> UpdateObjectLink(bucket=releases, link=latest.exe, target=my-app-v3.0.2.exe, expected_generation=12)
  -> check write on link key
  -> check read on target object
  -> compare generation 12
  -> write link generation 13
  -> emit object_link_updated watch event for latest.exe
  -> caches purge latest.exe
```

### 16.5 Region Drain

```text
operator/admin CLI
  -> AdminService.DrainRegion(region=eu-west-1)
  -> check anvil_admin#manage_regions
  -> write region state=draining generation=N+1
  -> placement stops new buckets in region
  -> list bucket locators for region
  -> apply disposition for each bucket
  -> drain cells and nodes
  -> verify no disallowed writable primary remains
  -> write region state=drained generation=N+2
```

## 17. Tests And Conformance

### 17.1 URL Routing Tests

Tests MUST prove:

1. path-style regional URLs route to the correct tenant bucket;
2. virtual-host regional URLs route to the correct tenant bucket;
3. custom host aliases route to the configured tenant bucket and prefix;
4. tenant-scoped duplicate bucket names in different tenants are allowed;
5. duplicate bucket names in the same tenant are rejected;
6. unknown tenant returns a tenant-not-found error;
7. unknown bucket returns a bucket-not-found error;
8. wrong-region requests redirect or proxy according to policy;
9. redirects do not leak private metadata;
10. host parsing rejects dotted tenant and bucket labels for the virtual-host form;
11. trusted forwarded host metadata is accepted only from configured proxy source ranges;
12. untrusted forwarded host metadata is ignored;
13. custom host alias prefix joining rejects traversal after percent decoding;
14. native Anvil hostnames cannot be overridden by host aliases.

### 17.2 Object Link Tests

Tests MUST prove:

1. many link keys can point to the same object;
2. moving a link changes the served target without copying bytes;
3. stale link generation updates fail;
4. followed-link ETags change when the link target changes;
5. `GET` and `HEAD` follow links by default;
6. explicit link metadata reads return the link descriptor;
7. link loops are detected;
8. link resolution enforces permission on both link and target;
9. deleting a link does not delete the target object;
10. deleting a target makes the link fail with a structured dangling-link error unless a retained target version still exists;
11. absent `target_version` follows the current target object version;
12. present `target_version` pins to the retained version;
13. unauthorised link reads do not reveal whether link or target caused denial;
14. `PUT`, `DELETE`, and metadata mutation operate on the link entry and not the target.

### 17.3 Admin Plane Tests

Tests MUST prove:

1. admin handlers are absent from the public listener;
2. admin handlers are present on the admin listener;
3. unauthenticated admin requests fail;
4. authenticated principals without required `anvil_admin` capability fail;
5. authorised admin requests succeed;
6. admin CLI can talk to the admin listener;
7. admin CLI can produce JSON output;
8. mutating admin CLI commands carry idempotency keys and audit reasons;
9. public listener rejects every admin gRPC service name;
10. public listener rejects admin HTTP routes by path, method, and content type;
11. admin errors return structured `AdminError`;
12. update commands fail without expected generation.

### 17.4 Node Lifecycle Tests

Tests MUST prove:

1. a node can be registered;
2. an unregistered node cannot become active;
3. a joining node does not receive new ownership;
4. an active node can be drained;
5. a draining node receives no new ownership or background work;
6. a drained node owns no partitions or tasks;
7. a removed node cannot rejoin without a new registration;
8. ephemeral `libp2p` peer changes do not create a second node identity;
9. stale node fences cannot commit work after drain or removal;
10. ownership renewal cannot be performed by another authenticated node using the same `owner_node_id`;
11. heartbeat expiry moves a node to `offline` only through generation-checked control mutation;
12. invalid lifecycle transitions are rejected.

### 17.5 Region Lifecycle Tests

Tests MUST prove:

1. a region can be created in `joining` state;
2. a region cannot become active without an active cell and node;
3. active regions can receive new bucket placement;
4. read-only regions do not receive writable bucket placement;
5. draining regions do not receive new bucket placement;
6. region drain applies a valid disposition to every active bucket;
7. region drain does not complete while disallowed writable primaries remain;
8. stale region ownership cannot commit after drain or failover;
9. every drain disposition has the specified effect;
10. `drained_with_exceptions` requires valid exception records;
11. invalid lifecycle transitions are rejected.

### 17.6 Mesh Directory Tests

Tests MUST prove:

1. tenant locators are partitioned by stable hash prefix;
2. bucket locators are partitioned by stable hash prefix;
3. host aliases are partitioned by stable hash prefix;
4. public object APIs cannot access `_anvil/control/*`;
5. routing does not scan all tenants or all buckets;
6. cache invalidation updates routing after locator changes;
7. stale caches cannot commit writes through stale ownership;
8. tenant-name index creation is CAS protected;
9. control stream replay is idempotent for the same sequence and digest;
10. control stream replay fails for the same sequence with a different digest;
11. watch readers stop on sequence gaps;
12. watch checkpoints include sequence and digest;
13. tenant creation partial failure recovers from reserved tenant-name records;
14. activation-checkpoint validation rejects missing streams;
15. activation-checkpoint validation rejects digest mismatches;
16. control-stream writer failover rejects stale fences;
17. internal proxy preserves request id, principal, idempotency key, and auth context.

## 18. Implementation Order

The implementation SHOULD proceed in this order:

1. add split public/admin listener support where missing;
2. make `admin` a first-class release artifact in local and CI builds;
3. define typed `AnvilAdminCapability` values for node, region, routing, host-alias, and link administration;
4. add durable region, cell, and node descriptors;
5. persist node identity and `libp2p` keypairs;
6. implement node registration and activation;
7. implement node drain and removal;
8. implement region and cell registration, activation, drain, and removal;
9. add tenant and bucket locator partitioned directories;
10. update bucket creation to write tenant-scoped bucket locators;
11. implement path-style and virtual-host routing;
12. implement host-alias routing;
13. implement object links;
14. add routing, lifecycle, and link watch events;
15. add stale-owner/fence checks to all affected write paths;
16. add conformance tests defined in Section 17.

## 19. Acceptance Criteria

This RFC is implemented when all of these are true:

1. tenants are globally unique;
2. buckets are tenant-scoped and are not required to be globally unique;
3. path-style regional URLs work;
4. `bucket.tenant.<region>.anvil-storage.com` virtual-host URLs work;
5. custom host aliases route to tenant bucket prefixes;
6. TLS termination remains outside Anvil and Anvil does not require certificate management;
7. object links are symlink-like metadata entries addressed by ordinary object keys;
8. many object links can point to the same target object;
9. object links can be moved atomically with compare-and-swap generation checks;
10. link resolution enforces authorisation on link and target;
11. the admin listener is separate from the public listener;
12. admin APIs are absent from the public listener;
13. the admin CLI is built and released as a first-class asset;
14. regions have durable descriptors and lifecycle state;
15. cells have durable descriptors and lifecycle state;
16. nodes have durable descriptors, stable node ids, and persisted `libp2p` keypairs;
17. adding a node is an explicit administrative workflow;
18. draining a node is an explicit administrative workflow;
19. adding a region is an explicit administrative workflow;
20. draining a region is an explicit administrative workflow;
21. `libp2p` is used for discovery, transport, gossip, and invalidation, not durable membership authority;
22. placement excludes draining, drained, offline, and removed nodes, cells, and regions;
23. stale owners cannot commit writes after drain, failover, or removal;
24. all conformance tests in Section 17 pass.
