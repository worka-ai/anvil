# RFC ANVIL-0002: Native Registry Gateways And Split Administrative Plane

## Status

Draft.

## Date

2026-07-01.

## Normative Language

The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, `MAY`, and `OPTIONAL` in this document are normative. They are to be interpreted as described in RFC 2119.

## 1. Abstract

Anvil will provide native package and artifact registry gateways backed by Anvil's object, metadata, authorisation, watch, and consistency primitives. The initial registry families are:

1. Docker and OCI container registry;
2. Maven repository;
3. Cargo sparse registry;
4. Python package registry compatible with PyPI clients;
5. npm registry, delivered in explicitly staged compatibility phases.

The registry gateways replace deployment dependencies on a separate artifact registry product. Registry data is stored as Anvil-owned object data. Registry protocol requests are translated into Anvil object reads, object writes, compare-and-swap manifest updates, prefix listings, metadata updates, and authorisation checks.

This RFC also splits Anvil's public API plane from its administrative API plane. Public listeners may be exposed to users or the internet for object and registry access. Administrative APIs MUST be served on a separate listener and MUST NOT be registered on the public listener. Administrative access is controlled by an Anvil-owned Zanzibar namespace named `anvil_admin`.

## 2. Goals

An implementation conforming to this RFC MUST:

1. store Docker/OCI, Maven, Cargo, Python, and npm registry state in Anvil-owned storage;
2. avoid depending on an external package registry or artifact repository service;
3. preserve Anvil's reserved namespace protections for registry internals;
4. expose registry-compatible HTTP APIs for standard ecosystem tools;
5. use Anvil object storage for registry payload bytes;
6. use Anvil metadata and path indexes for registry listings;
7. use Anvil compare-and-swap mechanisms for tag, version, index, and metadata updates;
8. use Anvil full-text and metadata-filter indexes for registry search where search is required;
9. avoid adding a new Anvil index engine solely for registry support;
10. enforce authorisation before reads, writes, deletes, searches, downloads, tag updates, version updates, and metadata updates;
11. support anonymous public reads only when registry policy explicitly allows them;
12. implement a separate administrative listener for administrative APIs;
13. ensure administrative APIs are absent from the public listener;
14. define Anvil administrative capabilities through typed authorisation objects rather than ad-hoc string checks;
15. support protocol-specific E2E tests using unmodified ecosystem clients.

## 3. Non-Goals

This RFC does not require:

1. a general-purpose SQL database;
2. an external embedded key-value database;
3. a new Anvil index type for registries;
4. a registry proxy cache for third-party upstream registries;
5. malware scanning, SBOM analysis, signature transparency, or policy admission engines;
6. full npm ecosystem compatibility in the first npm milestone;
7. public exposure of administrative APIs;
8. access to raw `_anvil/registry/*` objects through compatibility object APIs;
9. separate worker node classes for registry maintenance.

Security scanning, upstream proxying, provenance verification, and advanced policy engines MAY be added later, but they MUST build on the storage and authorisation model defined here.

## 4. System Overview

Anvil has three externally relevant planes:

1. **Public data plane**: object APIs, S3 compatibility, registry protocol gateways, and non-administrative native APIs.
2. **Administrative plane**: tenant, app, registry, bucket, index, policy, repair, and cluster administration.
3. **Cluster plane**: node-to-node internal traffic.

The public data plane and administrative plane MUST bind to different socket addresses. A deployment MAY place both listeners behind the same reverse proxy if the reverse proxy preserves the separation through distinct internal upstreams, but Anvil itself MUST NOT register administrative handlers on the public listener.

Registry protocol gateways run on the public data plane. Registry configuration and policy management run on the administrative plane.

```text
standard client tool
  -> public listener
  -> registry gateway
  -> registry authorisation
  -> RegistryStore
  -> Anvil object/CAS/list/watch primitives
  -> Anvil-owned registry objects

operator/admin automation
  -> admin listener
  -> admin authentication
  -> anvil_admin authorisation check
  -> admin service
  -> registry/bucket/tenant/authz/index control state
```

## 5. Listener Model

### 5.1 Public Listener

The public listener serves APIs that may be exposed to tenants, applications, package managers, container runtimes, CI systems, browsers, and anonymous readers.

The public listener MAY serve:

1. object reads and writes that are authorised through object permissions;
2. S3-compatible operations;
3. Docker/OCI registry routes under `/v2/`;
4. Maven repository routes under configured repository prefixes;
5. Cargo sparse registry routes under configured registry prefixes;
6. Python package routes under configured registry prefixes;
7. npm registry routes under configured registry prefixes;
8. token acquisition routes required by public client tools;
9. non-administrative native gRPC routes explicitly classified as public.

The public listener MUST NOT serve:

1. tenant creation;
2. app/client creation;
3. initial secret creation;
4. broad policy grant/revoke operations;
5. administrative registry creation/configuration;
6. administrative bucket policy mutation;
7. administrative index definition mutation;
8. repair operations;
9. cluster diagnostics;
10. authz namespace schema mutation;
11. raw internal object inspection;
12. local bootstrap operations.

### 5.2 Administrative Listener

The administrative listener serves only administrative APIs. It MUST require authentication on every route. It MUST require an `anvil_admin` authorisation check before every operation.

The administrative listener MUST NOT expose anonymous endpoints. A deployment MAY bind it to `127.0.0.1`, a private interface, a VPN-only interface, or an internal service network. The listener separation exists so that exposing public package/object reads does not expose administrative APIs by accident.

### 5.3 Cluster Listener

The cluster listener remains for node-to-node coordination and data movement. Registry gateway work MUST NOT introduce registry-specific node classes. Every Anvil node remains capable of serving APIs, owning partitions, executing in-process leases, and maintaining derived state.

## 6. Configuration

An implementation MUST add explicit configuration for the split planes and registry gateway base URLs.

Required configuration fields:

```text
api_listen_addr
admin_listen_addr
public_api_addr
public_registry_base_url
registry_system_bucket
registry_default_visibility
registry_upload_ttl_seconds
registry_max_manifest_bytes
registry_max_metadata_bytes
registry_max_single_upload_bytes
registry_enable_docker
registry_enable_maven
registry_enable_cargo
registry_enable_pypi
registry_enable_npm
```

`api_listen_addr` is the public listener bind address.

`admin_listen_addr` is the administrative listener bind address. The default SHOULD be loopback-only. Production deployment templates SHOULD require an explicit value.

`public_registry_base_url` is the externally reachable base URL used inside protocol metadata, redirects, token challenges, Cargo `config.json`, Python simple links, npm tarball URLs, and Docker upload locations.

`registry_system_bucket` is the Anvil-owned bucket where registry internals are stored. Public object APIs MUST NOT expose objects under reserved registry paths, regardless of bucket policy.

`registry_default_visibility` MUST be either `private` or `public-read`. The default MUST be `private`.

Protocol enable flags MAY disable an entire gateway without deleting stored data.

## 7. Registry Core Model

### 7.1 Registry Kinds

The core registry model MUST define a typed enum:

```text
RegistryKind = docker_oci | maven | cargo | pypi | npm
```

The implementation MUST NOT identify registry kind through free-form strings after parsing configuration or API input. Unknown registry kinds MUST be rejected.

### 7.2 Registry Identity

A registry is identified by:

```text
RegistryId = tenant_id + registry_name + registry_kind
```

`registry_name` MUST be normalised to lowercase ASCII using the grammar in Section 7.5. Registry names are unique per tenant and registry kind.

### 7.3 Repository And Package Identity

The term `repository` means different things across ecosystems. Internally Anvil MUST distinguish these concepts:

```text
Registry       = configured registry endpoint and policy boundary
Repository     = registry-specific grouping when a protocol has repositories
Package        = installable named artifact family
Version        = immutable or policy-controlled release of a package
ArtifactBlob   = content-addressed byte object
MutablePointer = tag, dist-tag, metadata file, index entry, or version-list pointer
```

For Docker/OCI, a repository is the image name and a package is equivalent to that repository.

For Maven, a package is `(group_id, artifact_id)`. The Maven repository prefix is the registry endpoint.

For Cargo, a package is the crate name.

For PyPI, a package is the normalised project name.

For npm, a package is the normalised npm package name, including scope when present.

### 7.4 Visibility

Each registry, repository, package, and artifact MAY have visibility metadata.

Allowed visibility values:

```text
private
public_read
```

`public_read` means anonymous read MAY be allowed after registry gateway policy evaluation. It MUST NOT bypass reserved path protection. It MUST NOT grant write, delete, admin, or metadata mutation capabilities.

### 7.5 Safe Identifier Grammar

Registry gateway identifiers MUST be parsed and normalised before being mapped to object keys. The following generic grammar applies unless a protocol-specific normalisation rule is stricter:

```abnf
lower-alpha      = %x61-7A
upper-alpha      = %x41-5A
digit            = %x30-39
safe-char        = lower-alpha / upper-alpha / digit / "." / "_" / "-"
registry-name    = 1*128safe-char
repo-segment      = 1*255(safe-char / "+")
path-segment      = 1*255(safe-char / "+")
version-segment   = 1*255(safe-char / "+" / "~")
```

The implementation MUST reject:

1. empty identifiers;
2. path traversal (`.` or `..` as a segment);
3. percent-encoded path traversal;
4. absolute paths;
5. control characters;
6. null bytes;
7. path separators inside a segment after decoding;
8. identifiers that normalise to a different security boundary than the original route implies.

### 7.6 Gateway Route Mounts

Each registry instance MUST have an explicit route mount. Route mounts are part of registry configuration and are not inferred from package names.

Required route mount fields:

```json
{
  "schema": "anvil.registry.route_mount.v1",
  "registry_id": "tenant-123/docker_oci/worka",
  "kind": "docker_oci",
  "public_prefix": "/v2",
  "token_realm_prefix": "/v2/auth/token",
  "enabled": true
}
```

Route mount requirements:

1. a public route prefix MUST resolve to exactly one registry instance;
2. overlapping route prefixes MUST be rejected at registry creation time;
3. route matching MUST happen before protocol path parsing;
4. disabled route mounts MUST return `404` or the protocol-native disabled response without leaking stored package names;
5. route prefixes MUST NOT overlap administrative API prefixes;
6. Docker/OCI MAY reserve `/v2/` because ecosystem clients expect it;
7. Maven, Cargo, Python, and npm SHOULD use explicit prefixes such as `/maven/{registry}`, `/cargo/{registry}`, `/pypi/{registry}`, and `/npm/{registry}` unless a deployment assigns separate hostnames per registry kind.

An implementation MAY support virtual host routing. If it does, hostnames are part of route mount identity and MUST be normalised before lookup.

### 7.7 Protocol Metadata Must Not Be Source Of Authority

Protocol metadata such as npm maintainers, Maven developers, Cargo owners, Python author fields, Docker labels, or OCI annotations MUST NOT grant Anvil permissions. They are package metadata only.

The only sources of registry authority are:

1. authenticated principal identity;
2. token scopes when scopes are explicitly supported;
3. Anvil registry relationship tuples;
4. Anvil administrative relationship tuples;
5. explicit public-reader relationships.

## 8. Registry Storage Layout

### 8.1 System Bucket

Registry internal objects MUST be stored under an Anvil-owned bucket. This RFC uses `anvil-system` as the default bucket name, but the name is configurable through `registry_system_bucket`.

All registry internal keys MUST be under:

```text
_anvil/registry/v1/
```

Public object APIs MUST reject GET, HEAD, LIST, PUT, COPY, COMPOSE, DELETE, PATCH, range GET, SELECT, and metadata APIs for this prefix. Native admin APIs MAY expose structured registry state after administrative authorisation. They MUST NOT expose raw internal bytes unless the caller has an explicit diagnostic capability.

### 8.2 Top-Level Layout

```text
_anvil/registry/v1/
  tenants/{tenant_id}/
    registries/{registry_kind}/{registry_name}/
      registry.json
      policy.json
      audit/
      uploads/
      blobs/
      docker/
      maven/
      cargo/
      pypi/
      npm/
```

Only the directory for the registry kind in use is required. Common `uploads/` and `blobs/` MAY be shared by multiple protocols inside one tenant when content deduplication policy allows it.

### 8.3 Registry Descriptor JSON

`registry.json` MUST be UTF-8 JSON with this schema:

```json
{
  "schema": "anvil.registry.registry.v1",
  "registry_id": "tenant-123/docker_oci/worka",
  "tenant_id": "tenant-123",
  "kind": "docker_oci",
  "name": "worka",
  "created_at": "2026-07-01T00:00:00Z",
  "created_by": "admin:user:alice",
  "visibility": "private",
  "base_url": "https://registry.example.com",
  "object_prefix": "_anvil/registry/v1/tenants/tenant-123/registries/docker_oci/worka/",
  "version": 1,
  "flags": {
    "delete_enabled": false,
    "anonymous_pull_enabled": false,
    "immutable_versions": true,
    "immutable_tags": false
  }
}
```

The implementation MUST reject unknown `kind` and `visibility` values. It MAY preserve unknown `flags` for forward compatibility, but MUST NOT act on unknown flags.

### 8.4 Policy JSON

`policy.json` is a cached projection of authorisation policy for diagnostics and fast display. It is not the source of truth for access decisions. Access decisions MUST use the authorisation engine.

`policy.json` MUST include the latest authorisation revision used to build it:

```json
{
  "schema": "anvil.registry.policy_projection.v1",
  "registry_id": "tenant-123/docker_oci/worka",
  "authz_revision": 42,
  "visibility": "private",
  "summary": {
    "pull_subjects": 10,
    "push_subjects": 3,
    "admin_subjects": 1
  }
}
```

### 8.5 Blob Storage

Registry payload bytes MUST be content-addressed when the protocol exposes a digest. Digest-addressed blob keys MUST use lowercase hexadecimal digest bytes.

```text
blobs/{algorithm}/{hex_digest}
```

Example:

```text
blobs/sha256/4f8f86a0...
```

The object metadata for a blob MUST include:

```text
anvil.registry.kind
anvil.registry.id
anvil.registry.digest.algorithm
anvil.registry.digest.hex
anvil.registry.content_type
anvil.registry.original_filename optional
anvil.registry.logical_size
anvil.registry.created_by
anvil.registry.created_at
```

When a protocol does not supply a digest, Anvil MUST compute at least SHA-256 and BLAKE3. Protocol responses MUST use the protocol-required digest. Internal repair MAY use BLAKE3.

### 8.6 Upload State

Resumable uploads MUST be represented as internal upload state objects:

```text
uploads/{upload_id}/state.json
uploads/{upload_id}/parts/{part_number}
```

`state.json` MUST include:

```json
{
  "schema": "anvil.registry.upload.v1",
  "upload_id": "uuid",
  "registry_id": "tenant-123/docker_oci/worka",
  "protocol": "docker_oci",
  "repository": "acme/api",
  "started_at": "2026-07-01T00:00:00Z",
  "expires_at": "2026-07-01T01:00:00Z",
  "started_by": "app:123",
  "expected_digest": null,
  "received_bytes": 1048576,
  "part_count": 4,
  "state": "open"
}
```

Allowed upload states:

```text
open
committing
committed
aborted
expired
```

State transitions MUST be monotonic:

```text
open -> committing -> committed
open -> aborted
open -> expired
committing -> aborted only if no committed blob was published
```

After `committed`, the upload state is immutable. Cleanup MAY delete upload parts after the blob has been durably published.

### 8.7 Mutable Pointers

Registry tags, dist-tags, Maven metadata, Cargo sparse index entries, npm packuments, and Python simple indexes are mutable pointers. They MUST be updated through compare-and-swap.

A mutable pointer update MUST include:

1. expected pointer revision or explicit create-if-absent precondition;
2. actor principal;
3. authorisation revision;
4. new payload hash;
5. old payload hash when replacing existing content;
6. audit reason.

Concurrent updates MUST NOT silently overwrite each other. A failed CAS MUST return the protocol-appropriate conflict or precondition failure response.

### 8.8 Common Package Record JSON

Each protocol MUST maintain a package record for package-level browsing, search, authorisation diagnostics, and repair. Protocol-specific files remain canonical for client compatibility, but this record is the canonical Anvil registry catalog projection.

Package record key:

```text
{kind}/packages/{package_id_hash}/package.json
```

Package record schema:

```json
{
  "schema": "anvil.registry.package.v1",
  "registry_id": "tenant-123/npm/internal",
  "kind": "npm",
  "package_id": "@scope/name",
  "display_name": "@scope/name",
  "normalised_name": "@scope/name",
  "visibility": "private",
  "created_at": "2026-07-01T00:00:00Z",
  "created_by": "app:123",
  "updated_at": "2026-07-01T00:00:00Z",
  "latest_version": "1.2.3",
  "version_count": 1,
  "tags": {
    "latest": "1.2.3"
  },
  "search": {
    "summary": "Short human description",
    "keywords": ["storage", "registry"],
    "readme_excerpt": "Optional text extracted for search"
  },
  "metadata": {
    "protocol_specific": true
  }
}
```

Package records MUST be updated through CAS. Search indexes MUST be built from package records and protocol version records, not from raw tarball, jar, wheel, crate, or image layer bytes.

### 8.9 Common Version Record JSON

Each published immutable version MUST have a version record.

Version record key:

```text
{kind}/packages/{package_id_hash}/versions/{version_id_hash}.json
```

Version record schema:

```json
{
  "schema": "anvil.registry.version.v1",
  "registry_id": "tenant-123/cargo/internal",
  "kind": "cargo",
  "package_id": "example",
  "version": "1.2.3",
  "created_at": "2026-07-01T00:00:00Z",
  "created_by": "app:123",
  "state": "active",
  "artifacts": [
    {
      "role": "primary",
      "object_key": "_anvil/registry/v1/tenants/tenant-123/registries/cargo/internal/cargo/crates/example/1.2.3/example-1.2.3.crate",
      "content_type": "application/x-tar",
      "size": 12345,
      "digests": {
        "sha256": "hex",
        "blake3": "hex"
      }
    }
  ],
  "protocol": {
    "cargo_index_line_hash": "hex"
  }
}
```

Allowed version states:

```text
active
yanked
deprecated
hidden
deleted_retained
```

The implementation MUST NOT physically delete artifact bytes merely because a version state changes to `yanked`, `deprecated`, `hidden`, or `deleted_retained`.

### 8.10 Key Hashing

Package names and versions may contain characters that are valid for their ecosystem but awkward or ambiguous in Anvil internal keys. Internal keys SHOULD use a stable hash segment plus a human-readable sidecar field.

Required package id hash:

```text
package_id_hash = lowerhex(blake3(canonical_package_id_utf8))
```

Required version id hash:

```text
version_id_hash = lowerhex(blake3(canonical_package_id_utf8 || 0x00 || canonical_version_utf8))
```

Human-readable package ids MUST remain inside JSON records. Route handlers MUST NOT derive authority from the hash alone; they MUST load the record and verify canonical package id equality.

## 9. Registry Authorisation Model

### 9.1 Registry Namespace

Anvil MUST define registry authorisation namespaces. These are Anvil-owned namespaces. Tenants MAY receive delegated authority inside those namespaces, but tenants MUST NOT redefine their schema.

Required namespaces:

```text
registry
registry_repository
registry_package
registry_artifact
```

### 9.2 Registry Object Names

Registry authorisation object ids MUST be canonical and deterministic:

```text
registry:{tenant_id}:{kind}:{registry_name}
registry_repository:{tenant_id}:{kind}:{registry_name}:{repository_id}
registry_package:{tenant_id}:{kind}:{registry_name}:{package_id}
registry_artifact:{tenant_id}:{kind}:{registry_name}:{artifact_digest_or_path_hash}
```

The implementation MUST use typed constructors for these ids. It MUST NOT build ids through ad-hoc string concatenation in route handlers.

### 9.3 Registry Relations

Required registry relations:

```text
owner
admin
maintainer
publisher
puller
reader
writer
public_reader
```

Required computed permissions:

```text
can_read      = reader | puller | publisher | maintainer | admin | owner | public_reader
can_pull      = puller | publisher | maintainer | admin | owner | public_reader
can_push      = publisher | maintainer | admin | owner
can_delete    = maintainer | admin | owner
can_manage    = admin | owner
can_grant     = owner | admin
can_audit     = admin | owner
```

`public_reader` MUST only be granted by administrative policy or a delegated registry administrator. Public visibility flags MUST compile to `public_reader` relationship facts or equivalent derived policy. A public flag alone MUST NOT bypass relationship evaluation.

### 9.4 Per-Protocol Action Mapping

The gateway MUST map protocol operations to registry permissions:

| Operation | Permission |
| --- | --- |
| Docker blob GET/HEAD | `can_pull` on repository/package |
| Docker manifest GET/HEAD | `can_pull` on repository/package |
| Docker blob upload | `can_push` on repository/package |
| Docker manifest PUT | `can_push` on repository/package |
| Docker manifest DELETE | `can_delete` on repository/package |
| Maven GET/HEAD | `can_read` on package/repository |
| Maven PUT | `can_push` on package |
| Maven metadata update | `can_push` on package |
| Cargo crate download | `can_pull` on package |
| Cargo publish | `can_push` on package |
| Cargo yank/unyank | `can_delete` or protocol-specific `can_yank` if added |
| PyPI file download | `can_pull` on package |
| PyPI upload | `can_push` on package |
| npm packument/tarball GET | `can_pull` on package |
| npm publish | `can_push` on package |
| npm dist-tag mutation | `can_push` on package |
| npm unpublish | `can_delete` on package |

### 9.5 Anonymous Access

Anonymous access is represented by a typed public principal:

```text
principal: public:anonymous
```

If an HTTP request has no credentials, the gateway MAY evaluate it as `public:anonymous` for read-only operations. It MUST NOT evaluate anonymous requests for writes, deletes, admin, token minting, or metadata mutation.

Anonymous access MUST use the same relationship evaluator as credentialed access. It MUST NOT be implemented as a route-specific bypass.

### 9.6 Credential Adapters

Different tools use different authentication conventions. Anvil MUST normalize them into an authenticated principal before authorisation.

Supported credential inputs SHOULD include:

1. `Authorization: Bearer <token>`;
2. `Authorization: Basic <base64(client_id:client_secret)>` where protocol tools expect Basic;
3. protocol-specific token headers such as Cargo's registry token header;
4. Docker Distribution bearer token exchange flow.

Credential adapters MUST NOT grant authority. They only authenticate a principal. Authorisation is still performed through registry relations and Anvil admin capabilities.

### 9.7 Registry Token Service

The registry gateway MUST expose token exchange endpoints required by ecosystem clients. Token exchange endpoints run on the public listener but are not administrative APIs.

The token service MUST:

1. authenticate supplied credentials;
2. identify the requested registry scope;
3. mint a short-lived token containing only the approved registry actions;
4. include tenant id, principal id, registry id, allowed actions, and expiry;
5. reject unknown scopes;
6. reject scopes that exceed the caller's relationship authority;
7. avoid returning broad administrative tokens.

Docker/OCI token responses MUST be compatible with the Distribution bearer token flow:

```json
{
  "token": "jwt-or-opaque-token",
  "access_token": "jwt-or-opaque-token",
  "expires_in": 3600,
  "issued_at": "2026-07-01T00:00:00Z"
}
```

Cargo, npm, Maven, and Python MAY use the same token issuer but their HTTP authentication adapters MUST preserve client compatibility.

### 9.8 Scope Shape

Registry tokens SHOULD use typed scopes that can be checked without reparsing protocol-specific route paths.

Required logical scope shape:

```text
registry:{kind}:{registry_name}:{operation}:{package_or_repository}
```

Required operations:

```text
pull
push
delete
manage
audit
```

The token scope is an optimisation and transport format. It MUST NOT replace relationship checks when a route mutates long-lived state unless the token was minted from a relationship check at token issue time and remains within its short expiry window.

## 10. Administrative Plane

### 10.1 Admin Listener Requirements

The administrative listener MUST be a separate socket from the public listener.

The administrative router MUST contain only administrative services. The public router MUST NOT contain those services.

A conformance test MUST start Anvil with both listeners and assert:

1. every admin operation succeeds on the admin listener with valid admin credentials;
2. the same operation is not routed on the public listener;
3. no public route can reach an admin handler by alternate content-type, path, HTTP method, or gRPC service name;
4. all admin routes reject unauthenticated calls;
5. all admin routes reject authenticated principals that lack the required `anvil_admin` capability.

### 10.2 AnvilAdmin Namespace

Anvil MUST define an Anvil-owned administrative namespace named:

```text
anvil_admin
```

The root object is:

```text
anvil_admin:cluster:{cluster_id}
```

Required relations:

```text
owner
operator
security_admin
registry_admin
tenant_admin
bucket_admin
index_admin
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
run_repair           = owner | repair_admin | operator
view_diagnostics     = owner | operator | auditor
view_audit_log       = owner | auditor
```

These capabilities MUST be represented as typed Rust values at the API boundary. Route handlers MUST call a helper equivalent to:

```text
require_admin_capability(claims, AnvilAdminCapability::ManageRegistries)
```

Handlers MUST NOT pass raw relation names around as unvalidated strings.

### 10.3 Admin API Surface

The administrative API MUST include operations for:

1. creating tenants;
2. creating applications/client credentials;
3. rotating application secrets;
4. granting and revoking coarse token scopes;
5. defining registry instances;
6. deleting or disabling registry instances;
7. changing registry visibility;
8. granting and revoking registry relationships;
9. creating buckets;
10. changing bucket public-read policy;
11. creating, updating, disabling, and dropping index definitions;
12. managing authz namespace schemas;
13. managing caveat definitions;
14. running repair operations;
15. reading diagnostics and audit logs.

The existing local bootstrap CLI MAY remain, but routine remote administration MUST use the admin API.


### 10.3.1 Required Admin Service Shape

The admin API MAY be implemented as gRPC, HTTP+JSON, or both. If gRPC is used, the service MUST provide operations equivalent to:

```protobuf
service AdminService {
  rpc CreateTenant(CreateTenantRequest) returns (TenantResponse);
  rpc CreateApplication(CreateApplicationRequest) returns (ApplicationSecretResponse);
  rpc RotateApplicationSecret(RotateApplicationSecretRequest) returns (ApplicationSecretResponse);
  rpc GrantApplicationScope(GrantApplicationScopeRequest) returns (AdminMutationResponse);
  rpc RevokeApplicationScope(RevokeApplicationScopeRequest) returns (AdminMutationResponse);

  rpc CreateRegistry(CreateRegistryRequest) returns (RegistryResponse);
  rpc DisableRegistry(DisableRegistryRequest) returns (RegistryResponse);
  rpc SetRegistryVisibility(SetRegistryVisibilityRequest) returns (RegistryResponse);
  rpc GrantRegistryRelation(GrantRegistryRelationRequest) returns (AdminMutationResponse);
  rpc RevokeRegistryRelation(RevokeRegistryRelationRequest) returns (AdminMutationResponse);
  rpc ListRegistries(ListRegistriesRequest) returns (ListRegistriesResponse);

  rpc CreateBucketAdmin(CreateBucketAdminRequest) returns (BucketAdminResponse);
  rpc SetBucketPublicAccessAdmin(SetBucketPublicAccessAdminRequest) returns (BucketAdminResponse);

  rpc CreateIndexAdmin(CreateIndexAdminRequest) returns (IndexAdminResponse);
  rpc UpdateIndexAdmin(UpdateIndexAdminRequest) returns (IndexAdminResponse);
  rpc DisableIndexAdmin(DisableIndexAdminRequest) returns (IndexAdminResponse);
  rpc DropIndexAdmin(DropIndexAdminRequest) returns (AdminMutationResponse);

  rpc RegisterAuthzNamespace(RegisterAuthzNamespaceRequest) returns (AuthzNamespaceResponse);
  rpc RegisterCaveatDefinition(RegisterCaveatDefinitionRequest) returns (CaveatDefinitionResponse);

  rpc RunRepair(RunRepairRequest) returns (RepairTaskResponse);
  rpc ListDiagnostics(ListDiagnosticsRequest) returns (DiagnosticsResponse);
  rpc ListAuditEvents(ListAuditEventsRequest) returns (AuditEventsResponse);
}
```

Each request MUST carry or derive:

1. request id;
2. authenticated principal;
3. required `AnvilAdminCapability`;
4. idempotency key for mutating calls;
5. audit reason for mutating calls.

Admin mutating calls MUST be idempotent when an idempotency key is supplied.

### 10.3.2 Admin Capability Mapping

Admin endpoints MUST map to capabilities as follows:

| Endpoint family | Required capability |
| --- | --- |
| tenant create/list/update | `manage_tenants` |
| application create/rotate | `manage_apps` |
| application scope grant/revoke | `manage_apps` and possibly `manage_authz_tuples` |
| registry create/disable/configure | `manage_registries` |
| registry relationship grant/revoke | `manage_registry_auth` |
| bucket create/delete/public policy | `manage_buckets` |
| index create/update/drop | `manage_indexes` |
| authz namespace/caveat definition | `manage_authz_schema` |
| repair run | `run_repair` |
| diagnostics read | `view_diagnostics` |
| audit read | `view_audit_log` |

### 10.4 Bootstrap

A new Anvil cluster has no trusted administrator. Bootstrap MUST be explicit.

Permitted bootstrap mechanisms:

1. local CLI that writes to local Anvil storage while the node is stopped or before public exposure;
2. one-time bootstrap token mounted through a deployment secret and accepted only on the admin listener;
3. operator-supplied initial admin tuple file consumed once on first cluster initialisation.

Bootstrap MUST create:

1. cluster id;
2. at least one admin principal;
3. `anvil_admin:cluster:{cluster_id}#owner@principal` relationship;
4. initial admin audit record.

After bootstrap completes, the bootstrap credential MUST be invalidated or marked consumed.

## 11. Registry Gateway Core API

### 11.1 RegistryStore

The registry gateways MUST use a shared internal API rather than directly calling object-manager functions from protocol handlers.

Required interface shape:

```text
RegistryStore::get_blob(registry, digest) -> BlobStream
RegistryStore::put_blob(registry, digest, stream, metadata) -> BlobCommit
RegistryStore::start_upload(registry, protocol, subject) -> UploadState
RegistryStore::append_upload(upload_id, offset, bytes) -> UploadState
RegistryStore::complete_upload(upload_id, expected_digest) -> BlobCommit
RegistryStore::abort_upload(upload_id) -> UploadState
RegistryStore::get_json_pointer(key) -> JsonDocument
RegistryStore::cas_json_pointer(key, expected_revision, new_json) -> PointerCommit
RegistryStore::list_prefix(prefix, cursor, limit) -> Page
RegistryStore::write_audit_event(event) -> AuditCursor
```

The API MUST enforce:

1. canonical key construction;
2. reserved namespace internal-write authority;
3. content digest validation;
4. maximum object sizes;
5. upload expiry;
6. audit writes;
7. request id propagation;
8. registry-specific metadata attachment.

### 11.2 Error Model

Registry gateways MUST return protocol-native error formats while preserving Anvil request ids.

Every registry error response MUST include or expose:

1. Anvil request id;
2. protocol error code;
3. stable Anvil error code in a header or structured detail where compatible;
4. no raw internal object key unless the caller has diagnostic authority.

Recommended header:

```text
x-anvil-request-id: <request-id>
x-anvil-error-code: <stable-code>
```

### 11.3 Audit Events

Every write, delete, permission change, registry creation, registry disable, and admin operation MUST produce an audit event.

Audit events MUST include:

```json
{
  "schema": "anvil.registry.audit.v1",
  "event_id": "uuid",
  "request_id": "uuid",
  "registry_id": "tenant-123/npm/internal",
  "actor": "app:123",
  "operation": "npm.publish",
  "target": "@scope/package@1.2.3",
  "authz_revision": 42,
  "result": "success",
  "created_at": "2026-07-01T00:00:00Z"
}
```

Audit objects are internal. They MUST be exposed only through structured admin APIs.

## 12. Docker And OCI Registry Gateway

### 12.1 Protocol Scope

The Docker/OCI gateway MUST implement the Docker Distribution API v2 routes required for `docker login`, `docker push`, `docker pull`, `docker manifest`, and compatible OCI clients.

Required media types:

```text
application/vnd.docker.distribution.manifest.v2+json
application/vnd.docker.distribution.manifest.list.v2+json
application/vnd.docker.container.image.v1+json
application/vnd.docker.image.rootfs.diff.tar.gzip
application/vnd.oci.image.manifest.v1+json
application/vnd.oci.image.index.v1+json
application/vnd.oci.image.config.v1+json
application/vnd.oci.image.layer.v1.tar
application/vnd.oci.image.layer.v1.tar+gzip
application/vnd.oci.image.layer.v1.tar+zstd
```

Unknown media types MAY be stored if policy allows opaque OCI artifacts. They MUST still be digest-verified.

### 12.2 Routes

The gateway MUST implement:

```text
GET  /v2/
HEAD /v2/{name}/blobs/{digest}
GET  /v2/{name}/blobs/{digest}
POST /v2/{name}/blobs/uploads/
PATCH /v2/{name}/blobs/uploads/{uuid}
PUT  /v2/{name}/blobs/uploads/{uuid}?digest={digest}
DELETE /v2/{name}/blobs/uploads/{uuid}
HEAD /v2/{name}/manifests/{reference}
GET  /v2/{name}/manifests/{reference}
PUT  /v2/{name}/manifests/{reference}
DELETE /v2/{name}/manifests/{reference}
GET  /v2/{name}/tags/list
```

`/v2/_catalog` MAY be implemented only for authenticated callers with administrative or registry listing authority. It MUST NOT reveal private repositories to anonymous callers.

### 12.3 Repository Name Parsing

Docker repository names may contain multiple path segments. The gateway MUST parse `{name}` greedily until one of these route markers:

```text
blobs
manifests
tags
```

The repository name MUST be normalised and validated according to Docker/OCI name rules. Uppercase repository names MUST be rejected.

### 12.4 Blob Upload State Machine

```text
POST uploads -> open upload
PATCH upload -> append bytes at current offset
PUT upload?digest -> validate digest and publish blob
DELETE upload -> abort upload
```

`POST /blobs/uploads/` MUST return:

```text
202 Accepted
Location: {public_url}/v2/{name}/blobs/uploads/{uuid}
Docker-Upload-UUID: {uuid}
Range: 0-0
```

`PATCH` MUST verify the current upload offset. If the client supplies `Content-Range`, it MUST match the server-side offset. The response MUST include the updated `Range`.

`PUT` MUST compute the digest over the uploaded bytes. If the computed digest does not match the supplied digest, the gateway MUST return Docker error code `DIGEST_INVALID` and MUST NOT publish the blob.

Blob mount via `POST ...?mount={digest}&from={repository}` SHOULD be supported. It MUST check pull permission on the source repository and push permission on the destination repository.

### 12.5 Blob Reads

`GET` and `HEAD` for blobs MUST:

1. check `can_pull`;
2. resolve the digest object;
3. return `Docker-Content-Digest`;
4. return exact `Content-Length`;
5. support byte ranges for `GET`;
6. return `404` using Docker error JSON when the blob does not exist or is not visible.

The gateway MUST NOT leak whether a private blob exists to an unauthorised caller.

### 12.6 Manifest Writes

`PUT /manifests/{reference}` MUST:

1. check `can_push`;
2. preserve the exact request body bytes;
3. compute digest over exact bytes;
4. validate that referenced blobs/manifests exist in the registry unless policy explicitly permits deferred references;
5. store the manifest by digest;
6. update the tag pointer when `{reference}` is not a digest;
7. update repository metadata;
8. write an audit event;
9. return `201 Created` with `Docker-Content-Digest` and `Location`.

Tag updates MUST use CAS. If immutable tags are enabled, a tag that already points to a digest MUST NOT be changed.

### 12.7 Manifest Reads

`GET` and `HEAD /manifests/{reference}` MUST support content negotiation through the `Accept` header. If a stored manifest media type is not acceptable, the gateway SHOULD return the most protocol-compatible response supported by the stored object or `MANIFEST_UNKNOWN` when no acceptable representation exists.

### 12.8 Docker Error Format

Docker gateway errors MUST use:

```json
{
  "errors": [
    {
      "code": "NAME_UNKNOWN",
      "message": "repository name not known to registry",
      "detail": {}
    }
  ]
}
```

The gateway MUST map Anvil errors to Docker codes such as:

```text
UNAUTHORIZED
DENIED
NAME_INVALID
NAME_UNKNOWN
BLOB_UNKNOWN
BLOB_UPLOAD_INVALID
BLOB_UPLOAD_UNKNOWN
DIGEST_INVALID
MANIFEST_INVALID
MANIFEST_UNKNOWN
TAG_INVALID
TOOMANYREQUESTS
UNSUPPORTED
```

## 13. Maven Repository Gateway

### 13.1 Protocol Scope

The Maven gateway MUST support Maven and Gradle clients resolving and publishing artifacts through ordinary HTTP repository paths.

Required operations:

```text
GET
HEAD
PUT
```

`DELETE` MAY be supported only for authenticated administrative or maintainer operations and MUST be disabled by default for ordinary publishers.

### 13.2 Path Mapping

Maven paths map directly to registry objects:

```text
/{repository}/{group_path}/{artifact_id}/{version}/{filename}
```

`group_path` is `group_id` with `.` replaced by `/`.

Example:

```text
/releases/com/acme/api/1.2.3/api-1.2.3.jar
```

Internal key:

```text
maven/repositories/releases/com/acme/api/1.2.3/api-1.2.3.jar
```

The gateway MUST reject paths containing traversal, empty group segments, or invalid filename characters.

### 13.3 Checksums

The gateway MUST support checksum sidecar files:

```text
.md5
.sha1
.sha256
.sha512
```

When a checksum sidecar is PUT by the client, Anvil MUST validate it against the already stored artifact if the artifact exists. When an artifact is PUT first, Anvil SHOULD generate checksum sidecars if client tooling expects them.

The gateway MUST prefer SHA-256 or SHA-512 for internal integrity. MD5 and SHA-1 are compatibility outputs only.

### 13.4 Maven Metadata

`maven-metadata.xml` files are mutable pointers and MUST be updated through CAS.

The gateway MUST support:

1. package-level metadata;
2. version-level snapshot metadata;
3. timestamped snapshot builds;
4. Gradle module metadata files where present;
5. POM files;
6. artifact classifiers such as `sources`, `javadoc`, and native classifiers.

Snapshot publish MUST atomically update:

1. timestamped artifact files;
2. version-level `maven-metadata.xml`;
3. checksum sidecars.

If any required write fails, metadata MUST NOT point to missing artifact files.

### 13.5 Immutability Policy

Release versions SHOULD be immutable by default. Snapshot versions MAY be mutable. Registry policy MUST make this explicit.

An attempt to overwrite an immutable release artifact MUST return `409 Conflict`.

## 14. Cargo Sparse Registry Gateway

### 14.1 Protocol Scope

The Cargo gateway MUST implement the sparse registry protocol and publish API required by current Cargo clients.

Required routes:

```text
GET /config.json
GET /{crate_index_path}
PUT /api/v1/crates/new
DELETE /api/v1/crates/{crate}/{version}/yank
PUT /api/v1/crates/{crate}/{version}/unyank
GET /api/v1/crates/{crate}/{version}/download
```

### 14.2 Sparse Index Paths

Crate names MUST be normalised according to Cargo registry rules. Sparse index paths MUST use Cargo's index path algorithm:

```text
1 character: 1/{name}
2 characters: 2/{name}
3 characters: 3/{first_char}/{name}
4+ chars: {first_two}/{second_two}/{name}
```

Examples:

```text
a       -> 1/a
ab      -> 2/ab
abc     -> 3/a/abc
serde   -> se/rd/serde
```

Internal key:

```text
cargo/sparse-index/{crate_index_path}
```

### 14.3 `config.json`

`config.json` MUST include a download URL template and API URL compatible with Cargo:

```json
{
  "dl": "https://registry.example.com/cargo/api/v1/crates/{crate}/{version}/download",
  "api": "https://registry.example.com/cargo"
}
```

The values MUST be generated from `public_registry_base_url` and the configured registry route prefix.

### 14.4 Publish Body

`PUT /api/v1/crates/new` MUST accept Cargo's publish body format:

```text
u32 little-endian JSON metadata length
JSON metadata bytes
u32 little-endian crate tarball length
crate tarball bytes
```

The gateway MUST:

1. parse and validate metadata;
2. verify crate name and version;
3. compute the crate tarball checksum;
4. store the tarball under `cargo/crates/{name}/{version}/{name}-{version}.crate`;
5. append a JSON line to the sparse index file using CAS;
6. reject duplicate versions unless policy explicitly allows replacement;
7. return a Cargo-compatible success or error response.

### 14.5 Index Entry

Each sparse index line MUST contain the fields Cargo expects, including:

```json
{
  "name": "example",
  "vers": "1.2.3",
  "deps": [],
  "cksum": "sha256hex",
  "features": {},
  "yanked": false,
  "links": null
}
```

The gateway MUST preserve unknown fields needed by newer Cargo versions when they are supplied and safe.

### 14.6 Yank And Unyank

Yank/unyank operations MUST update only the matching version line through CAS. They MUST NOT remove crate tarballs. They MUST require delete/yank authority on the package.

## 15. Python Package Gateway

### 15.1 Protocol Scope

The Python gateway MUST support standard Python packaging clients.

Required routes:

```text
GET  /simple/
GET  /simple/{project}/
GET  /simple/{project}/ as PEP 691 JSON when requested
GET  /packages/{path}
POST /legacy/
GET  /pypi/{project}/json
```

### 15.2 Project Normalisation

Project names MUST be normalised according to Python packaging rules:

1. lowercase;
2. replace runs of `.`, `_`, and `-` with a single `-`.

The gateway MUST redirect non-normalised simple API project URLs to the canonical normalised URL where client compatibility requires it.

### 15.3 Uploads

`POST /legacy/` MUST accept multipart uploads used by `twine`.

The gateway MUST parse:

1. project name;
2. version;
3. file type;
4. Python tag;
5. ABI tag;
6. platform tag;
7. metadata fields;
8. uploaded file bytes;
9. hashes if supplied.

The gateway MUST validate file name consistency with metadata. It MUST compute SHA-256 for every file and include it in simple index links.

### 15.4 Simple API HTML

The HTML response for `/simple/{project}/` MUST contain anchor links to all visible files for the project. Links MUST include hash fragments:

```html
<a href="/packages/.../example-1.2.3-py3-none-any.whl#sha256=...">example-1.2.3-py3-none-any.whl</a>
```

If a file is yanked, the link SHOULD include `data-yanked`.

### 15.5 PEP 691 JSON

When the client requests the JSON simple API through `Accept`, the gateway SHOULD return PEP 691-compatible JSON. The MVP for Python MUST include this unless implementation complexity proves incompatible with the release schedule; if omitted, the gateway MUST still serve the HTML Simple API.

### 15.6 Immutability And Deletion

Published Python files are immutable by default. Delete and yanking operations MUST be administrative or maintainer-only. Deleting files MUST not corrupt existing project metadata; metadata updates MUST use CAS.

## 16. npm Registry Gateway

npm compatibility is broad and historically uneven across clients. This RFC defines npm implementation in phases. Each phase MUST keep all completed earlier-phase behaviours working.

### 16.1 npm Name Normalisation

The gateway MUST support:

1. unscoped package names, e.g. `left-pad`;
2. scoped package names, e.g. `@acme/ui`;
3. URL-encoded scoped package paths, e.g. `@acme%2fui` where clients use it.

Package names MUST be validated according to npm package name rules. The internal package id for a scoped package MUST be canonical:

```text
@scope/name
```

Internal object keys MUST encode scoped package names without ambiguous slashes, for example:

```text
npm/packages/@scope%2fname/...
```

### 16.2 npm MVP Phase

The MVP phase targets private package publish/install for generated applications, CI, and internal developer workflows.

The npm MVP MUST implement:

```text
GET  /-/ping
GET  /-/whoami
GET  /{package}
PUT  /{package}
GET  /{package}/-/{tarball}.tgz
GET  /@{scope}%2f{name}
PUT  /@{scope}%2f{name}
GET  /@{scope}/{name}/-/{tarball}.tgz
```

The npm MVP MUST support:

1. bearer token authentication;
2. Basic authentication where npm client configuration requires it;
3. scoped packages;
4. package publish with one version and tarball attachment;
5. packument reads;
6. tarball downloads;
7. latest dist-tag on publish;
8. immutable versions by default;
9. SHA-1 and SHA-512 integrity fields where supplied or computable;
10. anonymous install for packages explicitly marked public-read.

The npm MVP MAY omit:

1. audit API;
2. npm search API;
3. team/org management;
4. deprecate;
5. unpublish;
6. access level mutation;
7. provenance/OIDC;
8. token creation APIs;
9. web UI metadata not needed by npm CLI.

### 16.3 npm Packument

The npm packument is a mutable JSON pointer. It MUST be updated through CAS.

The stored packument MUST include at least:

```json
{
  "_id": "@scope/name",
  "name": "@scope/name",
  "dist-tags": {
    "latest": "1.2.3"
  },
  "versions": {
    "1.2.3": {
      "name": "@scope/name",
      "version": "1.2.3",
      "dist": {
        "tarball": "https://registry.example.com/@scope/name/-/name-1.2.3.tgz",
        "shasum": "sha1hex",
        "integrity": "sha512-..."
      }
    }
  },
  "time": {
    "created": "2026-07-01T00:00:00.000Z",
    "modified": "2026-07-01T00:00:00.000Z",
    "1.2.3": "2026-07-01T00:00:00.000Z"
  }
}
```

Unknown npm metadata fields SHOULD be preserved where safe.

### 16.4 npm Publish

`PUT /{package}` MUST parse npm publish JSON. The implementation MUST extract:

1. package name;
2. version metadata;
3. `_attachments` tarball content;
4. dist-tags;
5. integrity fields;
6. maintainers metadata if supplied.

The gateway MUST:

1. validate package name matches route;
2. decode the tarball attachment;
3. compute SHA-1 and SHA-512 digests;
4. store tarball bytes;
5. update packument through CAS;
6. reject overwriting existing versions unless policy explicitly allows it;
7. write an audit event.

### 16.5 npm Phase 2: Dist-Tags, Deprecation, Search

Phase 2 MUST add:

```text
GET    /-/package/{package}/dist-tags
PUT    /-/package/{package}/dist-tags/{tag}
DELETE /-/package/{package}/dist-tags/{tag}
POST   /-/v1/search
PUT    /{package}/{version}/-tag/deprecated or equivalent deprecate route used by npm clients
```

Phase 2 MUST support:

1. independent dist-tag mutation through CAS;
2. package deprecation messages;
3. full-text-backed search over package name, description, keywords, author, and maintainers;
4. metadata-filter-backed search facets where useful;
5. authorisation filtering before returning search results.

### 16.6 npm Phase 3: Unpublish, Access, Tokens

Phase 3 MUST add broader npm operational compatibility:

1. unpublish according to registry policy;
2. package access mutation for public/private packages;
3. token creation/list/revoke compatibility where safe;
4. owner/maintainer metadata mutation;
5. npm organisation/team compatibility mapped to Anvil registry relationships.

Unpublish MUST be policy-controlled. It MUST NOT delete tarball bytes needed for audit or retention unless retention policy permits it. Package metadata MAY hide unpublished versions while retaining internal audit data.

### 16.7 npm Phase 4: Advanced Compatibility

Phase 4 MAY add:

1. audit API;
2. provenance and OIDC publish flows;
3. web-session compatibility endpoints;
4. replication/change feed compatibility;
5. advanced npm search scoring;
6. package readme rendering metadata;
7. package advisory surfaces.

These features MUST NOT weaken the storage, CAS, reserved namespace, or authorisation requirements defined in earlier phases.

## 17. Search And Catalog Behaviour

Registry search MUST be built from existing Anvil indexes:

1. metadata-filter index for exact filters such as registry kind, package name, version, owner, visibility, created time, and protocol;
2. full-text index for package descriptions, README snippets, keywords, tags, group ids, artifact ids, and maintainers;
3. path index for repository browsing and prefix navigation.

No registry-specific index engine is required by this RFC.

Search results MUST be authorisation-filtered before being returned. The gateway MUST NOT query a broad internal catalog and filter results after serialising a response.

## 18. Concurrency And Consistency

Registry mutable state MUST use CAS. This includes:

1. Docker tags;
2. Docker repository metadata;
3. Maven `maven-metadata.xml`;
4. Cargo sparse index files;
5. Cargo yanked state;
6. PyPI simple project indexes;
7. npm packuments;
8. npm dist-tags;
9. registry descriptors;
10. registry policy projections.

A publish operation MUST be ordered so that metadata never points to missing bytes. The general rule is:

```text
write immutable bytes
verify bytes and digest
write or update immutable version record
CAS mutable pointer
write audit event
return success
```

If the final pointer update fails, the immutable bytes MAY remain as unreferenced blobs. Cleanup MAY garbage-collect unreferenced blobs after retention and audit rules allow it.

## 19. Garbage Collection And Retention

Registry garbage collection MUST distinguish:

1. referenced blobs;
2. unreferenced upload parts;
3. unreferenced committed blobs;
4. deleted but retained package versions;
5. audit-retained objects;
6. legally held objects.

Upload parts MAY be removed after `registry_upload_ttl_seconds` if the upload is open and expired.

Committed blobs MUST NOT be removed while any visible or retained metadata points to them. Registry delete operations MUST create tombstones or hidden metadata records before any bytes are eligible for deletion.

## 20. Migration From External Registries

Anvil SHOULD provide import tools for existing registries. Import MUST use protocol-aware validation rather than blind object copying.

Required import modes:

1. Docker/OCI pull and re-publish by digest and tag;
2. Maven repository directory import with metadata validation;
3. Cargo sparse index and crate tarball import;
4. PyPI simple repository import;
5. npm packument and tarball import.

Import MUST preserve:

1. package names;
2. versions;
3. tags or dist-tags;
4. checksums;
5. media types;
6. publish times where known;
7. visibility where known;
8. audit event marking import actor and source.

Import MUST NOT preserve external credentials or external ACLs without explicit mapping to Anvil registry relationships.

## 21. Observability

Anvil MUST emit metrics for registry gateways:

```text
registry_request_latency
registry_request_count
registry_request_error_count
registry_upload_open_count
registry_upload_bytes_received
registry_blob_bytes_stored
registry_blob_dedup_hit_count
registry_pointer_cas_conflict_count
registry_authz_denied_count
registry_anonymous_read_count
registry_publish_count
registry_download_count
registry_garbage_collection_count
registry_protocol_error_count
admin_request_latency
admin_request_denied_count
admin_public_port_route_rejection_count
```

Metrics MUST include labels for registry kind, operation, result, and tenant where cardinality remains safe. Package names and object keys SHOULD NOT be unbounded metric labels.

Logs MUST include request id and registry id. Logs MUST NOT include credentials, bearer tokens, Basic auth payloads, private package tarball bytes, or raw internal object paths unless diagnostic logging is explicitly enabled in a safe environment.

## 22. Test Requirements

### 22.1 Public/Admin Listener Tests

Tests MUST prove:

1. admin services are absent from public listener;
2. admin services are present on admin listener;
3. unauthenticated admin requests fail;
4. authenticated non-admin requests fail;
5. `anvil_admin` authorised requests succeed;
6. public anonymous reads still work when explicitly allowed;
7. public anonymous writes fail.

### 22.2 Docker E2E Tests

Tests MUST use real Docker or an OCI-compatible client to perform:

1. login/token flow;
2. image push;
3. image pull;
4. tag list;
5. manifest GET/HEAD;
6. blob GET/HEAD;
7. anonymous pull for public repository;
8. denied pull for private repository;
9. denied push without authority;
10. digest mismatch rejection.

### 22.3 Maven E2E Tests

Tests MUST use Maven or Gradle to perform:

1. deploy release artifact;
2. resolve release artifact;
3. deploy snapshot artifact;
4. resolve latest snapshot;
5. checksum validation;
6. denied deploy without publisher authority;
7. denied private resolve without read authority.

### 22.4 Cargo E2E Tests

Tests MUST use Cargo to perform:

1. registry configuration;
2. publish crate;
3. build project depending on published crate;
4. yank version;
5. verify yanked version behaviour;
6. unyank version;
7. denied publish without authority.

### 22.5 Python E2E Tests

Tests MUST use `twine` and `pip` to perform:

1. upload wheel;
2. upload sdist;
3. install from simple API;
4. verify hash links;
5. private package denial;
6. public package anonymous install.

### 22.6 npm E2E Tests

MVP npm tests MUST use npm CLI to perform:

1. login/token configuration;
2. publish unscoped package;
3. install unscoped package;
4. publish scoped package;
5. install scoped package;
6. verify `latest` dist-tag;
7. denied publish without authority;
8. anonymous install for public package;
9. denied anonymous install for private package.

Later npm phases MUST add tests for each added compatibility surface.

### 22.7 Storage Invariant Tests

Tests MUST prove:

1. public object APIs cannot read `_anvil/registry/*`;
2. public object APIs cannot write `_anvil/registry/*`;
3. registry gateways can write internal paths only through internal authority;
4. metadata pointers never reference missing blobs after successful publish;
5. CAS conflicts are surfaced correctly;
6. package search does not leak private packages;
7. deletion does not remove audit-retained bytes prematurely.

## 23. Implementation Order

The implementation SHOULD proceed in this order:

1. split public/admin listeners;
2. define `anvil_admin` typed capabilities;
3. move remote administration behind admin APIs while keeping local bootstrap;
4. implement `RegistryStore` and internal registry layout;
5. implement registry authorisation objects and helpers;
6. implement Docker/OCI gateway;
7. implement Maven gateway;
8. implement Cargo sparse gateway;
9. implement PyPI gateway;
10. implement npm MVP;
11. implement registry search/catalog using existing indexes;
12. add import tooling;
13. add npm Phase 2;
14. add npm Phase 3;
15. add npm Phase 4 where required.

Docker is first because it exercises large blobs, resumable uploads, digest validation, mutable tags, anonymous reads, and token challenge flows. Cargo and Maven are structurally simpler and should follow after the shared registry store is proven. npm should be phased because full compatibility is broad and not necessary for the first useful replacement of external artifact storage.

## 24. Acceptance Criteria

This RFC is implemented when all of these are true:

1. Anvil starts separate public and admin listeners;
2. admin handlers are not registered on the public listener;
3. admin handlers require `anvil_admin` capabilities;
4. registry data is stored under `_anvil/registry/v1/` in Anvil-owned storage;
5. public object APIs cannot access registry internal paths;
6. Docker/OCI clients can push and pull images without an external registry;
7. Maven/Gradle clients can publish and resolve artifacts without an external repository;
8. Cargo clients can publish and consume crates through sparse registry support;
9. Python clients can upload and install packages through PyPI-compatible endpoints;
10. npm MVP clients can publish and install scoped and unscoped packages;
11. every registry write produces an audit event;
12. every registry read/write is authorisation-checked;
13. anonymous reads work only for explicitly public packages or repositories;
14. mutable protocol metadata is updated through CAS;
15. protocol E2E tests pass with unmodified ecosystem clients;
16. registry search uses existing Anvil metadata/full-text/path indexes;
17. no additional registry-specific Anvil index engine is required;
18. external artifact registry infrastructure is no longer required for Anvil-supported artifacts.
