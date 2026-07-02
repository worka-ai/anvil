# RFC ANVIL-0004: Authz Realms And Schema Persistence

## Status

Draft.

## Date

2026-07-02.

## Normative Language

The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, `MAY`, and `OPTIONAL` in this document are normative. They are to be interpreted as described in RFC 2119.

## 1. Abstract

Anvil will provide a durable, multi-realm Zanzibar/ReBAC substrate. An Anvil storage tenant is Anvil's storage and authorisation container. It is not the same thing as an application tenant, customer account, organisation, user, or autonomous system. A single Anvil storage tenant can contain many independent authorisation realms.

This RFC defines the terminology, API, persistence, and release plan required for the Zanzibar crate and Anvil backend to stop overloading `tenant_id`, drop `apply_schema`, persist versioned schemas, bind schemas to authz realms, and enforce realm isolation in tuple reads, tuple writes, permission checks, list APIs, watches, and returned decisions.

The first deliverable is a Zanzibar crate release and an Anvil release that supports these semantics. Mesh routing, node lifecycle, region lifecycle, object links, and registry gateways remain separate RFCs and MUST NOT block this work.

## 2. Goals

An implementation conforming to this RFC MUST:

1. distinguish Anvil storage tenancy from application/customer/product tenancy;
2. introduce `AuthzScope` as the mandatory isolation input for every ReBAC operation;
3. define `AuthzScope` as `AnvilStorageTenantId + AuthzRealmId`;
4. remove `apply_schema` from the public Zanzibar `RebacEngine` trait;
5. replace `apply_schema` with explicit `put_schema`, `get_schema`, and `bind_schema` semantics;
6. persist schema definitions durably in Anvil and Postgres backends;
7. version schemas immutably after they are put;
8. bind each authz realm to an explicit schema revision;
9. persist tuple data under an authz scope;
10. enforce authz-scope isolation in tuple writes, tuple reads, checks, batch checks, list-objects, list-subjects, and watches;
11. return schema and authz revision metadata with check/list responses where the API returns structured decisions;
12. add multi-realm isolation tests for Postgres and Anvil backends;
13. verify the Zanzibar crate against Postgres and Anvil backends before release;
14. release the updated Zanzibar crate to crates.io after tests pass;
15. release Anvil server Docker image and Anvil Rust client crate with the required backend support.

## 3. Non-Goals

This RFC does not require:

1. Anvil mesh routing;
2. globally unique buckets;
3. node or region draining;
4. registry gateway support;
5. Worka-specific type names inside Anvil or Zanzibar;
6. storing application objects in the Zanzibar crate;
7. removing the Postgres backend;
8. supporting old `tenant_id: i64` APIs indefinitely;
9. changing the relation algebra itself beyond scope and schema persistence;
10. a new external database or coordination system.

## 4. Terminology

### 4.1 Anvil Storage Tenant

An Anvil storage tenant is Anvil's top-level storage and authorisation container. It is identified by `AnvilStorageTenantId`.

A single Anvil storage tenant MAY contain many authz realms. For example, a SaaS platform MAY use one Anvil storage tenant for its production deployment and store millions or billions of customer/account/application realms inside it.

### 4.2 Authz Realm

An authz realm is the ReBAC isolation boundary inside an Anvil storage tenant. It is identified by `AuthzRealmId`.

A realm contains:

1. a schema binding;
2. tuple data;
3. tuple indexes;
4. authz revisions;
5. watch streams;
6. derived userset indexes.

A tuple, schema binding, check, list operation, or watch event MUST belong to exactly one authz realm.

### 4.3 Authz Scope

`AuthzScope` is the durable isolation key for ReBAC operations.

```rust
pub struct AuthzScope {
    pub anvil_storage_tenant_id: AnvilStorageTenantId,
    pub authz_realm_id: AuthzRealmId,
}
```

Every public Zanzibar `RebacEngine` operation MUST require an `AuthzScope`. Backend code MUST NOT infer authz realm isolation from a client object, token, in-memory cache key, or caller convention.

### 4.4 Actor

An actor is a subject that can participate in relationships. Actors include users, organisations, services, autonomous systems, agents, applications, devices, and external identities.

`Actor` is subject terminology. It is not the authz isolation boundary.

### 4.5 Schema

A schema defines valid namespaces, relations, and rewrite rules. A schema is durable and versioned. Once persisted, a schema revision is immutable.

### 4.6 Schema Binding

A schema binding connects an `AuthzScope` to one schema revision. It determines which schema is active for tuple validation and check/list evaluation in that realm.

## 5. Required Zanzibar API

### 5.1 Public Types

The Zanzibar crate MUST expose these core types or exact semantic equivalents:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AnvilStorageTenantId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthzRealmId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AuthzScope {
    pub anvil_storage_tenant_id: AnvilStorageTenantId,
    pub authz_realm_id: AuthzRealmId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaId(pub String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SchemaRevision(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BindingGeneration(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaRef {
    pub schema_id: SchemaId,
    pub schema_revision: SchemaRevision,
    pub schema_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaBinding {
    pub scope: AuthzScope,
    pub schema_ref: SchemaRef,
    pub binding_generation: BindingGeneration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzDecisionMetadata {
    pub scope: AuthzScope,
    pub schema_ref: SchemaRef,
    pub authz_revision: u64,
    pub zookie: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckDecision {
    pub allowed: bool,
    pub metadata: AuthzDecisionMetadata,
}
```

`AnvilStorageTenantId` is named for Anvil semantics because the Zanzibar crate already includes an Anvil backend. A non-Anvil backend MAY map it to an equivalent storage partition.

### 5.2 RebacEngine Trait

The public `RebacEngine` trait MUST drop `apply_schema`. It MUST use `AuthzScope` for all operations.

Required trait shape:

```rust
#[async_trait]
pub trait RebacEngine: Send + Sync {
    async fn put_schema(
        &self,
        storage_tenant: &AnvilStorageTenantId,
        schema_id: SchemaId,
        schema: Schema,
    ) -> Result<SchemaRef, RebacError>;

    async fn get_schema(
        &self,
        storage_tenant: &AnvilStorageTenantId,
        schema_id: &SchemaId,
        revision: Option<SchemaRevision>,
    ) -> Result<(SchemaRef, Schema), RebacError>;

    async fn bind_schema(
        &self,
        scope: &AuthzScope,
        schema_ref: SchemaRef,
        expected_generation: Option<BindingGeneration>,
    ) -> Result<SchemaBinding, RebacError>;

    async fn get_schema_binding(
        &self,
        scope: &AuthzScope,
    ) -> Result<SchemaBinding, RebacError>;

    async fn write_tuples(
        &self,
        scope: &AuthzScope,
        updates: Vec<TupleUpdate>,
    ) -> Result<AuthzWriteResult, RebacError>;

    async fn read_tuples(
        &self,
        scope: &AuthzScope,
        object: Option<Object>,
        relation: Option<String>,
        subject: Option<Subject>,
    ) -> Result<Vec<Tuple>, RebacError>;

    async fn check(
        &self,
        scope: &AuthzScope,
        subject: &Subject,
        relation: &str,
        object: &Object,
    ) -> Result<CheckDecision, RebacError>;

    async fn check_many(
        &self,
        scope: &AuthzScope,
        requests: Vec<CheckRequest>,
    ) -> Result<Vec<CheckDecision>, RebacError>;

    async fn list_objects(
        &self,
        scope: &AuthzScope,
        subject: &Subject,
        relation: &str,
        object_namespace: &str,
    ) -> Result<ListObjectsResult, RebacError>;

    async fn list_subjects(
        &self,
        scope: &AuthzScope,
        object: &Object,
        relation: &str,
        subject_namespace: &str,
    ) -> Result<ListSubjectsResult, RebacError>;
}
```

`AuthzWriteResult`, `ListObjectsResult`, and `ListSubjectsResult` MUST include `AuthzDecisionMetadata` or equivalent revision metadata.

### 5.3 Removed API

This API MUST be removed from the public trait:

```rust
async fn apply_schema(&self, tenant_id: i64, schema: Schema) -> Result<(), RebacError>;
```

The crate MAY provide a migration helper outside the trait:

```rust
pub async fn put_and_bind_schema(
    engine: &dyn RebacEngine,
    scope: &AuthzScope,
    schema_id: SchemaId,
    schema: Schema,
    expected_generation: Option<BindingGeneration>,
) -> Result<SchemaBinding, RebacError>;
```

This helper MUST call `put_schema` and `bind_schema`. It MUST NOT reintroduce mutable unversioned schema application semantics.

## 6. Schema API Semantics

### 6.1 `put_schema`

`put_schema` persists a schema definition as an immutable schema revision inside an Anvil storage tenant or backend storage partition.

Inputs:

1. storage tenant id;
2. schema id;
3. schema definition.

Required behaviour:

1. validate the schema definition;
2. canonicalise the schema;
3. compute a stable schema digest;
4. if an identical schema digest already exists for the same schema id and storage tenant, return the existing `SchemaRef`;
5. otherwise allocate the next schema revision for that schema id;
6. persist the schema revision durably;
7. return `SchemaRef`.

`put_schema` MUST NOT change any realm's active schema binding.

### 6.2 `get_schema`

`get_schema` retrieves a durable schema revision.

If `revision` is `Some`, it MUST return exactly that revision or `SchemaRevisionNotFound`.

If `revision` is `None`, it MUST return the latest revision for the schema id in that storage tenant.

`get_schema` MUST NOT use the authz realm binding unless a separate helper explicitly asks for the bound schema.

### 6.3 `bind_schema`

`bind_schema` changes the active schema revision for an authz realm.

Inputs:

1. authz scope;
2. schema ref;
3. optional expected binding generation.

Required behaviour:

1. verify the schema ref exists in `scope.anvil_storage_tenant_id`;
2. verify the caller is authorised to bind schemas for the authz realm;
3. if no binding exists, require `expected_generation = None` or `Some(0)`;
4. if a binding exists, require `expected_generation` to match current binding generation;
5. validate existing tuples against the target schema before switching;
6. write the new binding with generation incremented by one;
7. emit a schema-binding watch event;
8. return the new `SchemaBinding`.

If existing tuples do not validate against the target schema, `bind_schema` MUST fail with `SchemaBindingRejected` and MUST NOT change the active binding. A future API MAY support explicit force binding with quarantine semantics; that is out of scope for this RFC.

`bind_schema` MUST exclude concurrent tuple writes for the same authz scope while validation and binding update are in progress. The required mechanism is a realm schema gate.

A realm schema gate has this logical state:

```text
open(binding_generation=N)
binding(binding_generation=N, fence=F)
```

Tuple writes MUST enter the gate in shared mode before validation. They MUST validate against the binding generation they observed and commit only if the gate is still `open` with the same binding generation.

`bind_schema` MUST enter the gate in exclusive mode by changing the gate from `open(N)` to `binding(N, F)` using compare-and-swap. While the gate is `binding`, new tuple writes MUST fail with `SchemaBindingInProgress` or retry according to client policy. After existing tuples validate and the binding is updated, `bind_schema` MUST publish `open(N+1)`. If validation fails, it MUST publish `open(N)` without changing the binding.

Postgres MUST implement the gate with a transaction that locks the `zanzibar_realm_schema_binding` row using `FOR UPDATE` for binding changes and locks the same row during tuple-write validation/commit. Anvil MUST implement the gate with a realm-scoped fenced control record.

### 6.4 `get_schema_binding`

`get_schema_binding` returns the active schema binding for an authz scope.

If no schema is bound, operations that require a schema MUST fail with `SchemaBindingNotFound`.

### 6.5 Schema Cache Semantics

Backends MAY cache schemas. Caches are derived state only.

Cache key MUST include:

```text
(anvil_storage_tenant_id, authz_realm_id, schema_id, schema_revision, schema_digest)
```

A cached schema MUST NOT be used for a realm unless the current binding still references the same `SchemaRef`.

Anvil-backed caches MUST be invalidated by schema and schema-binding watch events.

## 7. Tuple And Decision Semantics

### 7.1 Tuple Storage Scope

Every tuple MUST be stored under exactly one authz scope.

Tuple uniqueness key:

```text
(anvil_storage_tenant_id, authz_realm_id, object_namespace, object_id, relation, subject_namespace, subject_id, subject_relation)
```

Tuple reads, tuple writes, watches, and derived indexes MUST include both storage tenant and authz realm in their durable keys.

### 7.2 Tuple Validation

Tuple writes MUST validate:

1. active schema binding exists;
2. object namespace exists in active schema;
3. relation exists for the object namespace;
4. subject namespace exists where applicable;
5. userset subject relation exists for the userset namespace;
6. caveat name and argument shape are valid when caveats are supplied.

A tuple write MUST fail before persistence if validation fails.

### 7.3 Check Evaluation

A check MUST:

1. resolve active schema binding for the scope;
2. read tuples only from the same scope;
3. evaluate rewrite rules from the bound schema revision;
4. return decision metadata with scope, schema ref, authz revision, and zookie.

A check MUST NOT use tuples from another authz realm even when object ids, subject ids, namespaces, and relations are identical.

### 7.4 List APIs

`list_objects` and `list_subjects` MUST use the same schema and rewrite semantics as `check`. They MUST NOT be implemented as simple tuple scans that ignore computed usersets or tuple-to-userset rules.

Pagination tokens MUST bind:

1. authz scope;
2. subject/object filter;
3. relation;
4. namespace;
5. schema ref;
6. authz revision;
7. caller identity or authorisation context where the backend supports caller-bound cursors.

### 7.5 Watch APIs

Authz watch APIs MUST be scoped by `AuthzScope`. A watcher for realm A MUST NOT observe tuple or schema-binding events for realm B.

Watch events MUST include:

1. authz scope;
2. authz revision;
3. schema ref or schema-binding generation where relevant;
4. event type;
5. tuple or schema-binding payload.

## 8. Anvil Backend Requirements

### 8.1 Storage Layout

Anvil MUST store authz records under reserved internal paths. Public object APIs MUST NOT read, list, write, copy, patch, or delete these paths.

Required conceptual layout:

```text
_anvil/authz/v1/storage-tenants/{anvil_storage_tenant_id}/
  schemas/{schema_id}/revisions/{schema_revision}.json
  schemas/{schema_id}/latest.json
  realms/{authz_realm_id}/schema-binding.json
  realms/{authz_realm_id}/tuples/{partition}.anlog
  realms/{authz_realm_id}/tuple-indexes/by-object/{partition}.anidx
  realms/{authz_realm_id}/tuple-indexes/by-subject/{partition}.anidx
  realms/{authz_realm_id}/watches/tuples.anwatch
  realms/{authz_realm_id}/watches/schema-binding.anwatch
```

The exact low-level file format MAY reuse existing Anvil authz journals and segments, but the durable key space MUST include both storage tenant and authz realm.

### 8.2 Native API Changes

Anvil's native auth service MUST replace unscoped authz schema APIs with scoped/versioned APIs.

Required service shape:

```protobuf
message AuthzScope {
  string anvil_storage_tenant_id = 1;
  string authz_realm_id = 2;
}

message SchemaRef {
  string schema_id = 1;
  uint64 schema_revision = 2;
  string schema_digest = 3;
}

message PutAuthzSchemaRequest {
  string anvil_storage_tenant_id = 1;
  string schema_id = 2;
  repeated AuthzNamespaceSchema namespaces = 3;
  string reason = 4;
}

message PutAuthzSchemaResponse {
  SchemaRef schema_ref = 1;
  uint64 authz_revision = 2;
  string zookie = 3;
}

message GetAuthzSchemaRequest {
  string anvil_storage_tenant_id = 1;
  string schema_id = 2;
  optional uint64 schema_revision = 3; // absent means latest
}

message GetAuthzSchemaResponse {
  SchemaRef schema_ref = 1;
  repeated AuthzNamespaceSchema namespaces = 2;
}

message BindAuthzSchemaRequest {
  AuthzScope scope = 1;
  SchemaRef schema_ref = 2;
  optional uint64 expected_binding_generation = 3; // absent means no current binding is required
  string reason = 4;
}

message BindAuthzSchemaResponse {
  AuthzScope scope = 1;
  SchemaRef schema_ref = 2;
  uint64 binding_generation = 3;
  uint64 authz_revision = 4;
  string zookie = 5;
}

message GetAuthzSchemaBindingRequest {
  AuthzScope scope = 1;
}

message GetAuthzSchemaBindingResponse {
  AuthzScope scope = 1;
  SchemaRef schema_ref = 2;
  uint64 binding_generation = 3;
}
```

Tuple/check/list requests MUST carry `AuthzScope`.

`ApplyAuthzSchema` MUST be removed from the Anvil native API for the release implementing this RFC. A short-lived compatibility endpoint MAY exist behind a feature flag only if it is not exposed in the released default server.

### 8.3 Anvil Client Changes

The Rust Anvil client crate MUST expose typed methods for:

1. `put_authz_schema`;
2. `get_authz_schema`;
3. `bind_authz_schema`;
4. `get_authz_schema_binding`;
5. scoped tuple writes;
6. scoped tuple reads;
7. scoped checks;
8. scoped batch checks;
9. scoped list objects;
10. scoped list subjects;
11. scoped watches.

Client APIs MUST require `AuthzScope` where the server requires it. They MUST NOT hide the scope in process-global client state.

### 8.4 Anvil Authorisation

Anvil MUST authorise every authz operation. Minimum action split:

```text
authz:schema_put
authz:schema_read
authz:schema_bind
authz:tuple_write
authz:tuple_read
authz:check
authz:list
authz:watch
```

The resource string or internal permission object MUST include storage tenant and authz realm for realm-scoped operations.

## 9. Postgres Backend Requirements

### 9.1 Schema Tables

The Postgres backend MUST adopt the same semantics as Anvil.

Required conceptual tables:

```sql
CREATE TABLE zanzibar_schema (
    storage_tenant_id TEXT NOT NULL,
    schema_id TEXT NOT NULL,
    schema_revision BIGINT NOT NULL,
    schema_digest TEXT NOT NULL,
    schema_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (storage_tenant_id, schema_id, schema_revision),
    UNIQUE (storage_tenant_id, schema_id, schema_digest)
);

CREATE TABLE zanzibar_schema_relation_config (
    storage_tenant_id TEXT NOT NULL,
    schema_id TEXT NOT NULL,
    schema_revision BIGINT NOT NULL,
    namespace TEXT NOT NULL,
    relation TEXT NOT NULL,
    rule_index INTEGER NOT NULL,
    inherited_relation TEXT,
    inherited_from_target_relation TEXT,
    PRIMARY KEY (
      storage_tenant_id, schema_id, schema_revision, namespace, relation, rule_index
    ),
    UNIQUE (
      storage_tenant_id, schema_id, schema_revision, namespace, relation,
      COALESCE(inherited_relation, ''), COALESCE(inherited_from_target_relation, '')
    )
);

CREATE TABLE zanzibar_realm_schema_binding (
    storage_tenant_id TEXT NOT NULL,
    authz_realm_id TEXT NOT NULL,
    schema_id TEXT NOT NULL,
    schema_revision BIGINT NOT NULL,
    schema_digest TEXT NOT NULL,
    binding_generation BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (storage_tenant_id, authz_realm_id)
);
```

### 9.2 Tuple Tables

The tuple table MUST include authz scope:

```sql
CREATE TABLE zanzibar_tuple (
    storage_tenant_id TEXT NOT NULL,
    authz_realm_id TEXT NOT NULL,
    object_namespace TEXT NOT NULL,
    object_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    subject_namespace TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    subject_relation TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

Unique and lookup indexes MUST include `storage_tenant_id` and `authz_realm_id` as leading columns.

### 9.3 Postgres Migration Semantics

The Postgres backend MAY keep the old `tenant_id BIGINT` tables only for migration tests or backwards migrations, but the implementation of the new public API MUST use the new scope-aware tables.

A migration helper MAY map old `tenant_id` values into:

```text
storage_tenant_id = "legacy-postgres"
authz_realm_id = "tenant-{tenant_id}"
```

That helper MUST NOT be part of the new `RebacEngine` trait.

## 10. Zanzibar Crate Implementation Requirements

### 10.1 Breaking Change

This RFC is a breaking Zanzibar crate change.

Required breaking changes:

1. remove `apply_schema` from `RebacEngine`;
2. replace `tenant_id: i64` parameters with `&AuthzScope` or `&AnvilStorageTenantId` as appropriate;
3. update `check` to return `CheckDecision` rather than bare `bool`;
4. update list APIs to return structured results with decision metadata;
5. update Postgres backend schema and SQL;
6. update Anvil backend to call scoped native Anvil APIs;
7. update all tests, examples, and README snippets.

### 10.2 Validation

The crate MUST validate schemas before persisting them. Validation MUST reject:

1. empty namespace names;
2. empty relation names;
3. duplicate relation rules;
4. rules that reference unknown relations in the same namespace where the relation must exist;
5. unsupported relation rule kinds;
6. invalid tuple-to-userset shapes.

Tuple writes MUST validate against the active bound schema.

### 10.3 Caching

The crate MAY keep an in-memory schema cache. The cache MUST be keyed by the full schema ref and scope. The cache MUST be safe to discard at any time.

The Anvil backend MUST be correct after process restart with an empty cache.

## 11. Test Requirements

### 11.1 Postgres Tests

Postgres tests MUST prove:

1. `put_schema` persists an immutable schema revision;
2. identical schema re-put returns the same schema ref;
3. modified schema creates a new revision;
4. `get_schema` retrieves exact revisions;
5. `bind_schema` changes a realm binding with generation checks;
6. stale binding generation is rejected;
7. tuple writes fail when no schema is bound;
8. tuple writes fail for unknown namespace or relation;
9. realm A and realm B can use the same object ids without collision;
10. realm A cannot read realm B tuples;
11. realm A checks do not use realm B tuples;
12. realm A list APIs do not return realm B data;
13. two authz realms inside one storage tenant stay isolated;
14. two storage tenants with the same realm id stay isolated;
15. list APIs honour computed usersets and tuple-to-userset rules;
16. all previous Postgres Zanzibar tests pass after migration to the new API.

### 11.2 Anvil Backend Tests

Anvil backend tests MUST prove the same cases as Postgres using the Anvil server/client API.

Additional Anvil tests MUST prove:

1. schema survives server restart;
2. tuple data survives server restart;
3. empty Zanzibar process cache does not change decisions;
4. Anvil returns scope, schema ref, authz revision, and zookie in decisions;
5. watch streams are scope-isolated;
6. Anvil authorisation prevents a token for one authz realm from mutating another realm;
7. native Anvil API no longer exposes default `ApplyAuthzSchema` in release builds.

### 11.3 Compatibility Tests

There MUST be no tests or examples that call `apply_schema` on the public `RebacEngine` trait.

The README and crate docs MUST show `put_schema` + `bind_schema`.

## 12. Release Order

The implementation and release order MUST be:

1. update this RFC and pass independent review for substantive architecture gaps;
2. implement Zanzibar crate API changes and Postgres backend;
3. run all Zanzibar Postgres tests;
4. implement Anvil native API and persistence changes;
5. update Anvil Rust client;
6. update Zanzibar Anvil backend;
7. run Zanzibar tests against Anvil;
8. run Anvil authz tests and Docker smoke tests;
9. release Anvil server Docker image;
10. release Anvil Rust client crate to crates.io;
11. release Zanzibar crate to crates.io;
12. update downstream projects to use `AuthzScope`.

The Anvil release MUST happen before the Zanzibar crate release if the Zanzibar crate depends on newly published Anvil client APIs.

## 13. Acceptance Criteria

This RFC is implemented when all of these are true:

1. `RebacEngine` no longer contains `apply_schema`;
2. every ReBAC operation uses `AuthzScope` or storage-tenant-only schema input;
3. Postgres backend implements `put_schema`, `get_schema`, `bind_schema`, and `get_schema_binding`;
4. Anvil backend implements the same semantics against durable Anvil storage;
5. Anvil persists schema revisions and realm bindings;
6. Anvil persists tuples under authz scope;
7. Anvil checks and list APIs enforce scope isolation;
8. schema cache is only derived state;
9. Postgres tests pass;
10. Anvil backend tests pass;
11. multi-realm isolation tests pass;
12. Anvil Docker image is released;
13. Anvil Rust client crate is released;
14. Zanzibar crate is released to crates.io.
