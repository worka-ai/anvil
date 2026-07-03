# RFC ANVIL-0005: Log Streams, Typed Indexes, And Fenced Mutations

## Status

Draft.

## Date

2026-07-03.

## Normative Language

The key words `MUST`, `MUST NOT`, `REQUIRED`, `SHOULD`, `SHOULD NOT`, `MAY`, and `OPTIONAL` in this document are normative. They are to be interpreted as described in RFC 2119.

## 1. Abstract

Anvil already stores objects, maintains indexes, exposes watch streams, supports task leases, and has an append-stream capability for ordered log records. This RFC upgrades those pieces into one coherent substrate for durable work queues, audit/event logs, operational ledgers, workflow state machines, artifact publication, ingestion pipelines, and user-facing activity timelines.

The central model is:

1. canonical state lives in ordinary Anvil objects;
2. immutable history lives in append streams;
3. due work and listing views are discovered through typed secondary indexes over canonical object JSON and append-record JSON;
4. correctness-sensitive updates are applied through atomic mutation batches guarded by object-version and lease-fence preconditions;
5. index lag is visible and controllable through watch-cursor catch-up requirements;
6. object metadata is exposed through the native gRPC API as well as S3-compatible paths.

The design is intentionally generic. It MUST NOT introduce mail-specific, workflow-specific, build-specific, or application-specific queue APIs. Higher-level systems build their own domain semantics on top of these primitives.

## 2. Goals

An implementation conforming to this RFC MUST:

1. keep canonical JSON objects as the source of truth for queue items, workflow items, documents, jobs, assets, and other application records;
2. support typed secondary indexes over canonical object JSON body fields;
3. support typed secondary indexes over append-stream record JSON payload fields;
4. support equality, set-membership, range, existence, ordering, limit, and stable pagination in typed index queries;
5. expose explicit index lag controls, including `require_caught_up_to_watch_cursor`;
6. expose user metadata and content type on native gRPC object writes and reads;
7. preserve object user metadata and content type in object metadata journals, head responses, list responses, and index hits;
8. keep append streams open across segment sealing;
9. add append-stream read and tail APIs;
10. assign monotonically increasing per-stream record sequences;
11. make append records immutable after success;
12. support idempotent appends using native mutation context;
13. support lease-fenced write preconditions on object mutations and append mutations;
14. support object-version preconditions on object mutations;
15. support atomic mutation batches containing object writes, deletes, JSON patches, append records, lease checkpoints, and lease commits;
16. derive lease ownership from the authenticated caller, never from a request field supplied by the caller;
17. reject stale lease fences before applying any protected mutation;
18. return structured error codes and request identifiers for every new public API path;
19. provide tests that prove stale workers cannot mutate state after lease loss;
20. provide tests that prove typed index pagination cannot silently skip or duplicate after incompatible query or index changes.

## 3. Non-Goals

This RFC does not require:

1. a domain-specific queue API;
2. globally exactly-once external side effects;
3. an external relational database;
4. an external queue or lock service;
5. a requirement that every workload use append streams;
6. synchronous global ordering across independent append streams;
7. synchronous index maintenance before every write returns;
8. cross-bucket mutation batches;
9. cross-tenant mutation batches;
10. permanent stream closure semantics as part of segment sealing.

## 4. Representative Scenarios

### 4.1 Durable Delivery Queue

A service accepts work items, stores each item as a canonical JSON object, indexes due items by queue name, state, available time, priority, and item id, and lets workers claim items using leases.

The system needs:

1. due-item discovery without maintaining a separate projection object;
2. state transitions guarded by “I still own this claim”;
3. immutable attempt history for audit and diagnostics;
4. recovery when a worker crashes after an external system may have accepted a request.

Anvil provides object JSON indexes for discovery, append streams for attempts, and lease-fenced mutation batches for safe state transitions. The external side effect remains outside Anvil and MUST NOT be described as globally exactly once.

### 4.2 Build And Release Publication

A build system stores build records as canonical JSON objects. Each build has status, source revision, artifact path, created time, and environment metadata. Indexes provide queries such as “latest successful build for project X”, “failed builds after time T”, and “pending builds ordered by priority”.

Append streams store compiler output, task messages, security reports, and release audit events. Object links can point stable names such as `latest` or `production` at immutable artifacts, while mutation batches guard promotion using expected object versions.

### 4.3 Workflow Orchestration

A workflow engine stores each workflow instance as a canonical object. Indexes discover runnable steps by workflow id, state, due time, and priority. Append streams store step attempts, user approvals, tool invocations, and compensation events.

Workers acquire task leases for individual steps. A stale worker that loses its lease MUST NOT be able to commit a step result or append a misleading completion event under the old claim.

### 4.4 Audit And Activity Timelines

An application records user-facing activity events, administrative actions, policy decisions, and background repairs in append streams. Operators can tail the stream, resume from a sequence, and index structured fields in JSON payloads for searches such as “all policy changes for object X” or “all failed actions after time T”.

### 4.5 IoT And Ingestion Pipelines

Devices or ingestion workers append ordered records to per-device or per-topic streams. Typed indexes over append payloads support queries by time, severity, source id, and extracted classification. Segment sealing rotates immutable storage units without closing the logical stream.

## 5. Data Model

### 5.1 Canonical Object

A canonical object is an ordinary Anvil object whose payload is the durable source of truth for an application record.

For JSON-indexed objects, the payload MUST be valid JSON at indexing time. If the payload is not valid JSON and an index definition requires JSON extraction, the indexer MUST record an index error for that object and MUST NOT synthesize partial values.

Example canonical queue-like record:

```json
{
  "state": {
    "queue_name": "outbound",
    "state": "pending",
    "available_at": "2026-07-03T12:00:00Z",
    "priority": 100,
    "item_id": "item_01J..."
  },
  "payload": {
    "kind": "notification",
    "target": "user_123"
  }
}
```

The field names above are examples. Anvil MUST treat them as ordinary JSON paths configured by an index definition.

### 5.2 Object Metadata

Object metadata contains data about the object, not the domain state inside the object.

Native gRPC object writes MUST support:

```text
content_type       = optional media type string
user_metadata_json = optional JSON object encoded as UTF-8 text
```

`user_metadata_json` MUST parse as a JSON object when present. Metadata values MUST be small enough to fit configured object-metadata limits. Anvil MAY reject oversized metadata before writing any object bytes.

Metadata MUST round-trip through:

1. `PutObject`;
2. `GetObject` metadata frames or headers;
3. `HeadObject`;
4. object list summaries;
5. object version summaries;
6. index hits;
7. watch events where object metadata is included;
8. S3 gateway metadata mappings where the S3 protocol permits it.

### 5.3 Append Stream

An append stream is a logical, ordered, append-only log inside one tenant bucket.

Durable stream identity:

```text
AppendStreamIdentity = tenant_id + bucket_name + stream_key
```

Each append stream contains zero or more records. Records are addressed by monotonically increasing sequence number starting at 1.

An append stream MAY be physically stored in sealed segments. Segment sealing MUST NOT close the logical stream.

### 5.4 Append Record

An append record contains:

```text
tenant_id
bucket_name
stream_key
record_sequence
payload
payload_sha256
payload_size
content_type
user_metadata_json
created_at
mutation_context
watch_cursor
segment_id
```

After a record append succeeds, the record MUST be immutable.

### 5.5 Segment

A segment is an immutable storage unit for a contiguous range of append-record sequences.

`SealAppendStreamSegment` MUST mean:

1. no further records are written to that segment;
2. already written records in the segment remain readable;
3. the logical stream remains open;
4. the next append MAY create or select a later segment.

If Anvil later needs permanent stream closure, it MUST add a separate `CloseAppendStream` operation. `SealAppendStreamSegment` MUST NOT be repurposed as logical stream closure.

## 6. Typed Index Model

### 6.1 Index Source Kind

Typed indexes MUST support these source kinds:

```text
object_current
object_version
append_record
```

`object_current` indexes the latest visible version of each object.

`object_version` indexes every object version when version history is retained.

`append_record` indexes append-stream records.

### 6.2 Extractor

An extractor maps source data to one typed index field.

Required extractor kinds:

```text
object_key
object_content_type
object_user_metadata_json_pointer
object_body_json_pointer
append_stream_key
append_record_sequence
append_content_type
append_user_metadata_json_pointer
append_payload_json_pointer
created_at
```

JSON pointer extractors MUST use RFC 6901 JSON Pointer syntax.

If a JSON path is absent, the indexed value is `null` unless the index definition marks the field as `required`. If a required field is absent, the indexer MUST record an index error and MUST NOT emit a row for that source item.

### 6.3 Field Types

Typed indexes MUST support these field types:

```text
string
bool
int64
float64
timestamp
bytes_sha256
```

`timestamp` values MUST be encoded and compared as UTC instants. JSON timestamp extraction MUST accept RFC 3339 strings. Invalid timestamp strings MUST produce an index error for that source item.

### 6.4 Index Definition

An index definition MUST include:

```text
index_id
bucket_name
source_kind
field_definitions
default_order
lag_policy
generation
```

Changing extractors, field types, source kind, or default order MUST create a new index generation.

### 6.5 Query Predicates

Typed queries MUST support:

```text
EQ
IN
LT
LTE
GT
GTE
EXISTS
IS_NULL
```

Predicates MUST be type checked against the index field definition. Anvil MUST reject a query with incompatible predicate values instead of coercing silently.

### 6.6 Ordering

Queries MUST support ordered results using one or more fields.

Each order term includes:

```text
field_name
direction = asc | desc
nulls = first | last
```

If an order does not uniquely identify rows, Anvil MUST append a deterministic tie-breaker. The tie-breaker MUST include a stable source identity such as object key plus version id, or stream key plus record sequence.

### 6.7 Stable Page Token

A page token MUST bind:

```text
tenant_id
bucket_name
index_id
index_generation
source_kind
predicate_hash
order_hash
last_sort_tuple
last_source_identity
issued_at
expires_at
```

When a client supplies a page token, Anvil MUST verify every bound field. If the token is incompatible with the current query or index generation, Anvil MUST reject the request with an explicit invalid-page-token error. It MUST NOT silently restart, skip, or duplicate records.

### 6.8 Index Lag Controls

Every typed index query MUST accept:

```text
require_caught_up_to_watch_cursor = optional WatchCursor
lag_timeout_ms                   = optional uint64
```

If `require_caught_up_to_watch_cursor` is absent, Anvil MAY return the best available indexed view and MUST include lag metadata in the response.

If `require_caught_up_to_watch_cursor` is present, Anvil MUST either:

1. wait until the index has processed at least that watch cursor and then answer; or
2. fail with `IndexLagging` after `lag_timeout_ms` or a server configured maximum.

The response MUST include:

```text
index_generation
source_watch_cursor_high
index_watch_cursor_applied
is_caught_up
lag_record_count_hint
next_page_token
```

## 7. Lease-Fenced Preconditions

### 7.1 Write Precondition

Object writes, object deletes, JSON patches, append records, manifest CAS operations, and mutation batches MUST accept a shared precondition shape:

```text
WritePrecondition {
  object_versions: repeated ObjectVersionPrecondition
  lease_fence: optional LeaseFencePrecondition
}

ObjectVersionPrecondition {
  bucket_name: string
  object_key: string
  expected_version_id: optional string
  must_not_exist: bool
}

LeaseFencePrecondition {
  task_id: string
  fence_token: uint64
}
```

### 7.2 Authenticated Lease Ownership

The caller MUST NOT be able to set lease owner identity through `LeaseFencePrecondition`.

Anvil MUST derive the authenticated owner from validated request credentials. A lease-fenced mutation passes only when:

1. the task lease exists in the same tenant;
2. the authenticated principal is the current lease owner;
3. the fence token matches the current lease fence;
4. the lease has not expired;
5. the caller is authorised for the mutation being attempted.

If another principal uses the same owner label string, the precondition MUST fail.

### 7.3 Stale Fence Rejection

A stale fence MUST fail before any mutation is applied.

This applies even if:

1. the object version preconditions still match;
2. the payload is otherwise valid;
3. the caller had a previous lease;
4. the caller can still authenticate to the tenant.

## 8. Mutation Batch

### 8.1 Purpose

`MutationBatch` is the atomic unit for applying related changes that must either all happen or all fail.

### 8.2 Scope

A mutation batch MUST be scoped to exactly one tenant and one bucket. Cross-bucket and cross-tenant batches are out of scope.

### 8.3 Operations

The initial batch operation set MUST include:

```text
put_object
patch_json_object
delete_object
append_stream_record
checkpoint_task_lease
commit_task_lease
compare_and_swap_manifest
```

### 8.4 Atomic Semantics

Anvil MUST validate:

1. authentication;
2. authorisation;
3. request shape;
4. object-version preconditions;
5. lease-fence preconditions;
6. idempotency context;
7. operation-specific constraints;

before applying any operation.

If validation fails, Anvil MUST apply no operations.

If validation succeeds, Anvil MUST apply operations in request order as one atomic metadata mutation. Payload durability rules MUST be at least as strong as the existing object-write durability rules.

### 8.5 Idempotency

A mutation batch MUST support native mutation context idempotency.

Retrying the same idempotency key with the same target and operation digest MUST return the same logical result.

Retrying the same idempotency key with a different operation digest MUST fail with conflict.

### 8.6 Response

The response MUST include:

```text
batch_id
operation_receipts
watch_cursor
mutation_revision
```

Each operation receipt MUST include enough information for a client to resume safely after a retry, such as object version id, append record sequence, lease cursor, or manifest revision.

## 9. Native API Shape

This section defines semantic API shapes. Exact protobuf field numbers are an implementation detail, but field names and semantics MUST remain stable after release.

### 9.1 Object Metadata

```protobuf
message ObjectMetadata {
  string bucket_name = 1;
  string object_key = 2;
  NativeMutationContext mutation_context = 3;
  optional string content_type = 4;
  string user_metadata_json = 5;
}
```

### 9.2 Typed Index Definition

```protobuf
enum TypedIndexSourceKind {
  TYPED_INDEX_SOURCE_KIND_UNSPECIFIED = 0;
  TYPED_INDEX_SOURCE_KIND_OBJECT_CURRENT = 1;
  TYPED_INDEX_SOURCE_KIND_OBJECT_VERSION = 2;
  TYPED_INDEX_SOURCE_KIND_APPEND_RECORD = 3;
}

enum TypedIndexFieldType {
  TYPED_INDEX_FIELD_TYPE_UNSPECIFIED = 0;
  TYPED_INDEX_FIELD_TYPE_STRING = 1;
  TYPED_INDEX_FIELD_TYPE_BOOL = 2;
  TYPED_INDEX_FIELD_TYPE_INT64 = 3;
  TYPED_INDEX_FIELD_TYPE_FLOAT64 = 4;
  TYPED_INDEX_FIELD_TYPE_TIMESTAMP = 5;
  TYPED_INDEX_FIELD_TYPE_BYTES_SHA256 = 6;
}

message TypedIndexField {
  string name = 1;
  TypedIndexFieldType field_type = 2;
  string extractor = 3;
  bool required = 4;
}
```

### 9.3 Typed Index Query

```protobuf
message TypedIndexPredicate {
  string field_name = 1;
  TypedIndexPredicateOperator operator = 2;
  repeated TypedIndexValue values = 3;
}

message TypedIndexOrder {
  string field_name = 1;
  SortDirection direction = 2;
  NullOrdering nulls = 3;
}

message QueryTypedIndexRequest {
  string bucket_name = 1;
  string index_id = 2;
  repeated TypedIndexPredicate predicates = 3;
  repeated TypedIndexOrder order_by = 4;
  uint32 limit = 5;
  string page_token = 6;
  string require_caught_up_to_watch_cursor = 7;
  uint64 lag_timeout_ms = 8;
}
```

### 9.4 Append Stream Read And Tail

```protobuf
message ReadAppendStreamRequest {
  string bucket_name = 1;
  string stream_key = 2;
  uint64 after_sequence = 3;
  uint32 limit = 4;
  bool include_payload = 5;
}

message TailAppendStreamRequest {
  string bucket_name = 1;
  string stream_key = 2;
  uint64 after_sequence = 3;
  bool include_payload = 4;
}
```

`TailAppendStream` MAY be unary polling or server streaming. If server streaming is not available in a release, unary polling MUST still provide correct ordered continuation.

### 9.5 Mutation Batch

```protobuf
message MutationBatchRequest {
  string bucket_name = 1;
  NativeMutationContext mutation_context = 2;
  WritePrecondition precondition = 3;
  repeated MutationBatchOperation operations = 4;
}
```

## 10. Error Semantics

The implementation MUST return distinct structured errors for:

```text
InvalidObjectMetadata
InvalidJsonPointer
InvalidTypedIndexDefinition
InvalidTypedIndexQuery
InvalidPageToken
IndexLagging
ObjectVersionPreconditionFailed
LeaseFencePreconditionFailed
MutationBatchConflict
MutationBatchTooLarge
AppendStreamNotFound
AppendSegmentSealed
```

Every returned error MUST include the request id in transport metadata or response metadata.

## 11. Compatibility

Existing append-stream APIs MAY remain as compatibility aliases if they preserve the new semantics.

The current `SealAppendStreamSegment` API MUST change behavior from logical stream closure to segment sealing if it currently rejects all later appends to the logical stream.

Existing metadata-filter indexes MAY remain, but typed indexes over object JSON MUST be the recommended path for new workloads requiring range, order, and stable pagination.

## 12. Required Tests

An implementation conforming to this RFC MUST include tests for:

1. gRPC `PutObject` writes content type and user metadata;
2. gRPC `HeadObject`, list, index hit, and get metadata expose the same metadata;
3. typed object-body index extracts `state.queue_name`, `state.state`, `state.available_at`, `state.priority`, and `state.item_id` from canonical JSON;
4. typed query supports `state.available_at <= now`;
5. typed query supports `state.state IN (...)`;
6. typed query supports ordering by `available_at ASC`, `priority DESC`, `item_id ASC`;
7. page token rejects changed predicates;
8. page token rejects changed order;
9. page token rejects changed index generation;
10. `require_caught_up_to_watch_cursor` waits or fails explicitly;
11. append-stream read returns records in sequence order;
12. append-stream tail resumes after a sequence;
13. sealing a segment does not close the stream;
14. stale lease fence cannot patch object JSON;
15. stale lease fence cannot append a protected record;
16. expired lease can be acquired by another principal and increments fence;
17. old principal cannot use the old fence after takeover;
18. mutation batch applies all operations on success;
19. mutation batch applies no operations on any precondition failure;
20. idempotent mutation batch retry returns the original logical result;
21. idempotency key with changed operation digest fails with conflict.

## 13. Operational Requirements

Operators MUST be able to inspect:

1. typed index definition;
2. typed index generation;
3. typed index lag;
4. typed index extraction errors;
5. append-stream segment layout;
6. append-stream latest sequence;
7. mutation-batch rejection reason;
8. lease fence owner and expiry for administrative diagnosis.

Administrative inspection MUST respect the administrative-plane authorisation model and MUST NOT expose data through public object APIs unless the caller is authorised.

## 14. Release Requirements

A release containing this RFC MUST include:

1. updated protobuf definitions;
2. updated Rust client exports;
3. server implementation;
4. migration or compatibility notes for existing append-stream users;
5. local tests proving the required test cases;
6. Docker image publication;
7. Rust client crate publication if the public Rust API changes;
8. release notes describing the generic primitives and their semantics.

## 15. Acceptance Criteria

This RFC is complete when an application can implement the following flow without a relational database, external queue, or separate projection objects:

1. write a canonical JSON work item object;
2. query due work by typed JSON fields with range and order;
3. acquire a task lease;
4. atomically transition the work item state and append an attempt record under the active lease fence;
5. lose the lease and prove the stale worker cannot commit;
6. acquire a replacement lease with a higher fence;
7. commit a new state and append a final record;
8. list the audit stream from sequence 1;
9. continue appending after segment sealing;
10. perform an administrative query that demands index catch-up to a known watch cursor.

