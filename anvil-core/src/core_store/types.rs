use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const CORE_OBJECT_MANIFEST_SCHEMA: &str = "anvil.core.object_manifest.v1";
pub const CORE_LOGICAL_FILE_MANIFEST_SCHEMA: &str = "anvil.core.logical_file_manifest.v1";
pub const CORE_REF_SCHEMA: &str = "anvil.core.ref_value.v1";
pub const CORE_REF_UPDATE_SCHEMA: &str = "anvil.core.ref_update.v1";
pub const CORE_TRANSACTION_SCHEMA: &str = "anvil.core.transaction.v1";
pub const CORE_WATCH_EVENT_SCHEMA: &str = "anvil.core.watch_event.v1";
pub const CORE_FENCE_SCHEMA: &str = "anvil.core.fence.v1";
pub const CORE_ROOT_CATALOG_SCHEMA: &str = "anvil.core.root_catalog.v1";
pub const CORE_QUORUM_PROFILE_SCHEMA: &str = "anvil.core.quorum_profile.v1";
pub const CORE_BOUNDARY_SCHEMA_SCHEMA: &str = "anvil.core.boundary_schema.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutBlob {
    pub logical_name: String,
    pub bytes: Vec<u8>,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub region_id: String,
    pub mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetBlob {
    pub object_ref: CoreObjectRef,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreByteRange {
    pub start: u64,
    pub end_exclusive: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetBlobRange {
    pub object_ref: CoreObjectRef,
    pub range: CoreByteRange,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteLogicalFileRequest {
    pub writer_family: String,
    pub generation: u64,
    pub logical_file_id: String,
    pub source: Vec<u8>,
    pub range_hints: Vec<CoreLogicalRangeHint>,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadLogicalRangeRequest {
    pub manifest: CoreLogicalFileManifest,
    pub ranges: Vec<CoreByteRange>,
    pub authz_scope: AuthzScopeRef,
    pub expected_boundary: Option<Vec<CoreBoundaryValue>>,
    pub prefetch_policy: CorePrefetchPolicy,
    pub trace_context: CoreTraceContext,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalRangeHint {
    pub range_id: String,
    pub byte_start: u64,
    pub byte_end: u64,
    pub writer_record_kind: String,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub writer_statistics: Vec<u8>,
    pub prefetch_next_range_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorePipelinePolicy {
    pub compression: String,
    pub encryption: String,
    pub erasure_profile_id: String,
    pub placement_scope: String,
}

impl Default for CorePipelinePolicy {
    fn default() -> Self {
        Self {
            compression: "none".to_string(),
            encryption: "none".to_string(),
            erasure_profile_id: "ec-4-2".to_string(),
            placement_scope: "region".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct CoreTraceContext {
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorePrefetchPolicy {
    pub enabled: bool,
    pub max_ranges: u32,
}

impl Default for CorePrefetchPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            max_ranges: 4,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalFileManifest {
    pub schema: String,
    pub logical_file_id: String,
    pub writer_family: String,
    pub writer_generation: u64,
    pub logical_size: u64,
    pub content_hash: String,
    pub boundary_schema_generation: u64,
    pub ranges: Vec<CoreLogicalRange>,
    pub blocks: Vec<CoreLogicalBlockRef>,
    pub compression: CoreCompressionDescriptor,
    pub encryption: CoreEncryptionDescriptor,
    pub erasure_profile_id: String,
    pub placement_epoch: u64,
    pub created_by_mutation_id: String,
    pub codec_id: String,
    pub data_shards: u32,
    pub parity_shards: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalRange {
    pub range_id: String,
    pub byte_start: u64,
    pub byte_end: u64,
    pub writer_record_kind: String,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub writer_statistics: Vec<u8>,
    pub block_ids: Vec<String>,
    pub prefetch_next_range_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalBlockRef {
    pub block_id: String,
    pub logical_offset: u64,
    pub logical_length: u64,
    pub compressed_length: u64,
    pub encrypted_length: u64,
    pub content_hash: String,
    pub erasure_set_id: String,
    pub shards: Vec<CoreLogicalShardRef>,
    pub codec_id: String,
    pub data_shards: u32,
    pub parity_shards: u32,
    pub plaintext_block_len: u64,
    pub shard_payload_len: u64,
    pub padding_len: u64,
    pub block_encoded_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalShardRef {
    pub node_id: String,
    pub region_id: String,
    pub cell_id: String,
    pub shard_index: u32,
    pub shard_hash: String,
    pub stored_length: u64,
    pub generation: u64,
    pub placement_epoch: u64,
    pub fsync_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreCompressionDescriptor {
    pub algorithm: String,
    pub level: u32,
    pub uncompressed_length: u64,
    pub compressed_length: u64,
    pub dictionary_id: String,
    pub descriptor_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreEncryptionDescriptor {
    pub algorithm: String,
    pub key_id: String,
    pub nonce: Vec<u8>,
    pub aad_hash: String,
    pub plaintext_hash: String,
    pub ciphertext_hash: String,
    pub descriptor_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreLogicalFileVerificationReport {
    pub verified: bool,
    pub logical_file_id: String,
    pub checked_blocks: u64,
    pub checked_shards: u64,
    pub content_hash: String,
}

pub fn core_object_ref_from_logical_file_manifest(
    manifest: &CoreLogicalFileManifest,
) -> CoreObjectRef {
    let storage_hash = manifest
        .blocks
        .first()
        .map(|block| block.block_encoded_hash.as_str())
        .unwrap_or(&manifest.content_hash);
    let manifest_hash = storage_hash.strip_prefix("sha256:").unwrap_or(storage_hash);
    let logical_size = manifest
        .blocks
        .first()
        .map(|block| block.encrypted_length)
        .unwrap_or(manifest.logical_size);
    CoreObjectRef {
        hash: storage_hash.to_string(),
        logical_size,
        manifest_ref: format!(
            "core-manifest-sha256:{manifest_hash}:profile:{}",
            manifest.erasure_profile_id
        ),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreObjectRef {
    pub hash: String,
    pub logical_size: u64,
    pub manifest_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreObjectManifest {
    pub schema: String,
    pub mesh_id: String,
    pub region_id: String,
    pub object_hash: String,
    pub logical_size: u64,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub encoding: CoreObjectEncoding,
    pub placements: Vec<CoreObjectPlacement>,
    pub created_at: String,
    pub mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreObjectEncoding {
    pub profile_id: String,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub minimum_read_shards: u16,
    pub minimum_write_ack_shards: u16,
    pub stripe_size: u64,
    pub placement_scope: String,
    pub repair_priority: String,
    pub encryption: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreObjectPlacement {
    pub shard_index: u16,
    pub node_id: String,
    pub region_id: String,
    pub cell_id: String,
    pub shard_hash: String,
    pub stored_size: u64,
    pub generation: u64,
    pub placement_epoch: u64,
    pub fsync_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreBoundarySchema {
    pub schema: String,
    pub bucket: String,
    pub generation: u64,
    pub dimensions: Vec<CoreBoundaryDimension>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreBoundaryDimension {
    pub name: String,
    pub source: CoreBoundarySource,
    pub value_type: String,
    pub categories: Vec<String>,
    pub required: bool,
    pub cardinality: String,
    pub max_values_per_block: u32,
    pub placement_affinity: String,
    pub compaction_scope: String,
    pub shared_ranges_allowed: bool,
    pub shared_record_kinds: Vec<String>,
    pub deprecated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CoreBoundaryValue {
    pub schema_generation: u64,
    pub name: String,
    pub value_type: String,
    pub value: String,
    pub categories: Vec<String>,
    pub source_kind: String,
    pub required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreBoundarySource {
    UserMetadataJsonPointer {
        pointer: String,
    },
    PathTemplate {
        template: String,
    },
    BodyJsonPointer {
        pointer: String,
        max_body_bytes: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutBoundarySchema {
    pub schema: CoreBoundarySchema,
    pub expected_generation: Option<u64>,
    pub mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoundarySchemaReceipt {
    pub bucket: String,
    pub generation: u64,
    pub ref_generation: u64,
    pub schema_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppendStreamRecord {
    pub stream_id: String,
    pub partition_id: String,
    pub record_kind: String,
    pub payload: Vec<u8>,
    pub fence: Option<CoreFencePrecondition>,
    pub transaction_id: Option<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadStream {
    pub stream_id: String,
    pub after_sequence: u64,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamAppendReceipt {
    pub stream_id: String,
    pub sequence: u64,
    pub cursor: String,
    pub event_hash: String,
    pub idempotent_replay: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamRecord {
    pub schema: String,
    pub stream_id: String,
    pub partition_id: String,
    pub sequence: u64,
    pub cursor: String,
    pub previous_event_hash: String,
    pub event_hash: String,
    pub record_kind: String,
    pub payload_hash: String,
    pub payload: Vec<u8>,
    pub transaction_id: Option<String>,
    pub idempotency_key_hash: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SealStreamSegment {
    pub stream_id: String,
    pub partition_id: String,
    pub through_sequence: Option<u64>,
    pub segment_kind: String,
    pub mutation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreSegmentRef {
    pub stream_id: String,
    pub partition_id: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub record_count: u64,
    pub segment_kind: String,
    pub object_ref: CoreObjectRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WatchRequest {
    pub stream_prefix: String,
    pub after_cursor: Option<String>,
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WatchEvent {
    pub stream_id: String,
    pub sequence: u64,
    pub cursor: String,
    pub previous_event_hash: String,
    pub event_hash: String,
    pub event_type: String,
    pub record_kind: String,
    pub payload_hash: String,
    pub transaction_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AcquireFence {
    pub fence_name: String,
    pub authenticated_principal: String,
    pub ttl_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReleaseFence {
    pub fence_name: String,
    pub authenticated_principal: String,
    pub fence_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FencedPermit {
    pub fence_name: String,
    pub owner_principal: String,
    pub fence_token: u64,
    pub expires_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreFenceRecord {
    pub schema: String,
    pub fence_name: String,
    pub owner_principal: String,
    pub fence_token: u64,
    pub expires_at_ms: i64,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRootCatalog {
    pub schema: String,
    pub mesh_id: String,
    pub generation: u64,
    pub previous_hash: String,
    pub root_partitions: Vec<CoreRootPartition>,
    pub placement_catalog_ref: String,
    pub stream_directory_ref: String,
    pub ref_directory_ref: String,
    pub authz_system_realm_ref: String,
    pub created_at: String,
    pub signed_by: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRootPartition {
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence: u64,
    pub placement_group: String,
    pub embedded_head_segment_manifest: CoreObjectManifest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRootCatalogReceipt {
    pub mesh_id: String,
    pub generation: u64,
    pub catalog_hash: String,
    pub ref_generation: u64,
    pub watch_cursor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreQuorumProfile {
    pub schema: String,
    pub placement_group: String,
    pub replica_count: u16,
    pub write_quorum: u16,
    pub read_quorum: u16,
    pub fence_quorum: u16,
    pub epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreQuorumProfileReceipt {
    pub placement_group: String,
    pub epoch: u64,
    pub profile_hash: String,
    pub ref_generation: u64,
    pub watch_cursor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompareAndSwapRef {
    pub ref_name: String,
    pub expected_generation: Option<u64>,
    pub expected_target: Option<String>,
    pub require_absent: bool,
    pub require_present: bool,
    pub fence: Option<CoreFencePrecondition>,
    pub authz_revision: Option<String>,
    pub source_watch_cursor: Option<String>,
    pub new_target: String,
    pub transaction_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreFencePrecondition {
    pub fence_name: String,
    pub fence_token: u64,
    pub authenticated_principal: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRefValue {
    pub schema: String,
    pub ref_name: String,
    pub generation: u64,
    pub target: String,
    pub transaction_id: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CasRefReceipt {
    pub ref_name: String,
    pub generation: u64,
    pub previous_target: Option<String>,
    pub new_target: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRefUpdateRecord {
    pub schema: String,
    pub ref_name: String,
    pub previous_generation: Option<u64>,
    pub new_generation: Option<u64>,
    pub previous_target: Option<String>,
    pub new_target: Option<String>,
    pub preconditions: CoreRefUpdatePreconditions,
    pub mutation_id: String,
    pub transaction_id: Option<String>,
    pub committed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreRefUpdatePreconditions {
    pub expected_generation: Option<u64>,
    pub expected_target: Option<String>,
    pub require_absent: bool,
    pub require_present: bool,
    pub fence_token: Option<u64>,
    pub authz_revision: Option<String>,
    pub source_watch_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreTransaction {
    pub schema: String,
    pub transaction_id: String,
    pub scope_partition: String,
    pub state: CoreTransactionState,
    pub preconditions_hash: String,
    pub operations_hash: String,
    pub prepared_refs: Vec<String>,
    pub visible_updates: Vec<CoreTransactionUpdate>,
    pub finalisation_error: Option<String>,
    pub committed_at: String,
    pub committed_by_principal: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoreTransactionState {
    Prepared,
    Committed,
    FinalisationFailed,
    Aborted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreTransactionUpdate {
    CoreRefUpdate {
        ref_name: String,
        new_generation: u64,
    },
    StreamAppend {
        stream_id: String,
        visible_sequence: u64,
        prepared_record_hash: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMutationBatch {
    pub transaction_id: String,
    pub scope_partition: String,
    pub committed_by_principal: String,
    pub preconditions: Vec<CoreMutationPrecondition>,
    pub operations: Vec<CoreMutationOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreMutationPrecondition {
    Ref {
        ref_name: String,
        expected_generation: Option<u64>,
        expected_target: Option<String>,
        require_absent: bool,
        require_present: bool,
        fence: Option<CoreFencePrecondition>,
        authz_revision: Option<String>,
        source_watch_cursor: Option<String>,
    },
    Fence {
        fence_name: String,
        fence_token: u64,
    },
    StreamHead {
        stream_id: String,
        expected_last_sequence: u64,
        expected_last_event_hash: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CoreMutationOperation {
    RefUpdate {
        partition_id: String,
        ref_name: String,
        new_target: String,
    },
    StreamAppend {
        partition_id: String,
        stream_id: String,
        record_kind: String,
        payload: Vec<u8>,
        idempotency_key: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMutationBatchReceipt {
    pub transaction_id: String,
    pub scope_partition: String,
    pub state: CoreTransactionState,
    pub visible_updates: Vec<CoreTransactionUpdate>,
    pub finalisation_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceId {
    pub schema: String,
    pub mesh_id: String,
    pub anvil_storage_tenant_id: String,
    pub authz_scope: AuthzScopeRef,
    pub kind: SourceKind,
    pub resource_namespace: String,
    pub resource_id: String,
    pub generation: u64,
    pub tombstone: bool,
    pub variant: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzScopeRef {
    pub anvil_storage_tenant_id: String,
    pub authz_realm_id: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    ObjectCurrent,
    ObjectVersion,
    AppendRecord,
    AuthzResource,
    PackageRepository,
    PackageVersion,
    PackageFile,
    PackageTag,
    GitObject,
    PersonalDatabaseRecord,
    MeshControlRecord,
}
