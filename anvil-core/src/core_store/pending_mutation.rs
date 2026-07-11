use super::transaction_manifest_proto::{
    decode_manifest_locator_proto, encode_manifest_locator_proto,
};
use super::types::{CoreBoundaryValue, CoreCompressionDescriptor, CoreManifestLocator};
use super::{CoreMetaRowCommonProto, core_meta_committed_row_common};
use anyhow::{Context, Result, anyhow, bail};
use prost::{Message, Oneof};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(super) const CORE_PENDING_MUTATION_HASH_INPUT_MAGIC: &[u8; 8] = b"ANPMH1\0\0";
pub(super) const CORE_PENDING_MUTATION_RECORD_SCHEMA: &str =
    "anvil.core.pending_mutation_record.v1";
pub(super) const CORE_PENDING_MUTATION_ROW_SCHEMA: &str = "anvil.core.pending_mutation_row.v1";
pub(super) const CORE_PENDING_MUTATION_FINALISATION_SCHEMA: &str =
    "anvil.core.pending_mutation_finalisation.v1";
pub(super) const CORE_PENDING_MUTATION_FINALISATION_INDEX_SCHEMA: &str =
    "anvil.core.pending_mutation_finalisation_index.v1";
pub(super) const CORE_MATERIALISATION_CURSOR_SCHEMA: &str = "anvil.core.materialisation_cursor.v1";
pub(super) const CORE_LOCAL_ADMISSION_RECEIPT_SCHEMA: &str = "anvil.admission.local_receipt.v1";
pub(super) const CORE_ADMISSION_COMMIT_CERTIFICATE_SCHEMA: &str =
    "anvil.admission.commit_certificate.v1";
pub(super) const CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND: &str =
    "core_pending_mutation.finalisation";
pub(super) const CORE_META_ADMISSION_PROFILE: &str = "metadata-r3-q2";
pub(super) const CORE_META_ADMISSION_PROFILE_EPOCH: u64 = 1;
pub(super) const CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES: usize = 16 * 1024;
pub(super) const CORE_PENDING_MUTATION_NODE_ID: &str = "local-node";
pub(super) const CORE_PENDING_MUTATION_EPOCH: u64 = 1;

pub(super) const CORE_TRANSACTION_ROOT_ANCHOR_KEY: &str = "system/core-control/0";

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0x82f6_3b78;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CorePendingLandedByte {
    pub(super) sha256: String,
    pub(super) length: u64,
    pub(super) landing_id: String,
    pub(super) relative_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CoreStoredLandedByteRef {
    pub(super) schema: String,
    pub(super) landed: CorePendingLandedByte,
    pub(super) mutation_id: String,
    pub(super) boundary_values: Vec<CoreBoundaryValue>,
    pub(super) created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CorePendingAuthzScope {
    pub(super) realm_id: String,
    pub(super) revision: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum CorePendingMutationTarget {
    ObjectPut {
        logical_name: String,
        region_id: String,
        erasure_profile_id: String,
        encryption: String,
        block_plain_hash: String,
        object_hash: String,
        object_logical_size: u64,
        compression: CoreCompressionDescriptor,
        writer_generation: u64,
        block_ordinal: u64,
    },
    StreamAppend {
        stream_id: String,
        partition_id: String,
        record_kind: String,
        transaction_id: Option<String>,
    },
    MutationBatch {
        transaction_id: String,
        scope_partition: String,
        operation_count: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) enum CorePendingMutationFinalisationResult {
    StreamStateLocator(CoreManifestLocator),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CorePendingMutationFinalisationIndexRow {
    pub(super) schema: String,
    pub(super) node_id: String,
    pub(super) mutation_epoch: u64,
    pub(super) mutation_sequence: u64,
    pub(super) mutation_id: String,
    pub(super) state: String,
    pub(super) result_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CorePendingMutationRecord {
    pub(super) schema: String,
    pub(super) node_id: String,
    pub(super) mutation_epoch: u64,
    pub(super) sequence: u64,
    pub(super) mutation_id: String,
    pub(super) idempotency_key_hash: Option<String>,
    pub(super) anvil_storage_tenant_id: String,
    pub(super) authz_scope: CorePendingAuthzScope,
    pub(super) operation_family: String,
    pub(super) writer_family: String,
    pub(super) target: CorePendingMutationTarget,
    pub(super) precondition_fingerprints: Vec<String>,
    pub(super) boundary_values: Vec<CoreBoundaryValue>,
    pub(super) landed_bytes: Vec<CorePendingLandedByte>,
    pub(super) created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CorePendingMutationFinalisationRecord {
    pub(super) schema: String,
    pub(super) node_id: String,
    pub(super) mutation_epoch: u64,
    pub(super) mutation_sequence: u64,
    pub(super) mutation_id: String,
    pub(super) operation_family: String,
    pub(super) writer_family: String,
    pub(super) target: CorePendingMutationTarget,
    pub(super) boundary_values: Vec<CoreBoundaryValue>,
    pub(super) landed_bytes: Vec<CorePendingLandedByte>,
    pub(super) state: String,
    pub(super) result: Option<CorePendingMutationFinalisationResult>,
    pub(super) finalised_at_unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct CoreAdmissionAttemptId {
    pub(super) mutation_id: String,
    pub(super) root_anchor_key: String,
    pub(super) root_key_hash: String,
    pub(super) source_node_id: String,
    pub(super) source_mutation_epoch: u64,
    pub(super) source_mutation_sequence: u64,
    pub(super) request_hash: String,
    pub(super) admission_profile: String,
    pub(super) admission_profile_epoch: u64,
    pub(super) metadata_replica_node_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct CoreLocalAdmissionReceipt {
    pub(super) schema: String,
    pub(super) attempt_id: CoreAdmissionAttemptId,
    pub(super) landed_byte_hashes: Vec<String>,
    pub(super) descriptor_hashes: Vec<String>,
    pub(super) pending_mutation_hash: String,
    pub(super) local_metadata_fsync_sequence: u64,
    pub(super) local_landed_fsync_sequence: u64,
    pub(super) signed_payload_hash: String,
    pub(super) source_signature: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct CoreAdmissionCommitCertificate {
    pub(super) schema: String,
    pub(super) attempt_id: CoreAdmissionAttemptId,
    pub(super) local_receipt: CoreLocalAdmissionReceipt,
    pub(super) core_meta_commit_certificate_hash: String,
    pub(super) certificate_persist_receipt_hashes: Vec<String>,
    pub(super) committed_at_unix_nanos: u64,
    pub(super) signed_payload_hash: String,
    pub(super) source_signature: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct CorePendingMutationKey {
    pub(super) node_id: String,
    pub(super) mutation_epoch: u64,
    pub(super) mutation_sequence: u64,
}

#[derive(Debug, Clone)]
pub(super) struct CorePendingMutationReplayOutcome {
    pub(super) state: &'static str,
    pub(super) result: Option<CorePendingMutationFinalisationResult>,
}

impl From<&CorePendingMutationRecord> for CorePendingMutationKey {
    fn from(record: &CorePendingMutationRecord) -> Self {
        Self {
            node_id: record.node_id.clone(),
            mutation_epoch: record.mutation_epoch,
            mutation_sequence: record.sequence,
        }
    }
}

pub(super) enum CorePendingMutationPayload<'a> {
    Empty,
    Inline(&'a [u8]),
    Landed(&'a [u8]),
}

#[derive(Clone, PartialEq, Message)]
struct CoreBoundaryValueProto {
    #[prost(uint64, tag = "1")]
    schema_generation: u64,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(string, tag = "3")]
    value_type: String,
    #[prost(string, tag = "4")]
    value: String,
    #[prost(string, repeated, tag = "5")]
    categories: Vec<String>,
    #[prost(string, tag = "6")]
    source_kind: String,
    #[prost(bool, tag = "7")]
    required: bool,
    #[prost(uint32, tag = "8")]
    max_values_per_block: u32,
    #[prost(string, tag = "9")]
    compaction_scope: String,
    #[prost(bool, tag = "10")]
    shared_ranges_allowed: bool,
    #[prost(string, repeated, tag = "11")]
    shared_record_kinds: Vec<String>,
    #[prost(string, tag = "12")]
    placement_affinity: String,
}

#[derive(Clone, PartialEq, Message)]
struct CorePendingLandedByteProto {
    #[prost(string, tag = "1")]
    sha256: String,
    #[prost(uint64, tag = "2")]
    length: u64,
    #[prost(string, tag = "3")]
    landing_id: String,
    #[prost(string, tag = "4")]
    relative_path: String,
}

#[derive(Clone, PartialEq, Message)]
struct CorePendingAuthzScopeProto {
    #[prost(string, tag = "1")]
    realm_id: String,
    #[prost(string, optional, tag = "2")]
    revision: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectPutTargetProto {
    #[prost(string, tag = "1")]
    logical_name: String,
    #[prost(string, tag = "2")]
    region_id: String,
    #[prost(string, tag = "3")]
    erasure_profile_id: String,
    #[prost(string, tag = "4")]
    encryption: String,
    #[prost(string, tag = "5")]
    block_plain_hash: String,
    #[prost(string, tag = "6")]
    object_hash: String,
    #[prost(uint64, tag = "7")]
    object_logical_size: u64,
    #[prost(message, optional, tag = "8")]
    compression: Option<CoreCompressionDescriptorProto>,
    #[prost(uint64, tag = "9")]
    writer_generation: u64,
    #[prost(uint64, tag = "10")]
    block_ordinal: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreCompressionDescriptorProto {
    #[prost(string, tag = "1")]
    algorithm: String,
    #[prost(uint32, tag = "2")]
    level: u32,
    #[prost(uint64, tag = "3")]
    uncompressed_length: u64,
    #[prost(uint64, tag = "4")]
    compressed_length: u64,
    #[prost(string, tag = "5")]
    dictionary_id: String,
    #[prost(string, tag = "6")]
    descriptor_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreStreamAppendTargetProto {
    #[prost(string, tag = "1")]
    stream_id: String,
    #[prost(string, tag = "2")]
    partition_id: String,
    #[prost(string, tag = "3")]
    record_kind: String,
    #[prost(string, optional, tag = "4")]
    transaction_id: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationBatchTargetProto {
    #[prost(string, tag = "1")]
    transaction_id: String,
    #[prost(string, tag = "2")]
    scope_partition: String,
    #[prost(uint64, tag = "3")]
    operation_count: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CorePendingMutationTargetProto {
    #[prost(oneof = "core_pending_mutation_target_proto::Kind", tags = "1, 2, 3")]
    kind: Option<core_pending_mutation_target_proto::Kind>,
}

mod core_pending_mutation_target_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(message, tag = "1")]
        ObjectPut(super::CoreObjectPutTargetProto),
        #[prost(message, tag = "2")]
        StreamAppend(super::CoreStreamAppendTargetProto),
        #[prost(message, tag = "3")]
        MutationBatch(super::CoreMutationBatchTargetProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CorePendingMutationFinalisationResultProto {
    #[prost(
        oneof = "core_pending_mutation_finalisation_result_proto::Kind",
        tags = "1"
    )]
    kind: Option<core_pending_mutation_finalisation_result_proto::Kind>,
}

mod core_pending_mutation_finalisation_result_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(bytes, tag = "1")]
        StreamStateLocator(Vec<u8>),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CorePendingMutationRecordProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    node_id: String,
    #[prost(uint64, tag = "3")]
    mutation_epoch: u64,
    #[prost(uint64, tag = "4")]
    sequence: u64,
    #[prost(string, tag = "5")]
    mutation_id: String,
    #[prost(string, optional, tag = "6")]
    idempotency_key_hash: Option<String>,
    #[prost(string, tag = "7")]
    anvil_storage_tenant_id: String,
    #[prost(message, optional, tag = "8")]
    authz_scope: Option<CorePendingAuthzScopeProto>,
    #[prost(string, tag = "9")]
    operation_family: String,
    #[prost(string, tag = "10")]
    writer_family: String,
    #[prost(message, optional, tag = "11")]
    target: Option<CorePendingMutationTargetProto>,
    #[prost(string, repeated, tag = "12")]
    precondition_fingerprints: Vec<String>,
    #[prost(message, repeated, tag = "13")]
    boundary_values: Vec<CoreBoundaryValueProto>,
    #[prost(message, repeated, tag = "14")]
    landed_bytes: Vec<CorePendingLandedByteProto>,
    #[prost(uint64, tag = "15")]
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreStoredPendingMutationRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    record: Option<CorePendingMutationRecordProto>,
    #[prost(bytes, tag = "4")]
    inline_payload: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreStoredLandedByteRefProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    landed: Option<CorePendingLandedByteProto>,
    #[prost(string, tag = "4")]
    mutation_id: String,
    #[prost(message, repeated, tag = "5")]
    boundary_values: Vec<CoreBoundaryValueProto>,
    #[prost(uint64, tag = "6")]
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CorePendingMutationFinalisationIndexRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    node_id: String,
    #[prost(uint64, tag = "4")]
    mutation_epoch: u64,
    #[prost(uint64, tag = "5")]
    mutation_sequence: u64,
    #[prost(string, tag = "6")]
    mutation_id: String,
    #[prost(string, tag = "7")]
    state: String,
    #[prost(string, tag = "8")]
    result_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CorePendingMutationFinalisationRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    node_id: String,
    #[prost(uint64, tag = "4")]
    mutation_epoch: u64,
    #[prost(uint64, tag = "5")]
    mutation_sequence: u64,
    #[prost(string, tag = "6")]
    mutation_id: String,
    #[prost(string, tag = "7")]
    operation_family: String,
    #[prost(string, tag = "8")]
    writer_family: String,
    #[prost(message, optional, tag = "9")]
    target: Option<CorePendingMutationTargetProto>,
    #[prost(message, repeated, tag = "10")]
    boundary_values: Vec<CoreBoundaryValueProto>,
    #[prost(message, repeated, tag = "11")]
    landed_bytes: Vec<CorePendingLandedByteProto>,
    #[prost(string, tag = "12")]
    state: String,
    #[prost(message, optional, tag = "13")]
    result: Option<CorePendingMutationFinalisationResultProto>,
    #[prost(uint64, tag = "14")]
    finalised_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMaterialisationCursorRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(uint64, tag = "3")]
    sequence: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreAdmissionAttemptIdProto {
    #[prost(string, tag = "1")]
    mutation_id: String,
    #[prost(string, tag = "2")]
    root_anchor_key: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(string, tag = "4")]
    source_node_id: String,
    #[prost(uint64, tag = "5")]
    source_mutation_epoch: u64,
    #[prost(uint64, tag = "6")]
    source_mutation_sequence: u64,
    #[prost(string, tag = "7")]
    request_hash: String,
    #[prost(string, tag = "8")]
    admission_profile: String,
    #[prost(uint64, tag = "9")]
    admission_profile_epoch: u64,
    #[prost(string, repeated, tag = "10")]
    metadata_replica_node_ids: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreLocalAdmissionReceiptProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(message, optional, tag = "2")]
    attempt_id: Option<CoreAdmissionAttemptIdProto>,
    #[prost(string, repeated, tag = "3")]
    landed_byte_hashes: Vec<String>,
    #[prost(string, repeated, tag = "4")]
    descriptor_hashes: Vec<String>,
    #[prost(string, tag = "5")]
    pending_mutation_hash: String,
    #[prost(uint64, tag = "6")]
    local_metadata_fsync_sequence: u64,
    #[prost(uint64, tag = "7")]
    local_landed_fsync_sequence: u64,
    #[prost(string, tag = "8")]
    signed_payload_hash: String,
    #[prost(bytes, tag = "9")]
    source_signature: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreAdmissionCommitCertificateProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    attempt_id: Option<CoreAdmissionAttemptIdProto>,
    #[prost(message, optional, tag = "4")]
    local_receipt: Option<CoreLocalAdmissionReceiptProto>,
    #[prost(uint64, tag = "5")]
    committed_at_unix_nanos: u64,
    #[prost(string, tag = "6")]
    signed_payload_hash: String,
    #[prost(bytes, tag = "7")]
    source_signature: Vec<u8>,
    #[prost(string, tag = "8")]
    core_meta_commit_certificate_hash: String,
    #[prost(string, repeated, tag = "9")]
    certificate_persist_receipt_hashes: Vec<String>,
}

fn pending_mutation_common(record: &CorePendingMutationRecord) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        format!("tenant/{}", record.anvil_storage_tenant_id),
        root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
        record.sequence,
        record.mutation_id.clone(),
        record.created_at_unix_nanos,
    )
}

pub(super) fn encode_stored_pending_mutation_row(
    record: &CorePendingMutationRecord,
    inline_payload: &[u8],
) -> Result<Vec<u8>> {
    if inline_payload.len() > CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES {
        bail!(
            "CoreStore pending mutation payload exceeds {} bytes",
            CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES
        );
    }
    let proto = CoreStoredPendingMutationRowProto {
        common: Some(pending_mutation_common(record)),
        schema: CORE_PENDING_MUTATION_ROW_SCHEMA.to_string(),
        record: Some(record_to_proto(record)?),
        inline_payload: inline_payload.to_vec(),
    };
    encode_deterministic(proto)
}

pub(super) fn decode_stored_pending_mutation_row(
    bytes: &[u8],
) -> Result<(CorePendingMutationRecord, Vec<u8>)> {
    let proto = CoreStoredPendingMutationRowProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "CoreStore pending mutation row")?;
    if proto.schema != CORE_PENDING_MUTATION_ROW_SCHEMA {
        bail!("CoreStore pending mutation row has invalid schema");
    }
    if proto.inline_payload.len() > CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES {
        bail!("CoreStore pending mutation row payload exceeds inline cap");
    }
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore pending mutation row missing CoreMeta common"))?;
    let record = record_from_proto(
        proto
            .record
            .ok_or_else(|| anyhow!("CoreStore pending mutation row is missing record"))?,
    )?;
    Ok((record, proto.inline_payload))
}

pub(super) fn encode_landed_byte_ref_row(row: &CoreStoredLandedByteRef) -> Result<Vec<u8>> {
    let proto = CoreStoredLandedByteRefProto {
        common: Some(core_meta_committed_row_common(
            "system",
            root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
            row.created_at_unix_nanos,
            row.mutation_id.clone(),
            row.created_at_unix_nanos,
        )),
        schema: row.schema.clone(),
        landed: Some(landed_to_proto(&row.landed)),
        mutation_id: row.mutation_id.clone(),
        boundary_values: row.boundary_values.iter().map(boundary_to_proto).collect(),
        created_at_unix_nanos: row.created_at_unix_nanos,
    };
    encode_deterministic(proto)
}

pub(super) fn decode_landed_byte_ref_row(bytes: &[u8]) -> Result<CoreStoredLandedByteRef> {
    let proto = CoreStoredLandedByteRefProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "CoreStore landed byte ref")?;
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore landed byte ref row missing CoreMeta common"))?;
    Ok(CoreStoredLandedByteRef {
        schema: proto.schema,
        landed: landed_from_proto(
            proto
                .landed
                .ok_or_else(|| anyhow!("CoreStore landed byte ref is missing landed bytes"))?,
        ),
        mutation_id: proto.mutation_id,
        boundary_values: proto
            .boundary_values
            .into_iter()
            .map(boundary_from_proto)
            .collect(),
        created_at_unix_nanos: proto.created_at_unix_nanos,
    })
}

pub(super) fn encode_pending_mutation_finalisation_index_row(
    row: &CorePendingMutationFinalisationIndexRow,
) -> Result<Vec<u8>> {
    let proto = CorePendingMutationFinalisationIndexRowProto {
        common: Some(core_meta_committed_row_common(
            "system",
            root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
            row.mutation_sequence,
            row.mutation_id.clone(),
            0,
        )),
        schema: row.schema.clone(),
        node_id: row.node_id.clone(),
        mutation_epoch: row.mutation_epoch,
        mutation_sequence: row.mutation_sequence,
        mutation_id: row.mutation_id.clone(),
        state: row.state.clone(),
        result_hash: row.result_hash.clone(),
    };
    encode_deterministic(proto)
}

pub(super) fn decode_pending_mutation_finalisation_index_row(
    bytes: &[u8],
) -> Result<CorePendingMutationFinalisationIndexRow> {
    let proto = CorePendingMutationFinalisationIndexRowProto::decode(bytes)?;
    ensure_round_trips(
        &proto,
        bytes,
        "CoreStore pending mutation finalisation index row",
    )?;
    proto.common.as_ref().ok_or_else(|| {
        anyhow!("CoreStore pending mutation finalisation index missing CoreMeta common")
    })?;
    Ok(CorePendingMutationFinalisationIndexRow {
        schema: proto.schema,
        node_id: proto.node_id,
        mutation_epoch: proto.mutation_epoch,
        mutation_sequence: proto.mutation_sequence,
        mutation_id: proto.mutation_id,
        state: proto.state,
        result_hash: proto.result_hash,
    })
}

pub(super) fn encode_pending_mutation_finalisation_record(
    record: &CorePendingMutationFinalisationRecord,
) -> Result<Vec<u8>> {
    let proto = CorePendingMutationFinalisationRecordProto {
        common: Some(core_meta_committed_row_common(
            "system",
            root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
            record.mutation_sequence,
            record.mutation_id.clone(),
            record.finalised_at_unix_nanos,
        )),
        schema: record.schema.clone(),
        node_id: record.node_id.clone(),
        mutation_epoch: record.mutation_epoch,
        mutation_sequence: record.mutation_sequence,
        mutation_id: record.mutation_id.clone(),
        operation_family: record.operation_family.clone(),
        writer_family: record.writer_family.clone(),
        target: Some(target_to_proto(&record.target)?),
        boundary_values: record
            .boundary_values
            .iter()
            .map(boundary_to_proto)
            .collect(),
        landed_bytes: record.landed_bytes.iter().map(landed_to_proto).collect(),
        state: record.state.clone(),
        result: record.result.as_ref().map(result_to_proto).transpose()?,
        finalised_at_unix_nanos: record.finalised_at_unix_nanos,
    };
    encode_deterministic(proto)
}

pub(super) fn decode_pending_mutation_finalisation_record(
    bytes: &[u8],
) -> Result<CorePendingMutationFinalisationRecord> {
    let proto = CorePendingMutationFinalisationRecordProto::decode(bytes)?;
    ensure_round_trips(
        &proto,
        bytes,
        "CoreStore pending mutation finalisation record",
    )?;
    if proto.schema != CORE_PENDING_MUTATION_FINALISATION_SCHEMA {
        bail!("CoreStore pending mutation finalisation record has invalid schema");
    }
    proto.common.as_ref().ok_or_else(|| {
        anyhow!("CoreStore pending mutation finalisation record missing CoreMeta common")
    })?;
    Ok(CorePendingMutationFinalisationRecord {
        schema: proto.schema,
        node_id: proto.node_id,
        mutation_epoch: proto.mutation_epoch,
        mutation_sequence: proto.mutation_sequence,
        mutation_id: proto.mutation_id,
        operation_family: proto.operation_family,
        writer_family: proto.writer_family,
        target: target_from_proto(proto.target.ok_or_else(|| {
            anyhow!("CoreStore pending mutation finalisation record is missing target")
        })?)?,
        boundary_values: proto
            .boundary_values
            .into_iter()
            .map(boundary_from_proto)
            .collect(),
        landed_bytes: proto
            .landed_bytes
            .into_iter()
            .map(landed_from_proto)
            .collect(),
        state: proto.state,
        result: proto.result.map(result_from_proto).transpose()?,
        finalised_at_unix_nanos: proto.finalised_at_unix_nanos,
    })
}

pub(super) fn encode_materialisation_cursor_row(sequence: u64) -> Result<Vec<u8>> {
    encode_deterministic(CoreMaterialisationCursorRowProto {
        common: Some(core_meta_committed_row_common(
            "system",
            root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
            sequence,
            format!("materialisation-cursor:{sequence}"),
            0,
        )),
        schema: CORE_MATERIALISATION_CURSOR_SCHEMA.to_string(),
        sequence,
    })
}

pub(super) fn decode_materialisation_cursor_row(bytes: &[u8]) -> Result<u64> {
    let proto = CoreMaterialisationCursorRowProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "CoreStore materialisation cursor row")?;
    if proto.schema != CORE_MATERIALISATION_CURSOR_SCHEMA {
        bail!("CoreStore materialisation cursor row has invalid schema");
    }
    let common = proto
        .common
        .ok_or_else(|| anyhow!("CoreStore materialisation cursor row missing CoreMeta common"))?;
    if common.realm_id != "system" {
        bail!("CoreStore materialisation cursor row realm mismatch");
    }
    if common.root_key_hash != root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY) {
        bail!("CoreStore materialisation cursor row root mismatch");
    }
    if common.root_generation != proto.sequence {
        bail!("CoreStore materialisation cursor row generation mismatch");
    }
    Ok(proto.sequence)
}

pub(super) fn encode_pending_mutation_hash_input(
    record: &CorePendingMutationRecord,
    payload: &[u8],
) -> Result<Vec<u8>> {
    if payload.len() > CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES {
        bail!(
            "CoreStore pending mutation payload exceeds {} bytes",
            CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES
        );
    }
    let record_proto = encode_pending_mutation_record_proto(record)?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CORE_PENDING_MUTATION_HASH_INPUT_MAGIC);
    write_u32_le(&mut bytes, record_proto.len())?;
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&record_proto);
    bytes.extend_from_slice(payload);
    let crc = crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

pub(super) fn encode_pending_mutation_record_proto(
    record: &CorePendingMutationRecord,
) -> Result<Vec<u8>> {
    encode_deterministic(record_to_proto(record)?)
}

pub(super) fn finalisation_result_hash(
    result: &Option<CorePendingMutationFinalisationResult>,
) -> Result<String> {
    let bytes = match result {
        Some(value) => encode_deterministic(result_to_proto(value)?)?,
        None => b"anvil.pending_mutation.finalisation_result.none.v1".to_vec(),
    };
    Ok(domain_hash_bytes(
        "anvil.pending_mutation.finalisation_result.v1",
        &bytes,
    ))
}

pub(super) fn encode_admission_commit_certificate(
    certificate: &CoreAdmissionCommitCertificate,
) -> Result<Vec<u8>> {
    let proto = CoreAdmissionCommitCertificateProto {
        common: Some(core_meta_committed_row_common(
            "system",
            certificate.attempt_id.root_key_hash.clone(),
            certificate.attempt_id.source_mutation_sequence,
            certificate.attempt_id.mutation_id.clone(),
            certificate.committed_at_unix_nanos,
        )),
        schema: certificate.schema.clone(),
        attempt_id: Some(admission_attempt_id_to_proto(&certificate.attempt_id)),
        local_receipt: Some(local_admission_receipt_to_proto(&certificate.local_receipt)),
        committed_at_unix_nanos: certificate.committed_at_unix_nanos,
        signed_payload_hash: certificate.signed_payload_hash.clone(),
        source_signature: certificate.source_signature.clone(),
        core_meta_commit_certificate_hash: certificate.core_meta_commit_certificate_hash.clone(),
        certificate_persist_receipt_hashes: certificate.certificate_persist_receipt_hashes.clone(),
    };
    encode_deterministic(proto)
}

pub(super) fn decode_admission_commit_certificate(
    bytes: &[u8],
) -> Result<CoreAdmissionCommitCertificate> {
    let proto = CoreAdmissionCommitCertificateProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "CoreStore admission commit certificate")?;
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore admission commit certificate missing CoreMeta common"))?;
    Ok(CoreAdmissionCommitCertificate {
        schema: proto.schema,
        attempt_id: admission_attempt_id_from_proto(proto.attempt_id.ok_or_else(|| {
            anyhow!("CoreStore admission commit certificate is missing attempt_id")
        })?),
        local_receipt: local_admission_receipt_from_proto(proto.local_receipt.ok_or_else(
            || anyhow!("CoreStore admission commit certificate is missing local_receipt"),
        )?)?,
        core_meta_commit_certificate_hash: proto.core_meta_commit_certificate_hash,
        certificate_persist_receipt_hashes: proto.certificate_persist_receipt_hashes,
        committed_at_unix_nanos: proto.committed_at_unix_nanos,
        signed_payload_hash: proto.signed_payload_hash,
        source_signature: proto.source_signature,
    })
}

pub(super) fn build_local_pending_mutation_commit_certificate(
    record: &CorePendingMutationRecord,
    pending_mutation_hash_input: &[u8],
    committed_at_unix_nanos: u64,
    local_fsync_sequence: u64,
    metadata_replica_node_ids: Vec<String>,
    core_meta_commit_certificate_hash: String,
    certificate_persist_receipt_hashes: Vec<String>,
) -> Result<CoreAdmissionCommitCertificate> {
    let attempt_id =
        admission_attempt_id_with_metadata_replicas(record, metadata_replica_node_ids)?;
    let landed_byte_hashes = record
        .landed_bytes
        .iter()
        .map(|landed| landed.sha256.clone())
        .collect::<Vec<_>>();
    let descriptor_hashes = record
        .landed_bytes
        .iter()
        .map(landed_byte_descriptor_hash)
        .collect::<Result<Vec<_>>>()?;
    let mut local_receipt = CoreLocalAdmissionReceipt {
        schema: CORE_LOCAL_ADMISSION_RECEIPT_SCHEMA.to_string(),
        attempt_id: attempt_id.clone(),
        landed_byte_hashes,
        descriptor_hashes,
        pending_mutation_hash: domain_hash_bytes(
            "anvil.admission.pending_mutation_hash_input.v1",
            pending_mutation_hash_input,
        ),
        local_metadata_fsync_sequence: local_fsync_sequence,
        local_landed_fsync_sequence: local_fsync_sequence,
        signed_payload_hash: String::new(),
        source_signature: Vec::new(),
    };
    local_receipt.signed_payload_hash = local_admission_receipt_payload_hash(&local_receipt)?;
    let mut certificate = CoreAdmissionCommitCertificate {
        schema: CORE_ADMISSION_COMMIT_CERTIFICATE_SCHEMA.to_string(),
        attempt_id,
        local_receipt,
        core_meta_commit_certificate_hash,
        certificate_persist_receipt_hashes,
        committed_at_unix_nanos,
        signed_payload_hash: String::new(),
        source_signature: Vec::new(),
    };
    certificate.signed_payload_hash = admission_commit_certificate_payload_hash(&certificate)?;
    Ok(certificate)
}

pub(super) fn validate_local_pending_mutation_commit_certificate(
    certificate: &CoreAdmissionCommitCertificate,
) -> Result<()> {
    if certificate.schema != CORE_ADMISSION_COMMIT_CERTIFICATE_SCHEMA {
        bail!("CoreStore admission commit certificate has invalid schema");
    }
    if certificate.local_receipt.schema != CORE_LOCAL_ADMISSION_RECEIPT_SCHEMA {
        bail!("CoreStore local admission receipt has invalid schema");
    }
    if certificate.attempt_id != certificate.local_receipt.attempt_id {
        bail!("CoreStore admission commit certificate attempt mismatch");
    }
    validate_hash(
        &certificate.attempt_id.root_key_hash,
        "admission attempt root key hash",
    )?;
    validate_hash(
        &certificate.attempt_id.request_hash,
        "admission attempt request hash",
    )?;
    validate_logical_id(
        &certificate.attempt_id.source_node_id,
        "admission source node id",
    )?;
    if certificate.attempt_id.admission_profile.is_empty()
        || certificate.attempt_id.admission_profile_epoch == 0
    {
        bail!("CoreStore admission attempt profile must be present");
    }
    if certificate.local_receipt.local_metadata_fsync_sequence == 0 {
        bail!("CoreStore local admission receipt metadata fsync sequence must be nonzero");
    }
    if !certificate.local_receipt.landed_byte_hashes.is_empty()
        && certificate.local_receipt.local_landed_fsync_sequence == 0
    {
        bail!("CoreStore local admission receipt landed fsync sequence must be nonzero");
    }
    if certificate.local_receipt.landed_byte_hashes.len()
        != certificate.local_receipt.descriptor_hashes.len()
    {
        bail!("CoreStore local admission receipt descriptor count mismatch");
    }
    validate_hash(
        &certificate.local_receipt.pending_mutation_hash,
        "admission pending mutation hash input hash",
    )?;
    for hash in &certificate.local_receipt.landed_byte_hashes {
        validate_hash(hash, "admission landed byte hash")?;
    }
    for hash in &certificate.local_receipt.descriptor_hashes {
        validate_hash(hash, "admission landed descriptor hash")?;
    }
    validate_hash(
        &certificate.core_meta_commit_certificate_hash,
        "admission CoreMeta commit certificate hash",
    )?;
    let mut seen_certificate_persist_receipt_hashes = std::collections::BTreeSet::new();
    for receipt_hash in &certificate.certificate_persist_receipt_hashes {
        validate_hash(
            receipt_hash,
            "admission CoreMeta certificate persist receipt hash",
        )?;
        if !seen_certificate_persist_receipt_hashes.insert(receipt_hash.clone()) {
            bail!("CoreStore admission CoreMeta certificate persist receipt hash is duplicated");
        }
    }
    let expected_local = local_admission_receipt_payload_hash(&certificate.local_receipt)?;
    if certificate.local_receipt.signed_payload_hash != expected_local {
        bail!("CoreStore local admission receipt payload hash mismatch");
    }
    if certificate.local_receipt.source_signature.is_empty() {
        bail!("CoreStore local admission receipt signature must not be empty");
    }
    let expected_certificate = admission_commit_certificate_payload_hash(certificate)?;
    if certificate.signed_payload_hash != expected_certificate {
        bail!("CoreStore admission commit certificate payload hash mismatch");
    }
    if certificate.source_signature.is_empty() {
        bail!("CoreStore admission commit certificate signature must not be empty");
    }
    Ok(())
}

pub(super) fn admission_attempt_id(
    record: &CorePendingMutationRecord,
) -> Result<CoreAdmissionAttemptId> {
    admission_attempt_id_with_metadata_replicas(record, Vec::new())
}

pub(super) fn admission_attempt_id_with_metadata_replicas(
    record: &CorePendingMutationRecord,
    metadata_replica_node_ids: Vec<String>,
) -> Result<CoreAdmissionAttemptId> {
    Ok(CoreAdmissionAttemptId {
        mutation_id: record.mutation_id.clone(),
        root_anchor_key: CORE_TRANSACTION_ROOT_ANCHOR_KEY.to_string(),
        root_key_hash: root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
        source_node_id: record.node_id.clone(),
        source_mutation_epoch: record.mutation_epoch,
        source_mutation_sequence: record.sequence,
        request_hash: admission_request_hash(record)?,
        admission_profile: CORE_META_ADMISSION_PROFILE.to_string(),
        admission_profile_epoch: CORE_META_ADMISSION_PROFILE_EPOCH,
        metadata_replica_node_ids,
    })
}

fn admission_request_hash(record: &CorePendingMutationRecord) -> Result<String> {
    let descriptor_hashes = record
        .landed_bytes
        .iter()
        .map(landed_byte_descriptor_hash)
        .collect::<Result<Vec<_>>>()?;
    let request = CoreAdmissionRequestHashProto {
        operation_family: record.operation_family.clone(),
        writer_family: record.writer_family.clone(),
        target: Some(target_to_proto(&record.target)?),
        precondition_fingerprints: record.precondition_fingerprints.clone(),
        boundary_values: record
            .boundary_values
            .iter()
            .map(boundary_to_proto)
            .collect(),
        body_descriptor_hashes: descriptor_hashes,
        idempotency_key_hash: record.idempotency_key_hash.clone(),
    };
    let bytes = encode_deterministic(request)?;
    Ok(domain_hash_bytes("anvil.request_hash.v1", &bytes))
}

#[derive(Clone, PartialEq, Message)]
struct CoreAdmissionRequestHashProto {
    #[prost(string, tag = "1")]
    operation_family: String,
    #[prost(string, tag = "2")]
    writer_family: String,
    #[prost(message, optional, tag = "3")]
    target: Option<CorePendingMutationTargetProto>,
    #[prost(string, repeated, tag = "4")]
    precondition_fingerprints: Vec<String>,
    #[prost(message, repeated, tag = "5")]
    boundary_values: Vec<CoreBoundaryValueProto>,
    #[prost(string, repeated, tag = "6")]
    body_descriptor_hashes: Vec<String>,
    #[prost(string, optional, tag = "7")]
    idempotency_key_hash: Option<String>,
}

fn landed_byte_descriptor_hash(landed: &CorePendingLandedByte) -> Result<String> {
    validate_hash(&landed.sha256, "landed byte descriptor hash")?;
    Ok(descriptor_hash(&[
        "anvil.landed_byte.descriptor.v1",
        &landed.landing_id,
        &landed.sha256,
        &landed.length.to_string(),
        "application/octet-stream",
    ]))
}

fn local_admission_receipt_payload_hash(receipt: &CoreLocalAdmissionReceipt) -> Result<String> {
    let mut proto = local_admission_receipt_to_proto(receipt);
    proto.signed_payload_hash.clear();
    proto.source_signature.clear();
    let bytes = encode_deterministic(proto)?;
    Ok(domain_hash_bytes("anvil.admission.receipt.v1", &bytes))
}

fn admission_commit_certificate_payload_hash(
    certificate: &CoreAdmissionCommitCertificate,
) -> Result<String> {
    let mut local_receipt = local_admission_receipt_to_proto(&certificate.local_receipt);
    local_receipt.source_signature.clear();
    let proto = CoreAdmissionCommitCertificateProto {
        common: Some(core_meta_committed_row_common(
            "system",
            certificate.attempt_id.root_key_hash.clone(),
            certificate.attempt_id.source_mutation_sequence,
            certificate.attempt_id.mutation_id.clone(),
            certificate.committed_at_unix_nanos,
        )),
        schema: certificate.schema.clone(),
        attempt_id: Some(admission_attempt_id_to_proto(&certificate.attempt_id)),
        local_receipt: Some(local_receipt),
        committed_at_unix_nanos: certificate.committed_at_unix_nanos,
        signed_payload_hash: String::new(),
        source_signature: Vec::new(),
        core_meta_commit_certificate_hash: certificate.core_meta_commit_certificate_hash.clone(),
        certificate_persist_receipt_hashes: certificate.certificate_persist_receipt_hashes.clone(),
    };
    let bytes = encode_deterministic(proto)?;
    Ok(domain_hash_bytes(
        "anvil.admission.commit_certificate.v1",
        &bytes,
    ))
}

fn record_to_proto(record: &CorePendingMutationRecord) -> Result<CorePendingMutationRecordProto> {
    Ok(CorePendingMutationRecordProto {
        schema: record.schema.clone(),
        node_id: record.node_id.clone(),
        mutation_epoch: record.mutation_epoch,
        sequence: record.sequence,
        mutation_id: record.mutation_id.clone(),
        idempotency_key_hash: record.idempotency_key_hash.clone(),
        anvil_storage_tenant_id: record.anvil_storage_tenant_id.clone(),
        authz_scope: Some(authz_scope_to_proto(&record.authz_scope)),
        operation_family: record.operation_family.clone(),
        writer_family: record.writer_family.clone(),
        target: Some(target_to_proto(&record.target)?),
        precondition_fingerprints: record.precondition_fingerprints.clone(),
        boundary_values: record
            .boundary_values
            .iter()
            .map(boundary_to_proto)
            .collect(),
        landed_bytes: record.landed_bytes.iter().map(landed_to_proto).collect(),
        created_at_unix_nanos: record.created_at_unix_nanos,
    })
}

fn record_from_proto(proto: CorePendingMutationRecordProto) -> Result<CorePendingMutationRecord> {
    Ok(CorePendingMutationRecord {
        schema: proto.schema,
        node_id: proto.node_id,
        mutation_epoch: proto.mutation_epoch,
        sequence: proto.sequence,
        mutation_id: proto.mutation_id,
        idempotency_key_hash: proto.idempotency_key_hash,
        anvil_storage_tenant_id: proto.anvil_storage_tenant_id,
        authz_scope: authz_scope_from_proto(
            proto.authz_scope.ok_or_else(|| {
                anyhow!("CoreStore pending mutation record is missing authz_scope")
            })?,
        ),
        operation_family: proto.operation_family,
        writer_family: proto.writer_family,
        target: target_from_proto(
            proto
                .target
                .ok_or_else(|| anyhow!("CoreStore pending mutation record is missing target"))?,
        )?,
        precondition_fingerprints: proto.precondition_fingerprints,
        boundary_values: proto
            .boundary_values
            .into_iter()
            .map(boundary_from_proto)
            .collect(),
        landed_bytes: proto
            .landed_bytes
            .into_iter()
            .map(landed_from_proto)
            .collect(),
        created_at_unix_nanos: proto.created_at_unix_nanos,
    })
}

fn authz_scope_to_proto(value: &CorePendingAuthzScope) -> CorePendingAuthzScopeProto {
    CorePendingAuthzScopeProto {
        realm_id: value.realm_id.clone(),
        revision: value.revision.clone(),
    }
}

fn authz_scope_from_proto(value: CorePendingAuthzScopeProto) -> CorePendingAuthzScope {
    CorePendingAuthzScope {
        realm_id: value.realm_id,
        revision: value.revision,
    }
}

fn compression_to_proto(value: &CoreCompressionDescriptor) -> CoreCompressionDescriptorProto {
    CoreCompressionDescriptorProto {
        algorithm: value.algorithm.clone(),
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

fn compression_from_proto(value: CoreCompressionDescriptorProto) -> CoreCompressionDescriptor {
    CoreCompressionDescriptor {
        algorithm: value.algorithm,
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id,
        descriptor_hash: value.descriptor_hash,
    }
}

fn target_to_proto(value: &CorePendingMutationTarget) -> Result<CorePendingMutationTargetProto> {
    use core_pending_mutation_target_proto::Kind;
    let kind = match value {
        CorePendingMutationTarget::ObjectPut {
            logical_name,
            region_id,
            erasure_profile_id,
            encryption,
            block_plain_hash,
            object_hash,
            object_logical_size,
            compression,
            writer_generation,
            block_ordinal,
        } => Kind::ObjectPut(CoreObjectPutTargetProto {
            logical_name: logical_name.clone(),
            region_id: region_id.clone(),
            erasure_profile_id: erasure_profile_id.clone(),
            encryption: encryption.clone(),
            block_plain_hash: block_plain_hash.clone(),
            object_hash: object_hash.clone(),
            object_logical_size: *object_logical_size,
            compression: Some(compression_to_proto(compression)),
            writer_generation: *writer_generation,
            block_ordinal: *block_ordinal,
        }),
        CorePendingMutationTarget::StreamAppend {
            stream_id,
            partition_id,
            record_kind,
            transaction_id,
        } => Kind::StreamAppend(CoreStreamAppendTargetProto {
            stream_id: stream_id.clone(),
            partition_id: partition_id.clone(),
            record_kind: record_kind.clone(),
            transaction_id: transaction_id.clone(),
        }),
        CorePendingMutationTarget::MutationBatch {
            transaction_id,
            scope_partition,
            operation_count,
        } => Kind::MutationBatch(CoreMutationBatchTargetProto {
            transaction_id: transaction_id.clone(),
            scope_partition: scope_partition.clone(),
            operation_count: *operation_count,
        }),
    };
    Ok(CorePendingMutationTargetProto { kind: Some(kind) })
}

fn target_from_proto(value: CorePendingMutationTargetProto) -> Result<CorePendingMutationTarget> {
    use core_pending_mutation_target_proto::Kind;
    let kind = value
        .kind
        .ok_or_else(|| anyhow!("CoreStore pending mutation target is empty"))?;
    Ok(match kind {
        Kind::ObjectPut(value) => CorePendingMutationTarget::ObjectPut {
            logical_name: value.logical_name,
            region_id: value.region_id,
            erasure_profile_id: value.erasure_profile_id,
            encryption: value.encryption,
            block_plain_hash: value.block_plain_hash,
            object_hash: value.object_hash,
            object_logical_size: value.object_logical_size,
            compression: compression_from_proto(value.compression.ok_or_else(|| {
                anyhow!("CoreStore object.put pending target is missing compression descriptor")
            })?),
            writer_generation: value.writer_generation,
            block_ordinal: value.block_ordinal,
        },
        Kind::StreamAppend(value) => CorePendingMutationTarget::StreamAppend {
            stream_id: value.stream_id,
            partition_id: value.partition_id,
            record_kind: value.record_kind,
            transaction_id: value.transaction_id,
        },
        Kind::MutationBatch(value) => CorePendingMutationTarget::MutationBatch {
            transaction_id: value.transaction_id,
            scope_partition: value.scope_partition,
            operation_count: value.operation_count,
        },
    })
}

fn result_to_proto(
    value: &CorePendingMutationFinalisationResult,
) -> Result<CorePendingMutationFinalisationResultProto> {
    let kind = match value {
        CorePendingMutationFinalisationResult::StreamStateLocator(locator) => {
            let locator_bytes = encode_manifest_locator_proto(locator)
                .with_context(|| "encode CoreStore stream state locator")?;
            core_pending_mutation_finalisation_result_proto::Kind::StreamStateLocator(locator_bytes)
        }
    };
    Ok(CorePendingMutationFinalisationResultProto { kind: Some(kind) })
}

fn result_from_proto(
    value: CorePendingMutationFinalisationResultProto,
) -> Result<CorePendingMutationFinalisationResult> {
    let kind = value
        .kind
        .ok_or_else(|| anyhow!("CoreStore pending mutation finalisation result is empty"))?;
    Ok(match kind {
        core_pending_mutation_finalisation_result_proto::Kind::StreamStateLocator(
            locator_bytes,
        ) => CorePendingMutationFinalisationResult::StreamStateLocator(
            decode_manifest_locator_proto(&locator_bytes)
                .with_context(|| "decode CoreStore stream state locator")?,
        ),
    })
}

fn boundary_to_proto(value: &CoreBoundaryValue) -> CoreBoundaryValueProto {
    CoreBoundaryValueProto {
        schema_generation: value.schema_generation,
        name: value.name.clone(),
        value_type: value.value_type.clone(),
        value: value.value.clone(),
        categories: value.categories.clone(),
        source_kind: value.source_kind.clone(),
        required: value.required,
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity.clone(),
        compaction_scope: value.compaction_scope.clone(),
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds.clone(),
    }
}

fn boundary_from_proto(value: CoreBoundaryValueProto) -> CoreBoundaryValue {
    CoreBoundaryValue {
        schema_generation: value.schema_generation,
        name: value.name,
        value_type: value.value_type,
        value: value.value,
        categories: value.categories,
        source_kind: value.source_kind,
        required: value.required,
        max_values_per_block: value.max_values_per_block,
        placement_affinity: value.placement_affinity,
        compaction_scope: value.compaction_scope,
        shared_ranges_allowed: value.shared_ranges_allowed,
        shared_record_kinds: value.shared_record_kinds,
    }
}

fn landed_to_proto(value: &CorePendingLandedByte) -> CorePendingLandedByteProto {
    CorePendingLandedByteProto {
        sha256: value.sha256.clone(),
        length: value.length,
        landing_id: value.landing_id.clone(),
        relative_path: value.relative_path.clone(),
    }
}

fn landed_from_proto(value: CorePendingLandedByteProto) -> CorePendingLandedByte {
    CorePendingLandedByte {
        sha256: value.sha256,
        length: value.length,
        landing_id: value.landing_id,
        relative_path: value.relative_path,
    }
}

fn admission_attempt_id_to_proto(value: &CoreAdmissionAttemptId) -> CoreAdmissionAttemptIdProto {
    CoreAdmissionAttemptIdProto {
        mutation_id: value.mutation_id.clone(),
        root_anchor_key: value.root_anchor_key.clone(),
        root_key_hash: value.root_key_hash.clone(),
        source_node_id: value.source_node_id.clone(),
        source_mutation_epoch: value.source_mutation_epoch,
        source_mutation_sequence: value.source_mutation_sequence,
        request_hash: value.request_hash.clone(),
        admission_profile: value.admission_profile.clone(),
        admission_profile_epoch: value.admission_profile_epoch,
        metadata_replica_node_ids: value.metadata_replica_node_ids.clone(),
    }
}

fn admission_attempt_id_from_proto(value: CoreAdmissionAttemptIdProto) -> CoreAdmissionAttemptId {
    CoreAdmissionAttemptId {
        mutation_id: value.mutation_id,
        root_anchor_key: value.root_anchor_key,
        root_key_hash: value.root_key_hash,
        source_node_id: value.source_node_id,
        source_mutation_epoch: value.source_mutation_epoch,
        source_mutation_sequence: value.source_mutation_sequence,
        request_hash: value.request_hash,
        admission_profile: value.admission_profile,
        admission_profile_epoch: value.admission_profile_epoch,
        metadata_replica_node_ids: value.metadata_replica_node_ids,
    }
}

fn local_admission_receipt_to_proto(
    value: &CoreLocalAdmissionReceipt,
) -> CoreLocalAdmissionReceiptProto {
    CoreLocalAdmissionReceiptProto {
        schema: value.schema.clone(),
        attempt_id: Some(admission_attempt_id_to_proto(&value.attempt_id)),
        landed_byte_hashes: value.landed_byte_hashes.clone(),
        descriptor_hashes: value.descriptor_hashes.clone(),
        pending_mutation_hash: value.pending_mutation_hash.clone(),
        local_metadata_fsync_sequence: value.local_metadata_fsync_sequence,
        local_landed_fsync_sequence: value.local_landed_fsync_sequence,
        signed_payload_hash: value.signed_payload_hash.clone(),
        source_signature: value.source_signature.clone(),
    }
}

fn local_admission_receipt_from_proto(
    value: CoreLocalAdmissionReceiptProto,
) -> Result<CoreLocalAdmissionReceipt> {
    Ok(CoreLocalAdmissionReceipt {
        schema: value.schema,
        attempt_id: admission_attempt_id_from_proto(
            value
                .attempt_id
                .ok_or_else(|| anyhow!("CoreStore admission receipt is missing attempt_id"))?,
        ),
        landed_byte_hashes: value.landed_byte_hashes,
        descriptor_hashes: value.descriptor_hashes,
        pending_mutation_hash: value.pending_mutation_hash,
        local_metadata_fsync_sequence: value.local_metadata_fsync_sequence,
        local_landed_fsync_sequence: value.local_landed_fsync_sequence,
        signed_payload_hash: value.signed_payload_hash,
        source_signature: value.source_signature,
    })
}

fn encode_deterministic(message: impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_round_trips(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    let mut canonical = Vec::new();
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} is not deterministically encoded");
    }
    Ok(())
}

fn write_u32_le(out: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| anyhow!("CoreStore frame length exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn root_key_hash(root_anchor_key: &str) -> String {
    descriptor_hash(&["anvil.root.key.v1", root_anchor_key])
}

fn descriptor_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn domain_hash_bytes(domain: &str, bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_le_bytes());
    hasher.update(domain.as_bytes());
    hasher.update((bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn validate_hash(value: &str, label: &str) -> Result<()> {
    let Some((algorithm, hex)) = value.split_once(':') else {
        bail!("CoreStore {label} must be an algorithm:hex hash");
    };
    if !matches!(algorithm, "sha256" | "blake3") {
        bail!("CoreStore {label} uses unsupported hash algorithm {algorithm}");
    }
    if hex.len() != 64 || !hex.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore {label} has invalid hash bytes");
    }
    Ok(())
}

fn validate_logical_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        bail!("CoreStore {label} must not be empty");
    }
    if value.contains('\0') || value.contains("..") {
        bail!("CoreStore {label} contains an invalid component");
    }
    Ok(())
}
