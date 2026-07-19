use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreStore, TABLE_MESH_PARTITION_ROW, core_meta_committed_row_common, core_meta_payload_digest,
    core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
    encode_deterministic_proto,
};
use crate::storage::Storage;
use anyhow::{Context, Result as AnyhowResult, anyhow};
use prost::Message;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;

pub const CONTROL_STREAM_MAGIC: &[u8; 8] = b"ANVCTL1\0";
pub const CONTROL_STREAM_VERSION: u16 = 1;
pub const CONTROL_STREAM_FIXED_HEADER_LEN: usize = 8 + 2 + 4 + 8 + 4 + 4;
pub const MAX_CONTROL_PROTO_PAYLOAD_LEN: usize = 64 * 1024;
pub const CONTROL_CHECKPOINT_SCHEMA: &str = "anvil.mesh.control_checkpoint.v1";
const CONTROL_STREAM_ID_PREFIX: &str = "mesh_control_stream:";
const CONTROL_CHECKPOINT_ROW_PREFIX: &str = "mesh-control-checkpoint";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ControlStreamFrameError {
    #[error("buffer is too short for {context}: need at least {needed} bytes, got {actual}")]
    TooShort {
        context: &'static str,
        needed: usize,
        actual: usize,
    },
    #[error("invalid control stream frame magic")]
    InvalidMagic,
    #[error("unsupported control stream frame version {0}")]
    UnsupportedVersion(u16),
    #[error("declared length is invalid for {context}")]
    InvalidDeclaredLength { context: &'static str },
    #[error("header CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    HeaderCrc32Mismatch { expected: u32, actual: u32 },
    #[error("payload CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}")]
    PayloadCrc32Mismatch { expected: u32, actual: u32 },
    #[error("control stream sequence must be greater than zero")]
    InvalidSequence,
    #[error("control stream digest is invalid")]
    InvalidDigest,
    #[error("control stream frame header metadata is missing field {field}")]
    MissingHeaderField { field: &'static str },
    #[error("control stream frame header protobuf is invalid")]
    InvalidHeaderProtobuf,
    #[error("control stream frame header protobuf is not deterministic")]
    NonDeterministicHeaderProtobuf,
    #[error("control stream protobuf payload is too large: {actual} bytes exceeds {max} bytes")]
    PayloadTooLarge { actual: usize, max: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControlStreamSequence(u64);

impl ControlStreamSequence {
    pub fn new(value: u64) -> std::result::Result<Self, ControlStreamFrameError> {
        if value == 0 {
            return Err(ControlStreamFrameError::InvalidSequence);
        }
        Ok(Self(value))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for ControlStreamSequence {
    type Error = ControlStreamFrameError;

    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<ControlStreamSequence> for u64 {
    fn from(sequence: ControlStreamSequence) -> Self {
        sequence.0
    }
}

impl Serialize for ControlStreamSequence {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for ControlStreamSequence {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ControlRecordDigest(String);

impl ControlRecordDigest {
    pub fn new(value: impl Into<String>) -> std::result::Result<Self, ControlStreamFrameError> {
        let value = value.into();
        if !is_valid_digest(&value) {
            return Err(ControlStreamFrameError::InvalidDigest);
        }
        Ok(Self(value))
    }

    pub fn blake3(bytes: &[u8]) -> Self {
        Self(format!("blake3:{}", blake3::hash(bytes).to_hex()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ControlRecordDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for ControlRecordDigest {
    type Error = ControlStreamFrameError;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ControlRecordDigest {
    type Error = ControlStreamFrameError;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for ControlRecordDigest {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ControlRecordDigest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlStreamPosition {
    pub sequence: ControlStreamSequence,
    pub digest: ControlRecordDigest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlFrameMetadata {
    pub sequence: ControlStreamSequence,
    pub record_digest: ControlRecordDigest,
}

impl From<ControlFrameMetadata> for ControlStreamPosition {
    fn from(metadata: ControlFrameMetadata) -> Self {
        Self {
            sequence: metadata.sequence,
            digest: metadata.record_digest,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamFrame {
    pub header_proto: Vec<u8>,
    pub payload_proto: Vec<u8>,
}

impl ControlStreamFrame {
    pub fn new(header_proto: Vec<u8>, payload_proto: Vec<u8>) -> Self {
        Self {
            header_proto,
            payload_proto,
        }
    }

    pub fn metadata(&self) -> std::result::Result<ControlFrameMetadata, ControlStreamFrameError> {
        metadata_from_header_proto(&self.header_proto)
    }

    pub fn encoded_len(&self) -> std::result::Result<usize, ControlStreamFrameError> {
        encoded_frame_len(self.header_proto.len(), self.payload_proto.len())
    }

    pub fn encode(&self) -> std::result::Result<Vec<u8>, ControlStreamFrameError> {
        encode_control_stream_frame(&self.header_proto, &self.payload_proto)
    }

    pub fn decode(input: &[u8]) -> std::result::Result<(Self, usize), ControlStreamFrameError> {
        decode_control_stream_frame(input)
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct ControlFrameHeaderProto {
    #[prost(string, tag = "1")]
    pub schema: String,
    #[prost(string, tag = "2")]
    pub mesh_id: String,
    #[prost(string, tag = "3")]
    pub stream_family: String,
    #[prost(string, tag = "4")]
    pub partition: String,
    #[prost(uint64, tag = "5")]
    pub sequence: u64,
    #[prost(string, tag = "6")]
    pub record_key: String,
    #[prost(string, tag = "7")]
    pub operation: String,
    #[prost(uint64, optional, tag = "8")]
    pub expected_generation: Option<u64>,
    #[prost(uint64, tag = "9")]
    pub new_generation: u64,
    #[prost(string, tag = "10")]
    pub writer_node_id: String,
    #[prost(uint64, tag = "11")]
    pub writer_fence: u64,
    #[prost(string, optional, tag = "12")]
    pub idempotency_key: Option<String>,
    #[prost(string, tag = "13")]
    pub record_digest: String,
    #[prost(string, tag = "14")]
    pub created_at: String,
    #[prost(uint64, tag = "15")]
    pub byte_offset: u64,
}

pub struct ControlMutationHeaderInput<'a> {
    pub schema: &'a str,
    pub mesh_id: &'a str,
    pub stream_family: &'a str,
    pub partition: &'a str,
    pub sequence: ControlStreamSequence,
    pub record_key: &'a str,
    pub operation: &'a str,
    pub expected_generation: Option<u64>,
    pub new_generation: u64,
    pub writer_node_id: &'a str,
    pub writer_fence: u64,
    pub idempotency_key: Option<&'a str>,
    pub record_digest: &'a ControlRecordDigest,
    pub created_at: &'a str,
    pub byte_offset: u64,
}

pub fn encode_control_mutation_header(input: ControlMutationHeaderInput<'_>) -> Vec<u8> {
    ControlFrameHeaderProto {
        schema: input.schema.to_string(),
        mesh_id: input.mesh_id.to_string(),
        stream_family: input.stream_family.to_string(),
        partition: input.partition.to_string(),
        sequence: input.sequence.get(),
        record_key: input.record_key.to_string(),
        operation: input.operation.to_string(),
        expected_generation: input.expected_generation,
        new_generation: input.new_generation,
        writer_node_id: input.writer_node_id.to_string(),
        writer_fence: input.writer_fence,
        idempotency_key: input.idempotency_key.map(str::to_string),
        record_digest: input.record_digest.as_str().to_string(),
        created_at: input.created_at.to_string(),
        byte_offset: input.byte_offset,
    }
    .encode_to_vec()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamLogRecord {
    pub offset: u64,
    pub encoded_len: usize,
    pub metadata: ControlFrameMetadata,
    pub frame: ControlStreamFrame,
}

impl ControlStreamLogRecord {
    pub fn position(&self) -> ControlStreamPosition {
        self.metadata.clone().into()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialFinalFrame {
    pub offset: u64,
    pub expected_len: usize,
    pub actual_len: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlStreamLog {
    pub records: Vec<ControlStreamLogRecord>,
    pub complete_len: u64,
    pub partial_final_frame: Option<PartialFinalFrame>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamAppend {
    pub offset: u64,
    pub encoded_len: usize,
    pub position: ControlStreamPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamAppendCursor {
    pub sequence: ControlStreamSequence,
    pub byte_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamLogPage {
    pub records: Vec<ControlStreamLogRecord>,
    pub next_sequence: u64,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlStreamPartitionPage {
    pub partitions: Vec<String>,
    pub next_stream_id: Option<String>,
}

mod store;

pub use store::{
    ControlStreamCurrentPage, ControlStreamCurrentRecord, append_control_stream_frame,
    control_stream_append_cursor, latest_projected_record_from_control_stream,
    list_control_stream_partitions_page, list_current_control_stream_records_page,
    read_control_stream_page,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlCheckpointRecord {
    pub schema: String,
    pub mesh_id: String,
    pub region: String,
    pub stream_family: String,
    pub partition: String,
    pub last_sequence: ControlStreamSequence,
    pub last_digest: ControlRecordDigest,
    pub updated_at: String,
}

impl ControlCheckpointRecord {
    pub fn new(
        mesh_id: impl Into<String>,
        region: impl Into<String>,
        stream_family: impl Into<String>,
        partition: impl Into<String>,
        last_sequence: ControlStreamSequence,
        last_digest: ControlRecordDigest,
        updated_at: impl Into<String>,
    ) -> Self {
        Self {
            schema: CONTROL_CHECKPOINT_SCHEMA.to_string(),
            mesh_id: mesh_id.into(),
            region: region.into(),
            stream_family: stream_family.into(),
            partition: partition.into(),
            last_sequence,
            last_digest,
            updated_at: updated_at.into(),
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct ControlCheckpointProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    mesh_id: String,
    #[prost(string, tag = "4")]
    region: String,
    #[prost(string, tag = "5")]
    stream_family: String,
    #[prost(string, tag = "6")]
    partition: String,
    #[prost(uint64, tag = "7")]
    last_sequence: u64,
    #[prost(string, tag = "8")]
    last_digest: String,
    #[prost(string, tag = "9")]
    updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlProjectionRecord {
    pub record_key: String,
    pub generation: u64,
    pub payload_json: Vec<u8>,
    pub deleted: bool,
}

impl ControlProjectionRecord {
    pub fn new(
        record_key: impl Into<String>,
        generation: u64,
        payload_json: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            record_key: record_key.into(),
            generation,
            payload_json: payload_json.into(),
            deleted: false,
        }
    }

    pub fn tombstone(record_key: impl Into<String>, generation: u64) -> Self {
        Self {
            record_key: record_key.into(),
            generation,
            payload_json: Vec::new(),
            deleted: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlProjectionDiagnostic {
    pub stream_family: String,
    pub partition: String,
    pub record_key: String,
    pub severity: &'static str,
    pub code: &'static str,
    pub message: String,
    pub stream_sequence: Option<u64>,
    pub stream_generation: Option<u64>,
    pub stream_digest: Option<String>,
    pub projection_generation: Option<u64>,
    pub projection_digest: Option<String>,
    pub repair_safe: bool,
    pub proposed_action: &'static str,
}

pub async fn diagnose_control_stream_projection(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    projected_records: &[ControlProjectionRecord],
) -> AnyhowResult<Vec<ControlProjectionDiagnostic>> {
    let mut diagnostics = Vec::new();
    let mut latest_by_key = BTreeMap::new();
    let mut expected_sequence = 1_u64;
    let mut after_sequence = 0;
    loop {
        let page = read_control_stream_page(storage, stream_family, partition, after_sequence, 512)
            .await?;
        for record in &page.records {
            let sequence = record.metadata.sequence.get();
            if sequence != expected_sequence {
                diagnostics.push(ControlProjectionDiagnostic {
                    stream_family: stream_family.to_string(),
                    partition: partition.to_string(),
                    record_key: String::new(),
                    severity: "error",
                    code: "mesh_control_stream_sequence_gap",
                    message: format!(
                        "control stream {stream_family}/{partition} expected sequence {expected_sequence} but found {sequence}"
                    ),
                    stream_sequence: Some(sequence),
                    stream_generation: None,
                    stream_digest: Some(record.metadata.record_digest.to_string()),
                    projection_generation: None,
                    projection_digest: None,
                    repair_safe: false,
                    proposed_action: "manual_review_rebuild_not_implemented",
                });
            }
            expected_sequence = sequence.saturating_add(1);

            let header = match decode_control_mutation_header(&record.frame.header_proto) {
                Ok(header) => header,
                Err(err) => {
                    diagnostics.push(ControlProjectionDiagnostic {
                        stream_family: stream_family.to_string(),
                        partition: partition.to_string(),
                        record_key: String::new(),
                        severity: "error",
                        code: "mesh_control_stream_header_invalid",
                        message: format!(
                            "control stream {stream_family}/{partition} sequence {sequence} has invalid mutation header: {err}"
                        ),
                        stream_sequence: Some(sequence),
                        stream_generation: None,
                        stream_digest: Some(record.metadata.record_digest.to_string()),
                        projection_generation: None,
                        projection_digest: None,
                        repair_safe: false,
                        proposed_action: "manual_review_rebuild_not_implemented",
                    });
                    continue;
                }
            };

            if header.stream_family != stream_family || header.partition != partition {
                diagnostics.push(ControlProjectionDiagnostic {
                    stream_family: stream_family.to_string(),
                    partition: partition.to_string(),
                    record_key: header.record_key.clone(),
                    severity: "error",
                    code: "mesh_control_stream_header_scope_mismatch",
                    message: format!(
                        "control stream header scope {}/{} does not match path {stream_family}/{partition}",
                        header.stream_family, header.partition
                    ),
                    stream_sequence: Some(sequence),
                    stream_generation: Some(header.new_generation),
                    stream_digest: Some(record.metadata.record_digest.to_string()),
                    projection_generation: None,
                    projection_digest: None,
                    repair_safe: false,
                    proposed_action: "manual_review_rebuild_not_implemented",
                });
            }

            let record_key = header.record_key.clone();
            if record_key.trim().is_empty() {
                diagnostics.push(ControlProjectionDiagnostic {
                    stream_family: stream_family.to_string(),
                    partition: partition.to_string(),
                    record_key: String::new(),
                    severity: "error",
                    code: "mesh_control_stream_record_key_missing",
                    message: format!(
                        "control stream {stream_family}/{partition} sequence {sequence} is missing record_key"
                    ),
                    stream_sequence: Some(sequence),
                    stream_generation: Some(header.new_generation),
                    stream_digest: Some(record.metadata.record_digest.to_string()),
                    projection_generation: None,
                    projection_digest: None,
                    repair_safe: false,
                    proposed_action: "manual_review_rebuild_not_implemented",
                });
                continue;
            }

            let payload_digest = ControlRecordDigest::blake3(&record.frame.payload_proto);
            if payload_digest.as_str() != record.metadata.record_digest.as_str() {
                diagnostics.push(ControlProjectionDiagnostic {
                    stream_family: stream_family.to_string(),
                    partition: partition.to_string(),
                    record_key: record_key.clone(),
                    severity: "error",
                    code: "mesh_control_stream_digest_mismatch",
                    message: format!(
                        "control stream {stream_family}/{partition} sequence {sequence} payload digest does not match header digest"
                    ),
                    stream_sequence: Some(sequence),
                    stream_generation: Some(header.new_generation),
                    stream_digest: Some(record.metadata.record_digest.to_string()),
                    projection_generation: None,
                    projection_digest: Some(payload_digest.to_string()),
                    repair_safe: false,
                    proposed_action: "manual_review_rebuild_not_implemented",
                });
            }

            let payload_json = match control_payload_operator_json(
                stream_family,
                &record_key,
                &record.frame.payload_proto,
            ) {
                Ok(payload_json) => payload_json,
                Err(err) => {
                    diagnostics.push(ControlProjectionDiagnostic {
                        stream_family: stream_family.to_string(),
                        partition: partition.to_string(),
                        record_key: record_key.clone(),
                        severity: "error",
                        code: "mesh_control_stream_payload_invalid",
                        message: format!(
                            "control stream {stream_family}/{partition} sequence {sequence} has invalid protobuf payload: {err}"
                        ),
                        stream_sequence: Some(sequence),
                        stream_generation: Some(header.new_generation),
                        stream_digest: Some(record.metadata.record_digest.to_string()),
                        projection_generation: None,
                        projection_digest: None,
                        repair_safe: false,
                        proposed_action: "manual_review_rebuild_not_implemented",
                    });
                    continue;
                }
            };

            latest_by_key.insert(
                record_key,
                StreamProjectionEntry {
                    sequence,
                    generation: Some(header.new_generation),
                    digest: record.metadata.record_digest.to_string(),
                    operation: header.operation,
                    payload_json,
                },
            );
        }
        if !page.has_more {
            break;
        }
        after_sequence = page.next_sequence;
    }

    let mut projection_by_key = BTreeMap::new();
    for projection in projected_records {
        if projection_by_key
            .insert(projection.record_key.clone(), projection)
            .is_some()
        {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: projection.record_key.clone(),
                severity: "error",
                code: "mesh_control_projection_duplicate_record",
                message: format!(
                    "projection for {stream_family}/{partition} contains duplicate record {}",
                    projection.record_key
                ),
                stream_sequence: None,
                stream_generation: None,
                stream_digest: None,
                projection_generation: Some(projection.generation),
                projection_digest: projection_digest(&projection.payload_json).ok(),
                repair_safe: false,
                proposed_action: "manual_review_rebuild_not_implemented",
            });
        }
    }

    for projection in projected_records {
        let projection_digest = projection_digest(&projection.payload_json)?;
        let Some(stream_entry) = latest_by_key.get(&projection.record_key) else {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: projection.record_key.clone(),
                severity: "error",
                code: "mesh_control_projection_missing_stream_record",
                message: format!(
                    "projection record {} has no matching control stream mutation in {stream_family}/{partition}",
                    projection.record_key
                ),
                stream_sequence: None,
                stream_generation: None,
                stream_digest: None,
                projection_generation: Some(projection.generation),
                projection_digest: Some(projection_digest),
                repair_safe: false,
                proposed_action: "manual_review_rebuild_not_implemented",
            });
            continue;
        };

        if stream_entry.generation != Some(projection.generation) {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: projection.record_key.clone(),
                severity: "error",
                code: "mesh_control_projection_generation_mismatch",
                message: format!(
                    "projection record {} generation {} does not match latest stream generation {:?}",
                    projection.record_key, projection.generation, stream_entry.generation
                ),
                stream_sequence: Some(stream_entry.sequence),
                stream_generation: stream_entry.generation,
                stream_digest: Some(stream_entry.digest.clone()),
                projection_generation: Some(projection.generation),
                projection_digest: Some(projection_digest.clone()),
                repair_safe: true,
                proposed_action: "repair_routing_record_from_control_stream",
            });
        }

        if semantic_json(&stream_entry.payload_json)? != semantic_json(&projection.payload_json)? {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: projection.record_key.clone(),
                severity: "error",
                code: "mesh_control_projection_payload_mismatch",
                message: format!(
                    "projection record {} payload does not match latest control stream payload",
                    projection.record_key
                ),
                stream_sequence: Some(stream_entry.sequence),
                stream_generation: stream_entry.generation,
                stream_digest: Some(stream_entry.digest.clone()),
                projection_generation: Some(projection.generation),
                projection_digest: Some(projection_digest),
                repair_safe: true,
                proposed_action: "repair_routing_record_from_control_stream",
            });
        }
    }

    for (record_key, stream_entry) in latest_by_key {
        if projection_by_key.contains_key(&record_key)
            || is_delete_operation(&stream_entry.operation)
        {
            continue;
        }
        diagnostics.push(ControlProjectionDiagnostic {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            record_key: record_key.clone(),
            severity: "error",
            code: "mesh_control_projection_missing_record",
            message: format!(
                "control stream latest mutation for {record_key} has no projected descriptor"
            ),
            stream_sequence: Some(stream_entry.sequence),
            stream_generation: stream_entry.generation,
            stream_digest: Some(stream_entry.digest),
            projection_generation: None,
            projection_digest: None,
            repair_safe: true,
            proposed_action: "repair_routing_record_from_control_stream",
        });
    }

    Ok(diagnostics)
}

pub async fn write_control_checkpoint(
    storage: &Storage,
    checkpoint: &ControlCheckpointRecord,
) -> AnyhowResult<()> {
    validate_control_checkpoint(checkpoint)?;
    let row_key = control_checkpoint_row_key(
        &checkpoint.region,
        &checkpoint.stream_family,
        &checkpoint.partition,
    )?;
    let store = CoreStore::new(storage.clone()).await?;
    let current_payload = store.read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?;
    if let Some(existing) = current_payload
        .as_deref()
        .map(decode_control_checkpoint_proto)
        .transpose()?
    {
        ensure_control_checkpoint_scope(
            &existing,
            &checkpoint.region,
            &checkpoint.stream_family,
            &checkpoint.partition,
        )?;
        if checkpoint.last_sequence < existing.last_sequence {
            return Err(anyhow!(
                "control checkpoint cannot move backwards for {}/{}/{}: existing sequence {}, new sequence {}",
                checkpoint.region,
                checkpoint.stream_family,
                checkpoint.partition,
                existing.last_sequence.get(),
                checkpoint.last_sequence.get()
            ));
        }
        if checkpoint.last_sequence == existing.last_sequence {
            if checkpoint.last_digest.as_str() != existing.last_digest.as_str() {
                return Err(anyhow!(
                    "ControlStreamDivergence: control checkpoint {}/{}/{} sequence {} has digest {}, existing digest {}",
                    checkpoint.region,
                    checkpoint.stream_family,
                    checkpoint.partition,
                    checkpoint.last_sequence.get(),
                    checkpoint.last_digest,
                    existing.last_digest
                ));
            }
            return Ok(());
        }
    }
    let payload = encode_control_checkpoint_proto(checkpoint)?;
    let partition_id = control_checkpoint_partition_id(
        &checkpoint.region,
        &checkpoint.stream_family,
        &checkpoint.partition,
    )?;
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "mesh-control-checkpoint:{}:{}:{}:{}:{}",
                checkpoint.mesh_id,
                checkpoint.region,
                checkpoint.stream_family,
                checkpoint.partition,
                checkpoint.last_sequence.get()
            ),
            scope_partition: partition_id.clone(),
            committed_by_principal: "mesh-control-checkpoint".to_string(),
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key.clone(),
                expected_payload_hash: current_payload
                    .as_ref()
                    .map(|payload| core_meta_payload_digest(TABLE_MESH_PARTITION_ROW, payload)),
                require_absent: current_payload.is_none(),
                require_present: current_payload.is_some(),
            }],
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id,
                cf: CF_MESH.to_string(),
                table_id: TABLE_MESH_PARTITION_ROW,
                tuple_key: row_key,
                payload,
            }],
        })
        .await?;
    Ok(())
}

pub async fn read_control_checkpoint(
    storage: &Storage,
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<Option<ControlCheckpointRecord>> {
    let row_key = control_checkpoint_row_key(region, stream_family, partition)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(payload) = store.read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)?
    else {
        return Ok(None);
    };
    let checkpoint = decode_control_checkpoint_proto(&payload)?;
    ensure_control_checkpoint_scope(&checkpoint, region, stream_family, partition)?;
    Ok(Some(checkpoint))
}

fn ensure_control_checkpoint_scope(
    checkpoint: &ControlCheckpointRecord,
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<()> {
    if checkpoint.region != region
        || checkpoint.stream_family != stream_family
        || checkpoint.partition != partition
    {
        return Err(anyhow!(
            "control checkpoint path does not match checkpoint body: expected {region}/{stream_family}/{partition}, got {}/{}/{}",
            checkpoint.region,
            checkpoint.stream_family,
            checkpoint.partition
        ));
    }
    Ok(())
}

fn validate_control_checkpoint(checkpoint: &ControlCheckpointRecord) -> AnyhowResult<()> {
    if checkpoint.schema != CONTROL_CHECKPOINT_SCHEMA {
        return Err(anyhow!(
            "control checkpoint schema must be {CONTROL_CHECKPOINT_SCHEMA}"
        ));
    }
    if checkpoint.mesh_id.trim().is_empty() {
        return Err(anyhow!("control checkpoint mesh_id must not be empty"));
    }
    if checkpoint.region.trim().is_empty() {
        return Err(anyhow!("control checkpoint region must not be empty"));
    }
    if checkpoint.stream_family.trim().is_empty() {
        return Err(anyhow!(
            "control checkpoint stream_family must not be empty"
        ));
    }
    if checkpoint.partition.trim().is_empty() {
        return Err(anyhow!("control checkpoint partition must not be empty"));
    }
    if checkpoint.updated_at.trim().is_empty() {
        return Err(anyhow!("control checkpoint updated_at must not be empty"));
    }
    Ok(())
}

fn encode_control_checkpoint_proto(checkpoint: &ControlCheckpointRecord) -> AnyhowResult<Vec<u8>> {
    validate_control_checkpoint(checkpoint)?;
    Ok(encode_deterministic_proto(&ControlCheckpointProto {
        common: Some(core_meta_committed_row_common(
            "mesh",
            core_meta_root_key_hash(&format!(
                "mesh/control-checkpoint/{}/{}/{}",
                checkpoint.region, checkpoint.stream_family, checkpoint.partition
            )),
            checkpoint.last_sequence.get(),
            format!(
                "mesh-control-checkpoint:{}:{}:{}:{}",
                checkpoint.mesh_id,
                checkpoint.region,
                checkpoint.stream_family,
                checkpoint.partition
            ),
            0,
        )),
        schema: checkpoint.schema.clone(),
        mesh_id: checkpoint.mesh_id.clone(),
        region: checkpoint.region.clone(),
        stream_family: checkpoint.stream_family.clone(),
        partition: checkpoint.partition.clone(),
        last_sequence: checkpoint.last_sequence.get(),
        last_digest: checkpoint.last_digest.to_string(),
        updated_at: checkpoint.updated_at.clone(),
    }))
}

fn decode_control_checkpoint_proto(bytes: &[u8]) -> AnyhowResult<ControlCheckpointRecord> {
    let proto = decode_deterministic_proto::<ControlCheckpointProto>(
        bytes,
        "control checkpoint CoreMeta row",
    )?;
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("control checkpoint CoreMeta row is missing common metadata"))?;
    let checkpoint = ControlCheckpointRecord {
        schema: proto.schema,
        mesh_id: proto.mesh_id,
        region: proto.region,
        stream_family: proto.stream_family,
        partition: proto.partition,
        last_sequence: ControlStreamSequence::new(proto.last_sequence)?,
        last_digest: ControlRecordDigest::new(proto.last_digest)?,
        updated_at: proto.updated_at,
    };
    validate_control_checkpoint(&checkpoint)?;
    Ok(checkpoint)
}

pub fn encode_control_stream_frame(
    header_proto: &[u8],
    payload_proto: &[u8],
) -> std::result::Result<Vec<u8>, ControlStreamFrameError> {
    let header_len = u32::try_from(header_proto.len()).map_err(|_| {
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame header_proto",
        }
    })?;
    if payload_proto.len() > MAX_CONTROL_PROTO_PAYLOAD_LEN {
        return Err(ControlStreamFrameError::PayloadTooLarge {
            actual: payload_proto.len(),
            max: MAX_CONTROL_PROTO_PAYLOAD_LEN,
        });
    }
    let payload_len = u64::try_from(payload_proto.len()).map_err(|_| {
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame payload_proto",
        }
    })?;
    let mut out = Vec::with_capacity(encoded_frame_len(header_proto.len(), payload_proto.len())?);
    out.extend_from_slice(CONTROL_STREAM_MAGIC);
    out.extend_from_slice(&CONTROL_STREAM_VERSION.to_be_bytes());
    out.extend_from_slice(&header_len.to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&crc32(header_proto).to_be_bytes());
    out.extend_from_slice(&crc32(payload_proto).to_be_bytes());
    out.extend_from_slice(header_proto);
    out.extend_from_slice(payload_proto);
    Ok(out)
}

pub fn decode_control_stream_frame(
    input: &[u8],
) -> std::result::Result<(ControlStreamFrame, usize), ControlStreamFrameError> {
    let fixed = decode_fixed_header(input)?;
    let header_start = CONTROL_STREAM_FIXED_HEADER_LEN;
    let header_end = header_start.checked_add(fixed.header_len).ok_or(
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame header_proto",
        },
    )?;
    let payload_end = header_end.checked_add(fixed.payload_len).ok_or(
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame payload_proto",
        },
    )?;
    if input.len() < payload_end {
        return Err(ControlStreamFrameError::TooShort {
            context: "control stream frame payload_proto",
            needed: payload_end,
            actual: input.len(),
        });
    }

    let header_proto = input[header_start..header_end].to_vec();
    let payload_proto = input[header_end..payload_end].to_vec();
    if payload_proto.len() > MAX_CONTROL_PROTO_PAYLOAD_LEN {
        return Err(ControlStreamFrameError::PayloadTooLarge {
            actual: payload_proto.len(),
            max: MAX_CONTROL_PROTO_PAYLOAD_LEN,
        });
    }
    let actual_header_crc32 = crc32(&header_proto);
    if actual_header_crc32 != fixed.header_crc32 {
        return Err(ControlStreamFrameError::HeaderCrc32Mismatch {
            expected: fixed.header_crc32,
            actual: actual_header_crc32,
        });
    }
    let actual_payload_crc32 = crc32(&payload_proto);
    if actual_payload_crc32 != fixed.payload_crc32 {
        return Err(ControlStreamFrameError::PayloadCrc32Mismatch {
            expected: fixed.payload_crc32,
            actual: actual_payload_crc32,
        });
    }

    Ok((
        ControlStreamFrame {
            header_proto,
            payload_proto,
        },
        payload_end,
    ))
}

pub fn decode_control_stream_log(
    input: &[u8],
) -> std::result::Result<ControlStreamLog, ControlStreamFrameError> {
    let mut offset = 0usize;
    let mut records = Vec::new();
    while offset < input.len() {
        match decode_control_stream_frame(&input[offset..]) {
            Ok((frame, encoded_len)) => {
                let metadata = frame.metadata()?;
                records.push(ControlStreamLogRecord {
                    offset: offset as u64,
                    encoded_len,
                    metadata,
                    frame,
                });
                offset = offset.checked_add(encoded_len).ok_or(
                    ControlStreamFrameError::InvalidDeclaredLength {
                        context: "control stream log offset",
                    },
                )?;
            }
            Err(ControlStreamFrameError::TooShort { needed, actual, .. }) => {
                return Ok(ControlStreamLog {
                    records,
                    complete_len: offset as u64,
                    partial_final_frame: Some(PartialFinalFrame {
                        offset: offset as u64,
                        expected_len: needed,
                        actual_len: actual,
                    }),
                });
            }
            Err(err) => return Err(err),
        }
    }
    Ok(ControlStreamLog {
        records,
        complete_len: offset as u64,
        partial_final_frame: None,
    })
}

fn decode_fixed_header(
    input: &[u8],
) -> std::result::Result<FixedControlFrameHeader, ControlStreamFrameError> {
    if input.len() < CONTROL_STREAM_FIXED_HEADER_LEN {
        return Err(ControlStreamFrameError::TooShort {
            context: "control stream frame header",
            needed: CONTROL_STREAM_FIXED_HEADER_LEN,
            actual: input.len(),
        });
    }
    if &input[0..8] != CONTROL_STREAM_MAGIC {
        return Err(ControlStreamFrameError::InvalidMagic);
    }
    let version = u16::from_be_bytes(input[8..10].try_into().unwrap());
    if version != CONTROL_STREAM_VERSION {
        return Err(ControlStreamFrameError::UnsupportedVersion(version));
    }
    let header_len = u32::from_be_bytes(input[10..14].try_into().unwrap()) as usize;
    let payload_len = usize::try_from(u64::from_be_bytes(input[14..22].try_into().unwrap()))
        .map_err(|_| ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame payload_proto",
        })?;
    Ok(FixedControlFrameHeader {
        header_len,
        payload_len,
        header_crc32: u32::from_be_bytes(input[22..26].try_into().unwrap()),
        payload_crc32: u32::from_be_bytes(input[26..30].try_into().unwrap()),
    })
}

fn encoded_frame_len(
    header_len: usize,
    payload_len: usize,
) -> std::result::Result<usize, ControlStreamFrameError> {
    CONTROL_STREAM_FIXED_HEADER_LEN
        .checked_add(header_len)
        .and_then(|len| len.checked_add(payload_len))
        .ok_or(ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame",
        })
}

fn metadata_from_header_proto(
    header_proto: &[u8],
) -> std::result::Result<ControlFrameMetadata, ControlStreamFrameError> {
    let raw = ControlFrameHeaderProto::decode(header_proto)
        .map_err(|_| ControlStreamFrameError::InvalidHeaderProtobuf)?;
    if raw.encode_to_vec() != header_proto {
        return Err(ControlStreamFrameError::NonDeterministicHeaderProtobuf);
    }
    let sequence = ControlStreamSequence::new(raw.sequence)?;
    if raw.record_digest.is_empty() {
        return Err(ControlStreamFrameError::MissingHeaderField {
            field: "record_digest",
        });
    }
    let record_digest = ControlRecordDigest::new(raw.record_digest)?;
    Ok(ControlFrameMetadata {
        sequence,
        record_digest,
    })
}

fn is_valid_digest(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("blake3:") else {
        return false;
    };
    hex.len() == 64
        && hex
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0xedb8_8320 & mask);
        }
    }
    !crc
}

#[derive(Debug, Clone, Copy)]
struct FixedControlFrameHeader {
    header_len: usize,
    payload_len: usize,
    header_crc32: u32,
    payload_crc32: u32,
}

#[derive(Debug, Clone)]
struct StreamProjectionEntry {
    sequence: u64,
    generation: Option<u64>,
    digest: String,
    operation: String,
    payload_json: Vec<u8>,
}

pub fn decode_control_mutation_header(
    header_proto: &[u8],
) -> AnyhowResult<ControlFrameHeaderProto> {
    let header =
        ControlFrameHeaderProto::decode(header_proto).context("parse control mutation header")?;
    if header.encode_to_vec() != header_proto {
        return Err(anyhow!(
            "control mutation header protobuf is not deterministic or contains unknown fields"
        ));
    }
    Ok(header)
}

fn semantic_json(payload_json: &[u8]) -> AnyhowResult<serde_json::Value> {
    serde_json::from_slice(payload_json).context("parse projected control payload")
}

fn control_payload_operator_json(
    stream_family: &str,
    record_key: &str,
    payload_proto: &[u8],
) -> AnyhowResult<Vec<u8>> {
    if let Some(family) =
        crate::mesh_directory::RoutingRecordFamily::from_stream_family(stream_family)
    {
        return crate::mesh_directory::control_payload_operator_json(
            family,
            record_key,
            payload_proto,
        )
        .map_err(Into::into);
    }
    if crate::mesh_lifecycle::lifecycle_control_stream_families().contains(&stream_family) {
        return crate::mesh_lifecycle::control_payload_operator_json(
            stream_family,
            record_key,
            payload_proto,
        )
        .map_err(Into::into);
    }
    Err(anyhow!(
        "unknown mesh control stream family {stream_family}"
    ))
}

fn projection_digest(payload_json: &[u8]) -> AnyhowResult<String> {
    semantic_json(payload_json)?;
    Ok(ControlRecordDigest::blake3(payload_json).to_string())
}

fn is_delete_operation(operation: &str) -> bool {
    matches!(operation, "delete" | "deleted" | "tombstone")
}

fn control_stream_id(stream_family: &str, partition: &str) -> AnyhowResult<String> {
    validate_control_stream_scope(stream_family, "control stream family")?;
    validate_control_stream_partition(partition)?;
    Ok(format!(
        "{CONTROL_STREAM_ID_PREFIX}{stream_family}:{partition}"
    ))
}

fn control_stream_prefix(stream_family: &str) -> String {
    format!("{CONTROL_STREAM_ID_PREFIX}{stream_family}:")
}

fn control_checkpoint_row_key(
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<Vec<u8>> {
    validate_control_stream_scope(region, "control checkpoint region")?;
    validate_control_stream_scope(stream_family, "control checkpoint stream family")?;
    validate_control_stream_partition(partition)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(CONTROL_CHECKPOINT_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(region),
        CoreMetaTuplePart::Utf8(stream_family),
        CoreMetaTuplePart::Utf8(partition),
    ])
}

fn control_checkpoint_partition_id(
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<String> {
    validate_control_stream_scope(region, "control checkpoint region")?;
    validate_control_stream_scope(stream_family, "control checkpoint stream family")?;
    validate_control_stream_partition(partition)?;
    Ok(format!(
        "mesh-control-checkpoint:{region}:{stream_family}:{partition}"
    ))
}

fn validate_control_stream_scope(value: &str, context: &str) -> AnyhowResult<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{context} is not a safe path component"));
    }
    Ok(())
}

fn validate_control_stream_partition(value: &str) -> AnyhowResult<()> {
    if value.len() != 4
        || !value
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(anyhow!(
            "control stream partition must be four lowercase hex characters"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_header(sequence: u64) -> Vec<u8> {
        sample_header_at(sequence, 0)
    }

    fn sample_header_at(sequence: u64, byte_offset: u64) -> Vec<u8> {
        encode_control_mutation_header(ControlMutationHeaderInput {
            schema: "anvil.mesh.control_mutation.v1",
            mesh_id: "mesh_01",
            stream_family: "bucket_locator",
            partition: "0a7f",
            sequence: ControlStreamSequence::new(sequence).unwrap(),
            record_key: "tenant_acme/releases",
            operation: "upsert",
            expected_generation: Some(18),
            new_generation: 19,
            writer_node_id: "node_01J0",
            writer_fence: 44,
            idempotency_key: Some("req-123"),
            record_digest: &ControlRecordDigest::blake3(b"record"),
            created_at: "2026-07-02T00:00:00Z",
            byte_offset,
        })
    }

    fn sample_payload() -> Vec<u8> {
        br#"{"tenant_id":"tenant_acme","bucket":"releases"}"#.to_vec()
    }

    fn bucket_locator_operator_json(home_region: &str) -> Vec<u8> {
        let descriptor = crate::mesh_directory::BucketLocatorDescriptor::active(
            crate::mesh_directory::MeshId::new("mesh_01").unwrap(),
            crate::mesh_directory::TenantId::new("tenant_acme").unwrap(),
            crate::mesh_directory::BucketName::canonicalize("releases").unwrap(),
            crate::mesh_directory::BucketId::new("bucket_01HY").unwrap(),
            crate::mesh_directory::RegionName::new(home_region).unwrap(),
            crate::mesh_directory::CellId::new("cell_a").unwrap(),
            "regional-primary",
            "objects/tenant_acme/releases/",
            "2026-07-02T00:00:00Z",
        )
        .unwrap();
        serde_json::to_vec(&descriptor).unwrap()
    }

    fn bucket_locator_payload_proto(home_region: &str) -> Vec<u8> {
        let operator_json = bucket_locator_operator_json(home_region);
        crate::mesh_directory::encode_control_payload_from_operator_json(
            crate::mesh_directory::RoutingRecordFamily::BucketLocator,
            &operator_json,
        )
        .unwrap()
    }

    fn sample_header_for_payload(sequence: u64, payload: &[u8]) -> Vec<u8> {
        encode_control_mutation_header(ControlMutationHeaderInput {
            schema: "anvil.mesh.control_mutation.v1",
            mesh_id: "mesh_01",
            stream_family: "bucket_locator",
            partition: "0a7f",
            sequence: ControlStreamSequence::new(sequence).unwrap(),
            record_key: "tenant_acme/releases",
            operation: "upsert",
            expected_generation: None,
            new_generation: 1,
            writer_node_id: "node_01J0",
            writer_fence: 44,
            idempotency_key: Some("req-123"),
            record_digest: &ControlRecordDigest::blake3(payload),
            created_at: "2026-07-02T00:00:00Z",
            byte_offset: 0,
        })
    }

    fn sample_header_for_record(
        cursor: &ControlStreamAppendCursor,
        record_key: &str,
        operation: &str,
        generation: u64,
        payload: &[u8],
    ) -> Vec<u8> {
        encode_control_mutation_header(ControlMutationHeaderInput {
            schema: "anvil.mesh.control_mutation.v1",
            mesh_id: "mesh_01",
            stream_family: "bucket_locator",
            partition: "0a7f",
            sequence: cursor.sequence,
            record_key,
            operation,
            expected_generation: generation.checked_sub(1),
            new_generation: generation,
            writer_node_id: "node_01J0",
            writer_fence: 44,
            idempotency_key: None,
            record_digest: &ControlRecordDigest::blake3(payload),
            created_at: "2026-07-02T00:00:00Z",
            byte_offset: cursor.byte_offset,
        })
    }

    #[test]
    fn crc32_matches_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    }

    #[test]
    fn frame_round_trips_with_big_endian_header_and_metadata() {
        let frame = ControlStreamFrame::new(sample_header(1844), sample_payload());
        let encoded = frame.encode().unwrap();

        assert_eq!(&encoded[0..8], CONTROL_STREAM_MAGIC);
        assert_eq!(
            u16::from_be_bytes(encoded[8..10].try_into().unwrap()),
            CONTROL_STREAM_VERSION
        );
        assert_eq!(
            u32::from_be_bytes(encoded[10..14].try_into().unwrap()),
            frame.header_proto.len() as u32
        );
        assert_eq!(
            u64::from_be_bytes(encoded[14..22].try_into().unwrap()),
            frame.payload_proto.len() as u64
        );
        assert_eq!(
            u32::from_be_bytes(encoded[22..26].try_into().unwrap()),
            crc32(&frame.header_proto)
        );
        assert_eq!(
            u32::from_be_bytes(encoded[26..30].try_into().unwrap()),
            crc32(&frame.payload_proto)
        );

        let (decoded, used) = ControlStreamFrame::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, frame);
        let metadata = decoded.metadata().unwrap();
        assert_eq!(metadata.sequence.get(), 1844);
        assert!(metadata.record_digest.as_str().starts_with("blake3:"));
    }

    #[test]
    fn frame_decode_validates_header_and_payload_crc32() {
        let frame = ControlStreamFrame::new(sample_header(1), sample_payload());
        let encoded = frame.encode().unwrap();

        let mut bad_header = encoded.clone();
        bad_header[CONTROL_STREAM_FIXED_HEADER_LEN] ^= 1;
        assert!(matches!(
            ControlStreamFrame::decode(&bad_header).unwrap_err(),
            ControlStreamFrameError::HeaderCrc32Mismatch { .. }
        ));

        let mut bad_payload = encoded;
        let payload_offset = CONTROL_STREAM_FIXED_HEADER_LEN + frame.header_proto.len();
        bad_payload[payload_offset] ^= 1;
        assert!(matches!(
            ControlStreamFrame::decode(&bad_payload).unwrap_err(),
            ControlStreamFrameError::PayloadCrc32Mismatch { .. }
        ));
    }

    #[test]
    fn log_decode_ignores_partial_final_frame() {
        let first = ControlStreamFrame::new(sample_header(1), sample_payload())
            .encode()
            .unwrap();
        let second = ControlStreamFrame::new(sample_header(2), sample_payload())
            .encode()
            .unwrap();
        let mut log = Vec::new();
        log.extend_from_slice(&first);
        log.extend_from_slice(&second[..CONTROL_STREAM_FIXED_HEADER_LEN + 5]);

        let decoded = decode_control_stream_log(&log).unwrap();
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].metadata.sequence.get(), 1);
        assert_eq!(decoded.complete_len, first.len() as u64);
        assert_eq!(
            decoded.partial_final_frame,
            Some(PartialFinalFrame {
                offset: first.len() as u64,
                expected_len: second.len(),
                actual_len: CONTROL_STREAM_FIXED_HEADER_LEN + 5,
            })
        );
    }

    #[test]
    fn metadata_requires_sequence_and_digest() {
        let mut missing_digest = ControlFrameHeaderProto::decode(&sample_header(1)[..]).unwrap();
        missing_digest.record_digest.clear();
        let missing_digest = missing_digest.encode_to_vec();
        assert_eq!(
            metadata_from_header_proto(&missing_digest).unwrap_err(),
            ControlStreamFrameError::MissingHeaderField {
                field: "record_digest"
            }
        );

        let mut zero_sequence = ControlFrameHeaderProto::decode(&sample_header(1)[..]).unwrap();
        zero_sequence.sequence = 0;
        let zero_sequence = zero_sequence.encode_to_vec();
        assert_eq!(
            metadata_from_header_proto(&zero_sequence).unwrap_err(),
            ControlStreamFrameError::InvalidSequence
        );

        let mut bad_digest = ControlFrameHeaderProto::decode(&sample_header(1)[..]).unwrap();
        bad_digest.record_digest = "sha256:abc".to_string();
        let bad_digest = bad_digest.encode_to_vec();
        assert_eq!(
            metadata_from_header_proto(&bad_digest).unwrap_err(),
            ControlStreamFrameError::InvalidDigest
        );
    }

    #[tokio::test]
    async fn append_and_read_control_stream_pages() {
        let dir = tempdir().unwrap();
        let storage = Storage::new_at(dir.path()).await.unwrap();
        let first = ControlStreamFrame::new(sample_header(1), sample_payload());

        let first_append =
            append_control_stream_frame(&storage, "bucket_locator", "0a7f", &first, None)
                .await
                .unwrap();
        let first_len = first.encode().unwrap().len();
        let second =
            ControlStreamFrame::new(sample_header_at(2, first_len as u64), sample_payload());
        let second_append =
            append_control_stream_frame(&storage, "bucket_locator", "0a7f", &second, None)
                .await
                .unwrap();

        assert_eq!(first_append.offset, 0);
        assert_eq!(first_append.encoded_len, first_len);
        assert_eq!(second_append.offset, first_len as u64);
        assert_eq!(second_append.position.sequence.get(), 2);

        let log = read_control_stream_page(&storage, "bucket_locator", "0a7f", 0, 8)
            .await
            .unwrap();
        assert_eq!(log.records.len(), 2);
        assert!(!log.has_more);
        assert_eq!(log.records[1].metadata.sequence.get(), 2);
    }

    #[tokio::test]
    async fn control_stream_pages_are_bounded_and_latest_lookup_is_key_scoped() {
        let dir = tempdir().unwrap();
        let storage = Storage::new_at(dir.path()).await.unwrap();
        let projected_payload = bucket_locator_payload_proto("eu-west-1");

        for (record_key, generation, payload) in [
            ("tenant_acme/releases", 1, projected_payload.as_slice()),
            ("unrelated/one", 1, &b"unrelated-one"[..]),
            ("unrelated/two", 1, &b"unrelated-two"[..]),
        ] {
            let cursor = control_stream_append_cursor(&storage, "bucket_locator", "0a7f")
                .await
                .unwrap();
            let frame = ControlStreamFrame::new(
                sample_header_for_record(&cursor, record_key, "upsert", generation, payload),
                payload.to_vec(),
            );
            append_control_stream_frame(&storage, "bucket_locator", "0a7f", &frame, None)
                .await
                .unwrap();
        }

        let first = read_control_stream_page(&storage, "bucket_locator", "0a7f", 0, 2)
            .await
            .unwrap();
        assert_eq!(first.records.len(), 2);
        assert!(first.has_more);
        let second =
            read_control_stream_page(&storage, "bucket_locator", "0a7f", first.next_sequence, 2)
                .await
                .unwrap();
        assert_eq!(second.records.len(), 1);
        assert!(!second.has_more);

        let current_first =
            list_current_control_stream_records_page(&storage, "bucket_locator", "0a7f", None, 2)
                .await
                .unwrap();
        assert_eq!(current_first.records.len(), 2);
        let current_second = list_current_control_stream_records_page(
            &storage,
            "bucket_locator",
            "0a7f",
            current_first.next_stream_id.as_deref(),
            2,
        )
        .await
        .unwrap();
        assert_eq!(current_second.records.len(), 1);
        assert!(current_second.next_stream_id.is_none());

        let latest = latest_projected_record_from_control_stream(
            &storage,
            "bucket_locator",
            "0a7f",
            "tenant_acme/releases",
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(latest.generation, 1);
        assert_eq!(
            latest.payload_json,
            bucket_locator_operator_json("eu-west-1")
        );

        let cursor = control_stream_append_cursor(&storage, "bucket_locator", "0a7f")
            .await
            .unwrap();
        let delete = ControlStreamFrame::new(
            sample_header_for_record(
                &cursor,
                "tenant_acme/releases",
                "delete",
                2,
                &projected_payload,
            ),
            projected_payload,
        );
        append_control_stream_frame(&storage, "bucket_locator", "0a7f", &delete, None)
            .await
            .unwrap();
        let latest = latest_projected_record_from_control_stream(
            &storage,
            "bucket_locator",
            "0a7f",
            "tenant_acme/releases",
        )
        .await
        .unwrap()
        .unwrap();
        assert!(latest.deleted);
        assert_eq!(latest.generation, 2);
        let mut current =
            list_current_control_stream_records_page(&storage, "bucket_locator", "0a7f", None, 8)
                .await
                .unwrap()
                .records;
        current.sort_by(|left, right| left.record_key.cmp(&right.record_key));
        assert_eq!(current.len(), 3);
        let deleted = current
            .iter()
            .find(|record| record.record_key == "tenant_acme/releases")
            .unwrap();
        assert!(deleted.deleted);
        assert_eq!(deleted.generation, 2);
        assert!(
            read_control_stream_page(&storage, "bucket_locator", "0a7f", 0, 0)
                .await
                .unwrap_err()
                .to_string()
                .contains("page size")
        );
        assert!(
            list_current_control_stream_records_page(&storage, "bucket_locator", "0a7f", None, 0,)
                .await
                .unwrap_err()
                .to_string()
                .contains("page size")
        );
    }

    #[tokio::test]
    async fn append_rejects_log_with_partial_final_frame() {
        let partial = ControlStreamFrame::new(sample_header(1), sample_payload())
            .encode()
            .unwrap();
        let err = decode_control_stream_log(&partial[..partial.len() - 1]).unwrap();
        assert!(
            err.partial_final_frame.is_some(),
            "partial frame must remain a byte-format validation concern"
        );
    }

    #[tokio::test]
    async fn control_checkpoint_round_trips_and_rejects_path_body_scope_mismatch() {
        let dir = tempdir().unwrap();
        let storage = Storage::new_at(dir.path()).await.unwrap();
        let digest = ControlRecordDigest::blake3(b"checkpointed-record");
        let checkpoint = ControlCheckpointRecord::new(
            "mesh-a",
            "eu-west-1",
            "bucket_locator",
            "0a7f",
            ControlStreamSequence::new(7).unwrap(),
            digest.clone(),
            "2026-07-02T00:00:00Z",
        );

        write_control_checkpoint(&storage, &checkpoint)
            .await
            .unwrap();
        assert_eq!(
            read_control_checkpoint(&storage, "eu-west-1", "bucket_locator", "0a7f")
                .await
                .unwrap(),
            Some(checkpoint)
        );

        let mismatched_body = ControlCheckpointRecord::new(
            "mesh-a",
            "us-east-1",
            "tenant_name",
            "ffff",
            ControlStreamSequence::new(7).unwrap(),
            digest,
            "2026-07-02T00:00:00Z",
        );
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let row_key = control_checkpoint_row_key("eu-west-1", "bucket_locator", "0a7f").unwrap();
        let current = store
            .read_coremeta_row(CF_MESH, TABLE_MESH_PARTITION_ROW, &row_key)
            .unwrap();
        let payload = encode_control_checkpoint_proto(&mismatched_body).unwrap();
        let partition_id =
            control_checkpoint_partition_id("eu-west-1", "bucket_locator", "0a7f").unwrap();
        store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: "mismatched-checkpoint-test".to_string(),
                scope_partition: partition_id.clone(),
                committed_by_principal: "mesh-control-checkpoint-test".to_string(),
                preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                    cf: CF_MESH.to_string(),
                    table_id: TABLE_MESH_PARTITION_ROW,
                    tuple_key: row_key.clone(),
                    expected_payload_hash: current
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(TABLE_MESH_PARTITION_ROW, payload)),
                    require_absent: current.is_none(),
                    require_present: current.is_some(),
                }],
                operations: vec![CoreMutationOperation::CoreMetaPut {
                    partition_id,
                    cf: CF_MESH.to_string(),
                    table_id: TABLE_MESH_PARTITION_ROW,
                    tuple_key: row_key,
                    payload,
                }],
            })
            .await
            .unwrap();

        let err = read_control_checkpoint(&storage, "eu-west-1", "bucket_locator", "0a7f")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("control checkpoint path does not match checkpoint body"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn control_checkpoint_rejects_unsafe_path_scopes() {
        let dir = tempdir().unwrap();
        let storage = Storage::new_at(dir.path()).await.unwrap();
        let checkpoint = ControlCheckpointRecord::new(
            "mesh-a",
            "../escape",
            "bucket_locator",
            "0a7f",
            ControlStreamSequence::new(1).unwrap(),
            ControlRecordDigest::blake3(b"checkpointed-record"),
            "2026-07-02T00:00:00Z",
        );

        let err = write_control_checkpoint(&storage, &checkpoint)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("control checkpoint region is not a safe path component"),
            "unexpected error: {err}"
        );

        let err = read_control_checkpoint(&storage, "eu-west-1", "bucket_locator", "0A7F")
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("control stream partition must be four lowercase hex characters"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn control_checkpoint_is_monotonic_idempotent_and_digest_scoped() {
        let dir = tempdir().unwrap();
        let storage = Storage::new_at(dir.path()).await.unwrap();
        let first_digest = ControlRecordDigest::blake3(b"first");
        let first = ControlCheckpointRecord::new(
            "mesh-a",
            "eu-west-1",
            "bucket_locator",
            "0a7f",
            ControlStreamSequence::new(4).unwrap(),
            first_digest.clone(),
            "2026-07-02T00:00:00Z",
        );
        write_control_checkpoint(&storage, &first).await.unwrap();
        write_control_checkpoint(&storage, &first).await.unwrap();

        let same_sequence_different_digest = ControlCheckpointRecord::new(
            "mesh-a",
            "eu-west-1",
            "bucket_locator",
            "0a7f",
            ControlStreamSequence::new(4).unwrap(),
            ControlRecordDigest::blake3(b"diverged"),
            "2026-07-02T00:01:00Z",
        );
        let err = write_control_checkpoint(&storage, &same_sequence_different_digest)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("ControlStreamDivergence"),
            "unexpected error: {err}"
        );

        let backwards = ControlCheckpointRecord::new(
            "mesh-a",
            "eu-west-1",
            "bucket_locator",
            "0a7f",
            ControlStreamSequence::new(3).unwrap(),
            first_digest,
            "2026-07-02T00:02:00Z",
        );
        let err = write_control_checkpoint(&storage, &backwards)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("control checkpoint cannot move backwards"),
            "unexpected error: {err}"
        );

        let advanced = ControlCheckpointRecord::new(
            "mesh-a",
            "eu-west-1",
            "bucket_locator",
            "0a7f",
            ControlStreamSequence::new(5).unwrap(),
            ControlRecordDigest::blake3(b"advanced"),
            "2026-07-02T00:03:00Z",
        );
        write_control_checkpoint(&storage, &advanced).await.unwrap();
        assert_eq!(
            read_control_checkpoint(&storage, "eu-west-1", "bucket_locator", "0a7f")
                .await
                .unwrap(),
            Some(advanced)
        );
    }

    #[tokio::test]
    async fn projection_diagnostic_detects_stream_projection_payload_mismatch() {
        let dir = tempdir().unwrap();
        let storage = Storage::new_at(dir.path()).await.unwrap();
        let stream_payload_proto = bucket_locator_payload_proto("eu-west-1");
        let stream_payload_json = bucket_locator_operator_json("eu-west-1");
        let projection_payload_json = bucket_locator_operator_json("us-east-1");
        let frame = ControlStreamFrame::new(
            sample_header_for_payload(1, &stream_payload_proto),
            stream_payload_proto,
        );
        append_control_stream_frame(&storage, "bucket_locator", "0a7f", &frame, None)
            .await
            .unwrap();

        let clean = diagnose_control_stream_projection(
            &storage,
            "bucket_locator",
            "0a7f",
            &[ControlProjectionRecord::new(
                "tenant_acme/releases",
                1,
                stream_payload_json,
            )],
        )
        .await
        .unwrap();
        assert!(clean.is_empty());

        let diagnostics = diagnose_control_stream_projection(
            &storage,
            "bucket_locator",
            "0a7f",
            &[ControlProjectionRecord::new(
                "tenant_acme/releases",
                1,
                projection_payload_json,
            )],
        )
        .await
        .unwrap();
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "mesh_control_projection_payload_mismatch"
                && diagnostic.record_key == "tenant_acme/releases"
                && diagnostic.repair_safe
                && diagnostic.proposed_action == "repair_routing_record_from_control_stream"
        }));
    }
}
