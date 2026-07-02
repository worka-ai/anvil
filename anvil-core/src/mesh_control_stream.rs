use crate::storage::Storage;
use anyhow::{Context, Result as AnyhowResult, anyhow};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::fmt;
use std::io::ErrorKind;
use std::path::Path;
use tokio::io::AsyncWriteExt;

pub const CONTROL_STREAM_MAGIC: &[u8; 8] = b"ANVCTL1\0";
pub const CONTROL_STREAM_VERSION: u16 = 1;
pub const CONTROL_STREAM_FIXED_HEADER_LEN: usize = 8 + 2 + 4 + 8 + 4 + 4;
pub const CONTROL_CHECKPOINT_SCHEMA: &str = "anvil.mesh.control_checkpoint.v1";

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
    #[error("control stream frame header JSON is invalid: {0}")]
    InvalidHeaderJson(String),
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
    pub header_json: Vec<u8>,
    pub payload_json: Vec<u8>,
}

impl ControlStreamFrame {
    pub fn new(header_json: Vec<u8>, payload_json: Vec<u8>) -> Self {
        Self {
            header_json,
            payload_json,
        }
    }

    pub fn metadata(&self) -> std::result::Result<ControlFrameMetadata, ControlStreamFrameError> {
        metadata_from_header_json(&self.header_json)
    }

    pub fn encoded_len(&self) -> std::result::Result<usize, ControlStreamFrameError> {
        encoded_frame_len(self.header_json.len(), self.payload_json.len())
    }

    pub fn encode(&self) -> std::result::Result<Vec<u8>, ControlStreamFrameError> {
        encode_control_stream_frame(&self.header_json, &self.payload_json)
    }

    pub fn decode(input: &[u8]) -> std::result::Result<(Self, usize), ControlStreamFrameError> {
        decode_control_stream_frame(input)
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlProjectionRecord {
    pub record_key: String,
    pub generation: u64,
    pub payload_json: Vec<u8>,
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
    path: impl AsRef<Path>,
    stream_family: &str,
    partition: &str,
    projected_records: &[ControlProjectionRecord],
) -> AnyhowResult<Vec<ControlProjectionDiagnostic>> {
    let log = read_control_stream_log(path).await?;
    let mut diagnostics = Vec::new();
    if let Some(partial) = &log.partial_final_frame {
        diagnostics.push(ControlProjectionDiagnostic {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            record_key: String::new(),
            severity: "error",
            code: "mesh_control_stream_partial_final_frame",
            message: format!(
                "control stream {stream_family}/{partition} has a partial final frame at offset {} ({} of {} bytes)",
                partial.offset, partial.actual_len, partial.expected_len
            ),
            stream_sequence: None,
            stream_generation: None,
            stream_digest: None,
            projection_generation: None,
            projection_digest: None,
            repair_safe: false,
            proposed_action: "manual_review_rebuild_not_implemented",
        });
    }

    let mut latest_by_key = BTreeMap::new();
    let mut expected_sequence = 1_u64;
    for record in &log.records {
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

        let header = match control_mutation_header(&record.frame.header_json) {
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

        if header.stream_family.as_deref() != Some(stream_family)
            || header.partition.as_deref() != Some(partition)
        {
            diagnostics.push(ControlProjectionDiagnostic {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                record_key: header.record_key.clone().unwrap_or_default(),
                severity: "error",
                code: "mesh_control_stream_header_scope_mismatch",
                message: format!(
                    "control stream header scope {:?}/{:?} does not match path {stream_family}/{partition}",
                    header.stream_family, header.partition
                ),
                stream_sequence: Some(sequence),
                stream_generation: header.new_generation,
                stream_digest: Some(record.metadata.record_digest.to_string()),
                projection_generation: None,
                projection_digest: None,
                repair_safe: false,
                proposed_action: "manual_review_rebuild_not_implemented",
            });
        }

        let Some(record_key) = header
            .record_key
            .clone()
            .filter(|record_key| !record_key.trim().is_empty())
        else {
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
                stream_generation: header.new_generation,
                stream_digest: Some(record.metadata.record_digest.to_string()),
                projection_generation: None,
                projection_digest: None,
                repair_safe: false,
                proposed_action: "manual_review_rebuild_not_implemented",
            });
            continue;
        };

        let payload_digest = ControlRecordDigest::blake3(&record.frame.payload_json);
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
                stream_generation: header.new_generation,
                stream_digest: Some(record.metadata.record_digest.to_string()),
                projection_generation: None,
                projection_digest: Some(payload_digest.to_string()),
                repair_safe: false,
                proposed_action: "manual_review_rebuild_not_implemented",
            });
        }

        latest_by_key.insert(
            record_key,
            StreamProjectionEntry {
                sequence,
                generation: header.new_generation,
                digest: record.metadata.record_digest.to_string(),
                operation: header.operation.unwrap_or_default(),
                payload_json: record.frame.payload_json.clone(),
            },
        );
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

pub async fn latest_projected_record_from_control_stream(
    path: impl AsRef<Path>,
    stream_family: &str,
    partition: &str,
    record_key: &str,
) -> AnyhowResult<Option<ControlProjectionRecord>> {
    let log = read_control_stream_log(path).await?;
    if log.partial_final_frame.is_some() {
        return Err(anyhow!(
            "control stream {stream_family}/{partition} has a partial final frame"
        ));
    }

    let mut latest = None;
    for record in &log.records {
        let header = control_mutation_header(&record.frame.header_json)?;
        if header.stream_family.as_deref() != Some(stream_family)
            || header.partition.as_deref() != Some(partition)
        {
            return Err(anyhow!(
                "control stream header scope {:?}/{:?} does not match path {stream_family}/{partition}",
                header.stream_family,
                header.partition
            ));
        }
        if header.record_key.as_deref() != Some(record_key) {
            continue;
        }
        if header.operation.as_deref().is_some_and(is_delete_operation) {
            latest = None;
            continue;
        }
        latest = Some(ControlProjectionRecord::new(
            record_key,
            header.new_generation.unwrap_or_default(),
            record.frame.payload_json.clone(),
        ));
    }
    Ok(latest)
}

pub async fn write_control_checkpoint(
    storage: &Storage,
    checkpoint: &ControlCheckpointRecord,
) -> AnyhowResult<()> {
    validate_control_checkpoint(checkpoint)?;
    let path = storage.mesh_control_checkpoint_path(
        &checkpoint.region,
        &checkpoint.stream_family,
        &checkpoint.partition,
    )?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp_path = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::File::create(&tmp_path).await?;
    let bytes = serde_json::to_vec_pretty(checkpoint)?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(tmp_path, path).await?;
    Ok(())
}

pub async fn read_control_checkpoint(
    storage: &Storage,
    region: &str,
    stream_family: &str,
    partition: &str,
) -> AnyhowResult<Option<ControlCheckpointRecord>> {
    let path = storage.mesh_control_checkpoint_path(region, stream_family, partition)?;
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let checkpoint: ControlCheckpointRecord = serde_json::from_slice(&bytes)?;
    validate_control_checkpoint(&checkpoint)?;
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
    Ok(Some(checkpoint))
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

pub fn encode_control_stream_frame(
    header_json: &[u8],
    payload_json: &[u8],
) -> std::result::Result<Vec<u8>, ControlStreamFrameError> {
    let header_len = u32::try_from(header_json.len()).map_err(|_| {
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame header_json",
        }
    })?;
    let payload_len = u64::try_from(payload_json.len()).map_err(|_| {
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame payload_json",
        }
    })?;
    let mut out = Vec::with_capacity(encoded_frame_len(header_json.len(), payload_json.len())?);
    out.extend_from_slice(CONTROL_STREAM_MAGIC);
    out.extend_from_slice(&CONTROL_STREAM_VERSION.to_be_bytes());
    out.extend_from_slice(&header_len.to_be_bytes());
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(&crc32(header_json).to_be_bytes());
    out.extend_from_slice(&crc32(payload_json).to_be_bytes());
    out.extend_from_slice(header_json);
    out.extend_from_slice(payload_json);
    Ok(out)
}

pub fn decode_control_stream_frame(
    input: &[u8],
) -> std::result::Result<(ControlStreamFrame, usize), ControlStreamFrameError> {
    let fixed = decode_fixed_header(input)?;
    let header_start = CONTROL_STREAM_FIXED_HEADER_LEN;
    let header_end = header_start.checked_add(fixed.header_len).ok_or(
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame header_json",
        },
    )?;
    let payload_end = header_end.checked_add(fixed.payload_len).ok_or(
        ControlStreamFrameError::InvalidDeclaredLength {
            context: "control stream frame payload_json",
        },
    )?;
    if input.len() < payload_end {
        return Err(ControlStreamFrameError::TooShort {
            context: "control stream frame payload_json",
            needed: payload_end,
            actual: input.len(),
        });
    }

    let header_json = input[header_start..header_end].to_vec();
    let payload_json = input[header_end..payload_end].to_vec();
    let actual_header_crc32 = crc32(&header_json);
    if actual_header_crc32 != fixed.header_crc32 {
        return Err(ControlStreamFrameError::HeaderCrc32Mismatch {
            expected: fixed.header_crc32,
            actual: actual_header_crc32,
        });
    }
    let actual_payload_crc32 = crc32(&payload_json);
    if actual_payload_crc32 != fixed.payload_crc32 {
        return Err(ControlStreamFrameError::PayloadCrc32Mismatch {
            expected: fixed.payload_crc32,
            actual: actual_payload_crc32,
        });
    }

    Ok((
        ControlStreamFrame {
            header_json,
            payload_json,
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

pub async fn read_control_stream_log(path: impl AsRef<Path>) -> AnyhowResult<ControlStreamLog> {
    let path = path.as_ref();
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(ControlStreamLog::default()),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    decode_control_stream_log(&bytes).map_err(Into::into)
}

pub async fn append_control_stream_frame(
    path: impl AsRef<Path>,
    frame: &ControlStreamFrame,
) -> AnyhowResult<ControlStreamAppend> {
    let path = path.as_ref();
    let existing = read_control_stream_log(path).await?;
    if let Some(partial) = existing.partial_final_frame {
        return Err(anyhow!(
            "control stream log has partial final frame at offset {} ({} of {} bytes)",
            partial.offset,
            partial.actual_len,
            partial.expected_len
        ));
    }

    let encoded = frame.encode()?;
    let metadata = frame.metadata()?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create control stream directory {}", parent.display()))?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("open control stream log {}", path.display()))?;
    file.write_all(&encoded)
        .await
        .with_context(|| format!("append control stream log {}", path.display()))?;
    file.sync_data()
        .await
        .with_context(|| format!("sync control stream log {}", path.display()))?;
    Ok(ControlStreamAppend {
        offset: existing.complete_len,
        encoded_len: encoded.len(),
        position: metadata.into(),
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
            context: "control stream frame payload_json",
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

fn metadata_from_header_json(
    header_json: &[u8],
) -> std::result::Result<ControlFrameMetadata, ControlStreamFrameError> {
    let raw: RawControlFrameMetadata = serde_json::from_slice(header_json)
        .map_err(|err| ControlStreamFrameError::InvalidHeaderJson(err.to_string()))?;
    let sequence = raw
        .sequence
        .ok_or(ControlStreamFrameError::MissingHeaderField { field: "sequence" })?;
    let sequence = ControlStreamSequence::new(sequence)?;
    let record_digest = raw
        .record_digest
        .ok_or(ControlStreamFrameError::MissingHeaderField {
            field: "record_digest",
        })?;
    let record_digest = ControlRecordDigest::new(record_digest)?;
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

#[derive(Debug, Deserialize)]
struct RawControlFrameMetadata {
    sequence: Option<u64>,
    record_digest: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawControlMutationHeader {
    stream_family: Option<String>,
    partition: Option<String>,
    record_key: Option<String>,
    operation: Option<String>,
    new_generation: Option<u64>,
}

#[derive(Debug, Clone)]
struct StreamProjectionEntry {
    sequence: u64,
    generation: Option<u64>,
    digest: String,
    operation: String,
    payload_json: Vec<u8>,
}

fn control_mutation_header(header_json: &[u8]) -> AnyhowResult<RawControlMutationHeader> {
    serde_json::from_slice(header_json).context("parse control mutation header")
}

fn semantic_json(payload_json: &[u8]) -> AnyhowResult<serde_json::Value> {
    serde_json::from_slice(payload_json).context("parse projected control payload")
}

fn projection_digest(payload_json: &[u8]) -> AnyhowResult<String> {
    let value = semantic_json(payload_json)?;
    Ok(ControlRecordDigest::blake3(&serde_json::to_vec(&value)?).to_string())
}

fn is_delete_operation(operation: &str) -> bool {
    matches!(operation, "delete" | "deleted")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn sample_header(sequence: u64) -> Vec<u8> {
        serde_json::json!({
            "schema": "anvil.mesh.control_mutation.v1",
            "mesh_id": "mesh_01",
            "stream_family": "bucket_locator",
            "partition": "0a7f",
            "sequence": sequence,
            "record_key": "tenant_acme/releases",
            "operation": "upsert",
            "expected_generation": 18,
            "new_generation": 19,
            "writer_node_id": "node_01J0",
            "writer_fence": 44,
            "idempotency_key": "req-123",
            "record_digest": ControlRecordDigest::blake3(b"record").as_str(),
            "created_at": "2026-07-02T00:00:00Z"
        })
        .to_string()
        .into_bytes()
    }

    fn sample_payload() -> Vec<u8> {
        br#"{"tenant_id":"tenant_acme","bucket":"releases"}"#.to_vec()
    }

    fn sample_header_for_payload(sequence: u64, payload: &[u8]) -> Vec<u8> {
        serde_json::json!({
            "schema": "anvil.mesh.control_mutation.v1",
            "mesh_id": "mesh_01",
            "stream_family": "bucket_locator",
            "partition": "0a7f",
            "sequence": sequence,
            "record_key": "tenant_acme/releases",
            "operation": "upsert",
            "expected_generation": null,
            "new_generation": 1,
            "writer_node_id": "node_01J0",
            "writer_fence": 44,
            "idempotency_key": "req-123",
            "record_digest": ControlRecordDigest::blake3(payload).as_str(),
            "created_at": "2026-07-02T00:00:00Z"
        })
        .to_string()
        .into_bytes()
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
            frame.header_json.len() as u32
        );
        assert_eq!(
            u64::from_be_bytes(encoded[14..22].try_into().unwrap()),
            frame.payload_json.len() as u64
        );
        assert_eq!(
            u32::from_be_bytes(encoded[22..26].try_into().unwrap()),
            crc32(&frame.header_json)
        );
        assert_eq!(
            u32::from_be_bytes(encoded[26..30].try_into().unwrap()),
            crc32(&frame.payload_json)
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
        let payload_offset = CONTROL_STREAM_FIXED_HEADER_LEN + frame.header_json.len();
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
        let missing_digest = br#"{"sequence":1}"#;
        assert_eq!(
            metadata_from_header_json(missing_digest).unwrap_err(),
            ControlStreamFrameError::MissingHeaderField {
                field: "record_digest"
            }
        );

        let zero_sequence = br#"{"sequence":0,"record_digest":"blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#;
        assert_eq!(
            metadata_from_header_json(zero_sequence).unwrap_err(),
            ControlStreamFrameError::InvalidSequence
        );

        let bad_digest = br#"{"sequence":1,"record_digest":"sha256:abc"}"#;
        assert_eq!(
            metadata_from_header_json(bad_digest).unwrap_err(),
            ControlStreamFrameError::InvalidDigest
        );
    }

    #[tokio::test]
    async fn append_and_read_control_stream_log() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("_anvil/control/v1/streams/bucket_locator/0a7f.anlog");
        let first = ControlStreamFrame::new(sample_header(1), sample_payload());
        let second = ControlStreamFrame::new(sample_header(2), sample_payload());

        let first_append = append_control_stream_frame(&path, &first).await.unwrap();
        let second_append = append_control_stream_frame(&path, &second).await.unwrap();
        let first_len = first.encode().unwrap().len();

        assert_eq!(first_append.offset, 0);
        assert_eq!(first_append.encoded_len, first_len);
        assert_eq!(second_append.offset, first_len as u64);
        assert_eq!(second_append.position.sequence.get(), 2);

        let log = read_control_stream_log(&path).await.unwrap();
        assert_eq!(log.records.len(), 2);
        assert_eq!(log.partial_final_frame, None);
        assert_eq!(log.records[1].metadata.sequence.get(), 2);
    }

    #[tokio::test]
    async fn append_rejects_log_with_partial_final_frame() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("stream.anlog");
        let encoded = ControlStreamFrame::new(sample_header(1), sample_payload())
            .encode()
            .unwrap();
        tokio::fs::write(&path, &encoded[..encoded.len() - 1])
            .await
            .unwrap();

        let err = append_control_stream_frame(
            &path,
            &ControlStreamFrame::new(sample_header(2), sample_payload()),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("control stream log has partial final frame")
        );
    }

    #[tokio::test]
    async fn projection_diagnostic_detects_stream_projection_payload_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir
            .path()
            .join("_anvil/control/v1/streams/bucket_locator/0a7f.anlog");
        let stream_payload =
            br#"{"tenant_id":"tenant_acme","bucket_name":"releases","home_region":"eu-west-1"}"#;
        let projection_payload =
            br#"{"tenant_id":"tenant_acme","bucket_name":"releases","home_region":"us-east-1"}"#;
        let frame = ControlStreamFrame::new(
            sample_header_for_payload(1, stream_payload),
            stream_payload.to_vec(),
        );
        append_control_stream_frame(&path, &frame).await.unwrap();

        let clean = diagnose_control_stream_projection(
            &path,
            "bucket_locator",
            "0a7f",
            &[ControlProjectionRecord::new(
                "tenant_acme/releases",
                1,
                stream_payload.to_vec(),
            )],
        )
        .await
        .unwrap();
        assert!(clean.is_empty());

        let diagnostics = diagnose_control_stream_projection(
            &path,
            "bucket_locator",
            "0a7f",
            &[ControlProjectionRecord::new(
                "tenant_acme/releases",
                1,
                projection_payload.to_vec(),
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
