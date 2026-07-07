use super::types::*;
use crate::error_codes::AnvilErrorCode;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const CORE_REF_LOCK_RETRY_ATTEMPTS: usize = 12_000;
const CORE_REF_LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);
const CORE_CONTROL_READ_RETRY_ATTEMPTS: usize = 400;
const LOCAL_ERASURE_PROFILE_ID: &str = "ec-4-2";
const LOCAL_PLACEMENT_EPOCH: u64 = 1;
const LOCAL_SHARD_FSYNC_SEQUENCE: u64 = 1;
#[cfg(test)]
const LOCAL_DATA_SHARDS: usize = 4;
#[cfg(test)]
const LOCAL_PARITY_SHARDS: usize = 2;
const LOCAL_NODE_ID_PREFIX: &str = "local-node";
const LOCAL_CONTROL_REPLICA_COUNT: usize = 5;
const LOCAL_CONTROL_WRITE_QUORUM: usize = 3;
const LOCAL_CONTROL_READ_QUORUM: usize = 3;
const LOCAL_CONTROL_NODE_ID_PREFIX: &str = "local-control-node";

#[derive(Debug, thiserror::Error)]
pub enum CoreStoreCommitError {
    #[error(
        "CoreStore stream {stream_id} head mismatch: expected {expected_last_sequence}/{expected_last_event_hash}, got {actual_sequence}/{actual_event_hash}"
    )]
    StreamHeadMismatch {
        stream_id: String,
        expected_last_sequence: u64,
        expected_last_event_hash: String,
        actual_sequence: u64,
        actual_event_hash: String,
    },
}

pub fn is_stream_head_mismatch(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.downcast_ref::<CoreStoreCommitError>().is_some())
}

fn is_incomplete_core_frame_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .to_string()
            .contains("CoreStore frame ended unexpectedly")
    })
}

const ZERO_HASH: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const MAX_CORE_FENCE_TTL_MS: u64 = 120_000;
const CORE_STREAM_SEGMENT_MAGIC: &[u8; 8] = b"ANSEG001";
const CORE_ACTIVE_STREAM_MAGIC: &[u8; 8] = b"ANASTR1\0";
const CORE_BLOCK_SHARD_MAGIC: &[u8; 8] = b"ANBLK\n\0\0";
const CORE_WAL_FILE_MAGIC: &[u8; 6] = b"ANWAL\n";
const CORE_WAL_FRAME_MAGIC: &[u8; 4] = b"AWF1";
const CORE_STREAM_SEGMENT_VERSION: u16 = 1;
const CORE_ACTIVE_STREAM_VERSION: u16 = 1;
const CORE_BLOCK_SHARD_VERSION: u16 = 1;
const CORE_WAL_VERSION: u16 = 1;
const CORE_WAL_EPOCH: u64 = 1;
const CORE_WAL_MAX_INLINE_PAYLOAD_BYTES: usize = 64 * 1024;
const CORE_WAL_SOFT_LIMIT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const CORE_WAL_HARD_LIMIT_BYTES: u64 = 12 * 1024 * 1024 * 1024;
const CORE_WAL_SOFT_LAG_SECONDS: u64 = 60;
const CORE_WAL_HARD_LAG_SECONDS: u64 = 300;
const CORE_LANDED_BYTES_SOFT_LIMIT_BYTES: u64 = 2 * CORE_WAL_SOFT_LIMIT_BYTES;
const CORE_LANDED_BYTES_HARD_LIMIT_BYTES: u64 = 3 * CORE_WAL_SOFT_LIMIT_BYTES;
const CORE_WAL_SOFT_BACKPRESSURE_DELAY: Duration = Duration::from_millis(1);
const CORE_WAL_NODE_ID: &str = "local-node";
const CORE_STREAM_SEGMENT_HEADER_SCHEMA: &str = "anvil.core.stream_segment_header.v1";
const CORE_STREAM_RECORD_HEADER_SCHEMA: &str = "anvil.core.stream_record_header.v1";
const CORE_STREAM_SEGMENT_TRAILER_SCHEMA: &str = "anvil.core.stream_segment_trailer.v1";
const CORE_BLOCK_SHARD_HEADER_SCHEMA: &str = "anvil.core.block_shard.v1";
const CORE_WAL_RECORD_SCHEMA: &str = "anvil.core.wal_record.v1";
const CORE_WAL_FINALISATION_SCHEMA: &str = "anvil.core.wal_finalisation.v1";
const CORE_WAL_FINALISATION_RECORD_KIND: &str = "core_wal.finalisation";
const CORE_TRANSACTION_STREAM_ID: &str = "core_transactions";
const CORE_TRANSACTION_PARTITION_ID: &str = "core-control";
const CORE_TRANSACTION_RECORD_KIND: &str = "core_transaction";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LocalErasureProfile {
    id: &'static str,
    codec_id: &'static str,
    data_shards: usize,
    parity_shards: usize,
    minimum_read_shards: usize,
    minimum_write_ack_shards: usize,
    logical_block_target_bytes: u64,
    max_shard_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalShardPlacement {
    node_id: String,
    region_id: String,
    cell_id: String,
}

#[derive(Debug, Clone)]
struct MaterializedLogicalBlock {
    object_manifest: CoreObjectManifest,
    logical_offset: u64,
    logical_length: u64,
    plaintext_hash: String,
}

impl LocalErasureProfile {
    fn total_shards(self) -> usize {
        self.data_shards + self.parity_shards
    }
}

const LOCAL_EC_4_2_PROFILE: LocalErasureProfile = LocalErasureProfile {
    id: "ec-4-2",
    codec_id: "rs-gf256-vandermonde-0x11d-v1/ec-4-2",
    data_shards: 4,
    parity_shards: 2,
    minimum_read_shards: 4,
    minimum_write_ack_shards: 6,
    logical_block_target_bytes: 64 * 1024 * 1024,
    max_shard_size_bytes: 16 * 1024 * 1024,
};

const LOCAL_EC_8_3_PROFILE: LocalErasureProfile = LocalErasureProfile {
    id: "ec-8-3",
    codec_id: "rs-gf256-vandermonde-0x11d-v1/ec-8-3",
    data_shards: 8,
    parity_shards: 3,
    minimum_read_shards: 8,
    minimum_write_ack_shards: 11,
    logical_block_target_bytes: 128 * 1024 * 1024,
    max_shard_size_bytes: 16 * 1024 * 1024,
};

const LOCAL_REPLICATED_3_PROFILE: LocalErasureProfile = LocalErasureProfile {
    id: "replicated-3",
    codec_id: "rs-gf256-vandermonde-0x11d-v1/replicated-3",
    data_shards: 1,
    parity_shards: 2,
    minimum_read_shards: 1,
    minimum_write_ack_shards: 3,
    logical_block_target_bytes: 16 * 1024 * 1024,
    max_shard_size_bytes: 16 * 1024 * 1024,
};

#[derive(Debug, Clone, Copy)]
struct CoreAdmissionCapacityLimits {
    wal_soft_limit_bytes: u64,
    wal_hard_limit_bytes: u64,
    wal_soft_lag_seconds: u64,
    wal_hard_lag_seconds: u64,
    landed_bytes_soft_limit_bytes: u64,
    landed_bytes_hard_limit_bytes: u64,
}

impl CoreAdmissionCapacityLimits {
    const fn production() -> Self {
        Self {
            wal_soft_limit_bytes: CORE_WAL_SOFT_LIMIT_BYTES,
            wal_hard_limit_bytes: CORE_WAL_HARD_LIMIT_BYTES,
            wal_soft_lag_seconds: CORE_WAL_SOFT_LAG_SECONDS,
            wal_hard_lag_seconds: CORE_WAL_HARD_LAG_SECONDS,
            landed_bytes_soft_limit_bytes: CORE_LANDED_BYTES_SOFT_LIMIT_BYTES,
            landed_bytes_hard_limit_bytes: CORE_LANDED_BYTES_HARD_LIMIT_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoreStore {
    storage: Storage,
    write_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamRecord {
    schema: String,
    stream_id: String,
    partition_id: String,
    sequence: u64,
    cursor: String,
    previous_event_hash: String,
    event_hash: String,
    record_kind: String,
    payload_hash: String,
    payload_b64: String,
    transaction_id: Option<String>,
    idempotency_key_hash: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamSegmentHeader {
    schema: String,
    stream_id: String,
    partition_id: String,
    segment_id: String,
    first_sequence: u64,
    last_sequence: u64,
    source_family: String,
    created_at: String,
    sealed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamRecordHeader {
    schema: String,
    stream_id: String,
    sequence: u64,
    record_kind: String,
    payload_hash: String,
    payload_content_type: String,
    mutation_id: String,
    idempotency_key_hash: Option<String>,
    previous_event_hash: String,
    event_hash: String,
    transaction_id: Option<String>,
    created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredStreamSegmentTrailer {
    schema: String,
    stream_id: String,
    partition_id: String,
    segment_id: String,
    first_sequence: u64,
    last_sequence: u64,
    record_count: u64,
    payload_hash: String,
    sealed_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoreWalLandedByte {
    sha256: String,
    length: u64,
    landing_id: String,
    relative_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoreWalAdmissionRecord {
    schema: String,
    node_id: String,
    wal_epoch: u64,
    sequence: u64,
    mutation_id: String,
    idempotency_key_hash: Option<String>,
    anvil_storage_tenant_id: String,
    authz_scope: serde_json::Value,
    operation_family: String,
    writer_family: String,
    target: serde_json::Value,
    preconditions: serde_json::Value,
    boundary_values: Vec<CoreBoundaryValue>,
    landed_bytes: Vec<CoreWalLandedByte>,
    created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CoreWalFinalisationRecord {
    schema: String,
    node_id: String,
    wal_epoch: u64,
    wal_sequence: u64,
    mutation_id: String,
    operation_family: String,
    writer_family: String,
    target: serde_json::Value,
    boundary_values: Vec<CoreBoundaryValue>,
    landed_bytes: Vec<CoreWalLandedByte>,
    state: String,
    finalised_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CoreWalRecordKey {
    node_id: String,
    wal_epoch: u64,
    wal_sequence: u64,
}

impl From<&CoreWalAdmissionRecord> for CoreWalRecordKey {
    fn from(record: &CoreWalAdmissionRecord) -> Self {
        Self {
            node_id: record.node_id.clone(),
            wal_epoch: record.wal_epoch,
            wal_sequence: record.sequence,
        }
    }
}

enum CoreWalPayload<'a> {
    Empty,
    Inline(&'a [u8]),
    Landed(&'a [u8]),
}

struct CoreStoreLock {
    path: PathBuf,
}

impl Drop for CoreStoreLock {
    fn drop(&mut self) {
        let started_at = Instant::now();
        let _ = std::fs::remove_file(&self.path);
        crate::perf::record_io_duration(
            "core_store",
            "lock_remove_on_drop",
            &self.path,
            0,
            started_at.elapsed(),
        );
    }
}

impl From<StoredStreamRecord> for StreamRecord {
    fn from(record: StoredStreamRecord) -> Self {
        Self {
            schema: record.schema,
            stream_id: record.stream_id,
            partition_id: record.partition_id,
            sequence: record.sequence,
            cursor: record.cursor,
            previous_event_hash: record.previous_event_hash,
            event_hash: record.event_hash,
            record_kind: record.record_kind,
            payload_hash: record.payload_hash,
            payload: URL_SAFE_NO_PAD
                .decode(record.payload_b64)
                .unwrap_or_default(),
            transaction_id: record.transaction_id,
            idempotency_key_hash: record.idempotency_key_hash,
            created_at: record.created_at,
        }
    }
}

impl From<&StreamRecord> for StoredStreamRecord {
    fn from(record: &StreamRecord) -> Self {
        Self {
            schema: record.schema.clone(),
            stream_id: record.stream_id.clone(),
            partition_id: record.partition_id.clone(),
            sequence: record.sequence,
            cursor: record.cursor.clone(),
            previous_event_hash: record.previous_event_hash.clone(),
            event_hash: record.event_hash.clone(),
            record_kind: record.record_kind.clone(),
            payload_hash: record.payload_hash.clone(),
            payload_b64: URL_SAFE_NO_PAD.encode(&record.payload),
            transaction_id: record.transaction_id.clone(),
            idempotency_key_hash: record.idempotency_key_hash.clone(),
            created_at: record.created_at.clone(),
        }
    }
}

impl CoreStore {
    pub async fn new(storage: Storage) -> Result<Self> {
        let store = Self {
            storage,
            write_lock: Arc::new(Mutex::new(())),
        };
        store.ensure_layout().await?;
        store.recover_core_wal().await?;
        Ok(store)
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub async fn put_blob(&self, input: PutBlob) -> Result<CoreObjectRef> {
        self.put_blob_with_profile(input, local_erasure_profile(LOCAL_ERASURE_PROFILE_ID)?)
            .await
    }

    async fn put_blob_with_profile(
        &self,
        input: PutBlob,
        profile: LocalErasureProfile,
    ) -> Result<CoreObjectRef> {
        let _perf_guard = crate::perf::guard("anvil_core_store_op", &[("operation", "put_blob")]);
        self.ensure_layout().await?;
        validate_logical_id(&input.logical_name, "blob logical name")?;
        let admission = self
            .admit_core_mutation(
                "object.put",
                "object_blob",
                serde_json::json!({
                    "logical_name": input.logical_name.clone(),
                    "region_id": input.region_id.clone(),
                    "erasure_profile_id": profile.id,
                }),
                input.mutation_id.clone(),
                None,
                CoreWalPayload::Landed(&input.bytes),
                input.boundary_values,
            )
            .await?;
        let landed =
            admission.landed_bytes.first().cloned().ok_or_else(|| {
                anyhow!("CoreStore put_blob admission did not produce landed bytes")
            })?;
        let materialised_bytes = self.read_landed_bytes(&landed).await?;
        let hash = strip_sha256_prefix(&landed.sha256)?.to_string();
        let object_ref = self
            .materialise_object_blob_bytes(
                &hash,
                &materialised_bytes,
                &admission.boundary_values,
                &admission.mutation_id,
                profile,
            )
            .await?;
        self.mark_core_wal_finalised_unlocked(&admission, "committed")
            .await?;
        Ok(object_ref)
    }

    async fn materialise_object_blob_bytes(
        &self,
        hash: &str,
        materialised_bytes: &[u8],
        boundary_values: &[CoreBoundaryValue],
        mutation_id: &str,
        profile: LocalErasureProfile,
    ) -> Result<CoreObjectRef> {
        if sha256_hex(materialised_bytes) != hash {
            bail!("CoreStore object materialisation hash mismatch");
        }
        let shards = encode_erasure_shards(materialised_bytes, profile)?;
        let placements = plan_local_shard_placements(profile)?;
        let block_id = local_block_id_for_object_hash(hash);
        let mut object_placements = Vec::with_capacity(shards.len());
        let mut stripe_size = 0u64;

        for (shard_index, shard) in shards.iter().enumerate() {
            let placement = placements.get(shard_index).ok_or_else(|| {
                anyhow!("CoreStore missing local placement for shard {shard_index}")
            })?;
            let shard_hash = sha256_hex(shard);
            let shard_path =
                self.shard_path(&placement.node_id, hash, shard_index as u16, &shard_hash);
            let logical_offset = shard_index as u64 * shard.len() as u64;
            let shard_file = encode_block_shard_file(
                BlockShardHeaderInput {
                    block_id: block_id.clone(),
                    erasure_set_id: "local-erasure-set".to_string(),
                    shard_index: shard_index as u16,
                    erasure_profile_id: profile.id.to_string(),
                    logical_file_id: format!("sha256:{hash}"),
                    logical_offset,
                    logical_length: shard.len() as u64,
                    payload_plain_hash: format!("sha256:{shard_hash}"),
                    payload_stored_hash: format!("sha256:{shard_hash}"),
                    compression: "none".to_string(),
                    encryption: "none".to_string(),
                    placement_epoch: LOCAL_PLACEMENT_EPOCH,
                    boundary_summary_hash: boundary_summary_hash(boundary_values)?,
                    writer_family: "object_blob".to_string(),
                    created_by_mutation_id: mutation_id.to_string(),
                },
                shard,
            )?;
            if let Some(parent) = shard_path.parent() {
                fs::create_dir_all(parent).await?;
            }
            write_file_atomic(&shard_path, &shard_file).await?;
            stripe_size =
                stripe_size.max((shard.len() as u64).saturating_mul(profile.data_shards as u64));
            object_placements.push(CoreObjectPlacement {
                shard_index: shard_index as u16,
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_hash: format!("sha256:{shard_hash}"),
                stored_size: shard.len() as u64,
                generation: 1,
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            });
        }

        Ok(CoreObjectRef {
            hash: format!("sha256:{hash}"),
            logical_size: materialised_bytes.len() as u64,
            manifest_ref: encode_manifest_ref_with_profile(hash, profile.id),
            encoding: CoreObjectEncoding {
                profile_id: profile.id.to_string(),
                data_shards: profile.data_shards as u16,
                parity_shards: profile.parity_shards as u16,
                minimum_read_shards: profile.minimum_read_shards as u16,
                minimum_write_ack_shards: profile.minimum_write_ack_shards as u16,
                stripe_size,
                placement_scope: "region".to_string(),
                repair_priority: "normal".to_string(),
                encryption: "none".to_string(),
            },
            placements: object_placements,
        })
    }

    pub async fn get_blob(&self, input: GetBlob) -> Result<Vec<u8>> {
        let _perf_guard = crate::perf::guard("anvil_core_store_op", &[("operation", "get_blob")]);
        let expected_hash = strip_sha256_prefix(&input.object_ref.hash)?;
        let manifest = self.read_object_manifest(&input.object_ref).await?;
        if manifest.object_hash != input.object_ref.hash {
            bail!(
                "CoreStore manifest hash mismatch: ref {}, manifest {}",
                input.object_ref.hash,
                manifest.object_hash
            );
        }
        if manifest.logical_size != input.object_ref.logical_size {
            bail!(
                "CoreStore manifest size mismatch: ref {}, manifest {}",
                input.object_ref.logical_size,
                manifest.logical_size
            );
        }
        let profile = local_erasure_profile(&manifest.encoding.profile_id)?;

        let data_shards = usize::from(manifest.encoding.data_shards);
        let parity_shards = usize::from(manifest.encoding.parity_shards);
        let minimum_read_shards = usize::from(manifest.encoding.minimum_read_shards);
        let minimum_write_ack_shards = usize::from(manifest.encoding.minimum_write_ack_shards);
        if data_shards == 0 || parity_shards == 0 {
            bail!("CoreStore erasure profile must include data and parity shards");
        }
        if minimum_read_shards != data_shards {
            bail!(
                "CoreStore unsupported minimum_read_shards {}; expected {}",
                minimum_read_shards,
                data_shards
            );
        }
        if minimum_write_ack_shards > data_shards + parity_shards {
            bail!(
                "CoreStore minimum_write_ack_shards {} exceeds total shard count {}",
                minimum_write_ack_shards,
                data_shards + parity_shards
            );
        }
        if manifest.encoding.placement_scope != "region" {
            bail!(
                "CoreStore unsupported placement_scope {}",
                manifest.encoding.placement_scope
            );
        }
        if manifest.encoding.repair_priority.is_empty() {
            bail!("CoreStore repair_priority must not be empty");
        }
        let total_shards = data_shards + parity_shards;
        let mut shards = vec![None; total_shards];
        for placement in &manifest.placements {
            let index = usize::from(placement.shard_index);
            if index >= total_shards {
                bail!(
                    "CoreStore manifest placement index {} exceeds total shard count {}",
                    index,
                    total_shards
                );
            }
            let shard_hash = strip_sha256_prefix(&placement.shard_hash)?;
            let shard_path = self.shard_path(
                &placement.node_id,
                expected_hash,
                placement.shard_index,
                shard_hash,
            );
            let expected_block_id = local_block_id_for_object_hash(expected_hash);
            let shard_bytes = match read_block_shard_file(
                &shard_path,
                BlockShardExpectation {
                    block_id: &expected_block_id,
                    shard_index: placement.shard_index,
                    erasure_profile_id: profile.id,
                    placement_epoch: placement.placement_epoch,
                    payload_hash: &placement.shard_hash,
                    payload_len: placement.stored_size,
                },
                "read_blob_shard",
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(err) if is_not_found_error(&err) => continue,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read CoreStore shard {}", shard_path.display()));
                }
            };
            shards[index] = Some(shard_bytes);
        }
        let present = shards.iter().filter(|shard| shard.is_some()).count();
        if present < data_shards {
            bail!(
                "CoreStore blob {} has only {} shards present; {} data shards required",
                input.object_ref.hash,
                present,
                data_shards
            );
        }
        let profile = local_erasure_profile_for_counts(
            &manifest.encoding.profile_id,
            data_shards,
            parity_shards,
        )?;
        reconstruct_data_shards(&mut shards, profile)?;
        let mut data = Vec::with_capacity(
            data_shards.saturating_mul(
                shards
                    .iter()
                    .find_map(|shard| shard.as_ref().map(Vec::len))
                    .unwrap_or_default(),
            ),
        );
        for shard in shards.iter().take(data_shards) {
            let Some(shard) = shard else {
                bail!("CoreStore erasure reconstruction left a missing data shard");
            };
            data.extend_from_slice(shard);
        }
        data.truncate(input.object_ref.logical_size as usize);
        let actual = sha256_hex(&data);
        if actual != expected_hash {
            bail!("CoreStore blob hash mismatch: expected {expected_hash}, got {actual}");
        }
        Ok(data)
    }

    pub async fn get_blob_range(&self, input: GetBlobRange) -> Result<Vec<u8>> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "get_blob_range")]);
        if input.range.start > input.range.end_exclusive {
            bail!("CoreStore range start must be <= end_exclusive");
        }
        if input.range.end_exclusive > input.object_ref.logical_size {
            bail!("CoreStore range end_exclusive exceeds logical object size");
        }
        if input.range.start == input.range.end_exclusive {
            return Ok(Vec::new());
        }

        let expected_hash = strip_sha256_prefix(&input.object_ref.hash)?;
        let manifest = self
            .read_object_manifest_for_range(&input.object_ref, &input.range)
            .await?;
        validate_manifest_for_object_ref(&manifest, &input.object_ref, expected_hash)?;

        let data_shards = usize::from(manifest.encoding.data_shards);
        let shard_len = input
            .object_ref
            .logical_size
            .div_ceil(data_shards as u64)
            .max(1);
        let mut out = Vec::with_capacity(
            usize::try_from(input.range.end_exclusive - input.range.start).unwrap_or(usize::MAX),
        );

        for shard_index in 0..data_shards {
            let shard_logical_start = shard_index as u64 * shard_len;
            let shard_logical_end =
                (shard_logical_start + shard_len).min(input.object_ref.logical_size);
            let overlap_start = input.range.start.max(shard_logical_start);
            let overlap_end = input.range.end_exclusive.min(shard_logical_end);
            if overlap_start >= overlap_end {
                continue;
            }

            let Some(placement) = manifest
                .placements
                .iter()
                .find(|placement| usize::from(placement.shard_index) == shard_index)
            else {
                return self.get_blob_range_via_full_reconstruction(input).await;
            };
            let shard_hash = strip_sha256_prefix(&placement.shard_hash)?;
            let shard_path = self.shard_path(
                &placement.node_id,
                expected_hash,
                placement.shard_index,
                shard_hash,
            );
            let expected_block_id = local_block_id_for_object_hash(expected_hash);
            let shard_bytes = match read_block_shard_file(
                &shard_path,
                BlockShardExpectation {
                    block_id: &expected_block_id,
                    shard_index: placement.shard_index,
                    erasure_profile_id: &manifest.encoding.profile_id,
                    placement_epoch: placement.placement_epoch,
                    payload_hash: &placement.shard_hash,
                    payload_len: placement.stored_size,
                },
                "read_blob_range_shard",
            )
            .await
            {
                Ok(bytes) => bytes,
                Err(err) if is_not_found_error(&err) => {
                    return self.get_blob_range_via_full_reconstruction(input).await;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("read CoreStore range shard {}", shard_path.display())
                    });
                }
            };
            let shard_offset = usize::try_from(overlap_start - shard_logical_start)
                .map_err(|_| anyhow!("CoreStore range offset exceeds usize"))?;
            let shard_end = usize::try_from(overlap_end - shard_logical_start)
                .map_err(|_| anyhow!("CoreStore range end exceeds usize"))?;
            out.extend_from_slice(&shard_bytes[shard_offset..shard_end]);
        }

        Ok(out)
    }

    async fn get_blob_range_via_full_reconstruction(&self, input: GetBlobRange) -> Result<Vec<u8>> {
        let full = self
            .get_blob(GetBlob {
                object_ref: input.object_ref,
            })
            .await?;
        let start = usize::try_from(input.range.start)
            .map_err(|_| anyhow!("CoreStore range start exceeds usize"))?;
        let end = usize::try_from(input.range.end_exclusive)
            .map_err(|_| anyhow!("CoreStore range end exceeds usize"))?;
        Ok(full[start..end].to_vec())
    }

    pub async fn write_logical_file(
        &self,
        mut request: WriteLogicalFileRequest,
    ) -> Result<CoreLogicalFileManifest> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "write_logical_file")],
        );
        validate_logical_id(&request.writer_family, "writer family")?;
        validate_logical_id(&request.logical_file_id, "logical file id")?;
        validate_logical_id(&request.mutation_id, "logical file mutation id")?;
        let profile = local_erasure_profile(&request.pipeline_policy.erasure_profile_id)?;
        if request.pipeline_policy.encryption != "none" {
            bail!("CoreStore logical file encryption policy is not implemented for local backend");
        }
        validate_pipeline_policy(&request.pipeline_policy, profile)?;

        let source = std::mem::take(&mut request.source);
        let plaintext_hash = format!("sha256:{}", sha256_hex(&source));
        let plaintext_len = source.len() as u64;
        let (blocks, compression) = if request.pipeline_policy.compression == "none" {
            let compression = none_compression_descriptor(&source);
            (
                self.write_uncompressed_logical_file_blocks(&request, source, profile)
                    .await?,
                compression,
            )
        } else {
            let (stored_source, compression) =
                encode_logical_file_source(&request.pipeline_policy.compression, source)?;
            let object_ref = self
                .put_blob_with_profile(
                    PutBlob {
                        logical_name: format!("{}/block-000000", request.logical_file_id),
                        bytes: stored_source,
                        boundary_values: request.boundary_values.clone(),
                        region_id: request.region_id.clone(),
                        mutation_id: format!("{}-block-000000", request.mutation_id),
                    },
                    profile,
                )
                .await?;
            let object_manifest = self.read_object_manifest(&object_ref).await?;
            (
                vec![MaterializedLogicalBlock {
                    object_manifest,
                    logical_offset: 0,
                    logical_length: plaintext_len,
                    plaintext_hash: plaintext_hash.clone(),
                }],
                compression,
            )
        };
        logical_file_manifest_from_object_manifests(
            &request,
            &blocks,
            plaintext_hash,
            plaintext_len,
            compression,
        )
    }

    async fn write_uncompressed_logical_file_blocks(
        &self,
        request: &WriteLogicalFileRequest,
        source: Vec<u8>,
        profile: LocalErasureProfile,
    ) -> Result<Vec<MaterializedLogicalBlock>> {
        let target_block_size = usize::try_from(effective_target_block_size(
            &request.pipeline_policy,
            profile,
        ))
        .map_err(|_| anyhow!("CoreStore target_block_size exceeds usize"))?;
        let mut blocks = Vec::new();
        if source.is_empty() {
            let object_ref = self
                .put_blob_with_profile(
                    PutBlob {
                        logical_name: format!("{}/block-000000", request.logical_file_id),
                        bytes: Vec::new(),
                        boundary_values: request.boundary_values.clone(),
                        region_id: request.region_id.clone(),
                        mutation_id: format!("{}-block-000000", request.mutation_id),
                    },
                    profile,
                )
                .await?;
            let object_manifest = self.read_object_manifest(&object_ref).await?;
            blocks.push(MaterializedLogicalBlock {
                object_manifest,
                logical_offset: 0,
                logical_length: 0,
                plaintext_hash: format!("sha256:{}", sha256_hex(&[])),
            });
            return Ok(blocks);
        }

        for (index, (start, end)) in
            logical_block_ranges_for_source(&source, request, target_block_size)
                .into_iter()
                .enumerate()
        {
            let logical_offset = start as u64;
            let chunk = &source[start..end];
            let chunk_bytes = chunk.to_vec();
            let chunk_hash = format!("sha256:{}", sha256_hex(&chunk_bytes));
            let object_ref = self
                .put_blob_with_profile(
                    PutBlob {
                        logical_name: format!("{}/block-{index:06}", request.logical_file_id),
                        bytes: chunk_bytes,
                        boundary_values: request.boundary_values.clone(),
                        region_id: request.region_id.clone(),
                        mutation_id: format!("{}-block-{index:06}", request.mutation_id),
                    },
                    profile,
                )
                .await?;
            let object_manifest = self.read_object_manifest(&object_ref).await?;
            blocks.push(MaterializedLogicalBlock {
                object_manifest,
                logical_offset,
                logical_length: chunk.len() as u64,
                plaintext_hash: chunk_hash,
            });
        }
        Ok(blocks)
    }

    pub async fn write_logical_file_ref(
        &self,
        request: WriteLogicalFileRequest,
    ) -> Result<CoreObjectRef> {
        let manifest = self.write_logical_file(request).await?;
        Ok(core_object_ref_from_logical_file_manifest(&manifest))
    }

    pub async fn read_logical_range(&self, request: ReadLogicalRangeRequest) -> Result<Vec<u8>> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "read_logical_range")],
        );
        validate_logical_file_manifest_shape(&request.manifest)?;
        if request.manifest.compression.algorithm != "none" {
            return self.read_compressed_logical_range(request).await;
        }
        let mut out = Vec::new();
        for range in request.ranges {
            if let Some(expected_boundary) = request.expected_boundary.as_ref() {
                ensure_range_is_inside_expected_boundary(
                    &request.manifest,
                    &range,
                    expected_boundary,
                )?;
            }
            out.extend(
                self.read_uncompressed_logical_range(&request.manifest, range)
                    .await?,
            );
        }
        Ok(out)
    }

    async fn read_uncompressed_logical_range(
        &self,
        manifest: &CoreLogicalFileManifest,
        range: CoreByteRange,
    ) -> Result<Vec<u8>> {
        if range.start > range.end_exclusive {
            bail!("CoreStore logical range start must be <= end_exclusive");
        }
        if range.end_exclusive > manifest.logical_size {
            bail!("CoreStore logical range exceeds logical file size");
        }
        if range.start == range.end_exclusive {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(
            usize::try_from(range.end_exclusive - range.start).unwrap_or(usize::MAX),
        );
        let mut blocks = manifest.blocks.iter().collect::<Vec<_>>();
        blocks.sort_by_key(|block| block.logical_offset);
        for block in blocks {
            let block_start = block.logical_offset;
            let block_end = block.logical_offset.saturating_add(block.logical_length);
            let overlap_start = range.start.max(block_start);
            let overlap_end = range.end_exclusive.min(block_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let object_ref =
                object_ref_from_logical_block_ref(block, &manifest.erasure_profile_id)?;
            out.extend(
                self.get_blob_range(GetBlobRange {
                    object_ref,
                    range: CoreByteRange {
                        start: overlap_start - block_start,
                        end_exclusive: overlap_end - block_start,
                    },
                })
                .await?,
            );
        }
        Ok(out)
    }

    async fn read_compressed_logical_range(
        &self,
        request: ReadLogicalRangeRequest,
    ) -> Result<Vec<u8>> {
        let plaintext = self.read_logical_file_plaintext(&request.manifest).await?;
        let mut out = Vec::new();
        for range in request.ranges {
            if range.start > range.end_exclusive || range.end_exclusive > plaintext.len() as u64 {
                bail!("CoreStore logical range exceeds logical file size");
            }
            if let Some(expected_boundary) = request.expected_boundary.as_ref() {
                ensure_range_is_inside_expected_boundary(
                    &request.manifest,
                    &range,
                    expected_boundary,
                )?;
            }
            let start = usize::try_from(range.start)
                .map_err(|_| anyhow!("CoreStore logical range start exceeds usize"))?;
            let end = usize::try_from(range.end_exclusive)
                .map_err(|_| anyhow!("CoreStore logical range end exceeds usize"))?;
            out.extend_from_slice(&plaintext[start..end]);
        }
        Ok(out)
    }

    async fn read_logical_file_plaintext(
        &self,
        manifest: &CoreLogicalFileManifest,
    ) -> Result<Vec<u8>> {
        validate_logical_file_manifest_shape(manifest)?;
        let stored = if manifest.compression.algorithm == "none" {
            self.read_uncompressed_logical_range(
                manifest,
                CoreByteRange {
                    start: 0,
                    end_exclusive: manifest.logical_size,
                },
            )
            .await?
        } else {
            let object_ref = object_ref_from_logical_file_manifest(manifest)?;
            self.get_blob(GetBlob { object_ref }).await?
        };
        let plaintext = decode_logical_file_source(&manifest.compression.algorithm, stored)?;
        let actual_hash = format!("sha256:{}", sha256_hex(&plaintext));
        if actual_hash != manifest.content_hash {
            bail!(
                "CoreStore logical file content hash mismatch: expected {}, got {}",
                manifest.content_hash,
                actual_hash
            );
        }
        Ok(plaintext)
    }

    pub async fn verify_logical_file_manifest(
        &self,
        manifest: &CoreLogicalFileManifest,
    ) -> Result<CoreLogicalFileVerificationReport> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "verify_logical_file_manifest")],
        );
        let _plaintext = self.read_logical_file_plaintext(manifest).await?;
        Ok(CoreLogicalFileVerificationReport {
            verified: true,
            logical_file_id: manifest.logical_file_id.clone(),
            checked_blocks: manifest.blocks.len() as u64,
            checked_shards: manifest
                .blocks
                .iter()
                .map(|block| block.shards.len() as u64)
                .sum(),
            content_hash: manifest.content_hash.clone(),
        })
    }

    pub async fn put_boundary_schema(
        &self,
        input: PutBoundarySchema,
    ) -> Result<BoundarySchemaReceipt> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "put_boundary_schema")],
        );
        validate_logical_id(&input.mutation_id, "boundary schema mutation id")?;
        let mut schema = input.schema;
        if schema.created_at.is_empty() {
            schema.created_at = now_rfc3339();
        }
        let current_ref = self
            .read_ref(&boundary_schema_ref_name(&schema.bucket))
            .await?;
        let current_schema = if let Some(ref_value) = current_ref.as_ref() {
            let object_ref = decode_core_object_ref_target(&ref_value.target)?;
            let bytes = self.get_blob(GetBlob { object_ref }).await?;
            Some(serde_json::from_slice::<CoreBoundarySchema>(&bytes)?)
        } else {
            None
        };
        validate_boundary_schema(&schema, current_schema.as_ref(), input.expected_generation)?;

        let bytes = serde_json::to_vec(&schema)?;
        let schema_hash = format!("sha256:{}", sha256_hex(&bytes));
        let object_ref = self
            .put_blob(PutBlob {
                logical_name: format!(
                    "boundary_schema/bucket:{}/generation:{}",
                    schema.bucket, schema.generation
                ),
                bytes,
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: input.mutation_id.clone(),
            })
            .await?;
        let ref_name = boundary_schema_ref_name(&schema.bucket);
        let receipt = self
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name,
                expected_generation: current_ref.as_ref().map(|value| value.generation),
                expected_target: current_ref.as_ref().map(|value| value.target.clone()),
                require_absent: current_ref.is_none(),
                require_present: current_ref.is_some(),
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&object_ref)?,
                transaction_id: None,
            })
            .await?;
        Ok(BoundarySchemaReceipt {
            bucket: schema.bucket,
            generation: schema.generation,
            ref_generation: receipt.generation,
            schema_hash,
        })
    }

    pub async fn read_boundary_schema(&self, bucket: &str) -> Result<Option<CoreBoundarySchema>> {
        validate_logical_id(bucket, "boundary schema bucket")?;
        let Some(ref_value) = self.read_ref(&boundary_schema_ref_name(bucket)).await? else {
            return Ok(None);
        };
        let object_ref = decode_core_object_ref_target(&ref_value.target)?;
        let bytes = self.get_blob(GetBlob { object_ref }).await?;
        let schema: CoreBoundarySchema = serde_json::from_slice(&bytes)?;
        if schema.schema != CORE_BOUNDARY_SCHEMA_SCHEMA {
            bail!("CoreStore boundary schema has invalid schema");
        }
        if schema.bucket != bucket {
            bail!("CoreStore boundary schema bucket mismatch");
        }
        Ok(Some(schema))
    }

    pub async fn append_stream(&self, input: AppendStreamRecord) -> Result<StreamAppendReceipt> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "append_stream")]);
        validate_logical_id(&input.stream_id, "stream id")?;
        validate_logical_id(&input.partition_id, "partition id")?;
        let _stream_guard = self.acquire_stream_lock(&input.stream_id).await?;
        let _guard = self.write_lock.lock().await;
        if let Some(receipt) = self.stream_idempotent_replay_unlocked(&input).await? {
            return Ok(receipt);
        }
        if let Some(fence) = input.fence.as_ref() {
            self.validate_fence_precondition_unlocked(fence).await?;
        }
        let wal_payload = if input.payload.len() <= CORE_WAL_MAX_INLINE_PAYLOAD_BYTES {
            CoreWalPayload::Inline(&input.payload)
        } else {
            CoreWalPayload::Landed(&input.payload)
        };
        let admission = self
            .admit_core_mutation(
                "stream.append",
                "stream",
                serde_json::json!({
                    "stream_id": input.stream_id.clone(),
                    "partition_id": input.partition_id.clone(),
                    "record_kind": input.record_kind.clone(),
                    "transaction_id": input.transaction_id.clone(),
                }),
                input
                    .transaction_id
                    .clone()
                    .unwrap_or_else(|| format!("stream-append:{}", uuid::Uuid::new_v4())),
                input.idempotency_key.clone(),
                wal_payload,
                Vec::new(),
            )
            .await?;
        match self.append_stream_unlocked(input).await {
            Ok(receipt) => {
                self.mark_core_wal_finalised_unlocked(&admission, "committed")
                    .await?;
                Ok(receipt)
            }
            Err(error) => {
                self.mark_core_wal_finalised_unlocked(&admission, "aborted")
                    .await
                    .with_context(|| "mark failed CoreStore stream append admission as aborted")?;
                Err(error)
            }
        }
    }

    pub(crate) async fn read_raw_stream(&self, stream_id: &str) -> Result<Vec<StreamRecord>> {
        validate_logical_id(stream_id, "stream id")?;
        self.read_all_stream_records(stream_id).await
    }

    pub(crate) async fn raw_stream_head(&self, stream_id: &str) -> Result<(u64, String)> {
        let records = self.read_raw_stream(stream_id).await?;
        Ok(stream_head_from_records(&records))
    }

    async fn append_stream_unlocked(
        &self,
        input: AppendStreamRecord,
    ) -> Result<StreamAppendReceipt> {
        let idempotency_key_hash = input
            .idempotency_key
            .as_deref()
            .map(|key| format!("sha256:{}", sha256_hex(key.as_bytes())));
        self.append_stream_unlocked_with_idempotency_hash(input, idempotency_key_hash)
            .await
    }

    async fn append_stream_unlocked_with_idempotency_hash(
        &self,
        input: AppendStreamRecord,
        idempotency_key_hash: Option<String>,
    ) -> Result<StreamAppendReceipt> {
        if let Some(fence) = input.fence.as_ref() {
            self.validate_fence_precondition_unlocked(fence).await?;
        }
        if let Some(receipt) = self
            .stream_idempotent_replay_by_hash_unlocked(
                &input.stream_id,
                &input.payload,
                idempotency_key_hash.as_deref(),
            )
            .await?
        {
            return Ok(receipt);
        }
        let mut records = self.read_all_stream_records(&input.stream_id).await?;
        let payload_hash = format!("sha256:{}", sha256_hex(&input.payload));

        let sequence = records
            .last()
            .map(|record| record.sequence + 1)
            .unwrap_or(1);
        let previous_event_hash = records
            .last()
            .map(|record| record.event_hash.clone())
            .unwrap_or_else(|| {
                "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .to_string()
            });
        let cursor = format!("{}:{sequence:020}", input.stream_id);
        let mut record = StreamRecord {
            schema: CORE_WATCH_EVENT_SCHEMA.to_string(),
            stream_id: input.stream_id.clone(),
            partition_id: input.partition_id,
            sequence,
            cursor,
            previous_event_hash,
            event_hash: String::new(),
            record_kind: input.record_kind,
            payload_hash,
            payload: input.payload,
            transaction_id: input.transaction_id,
            idempotency_key_hash,
            created_at: now_rfc3339(),
        };
        record.event_hash = format!("sha256:{}", sha256_hex(&event_hash_input(&record)?));
        records.push(record.clone());
        self.write_stream_records(&input.stream_id, &records)
            .await?;
        Ok(StreamAppendReceipt {
            stream_id: record.stream_id,
            sequence: record.sequence,
            cursor: record.cursor,
            event_hash: record.event_hash,
            idempotent_replay: false,
        })
    }

    async fn stream_idempotent_replay_unlocked(
        &self,
        input: &AppendStreamRecord,
    ) -> Result<Option<StreamAppendReceipt>> {
        let Some(idempotency_key) = input.idempotency_key.as_deref() else {
            return Ok(None);
        };
        let idempotency_key_hash = format!("sha256:{}", sha256_hex(idempotency_key.as_bytes()));
        self.stream_idempotent_replay_by_hash_unlocked(
            &input.stream_id,
            &input.payload,
            Some(&idempotency_key_hash),
        )
        .await
    }

    async fn stream_idempotent_replay_by_hash_unlocked(
        &self,
        stream_id: &str,
        payload: &[u8],
        idempotency_key_hash: Option<&str>,
    ) -> Result<Option<StreamAppendReceipt>> {
        let Some(idempotency_key_hash) = idempotency_key_hash else {
            return Ok(None);
        };
        let payload_hash = format!("sha256:{}", sha256_hex(payload));
        let records = self.read_all_stream_records(stream_id).await?;
        if let Some(existing) = records
            .iter()
            .find(|record| record.idempotency_key_hash.as_deref() == Some(idempotency_key_hash))
        {
            if existing.payload_hash != payload_hash {
                bail!(
                    "CoreStore stream idempotency conflict for stream {stream_id}: idempotency_key_hash={idempotency_key_hash}, existing_record_kind={}, existing_payload_hash={}, new_payload_hash={payload_hash}",
                    existing.record_kind,
                    existing.payload_hash
                );
            }
            return Ok(Some(StreamAppendReceipt {
                stream_id: existing.stream_id.clone(),
                sequence: existing.sequence,
                cursor: existing.cursor.clone(),
                event_hash: existing.event_hash.clone(),
                idempotent_replay: true,
            }));
        }
        Ok(None)
    }

    pub async fn read_stream(&self, input: ReadStream) -> Result<Vec<StreamRecord>> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "read_stream")]);
        validate_logical_id(&input.stream_id, "stream id")?;
        let records = self
            .read_stream_records_after(&input.stream_id, input.after_sequence, input.limit)
            .await?;
        self.filter_committed_stream_records(records).await
    }

    pub async fn seal_stream_segment(&self, input: SealStreamSegment) -> Result<CoreSegmentRef> {
        validate_logical_id(&input.stream_id, "stream id")?;
        validate_logical_id(&input.partition_id, "partition id")?;
        let records = self.read_all_stream_records(&input.stream_id).await?;
        if records.is_empty() {
            bail!(
                "CoreStore stream {} has no records to seal",
                input.stream_id
            );
        }
        let through_sequence = input
            .through_sequence
            .unwrap_or_else(|| records.last().map(|record| record.sequence).unwrap_or(0));
        let selected = records
            .into_iter()
            .filter(|record| record.sequence <= through_sequence)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            bail!(
                "CoreStore stream {} has no records at or before sequence {}",
                input.stream_id,
                through_sequence
            );
        }
        if selected
            .iter()
            .any(|record| record.partition_id != input.partition_id)
        {
            bail!(
                "CoreStore stream {} contains records outside partition {}",
                input.stream_id,
                input.partition_id
            );
        }
        let first_sequence = selected.first().map(|record| record.sequence).unwrap_or(0);
        let last_sequence = selected.last().map(|record| record.sequence).unwrap_or(0);
        let segment_id = format!(
            "seg:{}:{first_sequence:020}:{last_sequence:020}:{}",
            input.stream_id,
            sha256_hex(input.mutation_id.as_bytes())
        );
        let segment_bytes = encode_stream_segment(
            &input,
            &selected,
            &segment_id,
            first_sequence,
            last_sequence,
        )?;
        let segment_manifest = self
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "stream".to_string(),
                generation: 1,
                logical_file_id: format!(
                    "core_stream_segment:{}:{first_sequence:020}:{last_sequence:020}",
                    input.stream_id
                ),
                source: segment_bytes,
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy::default(),
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: input.mutation_id,
                region_id: "local".to_string(),
            })
            .await?;
        let object_ref = core_object_ref_from_logical_file_manifest(&segment_manifest);
        Ok(CoreSegmentRef {
            stream_id: input.stream_id,
            partition_id: input.partition_id,
            first_sequence,
            last_sequence,
            record_count: selected.len() as u64,
            segment_kind: input.segment_kind,
            object_ref,
        })
    }

    pub async fn read_stream_segment(&self, segment: &CoreSegmentRef) -> Result<Vec<StreamRecord>> {
        let bytes = self
            .get_blob(GetBlob {
                object_ref: segment.object_ref.clone(),
            })
            .await?;
        let records = decode_stream_segment(&bytes)?;
        if records.len() as u64 != segment.record_count {
            bail!("CoreStore stream segment record_count mismatch");
        }
        if records
            .first()
            .map(|record| record.sequence)
            .unwrap_or_default()
            != segment.first_sequence
        {
            bail!("CoreStore stream segment first_sequence mismatch");
        }
        if records
            .last()
            .map(|record| record.sequence)
            .unwrap_or_default()
            != segment.last_sequence
        {
            bail!("CoreStore stream segment last_sequence mismatch");
        }
        if records
            .iter()
            .any(|record| record.stream_id != segment.stream_id)
        {
            bail!("CoreStore stream segment stream_id mismatch");
        }
        Ok(records)
    }

    pub async fn watch(&self, input: WatchRequest) -> Result<Vec<WatchEvent>> {
        let stream_ids = self.list_stream_ids(&input.stream_prefix).await?;
        let after_cursor = input.after_cursor.as_deref();
        let mut events = Vec::new();
        for stream_id in stream_ids {
            for record in self
                .filter_committed_stream_records(self.read_all_stream_records(&stream_id).await?)
                .await?
            {
                if after_cursor.is_some_and(|cursor| record.cursor.as_str() <= cursor) {
                    continue;
                }
                events.push(WatchEvent {
                    stream_id: record.stream_id,
                    sequence: record.sequence,
                    cursor: record.cursor,
                    previous_event_hash: record.previous_event_hash,
                    event_hash: record.event_hash,
                    event_type: record.record_kind.clone(),
                    record_kind: record.record_kind,
                    payload_hash: record.payload_hash,
                    transaction_id: record.transaction_id,
                    created_at: record.created_at,
                });
            }
        }
        events.sort_by(|left, right| {
            (left.cursor.as_str(), left.stream_id.as_str(), left.sequence).cmp(&(
                right.cursor.as_str(),
                right.stream_id.as_str(),
                right.sequence,
            ))
        });
        if input.limit > 0 && events.len() > input.limit {
            events.truncate(input.limit);
        }
        Ok(events)
    }

    pub async fn acquire_fence(&self, input: AcquireFence) -> Result<FencedPermit> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "acquire_fence")]);
        validate_logical_id(&input.fence_name, "fence name")?;
        validate_logical_id(
            &input.authenticated_principal,
            "fence authenticated principal",
        )?;
        if input.ttl_ms == 0 {
            bail!("CoreStore fence ttl_ms must be nonzero");
        }
        if input.ttl_ms > MAX_CORE_FENCE_TTL_MS {
            bail!(
                "CoreStore fence ttl_ms {} exceeds maximum {}",
                input.ttl_ms,
                MAX_CORE_FENCE_TTL_MS
            );
        }
        let ref_name = core_fence_ref_name(&input.fence_name);
        let current = self.read_ref(&ref_name).await?;
        let now_ms = Utc::now().timestamp_millis();
        let current_record = match current.as_ref() {
            Some(value) => Some(read_core_fence_record(self, value).await?),
            None => None,
        };
        if let Some(record) = current_record.as_ref() {
            if record.expires_at_ms > now_ms
                && record.owner_principal != input.authenticated_principal
            {
                bail!(
                    "CoreStore fence {} is held by another owner",
                    input.fence_name
                );
            }
        }
        let next_token = current_record
            .as_ref()
            .map(|record| record.fence_token.saturating_add(1))
            .unwrap_or(1);
        let record = CoreFenceRecord {
            schema: CORE_FENCE_SCHEMA.to_string(),
            fence_name: input.fence_name.clone(),
            owner_principal: input.authenticated_principal.clone(),
            fence_token: next_token,
            expires_at_ms: now_ms.saturating_add(input.ttl_ms as i64),
            updated_at: now_rfc3339(),
        };
        let record_bytes = serde_json::to_vec(&record)?;
        let record_hash = sha256_hex(&record_bytes);
        let object_ref = self
            .put_blob(PutBlob {
                logical_name: ref_name.clone(),
                bytes: record_bytes,
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: format!(
                    "core-fence:{}:{}:{}",
                    input.fence_name, next_token, record_hash
                ),
            })
            .await?;
        self.compare_and_swap_ref(CompareAndSwapRef {
            ref_name,
            expected_generation: current.as_ref().map(|value| value.generation),
            expected_target: current.as_ref().map(|value| value.target.clone()),
            require_absent: current.is_none(),
            require_present: current.is_some(),
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
        Ok(FencedPermit {
            fence_name: record.fence_name,
            owner_principal: record.owner_principal,
            fence_token: record.fence_token,
            expires_at_ms: record.expires_at_ms,
        })
    }

    pub async fn release_fence(&self, input: ReleaseFence) -> Result<()> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "release_fence")]);
        validate_logical_id(&input.fence_name, "fence name")?;
        validate_logical_id(
            &input.authenticated_principal,
            "fence authenticated principal",
        )?;
        let ref_name = core_fence_ref_name(&input.fence_name);
        let Some(current) = self.read_ref(&ref_name).await? else {
            bail!("CoreStore fence {} is not held", input.fence_name);
        };
        let record = read_core_fence_record(self, &current).await?;
        if record.owner_principal != input.authenticated_principal
            || record.fence_token != input.fence_token
        {
            bail!(
                "CoreStore fence {} release owner/fence mismatch",
                input.fence_name
            );
        }
        let released = CoreFenceRecord {
            schema: CORE_FENCE_SCHEMA.to_string(),
            fence_name: record.fence_name,
            owner_principal: record.owner_principal,
            fence_token: record.fence_token,
            expires_at_ms: Utc::now().timestamp_millis(),
            updated_at: now_rfc3339(),
        };
        let object_ref = self
            .put_blob(PutBlob {
                logical_name: ref_name.clone(),
                bytes: serde_json::to_vec(&released)?,
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: format!(
                    "core-fence-release:{}:{}",
                    input.fence_name, input.fence_token
                ),
            })
            .await?;
        self.compare_and_swap_ref(CompareAndSwapRef {
            ref_name,
            expected_generation: Some(current.generation),
            expected_target: Some(current.target),
            require_absent: false,
            require_present: true,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
        Ok(())
    }

    pub async fn commit_root_catalog(
        &self,
        mut catalog: CoreRootCatalog,
        signing_key: &[u8],
    ) -> Result<CoreRootCatalogReceipt> {
        validate_logical_id(&catalog.mesh_id, "mesh id")?;
        validate_logical_id(&catalog.signed_by, "root catalog signer")?;
        if catalog.schema != CORE_ROOT_CATALOG_SCHEMA {
            bail!("CoreStore root catalog has invalid schema");
        }
        if catalog.root_partitions.is_empty() {
            bail!("CoreStore root catalog must include root partitions");
        }
        let current = self
            .read_latest_root_catalog(&catalog.mesh_id, signing_key)
            .await?;
        match current.as_ref() {
            Some(current) => {
                if catalog.generation <= current.generation {
                    bail!(
                        "CoreStore root catalog generation {} is not newer than current {}",
                        catalog.generation,
                        current.generation
                    );
                }
                let current_hash = hash_root_catalog(current)?;
                if catalog.previous_hash != current_hash {
                    bail!("CoreStore root catalog previous_hash does not match current catalog");
                }
            }
            None => {
                if catalog.generation == 0 {
                    bail!("CoreStore root catalog generation must be nonzero");
                }
                if catalog.previous_hash != ZERO_HASH {
                    bail!("CoreStore genesis root catalog must use the zero previous_hash");
                }
            }
        }
        for partition in &catalog.root_partitions {
            validate_root_partition(partition)?;
            self.verify_embedded_manifest_readable(&partition.embedded_head_segment_manifest)
                .await?;
        }
        catalog.signature = String::new();
        catalog.signature = sign_root_catalog(signing_key, &catalog)?;
        verify_root_catalog(&catalog, signing_key)?;
        let catalog_hash = hash_root_catalog(&catalog)?;
        let object_ref = self
            .put_blob(PutBlob {
                logical_name: format!("mesh:{}/system/mesh/root_catalog", catalog.mesh_id),
                bytes: serde_json::to_vec(&catalog)?,
                boundary_values: Vec::new(),
                region_id: root_catalog_region(&catalog),
                mutation_id: format!(
                    "root-catalog:{}:{}:{}",
                    catalog.mesh_id, catalog.generation, catalog_hash
                ),
            })
            .await?;
        let ref_name = root_catalog_ref_name(&catalog.mesh_id);
        let prior_ref = self.read_ref(&ref_name).await?;
        let ref_receipt = self
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: ref_name.clone(),
                expected_generation: prior_ref.as_ref().map(|value| value.generation),
                expected_target: prior_ref.as_ref().map(|value| value.target.clone()),
                require_absent: prior_ref.is_none(),
                require_present: prior_ref.is_some(),
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&object_ref)?,
                transaction_id: None,
            })
            .await?;
        let watch = self
            .append_stream(AppendStreamRecord {
                stream_id: root_catalog_stream_id(&catalog.mesh_id),
                partition_id: "core.root.catalog".to_string(),
                record_kind: "root_catalog.committed".to_string(),
                payload: serde_json::to_vec(&catalog)?,
                fence: None,
                transaction_id: None,
                idempotency_key: Some(format!(
                    "root-catalog:{}:{}",
                    catalog.mesh_id, catalog.generation
                )),
            })
            .await?;
        Ok(CoreRootCatalogReceipt {
            mesh_id: catalog.mesh_id,
            generation: catalog.generation,
            catalog_hash,
            ref_generation: ref_receipt.generation,
            watch_cursor: watch.cursor,
        })
    }

    pub async fn read_latest_root_catalog(
        &self,
        mesh_id: &str,
        signing_key: &[u8],
    ) -> Result<Option<CoreRootCatalog>> {
        validate_logical_id(mesh_id, "mesh id")?;
        let Some(ref_value) = self.read_ref(&root_catalog_ref_name(mesh_id)).await? else {
            return Ok(None);
        };
        let object_ref = decode_core_object_ref_target(&ref_value.target)?;
        let bytes = self.get_blob(GetBlob { object_ref }).await?;
        let catalog: CoreRootCatalog = serde_json::from_slice(&bytes)?;
        verify_root_catalog(&catalog, signing_key)?;
        if catalog.mesh_id != mesh_id {
            bail!("CoreStore root catalog mesh id mismatch");
        }
        Ok(Some(catalog))
    }

    pub async fn list_root_catalog_history(&self, mesh_id: &str) -> Result<Vec<CoreRootCatalog>> {
        validate_logical_id(mesh_id, "mesh id")?;
        let records = self
            .read_stream(ReadStream {
                stream_id: root_catalog_stream_id(mesh_id),
                after_sequence: 0,
                limit: 0,
            })
            .await?;
        let mut catalogs = Vec::new();
        for record in records {
            catalogs.push(serde_json::from_slice(&record.payload)?);
        }
        Ok(catalogs)
    }

    pub async fn commit_quorum_profile(
        &self,
        profile: CoreQuorumProfile,
    ) -> Result<CoreQuorumProfileReceipt> {
        validate_quorum_profile(&profile)?;
        let current = self
            .read_latest_quorum_profile(&profile.placement_group)
            .await?;
        match current.as_ref() {
            Some(current) => {
                if profile.epoch != current.epoch.saturating_add(1) {
                    bail!(
                        "CoreStore quorum profile epoch {} must immediately follow current epoch {}",
                        profile.epoch,
                        current.epoch
                    );
                }
            }
            None => {
                if profile.epoch == 0 {
                    bail!("CoreStore quorum profile genesis epoch must be nonzero");
                }
            }
        }

        let profile_hash = format!("sha256:{}", sha256_hex(&serde_json::to_vec(&profile)?));
        let object_ref = self
            .put_blob(PutBlob {
                logical_name: format!(
                    "mesh:local/system/quorum/{}/epoch:{}",
                    profile.placement_group, profile.epoch
                ),
                bytes: serde_json::to_vec(&profile)?,
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: format!(
                    "quorum-profile:{}:{}:{profile_hash}",
                    profile.placement_group, profile.epoch
                ),
            })
            .await?;
        let ref_name = quorum_profile_ref_name(&profile.placement_group);
        let prior_ref = self.read_ref(&ref_name).await?;
        let ref_receipt = self
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name,
                expected_generation: prior_ref.as_ref().map(|value| value.generation),
                expected_target: prior_ref.as_ref().map(|value| value.target.clone()),
                require_absent: prior_ref.is_none(),
                require_present: prior_ref.is_some(),
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&object_ref)?,
                transaction_id: None,
            })
            .await?;
        let watch = self
            .append_stream(AppendStreamRecord {
                stream_id: quorum_profile_stream_id(&profile.placement_group),
                partition_id: "core.quorum.profile".to_string(),
                record_kind: "quorum_profile.committed".to_string(),
                payload: serde_json::to_vec(&profile)?,
                fence: None,
                transaction_id: None,
                idempotency_key: Some(format!(
                    "quorum-profile:{}:{}",
                    profile.placement_group, profile.epoch
                )),
            })
            .await?;

        Ok(CoreQuorumProfileReceipt {
            placement_group: profile.placement_group,
            epoch: profile.epoch,
            profile_hash,
            ref_generation: ref_receipt.generation,
            watch_cursor: watch.cursor,
        })
    }

    pub async fn read_latest_quorum_profile(
        &self,
        placement_group: &str,
    ) -> Result<Option<CoreQuorumProfile>> {
        validate_logical_id(placement_group, "placement group")?;
        let Some(ref_value) = self
            .read_ref(&quorum_profile_ref_name(placement_group))
            .await?
        else {
            return Ok(None);
        };
        let object_ref = decode_core_object_ref_target(&ref_value.target)?;
        let bytes = self.get_blob(GetBlob { object_ref }).await?;
        let profile: CoreQuorumProfile = serde_json::from_slice(&bytes)?;
        validate_quorum_profile(&profile)?;
        if profile.placement_group != placement_group {
            bail!("CoreStore quorum profile placement group mismatch");
        }
        Ok(Some(profile))
    }

    pub async fn list_quorum_profile_history(
        &self,
        placement_group: &str,
    ) -> Result<Vec<CoreQuorumProfile>> {
        validate_logical_id(placement_group, "placement group")?;
        let records = self
            .read_stream(ReadStream {
                stream_id: quorum_profile_stream_id(placement_group),
                after_sequence: 0,
                limit: 0,
            })
            .await?;
        let mut profiles = Vec::new();
        for record in records {
            let profile: CoreQuorumProfile = serde_json::from_slice(&record.payload)?;
            validate_quorum_profile(&profile)?;
            if profile.placement_group != placement_group {
                bail!("CoreStore quorum profile stream scope mismatch");
            }
            profiles.push(profile);
        }
        Ok(profiles)
    }

    async fn admit_core_mutation(
        &self,
        operation_family: &str,
        writer_family: &str,
        target: serde_json::Value,
        mutation_id: String,
        idempotency_key: Option<String>,
        payload: CoreWalPayload<'_>,
        boundary_values: Vec<CoreBoundaryValue>,
    ) -> Result<CoreWalAdmissionRecord> {
        validate_logical_id(&mutation_id, "wal mutation id")?;
        let (inline_payload, landed_bytes) = match payload {
            CoreWalPayload::Empty => (Vec::new(), Vec::new()),
            CoreWalPayload::Inline(bytes) if bytes.len() <= CORE_WAL_MAX_INLINE_PAYLOAD_BYTES => {
                (bytes.to_vec(), Vec::new())
            }
            CoreWalPayload::Inline(bytes) | CoreWalPayload::Landed(bytes) => {
                let landed = self
                    .land_core_bytes(bytes, &mutation_id, &boundary_values)
                    .await?;
                (Vec::new(), vec![landed])
            }
        };
        self.append_core_wal_record(
            operation_family,
            writer_family,
            target,
            mutation_id,
            idempotency_key,
            landed_bytes,
            &inline_payload,
            boundary_values,
        )
        .await
    }

    async fn append_core_wal_record(
        &self,
        operation_family: &str,
        writer_family: &str,
        target: serde_json::Value,
        mutation_id: String,
        idempotency_key: Option<String>,
        landed_bytes: Vec<CoreWalLandedByte>,
        payload: &[u8],
        boundary_values: Vec<CoreBoundaryValue>,
    ) -> Result<CoreWalAdmissionRecord> {
        if payload.len() > CORE_WAL_MAX_INLINE_PAYLOAD_BYTES {
            bail!(
                "CoreStore WAL payload exceeds {} bytes",
                CORE_WAL_MAX_INLINE_PAYLOAD_BYTES
            );
        }
        let _wal_guard = self.acquire_named_lock("wal", "active").await?;
        self.ensure_wal_file_header().await?;
        self.enforce_admission_capacity(0, 0).await?;
        let sequence = self.next_core_wal_sequence().await?;
        let record = CoreWalAdmissionRecord {
            schema: CORE_WAL_RECORD_SCHEMA.to_string(),
            node_id: CORE_WAL_NODE_ID.to_string(),
            wal_epoch: CORE_WAL_EPOCH,
            sequence,
            mutation_id,
            idempotency_key_hash: idempotency_key
                .map(|value| format!("sha256:{}", sha256_hex(value.as_bytes()))),
            anvil_storage_tenant_id: "local".to_string(),
            authz_scope: serde_json::json!({"realm_id":"system","revision":null}),
            operation_family: operation_family.to_string(),
            writer_family: writer_family.to_string(),
            target,
            preconditions: serde_json::json!([]),
            boundary_values,
            landed_bytes,
            created_at_unix_nanos: unix_timestamp_nanos(),
        };
        let header_json = serde_json::to_vec(&record)?;
        let frame = encode_wal_frame(&header_json, payload)?;
        self.enforce_admission_capacity(frame.len() as u64, 0)
            .await?;
        let path = self.active_wal_path();
        let started_at = Instant::now();
        let mut file = OpenOptions::new().append(true).open(&path).await?;
        crate::perf::record_io_duration(
            "core_store",
            "wal_open_append",
            &path,
            0,
            started_at.elapsed(),
        );
        let started_at = Instant::now();
        file.write_all(&frame).await?;
        crate::perf::record_io_duration(
            "core_store",
            "wal_write_frame",
            &path,
            frame.len() as u64,
            started_at.elapsed(),
        );
        let started_at = Instant::now();
        file.sync_all().await?;
        crate::perf::record_io_duration(
            "core_store",
            "wal_sync_frame",
            &path,
            frame.len() as u64,
            started_at.elapsed(),
        );
        Ok(record)
    }

    async fn land_core_bytes(
        &self,
        bytes: &[u8],
        mutation_id: &str,
        boundary_values: &[CoreBoundaryValue],
    ) -> Result<CoreWalLandedByte> {
        let hash = sha256_hex(bytes);
        let final_path = self.landed_bytes_path(&hash);
        let landing_id = format!("{mutation_id}:{hash}");
        match fs::metadata(&final_path).await {
            Ok(metadata) => {
                if metadata.len() != bytes.len() as u64 {
                    bail!("CoreStore landed bytes existing length mismatch");
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                self.enforce_admission_capacity(0, bytes.len() as u64)
                    .await?;
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                let tmp_path =
                    final_path.with_extension(format!("landed.{}.tmp", uuid::Uuid::new_v4()));
                let started_at = Instant::now();
                let mut file = fs::File::create(&tmp_path).await?;
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_create",
                    &tmp_path,
                    0,
                    started_at.elapsed(),
                );
                let started_at = Instant::now();
                file.write_all(bytes).await?;
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_write",
                    &tmp_path,
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                let started_at = Instant::now();
                file.sync_all().await?;
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_sync",
                    &tmp_path,
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                drop(file);
                let started_at = Instant::now();
                fs::rename(&tmp_path, &final_path).await?;
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_rename",
                    &final_path,
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                sync_parent_dir(&final_path, "landed_file_sync_parent_dir").await?;
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("inspect CoreStore landed bytes {}", final_path.display())
                });
            }
        }
        let relative_path = self.storage.relative_storage_path(&final_path)?;
        let meta_path = final_path.with_extension("meta");
        let meta = serde_json::json!({
            "schema": "anvil.core.landed_bytes_meta.v1",
            "landing_id": landing_id,
            "sha256": format!("sha256:{hash}"),
            "length": bytes.len() as u64,
            "mutation_id": mutation_id,
            "boundary_values": boundary_values,
            "created_at_unix_nanos": unix_timestamp_nanos(),
        });
        write_file_atomic(&meta_path, &serde_json::to_vec(&meta)?).await?;
        Ok(CoreWalLandedByte {
            sha256: format!("sha256:{hash}"),
            length: bytes.len() as u64,
            landing_id,
            relative_path,
        })
    }

    async fn enforce_admission_capacity(
        &self,
        incoming_wal_bytes: u64,
        incoming_landed_bytes: u64,
    ) -> Result<()> {
        self.enforce_admission_capacity_with_limits(
            incoming_wal_bytes,
            incoming_landed_bytes,
            CoreAdmissionCapacityLimits::production(),
        )
        .await
    }

    async fn enforce_admission_capacity_with_limits(
        &self,
        incoming_wal_bytes: u64,
        incoming_landed_bytes: u64,
        limits: CoreAdmissionCapacityLimits,
    ) -> Result<()> {
        let wal_bytes = self.admission_wal_bytes().await?;
        let landed_bytes = self.admission_landed_bytes().await?;
        let projected_wal_bytes = wal_bytes.saturating_add(incoming_wal_bytes);
        let projected_landed_bytes = landed_bytes.saturating_add(incoming_landed_bytes);

        if projected_wal_bytes > limits.wal_hard_limit_bytes {
            bail!(
                "{}: CoreStore admission WAL hard limit exceeded: current={}, incoming={}, hard={}",
                AnvilErrorCode::ResourceExhaustedWalBacklog.as_str(),
                wal_bytes,
                incoming_wal_bytes,
                limits.wal_hard_limit_bytes
            );
        }

        if projected_landed_bytes > limits.landed_bytes_hard_limit_bytes {
            bail!(
                "{}: CoreStore landed bytes hard limit exceeded: current={}, incoming={}, hard={}",
                AnvilErrorCode::ResourceExhaustedWalBacklog.as_str(),
                landed_bytes,
                incoming_landed_bytes,
                limits.landed_bytes_hard_limit_bytes
            );
        }

        let wal_lag_seconds = self.admission_materialisation_lag_seconds().await?;
        if let Some(lag_seconds) = wal_lag_seconds
            && lag_seconds > limits.wal_hard_lag_seconds
        {
            bail!(
                "{}: CoreStore WAL materialisation lag hard limit exceeded: lag_seconds={}, hard={}",
                AnvilErrorCode::ResourceExhaustedWalBacklog.as_str(),
                lag_seconds,
                limits.wal_hard_lag_seconds
            );
        }

        if projected_wal_bytes > limits.wal_soft_limit_bytes
            || projected_landed_bytes > limits.landed_bytes_soft_limit_bytes
            || wal_lag_seconds.is_some_and(|lag_seconds| lag_seconds > limits.wal_soft_lag_seconds)
        {
            tokio::time::sleep(CORE_WAL_SOFT_BACKPRESSURE_DELAY).await;
        }

        Ok(())
    }

    async fn admission_wal_bytes(&self) -> Result<u64> {
        sum_files_with_extension(&self.admission_wal_dir(), &["anwal", "anw"])
            .await
            .with_context(|| {
                format!(
                    "measure CoreStore admission WAL bytes under {}",
                    self.admission_wal_dir().display()
                )
            })
    }

    async fn admission_landed_bytes(&self) -> Result<u64> {
        sum_files_with_extension(&self.admission_landed_bytes_root(), &["landed"])
            .await
            .with_context(|| {
                format!(
                    "measure CoreStore admission landed bytes under {}",
                    self.admission_landed_bytes_root().display()
                )
            })
    }

    async fn admission_materialisation_lag_seconds(&self) -> Result<Option<u64>> {
        let records = self.read_core_wal_records().await?;
        if records.is_empty() {
            return Ok(None);
        }

        let finalised = self.read_core_wal_finalisation_keys().await?;
        let oldest_unfinalised = records
            .iter()
            .filter(|record| !finalised.contains(&CoreWalRecordKey::from(*record)))
            .map(|record| record.created_at_unix_nanos)
            .min();

        let Some(oldest_unfinalised) = oldest_unfinalised else {
            return Ok(None);
        };

        let now = unix_timestamp_nanos();
        let lag_nanos = now.saturating_sub(oldest_unfinalised);
        Ok(Some(lag_nanos / 1_000_000_000))
    }

    async fn read_core_wal_records(&self) -> Result<Vec<CoreWalAdmissionRecord>> {
        self.read_core_wal_records_with_payload()
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(|(record, _payload)| record)
                    .collect()
            })
    }

    async fn read_core_wal_records_with_payload(
        &self,
    ) -> Result<Vec<(CoreWalAdmissionRecord, Vec<u8>)>> {
        let path = self.active_wal_path();
        for attempt in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            let bytes = match fs::read(&path).await {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
                Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
            };
            match decode_wal_records(&bytes)
                .with_context(|| format!("decode CoreStore admission WAL {}", path.display()))
            {
                Ok(records) => return Ok(records),
                Err(error)
                    if is_incomplete_core_frame_error(&error)
                        && attempt + 1 < CORE_CONTROL_READ_RETRY_ATTEMPTS =>
                {
                    tokio::time::sleep(CORE_REF_LOCK_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("CoreStore WAL decode retry loop must return")
    }

    async fn read_core_wal_finalisation_keys(&self) -> Result<BTreeSet<CoreWalRecordKey>> {
        let mut keys = BTreeSet::new();
        for record in self
            .read_all_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await?
        {
            if record.record_kind != CORE_WAL_FINALISATION_RECORD_KIND {
                continue;
            }
            let finalisation: CoreWalFinalisationRecord = serde_json::from_slice(&record.payload)?;
            if finalisation.schema != CORE_WAL_FINALISATION_SCHEMA {
                bail!("CoreStore WAL finalisation record has invalid schema");
            }
            keys.insert(CoreWalRecordKey {
                node_id: finalisation.node_id,
                wal_epoch: finalisation.wal_epoch,
                wal_sequence: finalisation.wal_sequence,
            });
        }
        Ok(keys)
    }

    async fn mark_core_wal_finalised_unlocked(
        &self,
        admission: &CoreWalAdmissionRecord,
        state: &str,
    ) -> Result<()> {
        let _transaction_guard = self.acquire_stream_lock(CORE_TRANSACTION_STREAM_ID).await?;
        let admission_key = CoreWalRecordKey::from(admission);
        for record in self
            .read_all_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await?
        {
            if record.record_kind != CORE_WAL_FINALISATION_RECORD_KIND {
                continue;
            }
            let existing: CoreWalFinalisationRecord = serde_json::from_slice(&record.payload)?;
            let existing_key = CoreWalRecordKey {
                node_id: existing.node_id.clone(),
                wal_epoch: existing.wal_epoch,
                wal_sequence: existing.wal_sequence,
            };
            if existing_key != admission_key {
                continue;
            }
            if existing.mutation_id == admission.mutation_id && existing.state == state {
                return Ok(());
            }
            bail!(
                "CoreStore WAL finalisation conflict for sequence {}: existing mutation/state {}/{}, new mutation/state {}/{}",
                admission.sequence,
                existing.mutation_id,
                existing.state,
                admission.mutation_id,
                state
            );
        }
        let finalisation = CoreWalFinalisationRecord {
            schema: CORE_WAL_FINALISATION_SCHEMA.to_string(),
            node_id: admission.node_id.clone(),
            wal_epoch: admission.wal_epoch,
            wal_sequence: admission.sequence,
            mutation_id: admission.mutation_id.clone(),
            operation_family: admission.operation_family.clone(),
            writer_family: admission.writer_family.clone(),
            target: admission.target.clone(),
            boundary_values: admission.boundary_values.clone(),
            landed_bytes: admission.landed_bytes.clone(),
            state: state.to_string(),
            finalised_at_unix_nanos: unix_timestamp_nanos(),
        };
        self.append_stream_unlocked(AppendStreamRecord {
            stream_id: CORE_TRANSACTION_STREAM_ID.to_string(),
            partition_id: CORE_TRANSACTION_PARTITION_ID.to_string(),
            record_kind: CORE_WAL_FINALISATION_RECORD_KIND.to_string(),
            payload: serde_json::to_vec(&finalisation)?,
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "{}:{}:{}:{}:{}",
                CORE_WAL_FINALISATION_RECORD_KIND,
                admission.node_id,
                admission.wal_epoch,
                admission.sequence,
                admission.mutation_id
            )),
        })
        .await?;
        self.checkpoint_core_wal_unlocked().await?;
        Ok(())
    }

    async fn checkpoint_core_wal_unlocked(&self) -> Result<()> {
        let _wal_guard = self.acquire_named_lock("wal", "active").await?;
        self.ensure_wal_file_header().await?;

        let path = self.active_wal_path();
        let bytes = match read_file(&path, "core_store", "wal_read_for_checkpoint").await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err).with_context(|| "read CoreStore admission WAL"),
        };
        let (first_sequence, records) = decode_wal_file(&bytes)?;
        if records.is_empty() {
            return Ok(());
        }

        let finalised = self.read_core_wal_finalisation_keys().await?;
        let max_sequence = records
            .iter()
            .map(|(record, _)| record.sequence)
            .max()
            .unwrap_or(first_sequence.saturating_sub(1));
        let finalised_prefix_len = records
            .iter()
            .take_while(|(record, _)| finalised.contains(&CoreWalRecordKey::from(record)))
            .count();
        if finalised_prefix_len == 0 {
            return Ok(());
        }

        let next_sequence = max_sequence.saturating_add(1);
        let finalised_records: Vec<_> = records
            .iter()
            .take(finalised_prefix_len)
            .map(|(record, _)| record.clone())
            .collect();
        let retained = records
            .into_iter()
            .skip(finalised_prefix_len)
            .collect::<Vec<_>>();
        let compacted_first_sequence = retained
            .first()
            .map(|(record, _)| record.sequence)
            .unwrap_or(next_sequence);
        let mut compacted =
            encode_wal_file_header(CORE_WAL_NODE_ID, CORE_WAL_EPOCH, compacted_first_sequence)?;
        for (record, payload) in &retained {
            let header_json = serde_json::to_vec(record)?;
            compacted.extend_from_slice(&encode_wal_frame(&header_json, payload)?);
        }

        write_file_atomic(&path, &compacted).await?;
        for record in finalised_records {
            self.remove_finalised_landed_bytes(&record).await?;
        }
        Ok(())
    }

    async fn remove_finalised_landed_bytes(&self, record: &CoreWalAdmissionRecord) -> Result<()> {
        for landed in &record.landed_bytes {
            let landed_path = self
                .storage
                .resolve_relative_storage_path(&landed.relative_path)?;
            match fs::remove_file(&landed_path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "remove finalised CoreStore landed bytes {}",
                            landed_path.display()
                        )
                    });
                }
            }
            let meta_path = landed_path.with_extension("meta");
            match fs::remove_file(&meta_path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "remove finalised CoreStore landed metadata {}",
                            meta_path.display()
                        )
                    });
                }
            }
        }
        Ok(())
    }

    async fn read_landed_bytes(&self, landed: &CoreWalLandedByte) -> Result<Vec<u8>> {
        validate_hash(&landed.sha256, "landed bytes hash")?;
        let path = self
            .storage
            .resolve_relative_storage_path(&landed.relative_path)?;
        let bytes = read_file(&path, "core_store", "read_landed_bytes").await?;
        if bytes.len() as u64 != landed.length {
            bail!("CoreStore landed bytes length mismatch");
        }
        let actual = format!("sha256:{}", sha256_hex(&bytes));
        if actual != landed.sha256 {
            bail!("CoreStore landed bytes hash mismatch");
        }
        Ok(bytes)
    }

    async fn recover_core_wal(&self) -> Result<()> {
        let _recovery_guard = self.acquire_named_lock("wal", "recovery").await?;
        let _guard = self.write_lock.lock().await;
        let records = self.read_core_wal_records_with_payload().await?;
        if records.is_empty() {
            return Ok(());
        }
        let finalised = self.read_core_wal_finalisation_keys().await?;
        for (record, payload) in records {
            let record_key = CoreWalRecordKey::from(&record);
            if finalised.contains(&record_key) {
                continue;
            }
            let state = match self
                .replay_core_wal_record_unlocked(&record, &payload)
                .await
            {
                Ok(state) => state,
                Err(error) => {
                    if self.wait_for_core_wal_finalisation(&record_key).await? {
                        continue;
                    }
                    return Err(error).with_context(|| {
                        format!(
                            "replay CoreStore WAL mutation {} sequence {}",
                            record.mutation_id, record.sequence
                        )
                    });
                }
            };
            if let Err(error) = self.mark_core_wal_finalised_unlocked(&record, state).await {
                if self.wait_for_core_wal_finalisation(&record_key).await? {
                    continue;
                }
                return Err(error);
            }
        }
        Ok(())
    }

    async fn wait_for_core_wal_finalisation(&self, key: &CoreWalRecordKey) -> Result<bool> {
        for _ in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            if self.read_core_wal_finalisation_keys().await?.contains(key) {
                return Ok(true);
            }
            tokio::time::sleep(CORE_REF_LOCK_RETRY_DELAY).await;
        }
        Ok(false)
    }

    async fn replay_core_wal_record_unlocked(
        &self,
        record: &CoreWalAdmissionRecord,
        payload: &[u8],
    ) -> Result<&'static str> {
        match record.operation_family.as_str() {
            "object.put" => {
                let profile_id = json_required_string(&record.target, "erasure_profile_id")?;
                let profile = local_erasure_profile(&profile_id)?;
                let materialised_bytes = self.core_wal_payload_bytes(record, payload).await?;
                let hash = sha256_hex(&materialised_bytes);
                if let Some(landed) = record.landed_bytes.first() {
                    let landed_hash = strip_sha256_prefix(&landed.sha256)?;
                    if landed_hash != hash {
                        bail!("CoreStore WAL object.put landed hash mismatch");
                    }
                }
                self.materialise_object_blob_bytes(
                    &hash,
                    &materialised_bytes,
                    &record.boundary_values,
                    &record.mutation_id,
                    profile,
                )
                .await?;
                Ok("committed")
            }
            "stream.append" => {
                let stream_id = json_required_string(&record.target, "stream_id")?;
                let partition_id = json_required_string(&record.target, "partition_id")?;
                let record_kind = json_required_string(&record.target, "record_kind")?;
                let transaction_id = json_optional_string(&record.target, "transaction_id")?;
                let payload = self.core_wal_payload_bytes(record, payload).await?;
                self.append_stream_unlocked_with_idempotency_hash(
                    AppendStreamRecord {
                        stream_id,
                        partition_id,
                        record_kind,
                        payload,
                        fence: None,
                        transaction_id,
                        idempotency_key: None,
                    },
                    record.idempotency_key_hash.clone(),
                )
                .await?;
                Ok("committed")
            }
            "mutation.batch" => {
                let payload = self.core_wal_payload_bytes(record, payload).await?;
                let batch: CoreMutationBatch = serde_json::from_slice(&payload)?;
                let receipt = self.recover_admitted_mutation_batch_unlocked(batch).await?;
                Ok(core_transaction_state_name(receipt.state))
            }
            "ref.compare_and_swap" => {
                let ref_name = json_required_string(&record.target, "ref_name")?;
                let new_target = json_required_string(&record.target, "new_target")?;
                let expected_generation = json_optional_u64(&record.target, "expected_generation")?;
                let expected_target = json_optional_string(&record.target, "expected_target")?;
                let require_absent = json_required_bool(&record.target, "require_absent")?;
                let require_present = json_required_bool(&record.target, "require_present")?;
                let transaction_id = json_optional_string(&record.target, "transaction_id")?;
                let current = self.read_ref(&ref_name).await?;
                if current
                    .as_ref()
                    .is_some_and(|value| value.target == new_target)
                {
                    return Ok("committed");
                }
                let precondition = CoreMutationPrecondition::Ref {
                    ref_name: ref_name.clone(),
                    expected_generation,
                    expected_target,
                    require_absent,
                    require_present,
                    fence: None,
                    authz_revision: None,
                    source_watch_cursor: None,
                };
                self.apply_ref_update_unlocked(
                    &ref_name,
                    &new_target,
                    transaction_id,
                    Some(&precondition),
                )
                .await?;
                Ok("committed")
            }
            "ref.delete" => {
                let ref_name = json_required_string(&record.target, "ref_name")?;
                let expected_generation = json_optional_u64(&record.target, "expected_generation")?;
                let expected_target = json_optional_string(&record.target, "expected_target")?;
                let require_present = json_required_bool(&record.target, "require_present")?;
                let transaction_id = json_optional_string(&record.target, "transaction_id")?;
                let current = self.read_ref(&ref_name).await?;
                let Some(previous) = current else {
                    return Ok("committed");
                };
                validate_ref_precondition(
                    Some(&previous),
                    &ref_name,
                    expected_generation,
                    expected_target.as_deref(),
                    false,
                    require_present,
                )?;
                let update = CoreRefUpdateRecord {
                    schema: CORE_REF_UPDATE_SCHEMA.to_string(),
                    ref_name: ref_name.clone(),
                    previous_generation: Some(previous.generation),
                    new_generation: None,
                    previous_target: Some(previous.target.clone()),
                    new_target: None,
                    preconditions: CoreRefUpdatePreconditions {
                        expected_generation,
                        expected_target,
                        require_absent: false,
                        require_present,
                        fence_token: None,
                        authz_revision: None,
                        source_watch_cursor: None,
                    },
                    mutation_id: record.mutation_id.clone(),
                    transaction_id,
                    committed_at: now_rfc3339(),
                };
                self.append_ref_update_unlocked(&update).await?;
                Ok("committed")
            }
            other => bail!(
                "CoreStore WAL recovery does not support operation family {other}; refusing startup with unfinalised WAL"
            ),
        }
    }

    async fn core_wal_payload_bytes(
        &self,
        record: &CoreWalAdmissionRecord,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        if !payload.is_empty() {
            return Ok(payload.to_vec());
        }
        let mut bytes = Vec::new();
        for landed in &record.landed_bytes {
            bytes.extend_from_slice(&self.read_landed_bytes(landed).await?);
        }
        Ok(bytes)
    }

    async fn ensure_wal_file_header(&self) -> Result<()> {
        let path = self.active_wal_path();
        if fs::metadata(&path).await.is_ok() {
            return Ok(());
        }
        let header = encode_wal_file_header(CORE_WAL_NODE_ID, CORE_WAL_EPOCH, 1)?;
        match write_file_atomic(&path, &header).await {
            Ok(()) => {}
            Err(error) if fs::metadata(&path).await.is_ok() => {
                drop(error);
            }
            Err(error) => return Err(error),
        }
        Ok(())
    }

    async fn next_core_wal_sequence(&self) -> Result<u64> {
        let path = self.active_wal_path();
        let bytes = match read_file(&path, "core_store", "wal_read_for_sequence").await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(1),
            Err(err) => return Err(err).with_context(|| "read CoreStore admission WAL"),
        };
        let (first_sequence, records) = decode_wal_file(&bytes)?;
        Ok(records
            .iter()
            .map(|(record, _)| record.sequence)
            .max()
            .map(|sequence| sequence.saturating_add(1))
            .unwrap_or(first_sequence))
    }

    pub async fn list_stream_ids(&self, prefix: &str) -> Result<Vec<String>> {
        let mut votes: BTreeMap<String, usize> = BTreeMap::new();
        for node_id in local_control_node_ids() {
            let dir = self.stream_data_replica_dir(&node_id);
            let mut entries = match fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("read CoreStore stream data directory {node_id}")
                    });
                }
            };
            while let Some(entry) = entries.next_entry().await? {
                if is_core_store_temp_entry(&entry.file_name()) {
                    continue;
                }
                let file_type = match entry.file_type().await {
                    Ok(file_type) => file_type,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => {
                        return Err(err).with_context(|| "read CoreStore stream entry type");
                    }
                };
                if !file_type.is_file() {
                    continue;
                }
                let path = entry.path();
                if path.extension().and_then(|value| value.to_str()) != Some("anstream") {
                    continue;
                }
                let bytes = match read_file(&path, "core_store", "read_stream_id_from_data").await {
                    Ok(bytes) => bytes,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => {
                        return Err(err).with_context(|| "read CoreStore stream data entry");
                    }
                };
                let stream_id = decode_active_stream_id(&bytes)?;
                if stream_id.starts_with(prefix) {
                    *votes.entry(stream_id).or_default() += 1;
                }
            }
        }
        let mut ids = votes
            .into_iter()
            .filter_map(|(stream_id, count)| {
                (count >= LOCAL_CONTROL_READ_QUORUM).then_some(stream_id)
            })
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }

    pub async fn compare_and_swap_ref(&self, input: CompareAndSwapRef) -> Result<CasRefReceipt> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "compare_and_swap_ref")],
        );
        validate_logical_id(&input.ref_name, "ref name")?;
        let ref_name = input.ref_name.clone();
        let expected_generation = input.expected_generation;
        let expected_target = input.expected_target.clone();
        let require_absent = input.require_absent;
        let require_present = input.require_present;
        let fence = input.fence.clone();
        let authz_revision = input.authz_revision.clone();
        let source_watch_cursor = input.source_watch_cursor.clone();
        let new_target = input.new_target.clone();
        let transaction_id = input.transaction_id.clone();
        let _ref_guard = self.acquire_ref_lock(&input.ref_name).await?;
        let _guard = self.write_lock.lock().await;
        let current = self.read_ref(&input.ref_name).await?;
        if input.require_absent && current.is_some() {
            bail!("CoreStore ref {} must be absent", input.ref_name);
        }
        if input.require_present && current.is_none() {
            bail!("CoreStore ref {} must be present", input.ref_name);
        }
        if let Some(expected_generation) = input.expected_generation {
            let actual = current.as_ref().map(|value| value.generation);
            if actual != Some(expected_generation) {
                bail!(
                    "CoreStore ref {} generation mismatch: expected {}, got {:?}",
                    input.ref_name,
                    expected_generation,
                    actual
                );
            }
        }
        if let Some(expected_target) = input.expected_target.as_deref() {
            let actual = current.as_ref().map(|value| value.target.as_str());
            if actual != Some(expected_target) {
                bail!(
                    "CoreStore ref {} target mismatch: expected {}, got {:?}",
                    input.ref_name,
                    expected_target,
                    actual
                );
            }
        }
        if let Some(fence) = fence.as_ref() {
            self.validate_fence_precondition_unlocked(fence).await?;
        }
        if let Some(cursor) = source_watch_cursor.as_deref() {
            self.validate_source_watch_cursor_unlocked(cursor).await?;
        }
        let admission = self
            .admit_core_mutation(
                "ref.compare_and_swap",
                "core-control",
                serde_json::json!({
                    "ref_name": ref_name.clone(),
                    "new_target": new_target.clone(),
                    "expected_generation": expected_generation,
                    "expected_target": expected_target.clone(),
                    "require_absent": require_absent,
                    "require_present": require_present,
                    "transaction_id": transaction_id.clone(),
                }),
                transaction_id
                    .clone()
                    .unwrap_or_else(|| format!("ref-cas:{ref_name}:{}", uuid::Uuid::new_v4())),
                None,
                CoreWalPayload::Empty,
                Vec::new(),
            )
            .await?;

        let latest_stream_generation = if current.is_none() {
            self.latest_ref_update_generation(&input.ref_name).await?
        } else {
            None
        };
        let next_generation = current
            .as_ref()
            .map(|value| value.generation)
            .or(latest_stream_generation)
            .unwrap_or(0)
            .saturating_add(1);
        let committed_at = now_rfc3339();
        let previous_generation = current.as_ref().map(|value| value.generation);
        let previous_target = current.as_ref().map(|value| value.target.clone());
        let update = CoreRefUpdateRecord {
            schema: CORE_REF_UPDATE_SCHEMA.to_string(),
            ref_name: ref_name.clone(),
            previous_generation,
            new_generation: Some(next_generation),
            previous_target: previous_target.clone(),
            new_target: Some(new_target.clone()),
            preconditions: CoreRefUpdatePreconditions {
                expected_generation,
                expected_target,
                require_absent,
                require_present,
                fence_token: fence.as_ref().map(|precondition| precondition.fence_token),
                authz_revision,
                source_watch_cursor,
            },
            mutation_id: transaction_id
                .clone()
                .unwrap_or_else(|| format!("core-ref-update:{ref_name}:{next_generation}")),
            transaction_id: transaction_id.clone(),
            committed_at,
        };
        self.append_ref_update_unlocked(&update).await?;
        self.mark_core_wal_finalised_unlocked(&admission, "committed")
            .await?;
        Ok(CasRefReceipt {
            ref_name: input.ref_name,
            generation: next_generation,
            previous_target: current.map(|value| value.target),
            new_target: input.new_target,
        })
    }

    pub async fn delete_ref(
        &self,
        ref_name: &str,
        expected_generation: Option<u64>,
        expected_target: Option<&str>,
        require_present: bool,
    ) -> Result<Option<CoreRefValue>> {
        validate_logical_id(ref_name, "ref name")?;
        let _ref_guard = self.acquire_ref_lock(ref_name).await?;
        let _guard = self.write_lock.lock().await;
        let current = self.read_ref(ref_name).await?;
        if require_present && current.is_none() {
            bail!("CoreStore ref {ref_name} must be present");
        }
        if let Some(expected_generation) = expected_generation {
            let actual = current.as_ref().map(|value| value.generation);
            if actual != Some(expected_generation) {
                bail!(
                    "CoreStore ref {ref_name} generation mismatch: expected {}, got {:?}",
                    expected_generation,
                    actual
                );
            }
        }
        if let Some(expected_target) = expected_target {
            let actual = current.as_ref().map(|value| value.target.as_str());
            if actual != Some(expected_target) {
                bail!(
                    "CoreStore ref {ref_name} target mismatch: expected {}, got {:?}",
                    expected_target,
                    actual
                );
            }
        }
        if current.is_some() {
            let previous = current.as_ref().expect("current checked above");
            let admission = self
                .admit_core_mutation(
                    "ref.delete",
                    "core-control",
                    serde_json::json!({
                        "ref_name": ref_name,
                        "expected_generation": expected_generation,
                        "expected_target": expected_target,
                        "require_present": require_present,
                        "transaction_id": null,
                    }),
                    format!("core-ref-delete:{ref_name}:{}", previous.generation),
                    None,
                    CoreWalPayload::Empty,
                    Vec::new(),
                )
                .await?;
            let update = CoreRefUpdateRecord {
                schema: CORE_REF_UPDATE_SCHEMA.to_string(),
                ref_name: ref_name.to_string(),
                previous_generation: Some(previous.generation),
                new_generation: None,
                previous_target: Some(previous.target.clone()),
                new_target: None,
                preconditions: CoreRefUpdatePreconditions {
                    expected_generation,
                    expected_target: expected_target.map(str::to_string),
                    require_absent: false,
                    require_present,
                    fence_token: None,
                    authz_revision: None,
                    source_watch_cursor: None,
                },
                mutation_id: format!("core-ref-delete:{ref_name}:{}", previous.generation),
                transaction_id: None,
                committed_at: now_rfc3339(),
            };
            self.append_ref_update_unlocked(&update).await?;
            self.mark_core_wal_finalised_unlocked(&admission, "committed")
                .await?;
        }
        Ok(current)
    }

    pub async fn read_ref(&self, ref_name: &str) -> Result<Option<CoreRefValue>> {
        validate_logical_id(ref_name, "ref name")?;
        self.recover_ref_from_updates(ref_name).await
    }

    pub async fn list_ref_names(&self, prefix: &str) -> Result<Vec<String>> {
        let stream_prefix = ref_update_stream_id(prefix);
        let mut names = self
            .list_stream_ids(&stream_prefix)
            .await?
            .into_iter()
            .filter_map(|stream_id| {
                stream_id
                    .strip_prefix("core_ref_update:")
                    .map(str::to_string)
            })
            .filter(|ref_name| ref_name.starts_with(prefix))
            .collect::<Vec<_>>();
        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn append_ref_update_unlocked(&self, update: &CoreRefUpdateRecord) -> Result<()> {
        self.append_stream_unlocked(AppendStreamRecord {
            stream_id: ref_update_stream_id(&update.ref_name),
            partition_id: "core.ref".to_string(),
            record_kind: "core_ref.update".to_string(),
            payload: serde_json::to_vec(update)?,
            fence: None,
            transaction_id: update.transaction_id.clone(),
            idempotency_key: update.transaction_id.clone(),
        })
        .await?;
        Ok(())
    }

    pub async fn read_ref_updates(
        &self,
        ref_name: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<CoreRefUpdateRecord>> {
        validate_logical_id(ref_name, "ref name")?;
        let records = self
            .read_stream(ReadStream {
                stream_id: ref_update_stream_id(ref_name),
                after_sequence,
                limit,
            })
            .await?;
        records
            .into_iter()
            .map(|record| {
                let update: CoreRefUpdateRecord = serde_json::from_slice(&record.payload)?;
                if update.schema != CORE_REF_UPDATE_SCHEMA {
                    bail!("CoreStore ref update record has invalid schema");
                }
                if update.ref_name != ref_name {
                    bail!("CoreStore ref update record scope mismatch");
                }
                Ok(update)
            })
            .collect()
    }

    pub async fn recover_ref_from_updates(&self, ref_name: &str) -> Result<Option<CoreRefValue>> {
        let mut current = None;
        for update in self.read_ref_updates(ref_name, 0, 0).await? {
            match (update.new_generation, update.new_target) {
                (Some(generation), Some(target)) => {
                    current = Some(CoreRefValue {
                        schema: CORE_REF_SCHEMA.to_string(),
                        ref_name: ref_name.to_string(),
                        generation,
                        target,
                        transaction_id: update.transaction_id,
                        updated_at: update.committed_at,
                    });
                }
                (None, None) => current = None,
                _ => bail!("CoreStore ref update record has inconsistent generation/target state"),
            }
        }
        Ok(current)
    }

    async fn latest_ref_update_generation(&self, ref_name: &str) -> Result<Option<u64>> {
        Ok(self
            .read_ref_updates(ref_name, 0, 0)
            .await?
            .into_iter()
            .flat_map(|update| [update.previous_generation, update.new_generation])
            .flatten()
            .max())
    }

    pub async fn commit_mutation_batch(
        &self,
        batch: CoreMutationBatch,
    ) -> Result<CoreMutationBatchReceipt> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "commit_mutation_batch")],
        );
        let total_start = std::time::Instant::now();
        let timing_name = batch.transaction_id.clone();
        validate_logical_id(&batch.transaction_id, "transaction id")?;
        validate_logical_id(&batch.scope_partition, "transaction scope partition")?;
        validate_logical_id(&batch.committed_by_principal, "transaction principal")?;
        if batch.operations.is_empty() {
            bail!("CoreStore mutation batch must include at least one operation");
        }
        validate_batch_partitions(&batch)?;

        let step_start = std::time::Instant::now();
        let _operation_guards = self.acquire_batch_locks(&batch).await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch acquire_batch_locks tx={timing_name}"),
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let _guard = self.write_lock.lock().await;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch write_lock tx={timing_name}"),
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        if let Some(transaction) = self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
        {
            return Ok(receipt_from_transaction(&transaction));
        }
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch read_transaction tx={timing_name}"),
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        self.validate_mutation_preconditions_unlocked(
            &batch.preconditions,
            &batch.committed_by_principal,
        )
        .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch validate_preconditions tx={timing_name}"),
            step_start.elapsed(),
        );
        let batch_payload = serde_json::to_vec(&batch)?;
        let wal_payload = if batch_payload.len() <= CORE_WAL_MAX_INLINE_PAYLOAD_BYTES {
            CoreWalPayload::Inline(&batch_payload)
        } else {
            CoreWalPayload::Landed(&batch_payload)
        };
        let admission = self
            .admit_core_mutation(
                "mutation.batch",
                "core-control",
                serde_json::json!({
                    "transaction_id": batch.transaction_id.clone(),
                    "scope_partition": batch.scope_partition.clone(),
                    "operation_count": batch.operations.len(),
                }),
                batch.transaction_id.clone(),
                Some(batch.transaction_id.clone()),
                wal_payload,
                Vec::new(),
            )
            .await?;

        let mut visible_updates = Vec::with_capacity(batch.operations.len());
        let step_start = std::time::Instant::now();
        let mut finalisation_error = None;
        for operation in &batch.operations {
            let operation_result = match operation {
                CoreMutationOperation::RefUpdate {
                    ref_name,
                    new_target,
                    ..
                } => self
                    .apply_ref_update_unlocked(
                        ref_name,
                        new_target,
                        Some(batch.transaction_id.clone()),
                        ref_precondition_for(&batch.preconditions, ref_name),
                    )
                    .await
                    .map(|update| CoreTransactionUpdate::CoreRefUpdate {
                        ref_name: ref_name.clone(),
                        new_generation: update.generation,
                    }),
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    record_kind,
                    payload,
                    idempotency_key,
                } => self
                    .append_stream_unlocked(AppendStreamRecord {
                        stream_id: stream_id.clone(),
                        partition_id: partition_id.clone(),
                        record_kind: record_kind.clone(),
                        payload: payload.clone(),
                        fence: None,
                        transaction_id: Some(batch.transaction_id.clone()),
                        idempotency_key: idempotency_key.clone(),
                    })
                    .await
                    .map(|receipt| CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: receipt.sequence,
                        prepared_record_hash: receipt.event_hash,
                    }),
            };
            match operation_result {
                Ok(update) => visible_updates.push(update),
                Err(error) => {
                    finalisation_error = Some(format!("{error:#}"));
                    break;
                }
            }
        }
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch operations tx={timing_name}"),
            step_start.elapsed(),
        );

        let transaction_state = if finalisation_error.is_some() {
            CoreTransactionState::FinalisationFailed
        } else {
            CoreTransactionState::Committed
        };
        let transaction_visible_updates = if finalisation_error.is_some() {
            Vec::new()
        } else {
            visible_updates.clone()
        };
        let transaction = CoreTransaction {
            schema: CORE_TRANSACTION_SCHEMA.to_string(),
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            state: transaction_state,
            preconditions_hash: format!(
                "sha256:{}",
                sha256_hex(&serde_json::to_vec(&batch.preconditions)?)
            ),
            operations_hash: format!(
                "sha256:{}",
                sha256_hex(&serde_json::to_vec(&batch.operations)?)
            ),
            prepared_refs: Vec::new(),
            visible_updates: transaction_visible_updates.clone(),
            finalisation_error: finalisation_error.clone(),
            committed_at: now_rfc3339(),
            committed_by_principal: batch.committed_by_principal.clone(),
        };
        let step_start = std::time::Instant::now();
        self.write_transaction_unlocked(&transaction).await?;
        self.mark_core_wal_finalised_unlocked(
            &admission,
            core_transaction_state_name(transaction_state),
        )
        .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch write_transaction tx={timing_name}"),
            step_start.elapsed(),
        );
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch total tx={timing_name}"),
            total_start.elapsed(),
        );

        Ok(CoreMutationBatchReceipt {
            transaction_id: batch.transaction_id,
            scope_partition: batch.scope_partition,
            state: transaction_state,
            visible_updates: transaction_visible_updates,
            finalisation_error,
        })
    }

    async fn recover_admitted_mutation_batch_unlocked(
        &self,
        batch: CoreMutationBatch,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(&batch.transaction_id, "transaction id")?;
        validate_logical_id(&batch.scope_partition, "transaction scope partition")?;
        validate_logical_id(&batch.committed_by_principal, "transaction principal")?;
        if batch.operations.is_empty() {
            bail!("CoreStore mutation batch must include at least one operation");
        }
        validate_batch_partitions(&batch)?;

        if let Some(transaction) = self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
        {
            return Ok(receipt_from_transaction(&transaction));
        }
        self.validate_mutation_preconditions_unlocked(
            &batch.preconditions,
            &batch.committed_by_principal,
        )
        .await?;

        let mut visible_updates = Vec::with_capacity(batch.operations.len());
        let mut finalisation_error = None;
        for operation in &batch.operations {
            let operation_result = match operation {
                CoreMutationOperation::RefUpdate {
                    ref_name,
                    new_target,
                    ..
                } => self
                    .apply_ref_update_unlocked(
                        ref_name,
                        new_target,
                        Some(batch.transaction_id.clone()),
                        ref_precondition_for(&batch.preconditions, ref_name),
                    )
                    .await
                    .map(|update| CoreTransactionUpdate::CoreRefUpdate {
                        ref_name: ref_name.clone(),
                        new_generation: update.generation,
                    }),
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    record_kind,
                    payload,
                    idempotency_key,
                } => self
                    .append_stream_unlocked(AppendStreamRecord {
                        stream_id: stream_id.clone(),
                        partition_id: partition_id.clone(),
                        record_kind: record_kind.clone(),
                        payload: payload.clone(),
                        fence: None,
                        transaction_id: Some(batch.transaction_id.clone()),
                        idempotency_key: idempotency_key.clone(),
                    })
                    .await
                    .map(|receipt| CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: receipt.sequence,
                        prepared_record_hash: receipt.event_hash,
                    }),
            };
            match operation_result {
                Ok(update) => visible_updates.push(update),
                Err(error) => {
                    finalisation_error = Some(format!("{error:#}"));
                    break;
                }
            }
        }

        let transaction_state = if finalisation_error.is_some() {
            CoreTransactionState::FinalisationFailed
        } else {
            CoreTransactionState::Committed
        };
        let transaction_visible_updates = if finalisation_error.is_some() {
            Vec::new()
        } else {
            visible_updates.clone()
        };
        let transaction = CoreTransaction {
            schema: CORE_TRANSACTION_SCHEMA.to_string(),
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            state: transaction_state,
            preconditions_hash: format!(
                "sha256:{}",
                sha256_hex(&serde_json::to_vec(&batch.preconditions)?)
            ),
            operations_hash: format!(
                "sha256:{}",
                sha256_hex(&serde_json::to_vec(&batch.operations)?)
            ),
            prepared_refs: Vec::new(),
            visible_updates: transaction_visible_updates.clone(),
            finalisation_error: finalisation_error.clone(),
            committed_at: now_rfc3339(),
            committed_by_principal: batch.committed_by_principal.clone(),
        };
        self.write_transaction_unlocked(&transaction).await?;

        Ok(CoreMutationBatchReceipt {
            transaction_id: batch.transaction_id,
            scope_partition: batch.scope_partition,
            state: transaction_state,
            visible_updates: transaction_visible_updates,
            finalisation_error,
        })
    }

    pub async fn read_transaction(&self, transaction_id: &str) -> Result<Option<CoreTransaction>> {
        validate_logical_id(transaction_id, "transaction id")?;
        self.read_transaction_unlocked(transaction_id).await
    }

    pub async fn commit_transaction(&self, transaction: CoreTransaction) -> Result<()> {
        if transaction.state != CoreTransactionState::Committed {
            bail!("CoreStore only persists committed transactions through commit_transaction");
        }
        validate_logical_id(&transaction.transaction_id, "transaction id")?;
        let _guard = self.write_lock.lock().await;
        self.write_transaction_unlocked(&transaction).await
    }

    async fn write_transaction_unlocked(&self, transaction: &CoreTransaction) -> Result<()> {
        let _transaction_guard = self.acquire_stream_lock(CORE_TRANSACTION_STREAM_ID).await?;
        if let Some(existing) = self
            .read_transaction_unlocked(&transaction.transaction_id)
            .await?
        {
            if existing.state == transaction.state
                && existing.preconditions_hash == transaction.preconditions_hash
                && existing.operations_hash == transaction.operations_hash
                && existing.visible_updates == transaction.visible_updates
                && existing.finalisation_error == transaction.finalisation_error
                && existing.committed_by_principal == transaction.committed_by_principal
            {
                return Ok(());
            }
            bail!(
                "CoreStore transaction {} idempotency conflict",
                transaction.transaction_id
            );
        }
        let bytes = serde_json::to_vec(&transaction)?;
        self.append_stream_unlocked(AppendStreamRecord {
            stream_id: CORE_TRANSACTION_STREAM_ID.to_string(),
            partition_id: CORE_TRANSACTION_PARTITION_ID.to_string(),
            record_kind: CORE_TRANSACTION_RECORD_KIND.to_string(),
            payload: bytes,
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "{}:{}",
                CORE_TRANSACTION_RECORD_KIND, transaction.transaction_id
            )),
        })
        .await?;
        Ok(())
    }

    async fn read_transaction_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CoreTransaction>> {
        let records = self
            .read_all_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await?;
        for record in records {
            if record.record_kind != CORE_TRANSACTION_RECORD_KIND {
                continue;
            }
            let transaction: CoreTransaction = serde_json::from_slice(&record.payload)?;
            if transaction.transaction_id == transaction_id {
                return Ok(Some(transaction));
            }
        }
        Ok(None)
    }

    async fn transaction_is_committed(&self, transaction_id: &str) -> Result<bool> {
        match self.read_transaction_unlocked(transaction_id).await {
            Ok(Some(transaction)) => Ok(transaction.state == CoreTransactionState::Committed),
            Ok(None) => Ok(false),
            Err(error) if is_quorum_visibility_gap(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn filter_committed_stream_records(
        &self,
        records: Vec<StreamRecord>,
    ) -> Result<Vec<StreamRecord>> {
        let mut visible = Vec::with_capacity(records.len());
        for record in records {
            if let Some(transaction_id) = record.transaction_id.as_deref()
                && !self.transaction_is_committed(transaction_id).await?
            {
                continue;
            }
            visible.push(record);
        }
        Ok(visible)
    }

    async fn validate_mutation_preconditions_unlocked(
        &self,
        preconditions: &[CoreMutationPrecondition],
        committed_by_principal: &str,
    ) -> Result<()> {
        for precondition in preconditions {
            match precondition {
                CoreMutationPrecondition::Ref {
                    ref_name,
                    expected_generation,
                    expected_target,
                    require_absent,
                    require_present,
                    fence,
                    source_watch_cursor,
                    ..
                } => {
                    validate_ref_precondition(
                        self.read_ref(ref_name).await?.as_ref(),
                        ref_name,
                        *expected_generation,
                        expected_target.as_deref(),
                        *require_absent,
                        *require_present,
                    )?;
                    if let Some(fence) = fence {
                        if fence.authenticated_principal != committed_by_principal {
                            bail!("CoreStore ref fence principal must match transaction principal");
                        }
                        self.validate_fence_precondition_unlocked(fence).await?;
                    }
                    if let Some(cursor) = source_watch_cursor.as_deref() {
                        self.validate_source_watch_cursor_unlocked(cursor).await?;
                    }
                }
                CoreMutationPrecondition::Fence {
                    fence_name,
                    fence_token,
                } => {
                    self.validate_fence_precondition_unlocked(&CoreFencePrecondition {
                        fence_name: fence_name.clone(),
                        fence_token: *fence_token,
                        authenticated_principal: committed_by_principal.to_string(),
                    })
                    .await?;
                }
                CoreMutationPrecondition::StreamHead {
                    stream_id,
                    expected_last_sequence,
                    expected_last_event_hash,
                } => {
                    let records = self.read_all_stream_records(stream_id).await?;
                    let (actual_sequence, actual_hash) = stream_head_from_records(&records);
                    if actual_sequence != *expected_last_sequence
                        || actual_hash != *expected_last_event_hash
                    {
                        return Err(CoreStoreCommitError::StreamHeadMismatch {
                            stream_id: stream_id.clone(),
                            expected_last_sequence: *expected_last_sequence,
                            expected_last_event_hash: expected_last_event_hash.clone(),
                            actual_sequence,
                            actual_event_hash: actual_hash,
                        }
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    async fn validate_source_watch_cursor_unlocked(&self, cursor: &str) -> Result<()> {
        let (stream_id, sequence) = parse_stream_cursor(cursor)?;
        let Some(record) = self
            .read_stream(ReadStream {
                stream_id,
                after_sequence: sequence.saturating_sub(1),
                limit: 1,
            })
            .await?
            .into_iter()
            .next()
        else {
            bail!("WatchCursorExpired: CoreStore source watch cursor is not retained");
        };
        if record.cursor != cursor {
            bail!("WatchCursorExpired: CoreStore source watch cursor is not retained");
        }
        Ok(())
    }

    async fn validate_fence_precondition_unlocked(
        &self,
        precondition: &CoreFencePrecondition,
    ) -> Result<()> {
        validate_logical_id(&precondition.fence_name, "fence name")?;
        validate_logical_id(
            &precondition.authenticated_principal,
            "fence authenticated principal",
        )?;
        let ref_name = core_fence_ref_name(&precondition.fence_name);
        let Some(current) = self.read_ref(&ref_name).await? else {
            bail!("CoreStore fence {} is not held", precondition.fence_name);
        };
        let record = read_core_fence_record(self, &current).await?;
        if record.owner_principal != precondition.authenticated_principal
            || record.fence_token != precondition.fence_token
            || record.expires_at_ms <= Utc::now().timestamp_millis()
        {
            bail!(
                "CoreStore fence {} precondition failed",
                precondition.fence_name
            );
        }
        Ok(())
    }

    async fn apply_ref_update_unlocked(
        &self,
        ref_name: &str,
        new_target: &str,
        transaction_id: Option<String>,
        precondition: Option<&CoreMutationPrecondition>,
    ) -> Result<CasRefReceipt> {
        let current = self.read_ref(ref_name).await?;
        let (
            expected_generation,
            expected_target,
            require_absent,
            require_present,
            fence_token,
            authz_revision,
            source_watch_cursor,
        ) = ref_update_precondition_parts(precondition);
        validate_ref_precondition(
            current.as_ref(),
            ref_name,
            expected_generation,
            expected_target.as_deref(),
            require_absent,
            require_present,
        )?;
        let latest_stream_generation = if current.is_none() {
            self.latest_ref_update_generation(ref_name).await?
        } else {
            None
        };
        let next_generation = current
            .as_ref()
            .map(|value| value.generation)
            .or(latest_stream_generation)
            .unwrap_or(0)
            .saturating_add(1);
        let committed_at = now_rfc3339();
        let update = CoreRefUpdateRecord {
            schema: CORE_REF_UPDATE_SCHEMA.to_string(),
            ref_name: ref_name.to_string(),
            previous_generation: current.as_ref().map(|value| value.generation),
            new_generation: Some(next_generation),
            previous_target: current.as_ref().map(|value| value.target.clone()),
            new_target: Some(new_target.to_string()),
            preconditions: CoreRefUpdatePreconditions {
                expected_generation,
                expected_target,
                require_absent,
                require_present,
                fence_token,
                authz_revision,
                source_watch_cursor,
            },
            mutation_id: transaction_id
                .clone()
                .unwrap_or_else(|| format!("core-ref-update:{ref_name}:{next_generation}")),
            transaction_id,
            committed_at,
        };
        self.append_ref_update_unlocked(&update).await?;
        Ok(CasRefReceipt {
            ref_name: ref_name.to_string(),
            generation: next_generation,
            previous_target: current.map(|value| value.target),
            new_target: new_target.to_string(),
        })
    }

    async fn ensure_layout(&self) -> Result<()> {
        for path in [
            self.storage.core_store_root_path(),
            self.storage.core_store_replicas_path(),
            self.storage.core_store_staging_path(),
            self.admission_wal_dir(),
            self.admission_landed_bytes_root(),
        ] {
            let started_at = Instant::now();
            fs::create_dir_all(&path).await?;
            crate::perf::record_io_duration(
                "core_store",
                "ensure_layout_create_dir_all",
                &path,
                0,
                started_at.elapsed(),
            );
        }
        Ok(())
    }

    pub async fn read_object_manifest(
        &self,
        object_ref: &CoreObjectRef,
    ) -> Result<CoreObjectManifest> {
        let manifest_hash = decode_manifest_ref(&object_ref.manifest_ref)?;
        let object_hash = strip_sha256_prefix(&object_ref.hash)?;
        if object_hash != manifest_hash {
            bail!("CoreStore object manifest ref/hash mismatch");
        }
        self.reconstruct_object_manifest_from_shards(object_ref, manifest_hash)
            .await
    }

    async fn read_object_manifest_for_range(
        &self,
        object_ref: &CoreObjectRef,
        range: &CoreByteRange,
    ) -> Result<CoreObjectManifest> {
        let manifest_hash = decode_manifest_ref(&object_ref.manifest_ref)?;
        let object_hash = strip_sha256_prefix(&object_ref.hash)?;
        if object_hash != manifest_hash {
            bail!("CoreStore object manifest ref/hash mismatch");
        }
        let profile =
            local_erasure_profile(decode_manifest_ref_profile(&object_ref.manifest_ref)?)?;
        let required_indices = required_data_shard_indices_for_range(
            object_ref.logical_size,
            profile.data_shards,
            range,
        )?;
        let manifest = self
            .reconstruct_object_manifest_from_shards_with_required_indices(
                object_ref,
                manifest_hash,
                Some(&required_indices),
            )
            .await?;
        let present = manifest
            .placements
            .iter()
            .map(|placement| placement.shard_index)
            .collect::<BTreeSet<_>>();
        let missing = required_indices
            .difference(&present)
            .copied()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            bail!(
                "CoreStore manifest {} is missing required range shards {:?}",
                object_ref.manifest_ref,
                missing
            );
        }
        Ok(manifest)
    }

    async fn reconstruct_object_manifest_from_shards(
        &self,
        object_ref: &CoreObjectRef,
        object_hash: &str,
    ) -> Result<CoreObjectManifest> {
        self.reconstruct_object_manifest_from_shards_with_required_indices(
            object_ref,
            object_hash,
            None,
        )
        .await
    }

    async fn reconstruct_object_manifest_from_shards_with_required_indices(
        &self,
        object_ref: &CoreObjectRef,
        object_hash: &str,
        required_indices: Option<&BTreeSet<u16>>,
    ) -> Result<CoreObjectManifest> {
        let profile = local_erasure_profile_for_counts(
            decode_manifest_ref_profile(&object_ref.manifest_ref)?,
            usize::from(object_ref.encoding.data_shards),
            usize::from(object_ref.encoding.parity_shards),
        )?;
        if object_ref.encoding.profile_id != profile.id {
            bail!("CoreStore object ref encoding profile does not match manifest ref");
        }
        let mut placements = Vec::new();
        let mut stripe_size = 0u64;
        let expected_block_id = local_block_id_for_object_hash(object_hash);
        for placement in &object_ref.placements {
            if usize::from(placement.shard_index) >= profile.total_shards() {
                bail!("CoreStore object ref contains shard index outside profile");
            }
            let shard_hash = strip_sha256_prefix(&placement.shard_hash)?;
            let path = self.shard_path(
                &placement.node_id,
                object_hash,
                placement.shard_index,
                shard_hash,
            );
            let decoded = match read_block_shard_file_dynamic(
                &path,
                &expected_block_id,
                placement.shard_index,
                &placement.shard_hash,
                "read_manifest_shard_header",
            )
            .await
            {
                Ok(payload) => payload,
                Err(err) if is_not_found_error(&err) => continue,
                Err(_) => continue,
            };
            if decoded.erasure_profile_id != profile.id {
                continue;
            }
            stripe_size = stripe_size
                .max((decoded.payload.len() as u64).saturating_mul(profile.data_shards as u64));
            let mut present = placement.clone();
            present.stored_size = decoded.payload.len() as u64;
            placements.push(present);
        }

        placements.sort_by_key(|placement| placement.shard_index);
        placements.dedup_by_key(|placement| placement.shard_index);
        if let Some(required_indices) = required_indices {
            let present = placements
                .iter()
                .map(|placement| placement.shard_index)
                .collect::<BTreeSet<_>>();
            let missing = required_indices
                .difference(&present)
                .copied()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                bail!(
                    "CoreStore manifest {} is missing required range shards {:?}",
                    object_ref.manifest_ref,
                    missing
                );
            }
        } else if placements.len() < profile.minimum_read_shards {
            bail!(
                "CoreStore manifest {} has only {} shard placements; {} data shards required",
                object_ref.manifest_ref,
                placements.len(),
                profile.minimum_read_shards
            );
        }

        let boundary_values = self
            .object_boundary_values_from_finalisation_records(object_ref)
            .await?;
        Ok(CoreObjectManifest {
            schema: CORE_OBJECT_MANIFEST_SCHEMA.to_string(),
            mesh_id: "local-mesh".to_string(),
            region_id: "local".to_string(),
            object_hash: object_ref.hash.clone(),
            logical_size: object_ref.logical_size,
            boundary_values,
            encoding: CoreObjectEncoding {
                profile_id: profile.id.to_string(),
                data_shards: profile.data_shards as u16,
                parity_shards: profile.parity_shards as u16,
                minimum_read_shards: profile.minimum_read_shards as u16,
                minimum_write_ack_shards: profile.minimum_write_ack_shards as u16,
                stripe_size,
                placement_scope: "region".to_string(),
                repair_priority: "normal".to_string(),
                encryption: "none".to_string(),
            },
            placements,
            created_at: "reconstructed-from-shards".to_string(),
            mutation_id: format!("reconstructed:{}", object_ref.hash),
        })
    }

    async fn object_boundary_values_from_finalisation_records(
        &self,
        object_ref: &CoreObjectRef,
    ) -> Result<Vec<CoreBoundaryValue>> {
        let mut values = Vec::new();
        for record in self
            .read_all_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await?
        {
            if record.record_kind != CORE_WAL_FINALISATION_RECORD_KIND {
                continue;
            }
            let finalisation: CoreWalFinalisationRecord = serde_json::from_slice(&record.payload)?;
            if finalisation.schema != CORE_WAL_FINALISATION_SCHEMA {
                bail!("CoreStore WAL finalisation record has invalid schema");
            }
            if finalisation.operation_family != "object.put" || finalisation.state != "committed" {
                continue;
            }
            if finalisation
                .landed_bytes
                .iter()
                .any(|landed| landed.sha256 == object_ref.hash)
            {
                values = finalisation.boundary_values;
            }
        }
        Ok(values)
    }

    async fn verify_embedded_manifest_readable(&self, manifest: &CoreObjectManifest) -> Result<()> {
        if manifest.schema != CORE_OBJECT_MANIFEST_SCHEMA {
            bail!("CoreStore embedded root segment manifest has invalid schema");
        }
        let object_ref = object_ref_from_object_manifest(manifest)?;
        let bytes = self
            .get_blob(GetBlob { object_ref })
            .await
            .with_context(|| "read embedded root segment manifest payload")?;
        if bytes.len() as u64 != manifest.logical_size {
            bail!("CoreStore embedded root segment logical size mismatch");
        }
        Ok(())
    }

    async fn read_all_stream_records(&self, stream_id: &str) -> Result<Vec<StreamRecord>> {
        for attempt in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            let Some(bytes) = self
                .read_bytes_from_quorum(
                    &format!("CoreStore stream {stream_id}"),
                    |store, node_id| store.stream_replica_path(node_id, stream_id),
                )
                .await?
            else {
                return Ok(Vec::new());
            };
            match decode_active_stream_records(stream_id, &bytes)
                .with_context(|| format!("decode CoreStore active stream {stream_id}"))
            {
                Ok(records) => return Ok(records),
                Err(error)
                    if is_incomplete_core_frame_error(&error)
                        && attempt + 1 < CORE_CONTROL_READ_RETRY_ATTEMPTS =>
                {
                    tokio::time::sleep(CORE_REF_LOCK_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("CoreStore stream decode retry loop must return")
    }

    async fn read_stream_records_after(
        &self,
        stream_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>> {
        for attempt in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            let Some(bytes) = self
                .read_bytes_from_quorum(
                    &format!("CoreStore stream {stream_id}"),
                    |store, node_id| store.stream_replica_path(node_id, stream_id),
                )
                .await?
            else {
                return Ok(Vec::new());
            };
            match decode_active_stream_records_page(stream_id, &bytes, after_sequence, limit)
                .with_context(|| format!("decode CoreStore active stream page {stream_id}"))
            {
                Ok(records) => return Ok(records),
                Err(error)
                    if is_incomplete_core_frame_error(&error)
                        && attempt + 1 < CORE_CONTROL_READ_RETRY_ATTEMPTS =>
                {
                    tokio::time::sleep(CORE_REF_LOCK_RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("CoreStore stream page decode retry loop must return")
    }

    async fn write_stream_records(&self, stream_id: &str, records: &[StreamRecord]) -> Result<()> {
        let bytes = encode_active_stream_records(stream_id, records)?;
        self.write_bytes_to_quorum(
            &format!("CoreStore stream {stream_id}"),
            &bytes,
            |store, node_id| store.stream_replica_path(node_id, stream_id),
        )
        .await
    }

    async fn acquire_batch_locks(&self, batch: &CoreMutationBatch) -> Result<Vec<CoreStoreLock>> {
        let mut locks = BTreeSet::new();
        for precondition in &batch.preconditions {
            match precondition {
                CoreMutationPrecondition::Ref { ref_name, .. } => {
                    validate_logical_id(ref_name, "precondition ref name")?;
                    locks.insert(("refs", ref_name.clone()));
                }
                CoreMutationPrecondition::StreamHead { stream_id, .. } => {
                    validate_logical_id(stream_id, "precondition stream id")?;
                    locks.insert(("streams", stream_id.clone()));
                }
                CoreMutationPrecondition::Fence { fence_name, .. } => {
                    validate_logical_id(fence_name, "precondition fence name")?;
                }
            }
        }
        for operation in &batch.operations {
            match operation {
                CoreMutationOperation::RefUpdate { ref_name, .. } => {
                    locks.insert(("refs", ref_name.clone()));
                }
                CoreMutationOperation::StreamAppend { stream_id, .. } => {
                    locks.insert(("streams", stream_id.clone()));
                }
            }
        }

        let mut guards = Vec::with_capacity(locks.len());
        for (kind, id) in locks {
            guards.push(self.acquire_named_lock(kind, &id).await?);
        }
        Ok(guards)
    }

    async fn acquire_ref_lock(&self, ref_name: &str) -> Result<CoreStoreLock> {
        self.acquire_named_lock("refs", ref_name).await
    }

    async fn acquire_stream_lock(&self, stream_id: &str) -> Result<CoreStoreLock> {
        self.acquire_named_lock("streams", stream_id).await
    }

    async fn acquire_named_lock(&self, kind: &str, id: &str) -> Result<CoreStoreLock> {
        let lock_path = self
            .storage
            .core_store_staging_path()
            .join("locks")
            .join(kind)
            .join(format!("{}.lock", logical_file_name(id)));
        if let Some(parent) = lock_path.parent() {
            let started_at = Instant::now();
            fs::create_dir_all(parent).await?;
            crate::perf::record_io_duration(
                "core_store",
                "lock_create_dir_all",
                parent,
                0,
                started_at.elapsed(),
            );
        }
        for _ in 0..CORE_REF_LOCK_RETRY_ATTEMPTS {
            let started_at = Instant::now();
            let open_result = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .await;
            crate::perf::record_io_duration(
                "core_store",
                "lock_create_new",
                &lock_path,
                0,
                started_at.elapsed(),
            );
            match open_result {
                Ok(_) => return Ok(CoreStoreLock { path: lock_path }),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    tokio::time::sleep(CORE_REF_LOCK_RETRY_DELAY).await;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("create CoreStore ref lock {}", lock_path.display())
                    });
                }
            }
        }
        bail!("CoreStore {kind} {id} lock was not acquired")
    }

    fn shard_path(
        &self,
        node_id: &str,
        object_hash: &str,
        shard_index: u16,
        shard_hash: &str,
    ) -> PathBuf {
        let prefix = &object_hash[0..2];
        self.storage
            .core_store_replica_path(node_id)
            .join("blobs")
            .join("sha256")
            .join(prefix)
            .join(object_hash)
            .join(format!("shard-{shard_index:05}-{shard_hash}.bin"))
    }

    fn admission_root(&self) -> PathBuf {
        self.storage.core_store_root_path().join("admission")
    }

    fn admission_wal_dir(&self) -> PathBuf {
        self.admission_root().join("wal").join(CORE_WAL_NODE_ID)
    }

    fn active_wal_path(&self) -> PathBuf {
        self.admission_wal_dir().join("active.anwal")
    }

    fn admission_landed_bytes_root(&self) -> PathBuf {
        self.admission_root().join("landed-bytes")
    }

    fn landed_bytes_path(&self, hash: &str) -> PathBuf {
        self.admission_landed_bytes_root()
            .join("sha256")
            .join(&hash[0..2])
            .join(format!("{hash}.landed"))
    }

    fn stream_data_replica_dir(&self, node_id: &str) -> PathBuf {
        self.storage
            .core_store_replica_path(node_id)
            .join("streams")
            .join("data")
    }

    fn stream_replica_path(&self, node_id: &str, stream_id: &str) -> PathBuf {
        self.stream_data_replica_dir(node_id)
            .join(format!("{}.anstream", logical_file_name(stream_id)))
    }

    async fn read_bytes_from_quorum<F>(
        &self,
        label: &str,
        mut replica_path: F,
    ) -> Result<Option<Vec<u8>>>
    where
        F: FnMut(&Self, &str) -> PathBuf,
    {
        for attempt in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            let mut votes: BTreeMap<String, (Vec<u8>, usize)> = BTreeMap::new();
            let mut found = 0usize;
            for node_id in local_control_node_ids() {
                let path = replica_path(self, &node_id);
                let bytes = match read_file(&path, "core_store", "read_quorum_replica").await {
                    Ok(bytes) => bytes,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => {
                        return Err(err).with_context(|| format!("read {label} replica {node_id}"));
                    }
                };
                found += 1;
                let hash = sha256_hex(&bytes);
                let entry = votes.entry(hash).or_insert((bytes, 0));
                entry.1 += 1;
            }

            if found == 0 {
                return Ok(None);
            }
            let Some((_, (bytes, count))) = votes.iter().max_by_key(|(_, (_, count))| *count)
            else {
                return Ok(None);
            };
            if *count >= LOCAL_CONTROL_READ_QUORUM {
                return Ok(Some(bytes.clone()));
            }
            if attempt + 1 == CORE_CONTROL_READ_RETRY_ATTEMPTS {
                bail!(
                    "{label} did not reach read quorum: {} matching replicas, {} required",
                    count,
                    LOCAL_CONTROL_READ_QUORUM
                );
            }
            tokio::time::sleep(CORE_REF_LOCK_RETRY_DELAY).await;
        }
        unreachable!("CoreStore control read retry loop must return")
    }

    async fn write_bytes_to_quorum<F>(
        &self,
        label: &str,
        bytes: &[u8],
        mut replica_path: F,
    ) -> Result<()>
    where
        F: FnMut(&Self, &str) -> PathBuf,
    {
        let mut acks = 0usize;
        let mut errors = Vec::new();
        for node_id in local_control_node_ids() {
            let path = replica_path(self, &node_id);
            match write_file_atomic(&path, bytes).await {
                Ok(()) => acks += 1,
                Err(err) => errors.push(format!("{node_id}: {err:#}")),
            }
        }
        if acks < LOCAL_CONTROL_WRITE_QUORUM {
            bail!(
                "{label} write quorum failed: {} acks, {} required; errors={:?}",
                acks,
                LOCAL_CONTROL_WRITE_QUORUM,
                errors
            );
        }
        Ok(())
    }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn local_block_id_for_object_hash(hash: &str) -> String {
    let hash = hash.strip_prefix("sha256:").unwrap_or(hash);
    let mut hasher = Sha256::new();
    for part in ["anvil.block.id.v1", "object_blob", "0", hash] {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("blk_{}", hex::encode(hasher.finalize()))
}

fn encode_erasure_shards(bytes: &[u8], profile: LocalErasureProfile) -> Result<Vec<Vec<u8>>> {
    let shard_len = bytes.len().div_ceil(profile.data_shards).max(1);
    let total_shards = profile.total_shards();
    let mut shards = vec![vec![0u8; shard_len]; total_shards];
    for (index, shard) in shards.iter_mut().take(profile.data_shards).enumerate() {
        let start = index.saturating_mul(shard_len);
        if start >= bytes.len() {
            break;
        }
        let end = usize::min(start + shard_len, bytes.len());
        shard[..end - start].copy_from_slice(&bytes[start..end]);
    }
    for parity_row in 0..profile.parity_shards {
        let parity_index = profile.data_shards + parity_row;
        for byte_index in 0..shard_len {
            let mut acc = 0u8;
            for data_index in 0..profile.data_shards {
                let coefficient = gf_pow((data_index + 1) as u8, parity_row as u32);
                acc ^= gf_mul(coefficient, shards[data_index][byte_index]);
            }
            shards[parity_index][byte_index] = acc;
        }
    }
    Ok(shards)
}

fn reconstruct_data_shards(
    shards: &mut [Option<Vec<u8>>],
    profile: LocalErasureProfile,
) -> Result<()> {
    let total_shards = profile.total_shards();
    if shards.len() != total_shards {
        bail!(
            "CoreStore erasure reconstruction expected {} shards for {}, got {}",
            total_shards,
            profile.id,
            shards.len()
        );
    }
    let shard_len = shards
        .iter()
        .find_map(|shard| shard.as_ref().map(Vec::len))
        .ok_or_else(|| anyhow!("CoreStore erasure reconstruction has no shards"))?;
    for shard in shards.iter().flatten() {
        if shard.len() != shard_len {
            bail!("CoreStore erasure reconstruction shard lengths differ");
        }
    }
    if shards.iter().filter(|shard| shard.is_some()).count() < profile.minimum_read_shards {
        bail!(
            "CoreStore erasure reconstruction has fewer than {} readable shards for {}",
            profile.minimum_read_shards,
            profile.id
        );
    }
    if shards.iter().take(profile.data_shards).all(Option::is_some) {
        return Ok(());
    }

    let selected = shards
        .iter()
        .enumerate()
        .filter_map(|(index, shard)| shard.as_ref().map(|payload| (index, payload.clone())))
        .take(profile.data_shards)
        .collect::<Vec<_>>();
    if selected.len() < profile.data_shards {
        bail!("CoreStore erasure reconstruction cannot select enough shards");
    }

    let matrix = selected
        .iter()
        .map(|(shard_index, _)| erasure_coding_row(*shard_index, profile.data_shards))
        .collect::<Vec<_>>();
    let inverse = invert_gf256_matrix(&matrix)?;
    for data_index in 0..profile.data_shards {
        if shards[data_index].is_some() {
            continue;
        }
        let mut reconstructed = vec![0u8; shard_len];
        for (source_row, (_, source_payload)) in selected.iter().enumerate() {
            let coefficient = inverse[data_index][source_row];
            if coefficient == 0 {
                continue;
            }
            for byte_index in 0..shard_len {
                reconstructed[byte_index] ^= gf_mul(coefficient, source_payload[byte_index]);
            }
        }
        shards[data_index] = Some(reconstructed);
    }

    Ok(())
}

fn erasure_coding_row(shard_index: usize, data_shards: usize) -> Vec<u8> {
    if shard_index < data_shards {
        let mut row = vec![0u8; data_shards];
        row[shard_index] = 1;
        return row;
    }
    let parity_row = shard_index - data_shards;
    (0..data_shards)
        .map(|data_index| gf_pow((data_index + 1) as u8, parity_row as u32))
        .collect()
}

fn invert_gf256_matrix(matrix: &[Vec<u8>]) -> Result<Vec<Vec<u8>>> {
    let n = matrix.len();
    if n == 0 {
        bail!("CoreStore cannot invert an empty erasure matrix");
    }
    if matrix.iter().any(|row| row.len() != n) {
        bail!("CoreStore erasure matrix must be square");
    }

    let mut augmented = vec![vec![0u8; n * 2]; n];
    for row in 0..n {
        augmented[row][..n].copy_from_slice(&matrix[row]);
        augmented[row][n + row] = 1;
    }

    for col in 0..n {
        let pivot = (col..n)
            .find(|row| augmented[*row][col] != 0)
            .ok_or_else(|| anyhow!("CoreStore erasure matrix is singular"))?;
        if pivot != col {
            augmented.swap(pivot, col);
        }
        let inv_pivot = gf_inv(augmented[col][col])?;
        for value in &mut augmented[col] {
            *value = gf_mul(*value, inv_pivot);
        }
        for row in 0..n {
            if row == col {
                continue;
            }
            let factor = augmented[row][col];
            if factor == 0 {
                continue;
            }
            for idx in 0..(n * 2) {
                augmented[row][idx] ^= gf_mul(factor, augmented[col][idx]);
            }
        }
    }

    Ok(augmented.into_iter().map(|row| row[n..].to_vec()).collect())
}

fn gf_pow(value: u8, exponent: u32) -> u8 {
    let mut acc = 1u8;
    for _ in 0..exponent {
        acc = gf_mul(acc, value);
    }
    acc
}

fn gf_inv(value: u8) -> Result<u8> {
    if value == 0 {
        bail!("CoreStore cannot invert zero in GF(2^8)");
    }
    Ok(gf_pow(value, 254))
}

fn gf_mul(mut lhs: u8, mut rhs: u8) -> u8 {
    let mut acc = 0u8;
    for _ in 0..8 {
        if rhs & 1 != 0 {
            acc ^= lhs;
        }
        let carry = lhs & 0x80 != 0;
        lhs <<= 1;
        if carry {
            lhs ^= 0x1d;
        }
        rhs >>= 1;
    }
    acc
}

fn required_data_shard_indices_for_range(
    logical_size: u64,
    data_shards: usize,
    range: &CoreByteRange,
) -> Result<BTreeSet<u16>> {
    if data_shards == 0 {
        bail!("CoreStore range read requires at least one data shard");
    }
    if range.start > range.end_exclusive {
        bail!("CoreStore range start must be <= end_exclusive");
    }
    if range.end_exclusive > logical_size {
        bail!("CoreStore range end_exclusive exceeds logical object size");
    }

    let shard_len = logical_size.div_ceil(data_shards as u64).max(1);
    let mut indices = BTreeSet::new();
    for shard_index in 0..data_shards {
        let shard_start = shard_index as u64 * shard_len;
        let shard_end = (shard_start + shard_len).min(logical_size);
        if range.start.max(shard_start) < range.end_exclusive.min(shard_end) {
            indices.insert(shard_index as u16);
        }
    }
    Ok(indices)
}

fn logical_file_manifest_from_object_manifests(
    request: &WriteLogicalFileRequest,
    blocks: &[MaterializedLogicalBlock],
    plaintext_hash: String,
    plaintext_len: u64,
    compression: CoreCompressionDescriptor,
) -> Result<CoreLogicalFileManifest> {
    if blocks.is_empty() {
        bail!("CoreStore logical file manifest must contain at least one materialised block");
    }
    for block in blocks {
        validate_manifest_for_object_ref(
            &block.object_manifest,
            &object_ref_from_object_manifest(&block.object_manifest)?,
            strip_sha256_prefix(&block.object_manifest.object_hash)?,
        )?;
    }

    let first_manifest = &blocks[0].object_manifest;
    let data_shards = u32::from(first_manifest.encoding.data_shards);
    let parity_shards = u32::from(first_manifest.encoding.parity_shards);
    let profile = local_erasure_profile_for_counts(
        &first_manifest.encoding.profile_id,
        data_shards as usize,
        parity_shards as usize,
    )?;
    if blocks.iter().any(|block| {
        block.object_manifest.encoding.profile_id != first_manifest.encoding.profile_id
            || block.object_manifest.encoding.data_shards != first_manifest.encoding.data_shards
            || block.object_manifest.encoding.parity_shards != first_manifest.encoding.parity_shards
    }) {
        bail!("CoreStore logical file blocks must use one erasure profile");
    }
    let logical_blocks = blocks
        .iter()
        .map(|block| logical_block_ref_from_materialized_block(block, profile))
        .collect::<Result<Vec<_>>>()?;
    let boundary_schema_generation = request
        .boundary_values
        .iter()
        .map(|value| value.schema_generation)
        .max()
        .unwrap_or(0);
    let ranges = if request.range_hints.is_empty() {
        vec![CoreLogicalRange {
            range_id: "full".to_string(),
            byte_start: 0,
            byte_end: plaintext_len,
            writer_record_kind: request.writer_family.clone(),
            boundary_values: request.boundary_values.clone(),
            writer_statistics: Vec::new(),
            block_ids: logical_blocks
                .iter()
                .map(|block| block.block_id.clone())
                .collect(),
            prefetch_next_range_ids: Vec::new(),
            preferred_block_boundary: "writer_defined".to_string(),
            boundary_dimension_ids: Vec::new(),
            shared_range: None,
        }]
    } else {
        request
            .range_hints
            .iter()
            .map(|hint| {
                validate_logical_range_hint(hint)?;
                if hint.byte_start > hint.byte_end {
                    bail!("CoreStore logical range hint start must be <= end");
                }
                if hint.byte_end > plaintext_len {
                    bail!("CoreStore logical range hint exceeds logical file size");
                }
                Ok(CoreLogicalRange {
                    range_id: hint.range_id.clone(),
                    byte_start: hint.byte_start,
                    byte_end: hint.byte_end,
                    writer_record_kind: hint.writer_record_kind.clone(),
                    boundary_values: hint.boundary_values.clone(),
                    writer_statistics: hint.writer_statistics.clone(),
                    block_ids: logical_block_ids_for_range(
                        &logical_blocks,
                        hint.byte_start,
                        hint.byte_end,
                    ),
                    prefetch_next_range_ids: hint.prefetch_next_range_ids.clone(),
                    preferred_block_boundary: hint.preferred_block_boundary.clone(),
                    boundary_dimension_ids: hint.boundary_dimension_ids.clone(),
                    shared_range: hint.shared_range.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?
    };

    Ok(CoreLogicalFileManifest {
        schema: CORE_LOGICAL_FILE_MANIFEST_SCHEMA.to_string(),
        logical_file_id: request.logical_file_id.clone(),
        writer_family: request.writer_family.clone(),
        writer_generation: request.generation,
        logical_size: plaintext_len,
        content_hash: plaintext_hash.clone(),
        boundary_schema_generation,
        ranges,
        blocks: logical_blocks,
        compression,
        encryption: CoreEncryptionDescriptor {
            algorithm: first_manifest.encoding.encryption.clone(),
            key_id: String::new(),
            nonce: Vec::new(),
            aad_hash: String::new(),
            plaintext_hash,
            ciphertext_hash: first_manifest.object_hash.clone(),
            descriptor_hash: descriptor_hash(&[
                "encryption",
                &first_manifest.encoding.encryption,
                &first_manifest.object_hash,
            ]),
        },
        erasure_profile_id: first_manifest.encoding.profile_id.clone(),
        placement_epoch: LOCAL_PLACEMENT_EPOCH,
        created_by_mutation_id: request.mutation_id.clone(),
        codec_id: profile.codec_id.to_string(),
        data_shards,
        parity_shards,
    })
}

fn logical_block_ref_from_materialized_block(
    block: &MaterializedLogicalBlock,
    profile: LocalErasureProfile,
) -> Result<CoreLogicalBlockRef> {
    let object_manifest = &block.object_manifest;
    let object_hash = strip_sha256_prefix(&object_manifest.object_hash)?;
    let shard_payload_len = object_manifest
        .placements
        .iter()
        .map(|placement| placement.stored_size)
        .max()
        .unwrap_or(0);
    let data_shards = u32::from(object_manifest.encoding.data_shards);
    let parity_shards = u32::from(object_manifest.encoding.parity_shards);
    Ok(CoreLogicalBlockRef {
        block_id: local_block_id_for_object_hash(object_hash),
        logical_offset: block.logical_offset,
        logical_length: block.logical_length,
        compressed_length: object_manifest.logical_size,
        encrypted_length: object_manifest.logical_size,
        content_hash: block.plaintext_hash.clone(),
        erasure_set_id: "local-erasure-set".to_string(),
        shards: object_manifest
            .placements
            .iter()
            .map(|placement| CoreLogicalShardRef {
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_index: u32::from(placement.shard_index),
                shard_hash: placement.shard_hash.clone(),
                stored_length: placement.stored_size,
                generation: placement.generation,
                placement_epoch: placement.placement_epoch,
                fsync_sequence: placement.fsync_sequence,
            })
            .collect(),
        codec_id: profile.codec_id.to_string(),
        data_shards,
        parity_shards,
        plaintext_block_len: block.logical_length,
        shard_payload_len,
        padding_len: shard_payload_len
            .saturating_mul(u64::from(data_shards))
            .saturating_sub(object_manifest.logical_size),
        block_encoded_hash: object_manifest.object_hash.clone(),
    })
}

fn logical_block_ids_for_range(
    blocks: &[CoreLogicalBlockRef],
    range_start: u64,
    range_end: u64,
) -> Vec<String> {
    blocks
        .iter()
        .filter(|block| {
            let block_start = block.logical_offset;
            let block_end = block.logical_offset.saturating_add(block.logical_length);
            range_start.max(block_start) < range_end.min(block_end)
        })
        .map(|block| block.block_id.clone())
        .collect()
}

fn validate_logical_file_manifest_shape(manifest: &CoreLogicalFileManifest) -> Result<()> {
    if manifest.schema != CORE_LOGICAL_FILE_MANIFEST_SCHEMA {
        bail!("CoreStore logical file manifest has invalid schema");
    }
    validate_logical_id(&manifest.logical_file_id, "logical file id")?;
    validate_logical_id(&manifest.writer_family, "writer family")?;
    let profile = local_erasure_profile_for_counts(
        &manifest.erasure_profile_id,
        manifest.data_shards as usize,
        manifest.parity_shards as usize,
    )?;
    if manifest.codec_id != profile.codec_id {
        bail!("CoreStore logical file manifest codec id does not match erasure profile");
    }
    if manifest.blocks.is_empty() {
        bail!("CoreStore logical file manifest must contain at least one block");
    }
    if manifest.compression.algorithm == "zstd" && manifest.blocks.len() != 1 {
        bail!("CoreStore zstd logical files must use one compressed block");
    }
    match manifest.compression.algorithm.as_str() {
        "none" => {
            if manifest.compression.level != 0
                || manifest.compression.uncompressed_length != manifest.logical_size
                || manifest.compression.compressed_length != manifest.logical_size
            {
                bail!("CoreStore none compression descriptor does not match logical size");
            }
        }
        "zstd" => {
            if manifest.compression.level == 0
                || manifest.compression.uncompressed_length != manifest.logical_size
            {
                bail!("CoreStore zstd compression descriptor is invalid");
            }
        }
        other => bail!("CoreStore unsupported logical file compression descriptor {other}"),
    }
    let mut ordered_blocks = manifest.blocks.iter().collect::<Vec<_>>();
    ordered_blocks.sort_by_key(|block| block.logical_offset);
    let mut expected_offset = 0u64;
    let mut stored_len = 0u64;
    for block in ordered_blocks {
        if block.logical_offset != expected_offset {
            bail!("CoreStore logical file blocks must cover the file without gaps or overlap");
        }
        expected_offset = expected_offset.saturating_add(block.logical_length);
        stored_len = stored_len.saturating_add(block.compressed_length);
        if block.data_shards != manifest.data_shards
            || block.parity_shards != manifest.parity_shards
        {
            bail!("CoreStore logical file block shard counts mismatch manifest");
        }
        if block.codec_id != profile.codec_id {
            bail!("CoreStore logical file block codec id does not match erasure profile");
        }
        if manifest.blocks.len() == 1
            && block.compressed_length != manifest.compression.compressed_length
        {
            bail!("CoreStore logical file block compressed length does not match descriptor");
        }
        if block.shards.len() < profile.minimum_read_shards {
            bail!("CoreStore logical file block does not contain enough shard receipts");
        }
        for shard in &block.shards {
            if shard.placement_epoch != LOCAL_PLACEMENT_EPOCH {
                bail!("CoreStore logical file shard has stale placement epoch");
            }
            if shard.fsync_sequence == 0 {
                bail!("CoreStore logical file shard is missing fsync evidence");
            }
        }
    }
    if expected_offset != manifest.logical_size {
        bail!("CoreStore logical file blocks must cover the complete logical file");
    }
    if manifest.compression.algorithm == "none"
        && stored_len != manifest.compression.compressed_length
    {
        bail!("CoreStore logical file block stored lengths do not match compression descriptor");
    }
    Ok(())
}

fn object_ref_from_logical_file_manifest(
    manifest: &CoreLogicalFileManifest,
) -> Result<CoreObjectRef> {
    validate_logical_file_manifest_shape(manifest)?;
    Ok(core_object_ref_from_logical_file_manifest(manifest))
}

fn object_ref_from_logical_block_ref(
    block: &CoreLogicalBlockRef,
    erasure_profile_id: &str,
) -> Result<CoreObjectRef> {
    Ok(CoreObjectRef {
        hash: block.block_encoded_hash.clone(),
        logical_size: block.encrypted_length,
        manifest_ref: encode_manifest_ref_with_profile(
            strip_sha256_prefix(&block.block_encoded_hash)?,
            erasure_profile_id,
        ),
        encoding: CoreObjectEncoding {
            profile_id: erasure_profile_id.to_string(),
            data_shards: block.data_shards as u16,
            parity_shards: block.parity_shards as u16,
            minimum_read_shards: block.data_shards as u16,
            minimum_write_ack_shards: (block.data_shards + block.parity_shards) as u16,
            stripe_size: block
                .shard_payload_len
                .saturating_mul(u64::from(block.data_shards)),
            placement_scope: "region".to_string(),
            repair_priority: "normal".to_string(),
            encryption: "none".to_string(),
        },
        placements: block
            .shards
            .iter()
            .map(|shard| CoreObjectPlacement {
                shard_index: shard.shard_index as u16,
                node_id: shard.node_id.clone(),
                region_id: shard.region_id.clone(),
                cell_id: shard.cell_id.clone(),
                shard_hash: shard.shard_hash.clone(),
                stored_size: shard.stored_length,
                generation: shard.generation,
                placement_epoch: shard.placement_epoch,
                fsync_sequence: shard.fsync_sequence,
            })
            .collect(),
    })
}

fn object_ref_from_object_manifest(manifest: &CoreObjectManifest) -> Result<CoreObjectRef> {
    Ok(CoreObjectRef {
        hash: manifest.object_hash.clone(),
        logical_size: manifest.logical_size,
        manifest_ref: encode_manifest_ref_with_profile(
            strip_sha256_prefix(&manifest.object_hash)?,
            &manifest.encoding.profile_id,
        ),
        encoding: manifest.encoding.clone(),
        placements: manifest.placements.clone(),
    })
}

fn ensure_range_is_inside_expected_boundary(
    manifest: &CoreLogicalFileManifest,
    range: &CoreByteRange,
    expected_boundary: &[CoreBoundaryValue],
) -> Result<()> {
    if expected_boundary.is_empty() {
        return Ok(());
    }
    let matching_range = manifest.ranges.iter().any(|candidate| {
        candidate.byte_start <= range.start
            && range.end_exclusive <= candidate.byte_end
            && expected_boundary
                .iter()
                .all(|expected| candidate.boundary_values.contains(expected))
    });
    if !matching_range {
        bail!("CoreStore logical range is outside expected boundary values");
    }
    Ok(())
}

fn encode_logical_file_source(
    compression: &str,
    source: Vec<u8>,
) -> Result<(Vec<u8>, CoreCompressionDescriptor)> {
    let uncompressed_length = source.len() as u64;
    let uncompressed_hash = format!("sha256:{}", sha256_hex(&source));
    match compression {
        "none" => Ok((
            source,
            CoreCompressionDescriptor {
                algorithm: "none".to_string(),
                level: 0,
                uncompressed_length,
                compressed_length: uncompressed_length,
                dictionary_id: String::new(),
                descriptor_hash: descriptor_hash(&[
                    "compression",
                    "none",
                    &uncompressed_length.to_string(),
                    &uncompressed_hash,
                ]),
            },
        )),
        "zstd" => {
            let level = 3;
            let compressed = zstd::stream::encode_all(Cursor::new(&source), level)?;
            let compressed_length = compressed.len() as u64;
            let compressed_hash = format!("sha256:{}", sha256_hex(&compressed));
            Ok((
                compressed,
                CoreCompressionDescriptor {
                    algorithm: "zstd".to_string(),
                    level: level as u32,
                    uncompressed_length,
                    compressed_length,
                    dictionary_id: String::new(),
                    descriptor_hash: descriptor_hash(&[
                        "compression",
                        "zstd",
                        &level.to_string(),
                        &uncompressed_length.to_string(),
                        &compressed_length.to_string(),
                        &uncompressed_hash,
                        &compressed_hash,
                    ]),
                },
            ))
        }
        other => bail!("CoreStore unsupported logical file compression policy {other}"),
    }
}

fn none_compression_descriptor(source: &[u8]) -> CoreCompressionDescriptor {
    let uncompressed_length = source.len() as u64;
    let uncompressed_hash = format!("sha256:{}", sha256_hex(source));
    CoreCompressionDescriptor {
        algorithm: "none".to_string(),
        level: 0,
        uncompressed_length,
        compressed_length: uncompressed_length,
        dictionary_id: String::new(),
        descriptor_hash: descriptor_hash(&[
            "compression",
            "none",
            &uncompressed_length.to_string(),
            &uncompressed_hash,
        ]),
    }
}

fn decode_logical_file_source(compression: &str, stored: Vec<u8>) -> Result<Vec<u8>> {
    match compression {
        "none" => Ok(stored),
        "zstd" => Ok(zstd::stream::decode_all(Cursor::new(stored))?),
        other => bail!("CoreStore unsupported logical file compression descriptor {other}"),
    }
}

fn validate_pipeline_policy(
    policy: &CorePipelinePolicy,
    _profile: LocalErasureProfile,
) -> Result<()> {
    match policy.compression.as_str() {
        "none" | "zstd" => {}
        other => bail!("CoreStore unsupported logical file compression policy {other}"),
    }
    match policy.encryption.as_str() {
        "none" | "aes_gcm_siv" => {}
        other => bail!("CoreStore unsupported logical file encryption policy {other}"),
    }
    match policy.boundary_mode.as_str() {
        "honour" | "prefer" | "ignore_for_diagnostic_only" => {}
        other => bail!("CoreStore unsupported boundary mode {other}"),
    }
    if policy.target_block_size == 0 {
        bail!("CoreStore target_block_size must be greater than zero");
    }
    Ok(())
}

fn effective_target_block_size(policy: &CorePipelinePolicy, profile: LocalErasureProfile) -> u64 {
    policy
        .target_block_size
        .min(profile.logical_block_target_bytes)
}

fn logical_block_ranges_for_source(
    source: &[u8],
    request: &WriteLogicalFileRequest,
    target_block_size: usize,
) -> Vec<(usize, usize)> {
    let len = source.len();
    if len == 0 {
        return vec![(0, 0)];
    }

    let mut cuts = BTreeSet::from([0usize, len]);
    for offset in (target_block_size..len).step_by(target_block_size) {
        cuts.insert(offset);
    }
    for hint in &request.range_hints {
        if hint.preferred_block_boundary != "required" {
            continue;
        }
        for boundary in [hint.byte_start, hint.byte_end] {
            if boundary == 0 || boundary >= len as u64 {
                continue;
            }
            if let Ok(boundary) = usize::try_from(boundary) {
                cuts.insert(boundary);
            }
        }
    }

    let ordered = cuts.into_iter().collect::<Vec<_>>();
    ordered
        .windows(2)
        .filter_map(|window| match window {
            [start, end] if start < end => Some((*start, *end)),
            _ => None,
        })
        .collect()
}

fn validate_logical_range_hint(hint: &CoreLogicalRangeHint) -> Result<()> {
    validate_logical_id(&hint.range_id, "logical range id")?;
    validate_logical_id(&hint.writer_record_kind, "logical range writer record kind")?;
    match hint.preferred_block_boundary.as_str() {
        "required" | "preferred" | "writer_defined" | "none" => {}
        other => bail!("CoreStore unsupported preferred block boundary {other}"),
    }
    if let Some(shared) = &hint.shared_range {
        validate_logical_id(&shared.record_kind, "shared range record kind")?;
        if shared.reason.trim().is_empty() {
            bail!("CoreStore shared range marker reason must not be empty");
        }
        if shared.boundary_dimension_ids.is_empty() {
            bail!("CoreStore shared range marker must name crossed dimensions");
        }
    }
    Ok(())
}

fn descriptor_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum MinimalCborValue {
    Text(String),
    U64(u64),
}

#[derive(Debug, Clone)]
struct BlockShardHeaderInput {
    block_id: String,
    erasure_set_id: String,
    shard_index: u16,
    erasure_profile_id: String,
    logical_file_id: String,
    logical_offset: u64,
    logical_length: u64,
    payload_plain_hash: String,
    payload_stored_hash: String,
    compression: String,
    encryption: String,
    placement_epoch: u64,
    boundary_summary_hash: String,
    writer_family: String,
    created_by_mutation_id: String,
}

#[derive(Debug, Clone, Copy)]
struct BlockShardExpectation<'a> {
    block_id: &'a str,
    shard_index: u16,
    erasure_profile_id: &'a str,
    placement_epoch: u64,
    payload_hash: &'a str,
    payload_len: u64,
}

#[derive(Debug)]
struct DecodedBlockShard {
    erasure_profile_id: String,
    payload: Vec<u8>,
}

fn encode_block_shard_file(header: BlockShardHeaderInput, payload: &[u8]) -> Result<Vec<u8>> {
    let header_cbor = encode_minimal_cbor_map(&block_shard_header_map(header)?);
    let mut out = Vec::with_capacity(
        CORE_BLOCK_SHARD_MAGIC.len() + 2 + 4 + header_cbor.len() + 8 + payload.len() + 4 + 32,
    );
    out.extend_from_slice(CORE_BLOCK_SHARD_MAGIC);
    out.extend_from_slice(&CORE_BLOCK_SHARD_VERSION.to_le_bytes());
    write_u32_le(&mut out, header_cbor.len())?;
    out.extend_from_slice(&header_cbor);
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(payload);
    let mut crc_input = Vec::with_capacity(header_cbor.len() + payload.len());
    crc_input.extend_from_slice(&header_cbor);
    crc_input.extend_from_slice(payload);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    let file_hash = Sha256::digest(&out);
    out.extend_from_slice(file_hash.as_ref());
    Ok(out)
}

async fn read_block_shard_file(
    path: &PathBuf,
    expectation: BlockShardExpectation<'_>,
    operation: &'static str,
) -> Result<Vec<u8>> {
    let bytes = read_file(path, "core_store", operation)
        .await
        .with_context(|| format!("read CoreStore block shard {}", path.display()))?;
    let (header, payload) = decode_block_shard_file(&bytes)?;
    expect_cbor_text(&header, "schema", CORE_BLOCK_SHARD_HEADER_SCHEMA)?;
    expect_cbor_text(&header, "block_id", expectation.block_id)?;
    expect_cbor_u64(&header, "shard_index", u64::from(expectation.shard_index))?;
    expect_cbor_text(
        &header,
        "erasure_profile_id",
        expectation.erasure_profile_id,
    )?;
    expect_cbor_u64(&header, "placement_epoch", expectation.placement_epoch)?;
    expect_cbor_text(&header, "payload_stored_hash", expectation.payload_hash)?;
    expect_cbor_text(&header, "payload_plain_hash", expectation.payload_hash)?;
    if expectation.payload_len > 0 {
        expect_cbor_u64(&header, "logical_length", expectation.payload_len)?;
    }
    let actual_hash = format!("sha256:{}", sha256_hex(&payload));
    if actual_hash != expectation.payload_hash {
        bail!(
            "CoreStore block shard payload hash mismatch: expected {}, got {}",
            expectation.payload_hash,
            actual_hash
        );
    }
    if expectation.payload_len > 0 && payload.len() as u64 != expectation.payload_len {
        bail!("CoreStore block shard payload length mismatch");
    }
    Ok(payload)
}

async fn read_block_shard_file_dynamic(
    path: &PathBuf,
    expected_block_id: &str,
    expected_shard_index: u16,
    expected_payload_hash: &str,
    operation: &'static str,
) -> Result<DecodedBlockShard> {
    let bytes = read_file(path, "core_store", operation)
        .await
        .with_context(|| format!("read CoreStore block shard {}", path.display()))?;
    let (header, payload) = decode_block_shard_file(&bytes)?;
    expect_cbor_text(&header, "schema", CORE_BLOCK_SHARD_HEADER_SCHEMA)?;
    expect_cbor_text(&header, "block_id", expected_block_id)?;
    expect_cbor_u64(&header, "shard_index", u64::from(expected_shard_index))?;
    expect_cbor_u64(&header, "placement_epoch", LOCAL_PLACEMENT_EPOCH)?;
    expect_cbor_text(&header, "payload_stored_hash", expected_payload_hash)?;
    expect_cbor_text(&header, "payload_plain_hash", expected_payload_hash)?;
    let actual_hash = format!("sha256:{}", sha256_hex(&payload));
    if actual_hash != expected_payload_hash {
        bail!(
            "CoreStore block shard payload hash mismatch: expected {}, got {}",
            expected_payload_hash,
            actual_hash
        );
    }
    Ok(DecodedBlockShard {
        erasure_profile_id: cbor_text_value(&header, "erasure_profile_id")?.to_string(),
        payload,
    })
}

fn decode_block_shard_file(bytes: &[u8]) -> Result<(BTreeMap<String, MinimalCborValue>, Vec<u8>)> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_BLOCK_SHARD_MAGIC.len())?;
    if magic != CORE_BLOCK_SHARD_MAGIC {
        bail!("CoreStore block shard has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_BLOCK_SHARD_VERSION {
        bail!("CoreStore block shard has unsupported version {version}");
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let header_cbor = read_exact(bytes, &mut offset, header_len)?;
    let header = decode_minimal_cbor_map(header_cbor)?;
    if encode_minimal_cbor_map(&header) != header_cbor {
        bail!("CoreStore block shard header is not canonical CBOR");
    }
    let payload_len = read_u64_le(bytes, &mut offset)? as usize;
    let payload = read_exact(bytes, &mut offset, payload_len)?.to_vec();
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    let mut crc_input = Vec::with_capacity(header_cbor.len() + payload.len());
    crc_input.extend_from_slice(header_cbor);
    crc_input.extend_from_slice(&payload);
    if crc32c(&crc_input) != expected_crc {
        bail!("CoreStore block shard checksum mismatch");
    }
    let file_hash_start = offset;
    let expected_file_hash = read_exact(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        bail!("CoreStore block shard has trailing bytes");
    }
    let actual_file_hash = Sha256::digest(&bytes[..file_hash_start]);
    let actual_file_hash: &[u8] = actual_file_hash.as_ref();
    if expected_file_hash != actual_file_hash {
        bail!("CoreStore block shard file hash mismatch");
    }
    Ok((header, payload))
}

fn block_shard_header_map(
    header: BlockShardHeaderInput,
) -> Result<BTreeMap<String, MinimalCborValue>> {
    let mut map = BTreeMap::new();
    map.insert(
        "schema".to_string(),
        MinimalCborValue::Text(CORE_BLOCK_SHARD_HEADER_SCHEMA.to_string()),
    );
    map.insert(
        "block_id".to_string(),
        MinimalCborValue::Text(header.block_id),
    );
    map.insert(
        "erasure_set_id".to_string(),
        MinimalCborValue::Text(header.erasure_set_id),
    );
    map.insert(
        "shard_index".to_string(),
        MinimalCborValue::U64(u64::from(header.shard_index)),
    );
    map.insert(
        "erasure_profile_id".to_string(),
        MinimalCborValue::Text(header.erasure_profile_id),
    );
    map.insert(
        "logical_file_id".to_string(),
        MinimalCborValue::Text(header.logical_file_id),
    );
    map.insert(
        "logical_offset".to_string(),
        MinimalCborValue::U64(header.logical_offset),
    );
    map.insert(
        "logical_length".to_string(),
        MinimalCborValue::U64(header.logical_length),
    );
    map.insert(
        "payload_plain_hash".to_string(),
        MinimalCborValue::Text(header.payload_plain_hash),
    );
    map.insert(
        "payload_stored_hash".to_string(),
        MinimalCborValue::Text(header.payload_stored_hash),
    );
    map.insert(
        "compression".to_string(),
        MinimalCborValue::Text(header.compression),
    );
    map.insert(
        "encryption".to_string(),
        MinimalCborValue::Text(header.encryption),
    );
    map.insert(
        "placement_epoch".to_string(),
        MinimalCborValue::U64(header.placement_epoch),
    );
    map.insert(
        "boundary_summary_hash".to_string(),
        MinimalCborValue::Text(header.boundary_summary_hash),
    );
    map.insert(
        "writer_family".to_string(),
        MinimalCborValue::Text(header.writer_family),
    );
    map.insert(
        "created_by_mutation_id".to_string(),
        MinimalCborValue::Text(header.created_by_mutation_id),
    );
    Ok(map)
}

fn boundary_summary_hash(boundary_values: &[CoreBoundaryValue]) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        sha256_hex(&serde_json::to_vec(boundary_values)?)
    ))
}

fn expect_cbor_text(
    header: &BTreeMap<String, MinimalCborValue>,
    key: &str,
    expected: &str,
) -> Result<()> {
    match header.get(key) {
        Some(MinimalCborValue::Text(actual)) if actual == expected => Ok(()),
        Some(MinimalCborValue::Text(actual)) => {
            bail!("CoreStore block shard header {key} mismatch: expected {expected}, got {actual}")
        }
        _ => bail!("CoreStore block shard header missing text field {key}"),
    }
}

fn cbor_text_value<'a>(
    header: &'a BTreeMap<String, MinimalCborValue>,
    key: &str,
) -> Result<&'a str> {
    match header.get(key) {
        Some(MinimalCborValue::Text(value)) => Ok(value),
        _ => bail!("CoreStore block shard header missing text field {key}"),
    }
}

fn expect_cbor_u64(
    header: &BTreeMap<String, MinimalCborValue>,
    key: &str,
    expected: u64,
) -> Result<()> {
    match header.get(key) {
        Some(MinimalCborValue::U64(actual)) if *actual == expected => Ok(()),
        Some(MinimalCborValue::U64(actual)) => {
            bail!("CoreStore block shard header {key} mismatch: expected {expected}, got {actual}")
        }
        _ => bail!("CoreStore block shard header missing u64 field {key}"),
    }
}

fn encode_minimal_cbor_map(map: &BTreeMap<String, MinimalCborValue>) -> Vec<u8> {
    let mut out = Vec::new();
    push_cbor_type_len(&mut out, 5, map.len() as u64);
    for (key, value) in map {
        push_cbor_text(&mut out, key);
        match value {
            MinimalCborValue::Text(value) => push_cbor_text(&mut out, value),
            MinimalCborValue::U64(value) => push_cbor_type_len(&mut out, 0, *value),
        }
    }
    out
}

fn decode_minimal_cbor_map(bytes: &[u8]) -> Result<BTreeMap<String, MinimalCborValue>> {
    let mut offset = 0usize;
    let (major, len) = read_cbor_type_len(bytes, &mut offset)?;
    if major != 5 {
        bail!("CoreStore block shard header CBOR is not a map");
    }
    let mut previous_key = None::<String>;
    let mut map = BTreeMap::new();
    for _ in 0..len {
        let key = read_cbor_text(bytes, &mut offset)?;
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            bail!("CoreStore block shard header CBOR map keys are not canonical");
        }
        previous_key = Some(key.clone());
        let (major, value_len) = read_cbor_type_len(bytes, &mut offset)?;
        let value = match major {
            0 => MinimalCborValue::U64(value_len),
            3 => {
                let raw = read_exact(bytes, &mut offset, value_len as usize)?;
                MinimalCborValue::Text(std::str::from_utf8(raw)?.to_string())
            }
            _ => bail!("CoreStore block shard header CBOR has unsupported value type"),
        };
        map.insert(key, value);
    }
    if offset != bytes.len() {
        bail!("CoreStore block shard header CBOR has trailing bytes");
    }
    Ok(map)
}

fn push_cbor_text(out: &mut Vec<u8>, value: &str) {
    push_cbor_type_len(out, 3, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn push_cbor_type_len(out: &mut Vec<u8>, major: u8, value: u64) {
    let prefix = major << 5;
    match value {
        0..=23 => out.push(prefix | value as u8),
        24..=0xff => {
            out.push(prefix | 24);
            out.push(value as u8);
        }
        0x100..=0xffff => {
            out.push(prefix | 25);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(prefix | 26);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        }
        _ => {
            out.push(prefix | 27);
            out.extend_from_slice(&value.to_be_bytes());
        }
    }
}

fn read_cbor_text(bytes: &[u8], offset: &mut usize) -> Result<String> {
    let (major, len) = read_cbor_type_len(bytes, offset)?;
    if major != 3 {
        bail!("CoreStore block shard header CBOR key is not text");
    }
    let raw = read_exact(bytes, offset, len as usize)?;
    Ok(std::str::from_utf8(raw)?.to_string())
}

fn read_cbor_type_len(bytes: &[u8], offset: &mut usize) -> Result<(u8, u64)> {
    let first = *read_exact(bytes, offset, 1)?
        .first()
        .ok_or_else(|| anyhow!("CoreStore CBOR ended unexpectedly"))?;
    let major = first >> 5;
    let additional = first & 0x1f;
    let value = match additional {
        value @ 0..=23 => u64::from(value),
        24 => {
            let value = u64::from(*read_exact(bytes, offset, 1)?.first().unwrap());
            if value < 24 {
                bail!("CoreStore CBOR length is not canonical");
            }
            value
        }
        25 => {
            let raw = read_exact(bytes, offset, 2)?;
            let value = u64::from(u16::from_be_bytes(raw.try_into()?));
            if value <= 0xff {
                bail!("CoreStore CBOR length is not canonical");
            }
            value
        }
        26 => {
            let raw = read_exact(bytes, offset, 4)?;
            let value = u64::from(u32::from_be_bytes(raw.try_into()?));
            if value <= 0xffff {
                bail!("CoreStore CBOR length is not canonical");
            }
            value
        }
        27 => {
            let raw = read_exact(bytes, offset, 8)?;
            let value = u64::from_be_bytes(raw.try_into()?);
            if value <= 0xffff_ffff {
                bail!("CoreStore CBOR length is not canonical");
            }
            value
        }
        _ => bail!("CoreStore CBOR indefinite length is not allowed"),
    };
    Ok((major, value))
}

fn validate_manifest_for_object_ref(
    manifest: &CoreObjectManifest,
    object_ref: &CoreObjectRef,
    expected_hash: &str,
) -> Result<()> {
    if manifest.object_hash != object_ref.hash {
        bail!(
            "CoreStore manifest hash mismatch: ref {}, manifest {}",
            object_ref.hash,
            manifest.object_hash
        );
    }
    if strip_sha256_prefix(&manifest.object_hash)? != expected_hash {
        bail!("CoreStore manifest hash does not match requested object hash");
    }
    if manifest.logical_size != object_ref.logical_size {
        bail!(
            "CoreStore manifest size mismatch: ref {}, manifest {}",
            object_ref.logical_size,
            manifest.logical_size
        );
    }
    let manifest_ref_profile = decode_manifest_ref_profile(&object_ref.manifest_ref)?;
    if manifest_ref_profile != manifest.encoding.profile_id {
        bail!(
            "CoreStore manifest profile mismatch: ref {}, manifest {}",
            manifest_ref_profile,
            manifest.encoding.profile_id
        );
    }
    let data_shards = usize::from(manifest.encoding.data_shards);
    let parity_shards = usize::from(manifest.encoding.parity_shards);
    let profile = local_erasure_profile_for_counts(
        &manifest.encoding.profile_id,
        data_shards,
        parity_shards,
    )?;
    let minimum_read_shards = usize::from(manifest.encoding.minimum_read_shards);
    let minimum_write_ack_shards = usize::from(manifest.encoding.minimum_write_ack_shards);
    if data_shards == 0 || parity_shards == 0 {
        bail!("CoreStore erasure profile must include data and parity shards");
    }
    if minimum_read_shards != data_shards {
        bail!(
            "CoreStore unsupported minimum_read_shards {}; expected {}",
            minimum_read_shards,
            data_shards
        );
    }
    if minimum_write_ack_shards != profile.minimum_write_ack_shards {
        bail!(
            "CoreStore minimum_write_ack_shards {} does not match profile {} requirement {}",
            minimum_write_ack_shards,
            profile.id,
            profile.minimum_write_ack_shards
        );
    }
    if manifest.encoding.placement_scope != "region" {
        bail!(
            "CoreStore unsupported placement_scope {}",
            manifest.encoding.placement_scope
        );
    }
    if manifest.encoding.repair_priority.is_empty() {
        bail!("CoreStore repair_priority must not be empty");
    }
    Ok(())
}

fn strip_sha256_prefix(hash: &str) -> Result<&str> {
    hash.strip_prefix("sha256:")
        .ok_or_else(|| anyhow!("CoreStore hash must have sha256: prefix"))
}

fn validate_hash(hash: &str, label: &str) -> Result<()> {
    let value = strip_sha256_prefix(hash)?;
    if value.len() != 64 || !value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore {label} must be a sha256 hash");
    }
    Ok(())
}

fn logical_file_name(value: &str) -> String {
    sha256_hex(value.as_bytes())
}

fn is_quorum_visibility_gap(error: &anyhow::Error) -> bool {
    error
        .chain()
        .any(|cause| cause.to_string().contains("did not reach read quorum"))
}

fn receipt_from_transaction(transaction: &CoreTransaction) -> CoreMutationBatchReceipt {
    CoreMutationBatchReceipt {
        transaction_id: transaction.transaction_id.clone(),
        scope_partition: transaction.scope_partition.clone(),
        state: transaction.state,
        visible_updates: if transaction.state == CoreTransactionState::Committed {
            transaction.visible_updates.clone()
        } else {
            Vec::new()
        },
        finalisation_error: transaction.finalisation_error.clone(),
    }
}

fn core_transaction_state_name(state: CoreTransactionState) -> &'static str {
    match state {
        CoreTransactionState::Prepared => "prepared",
        CoreTransactionState::Committed => "committed",
        CoreTransactionState::FinalisationFailed => "finalisation_failed",
        CoreTransactionState::Aborted => "aborted",
    }
}

fn local_control_node_id(index: usize) -> String {
    format!("{LOCAL_CONTROL_NODE_ID_PREFIX}-{index}")
}

fn local_control_node_ids() -> Vec<String> {
    (1..=LOCAL_CONTROL_REPLICA_COUNT)
        .map(local_control_node_id)
        .collect()
}

fn local_erasure_profile(id: &str) -> Result<LocalErasureProfile> {
    match id {
        "ec-4-2" => Ok(LOCAL_EC_4_2_PROFILE),
        "ec-8-3" => Ok(LOCAL_EC_8_3_PROFILE),
        "replicated-3" => Ok(LOCAL_REPLICATED_3_PROFILE),
        _ => bail!("CoreStore unsupported erasure profile {id}"),
    }
}

fn local_erasure_profile_for_counts(
    profile_id: &str,
    data_shards: usize,
    parity_shards: usize,
) -> Result<LocalErasureProfile> {
    let profile = local_erasure_profile(profile_id)?;
    if profile.data_shards != data_shards || profile.parity_shards != parity_shards {
        bail!(
            "CoreStore erasure profile {} count mismatch: expected {}+{}, got {}+{}",
            profile.id,
            profile.data_shards,
            profile.parity_shards,
            data_shards,
            parity_shards
        );
    }
    Ok(profile)
}

fn plan_local_shard_placements(profile: LocalErasureProfile) -> Result<Vec<LocalShardPlacement>> {
    let placements = (0..profile.total_shards())
        .map(|shard_index| LocalShardPlacement {
            node_id: format!("{LOCAL_NODE_ID_PREFIX}-{}", shard_index + 1),
            region_id: "local".to_string(),
            cell_id: local_cell_id_for_shard(profile, shard_index),
        })
        .collect::<Vec<_>>();
    validate_local_publish_placements(profile, &placements)?;
    Ok(placements)
}

fn local_cell_count_for_profile(profile: LocalErasureProfile) -> usize {
    match profile.id {
        "ec-8-3" => 4,
        _ => 3,
    }
}

fn local_cell_id_for_shard(profile: LocalErasureProfile, shard_index: usize) -> String {
    format!(
        "local-cell-{}",
        (shard_index % local_cell_count_for_profile(profile)) + 1
    )
}

fn validate_local_publish_placements(
    profile: LocalErasureProfile,
    placements: &[LocalShardPlacement],
) -> Result<()> {
    if placements.len() != profile.total_shards() {
        bail!(
            "CoreStore placement for {} expected {} shards, got {}",
            profile.id,
            profile.total_shards(),
            placements.len()
        );
    }
    let unique_nodes = placements
        .iter()
        .map(|placement| placement.node_id.as_str())
        .collect::<BTreeSet<_>>();
    if unique_nodes.len() != placements.len() {
        bail!("CoreStore placement must put at most one shard on each node");
    }
    let mut cell_counts = BTreeMap::<&str, usize>::new();
    for placement in placements {
        *cell_counts.entry(placement.cell_id.as_str()).or_default() += 1;
    }
    match profile.id {
        "ec-4-2" => {
            if cell_counts.len() < 3 || cell_counts.values().any(|count| *count > 2) {
                bail!(
                    "CoreStore ec-4-2 placement requires at least 3 cells and at most 2 shards per cell"
                );
            }
        }
        "ec-8-3" => {
            if cell_counts.len() < 4 || cell_counts.values().any(|count| *count > 3) {
                bail!(
                    "CoreStore ec-8-3 placement requires at least 4 cells and at most 3 shards per cell"
                );
            }
        }
        "replicated-3" => {
            if placements.len() < 3 || unique_nodes.len() < 3 {
                bail!("CoreStore replicated-3 placement requires at least 3 distinct nodes");
            }
        }
        _ => bail!("CoreStore unsupported erasure profile {}", profile.id),
    }
    Ok(())
}

fn boundary_schema_ref_name(bucket: &str) -> String {
    format!("boundary_schema/bucket/{bucket}/current")
}

fn validate_boundary_schema(
    schema: &CoreBoundarySchema,
    current: Option<&CoreBoundarySchema>,
    expected_generation: Option<u64>,
) -> Result<()> {
    if schema.schema != CORE_BOUNDARY_SCHEMA_SCHEMA {
        bail!("CoreStore boundary schema has invalid schema");
    }
    validate_logical_id(&schema.bucket, "boundary schema bucket")?;
    if schema.dimensions.is_empty() {
        bail!("CoreStore boundary schema must include at least one dimension");
    }
    let mut names = BTreeSet::new();
    for dimension in &schema.dimensions {
        validate_boundary_dimension(dimension)?;
        if !names.insert(&dimension.name) {
            bail!(
                "CoreStore boundary schema dimension {} is duplicated",
                dimension.name
            );
        }
    }

    match current {
        Some(current) => {
            if current.bucket != schema.bucket {
                bail!("CoreStore boundary schema bucket mismatch");
            }
            if expected_generation != Some(current.generation) {
                bail!(
                    "{}: CoreStore boundary schema generation conflict",
                    AnvilErrorCode::BoundarySchemaGenerationConflict.as_str()
                );
            }
            if schema.generation != current.generation.saturating_add(1) {
                bail!(
                    "{}: CoreStore boundary schema generation must increment by one",
                    AnvilErrorCode::BoundarySchemaGenerationConflict.as_str()
                );
            }
            validate_boundary_schema_evolution(current, schema)?;
        }
        None => {
            if expected_generation.is_some() || schema.generation != 1 {
                bail!(
                    "{}: CoreStore boundary schema genesis generation must be 1",
                    AnvilErrorCode::BoundarySchemaGenerationConflict.as_str()
                );
            }
        }
    }
    Ok(())
}

fn validate_boundary_dimension(dimension: &CoreBoundaryDimension) -> Result<()> {
    validate_logical_id(&dimension.name, "boundary dimension name")?;
    validate_boundary_value_type(&dimension.value_type)?;
    validate_boundary_source(&dimension.source, &dimension.value_type)?;
    if dimension.categories.is_empty() {
        bail!("CoreStore boundary dimension must include at least one category");
    }
    for category in &dimension.categories {
        validate_boundary_category(category)?;
    }
    validate_boundary_hint(
        &dimension.cardinality,
        &["low", "medium", "high", "extreme"],
        "cardinality",
    )?;
    validate_boundary_hint(
        &dimension.placement_affinity,
        &["none", "prefer_colocate", "prefer_spread"],
        "placement affinity",
    )?;
    validate_boundary_hint(
        &dimension.compaction_scope,
        &["none", "prefer_same_value", "require_same_value"],
        "compaction scope",
    )?;
    if dimension.max_values_per_block == 0 {
        bail!("CoreStore boundary max_values_per_block must be positive");
    }
    if dimension.shared_ranges_allowed && dimension.shared_record_kinds.is_empty() {
        bail!("CoreStore boundary shared ranges must list shared record kinds");
    }
    Ok(())
}

fn validate_boundary_schema_evolution(
    current: &CoreBoundarySchema,
    next: &CoreBoundarySchema,
) -> Result<()> {
    let current_dimensions = current
        .dimensions
        .iter()
        .map(|dimension| (dimension.name.as_str(), dimension))
        .collect::<BTreeMap<_, _>>();
    for dimension in &next.dimensions {
        let Some(existing) = current_dimensions.get(dimension.name.as_str()) else {
            if dimension.required {
                bail!(
                    "{}: CoreStore boundary schema cannot add required dimension {}",
                    AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str(),
                    dimension.name
                );
            }
            continue;
        };
        if existing.value_type != dimension.value_type {
            bail!(
                "{}: CoreStore boundary schema cannot change value type for {}",
                AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str(),
                dimension.name
            );
        }
        if has_boundary_category(existing, "security_realm")
            != has_boundary_category(dimension, "security_realm")
        {
            bail!(
                "{}: CoreStore boundary schema cannot change security_realm category for {}",
                AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str(),
                dimension.name
            );
        }
    }
    Ok(())
}

fn has_boundary_category(dimension: &CoreBoundaryDimension, category: &str) -> bool {
    dimension
        .categories
        .iter()
        .any(|candidate| candidate == category)
}

fn validate_boundary_source(source: &CoreBoundarySource, value_type: &str) -> Result<()> {
    match source {
        CoreBoundarySource::UserMetadataJsonPointer { pointer }
        | CoreBoundarySource::BodyJsonPointer { pointer, .. } => {
            if !pointer.starts_with('/') {
                bail!(
                    "{}: CoreStore boundary JSON pointer must start with /",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                );
            }
        }
        CoreBoundarySource::PathTemplate { template } => validate_boundary_path_template(template)?,
    }
    validate_boundary_value_type(value_type)
}

fn validate_boundary_path_template(template: &str) -> Result<()> {
    if !template.starts_with('/') {
        bail!("CoreStore boundary path template must start with /");
    }
    if template.contains("//") || template.contains("..") {
        bail!("CoreStore boundary path template contains an invalid path component");
    }
    Ok(())
}

fn validate_boundary_value_type(value_type: &str) -> Result<()> {
    validate_boundary_hint(
        value_type,
        &["string", "uuid", "u64", "i64", "date", "timestamp"],
        "value type",
    )
}

fn validate_boundary_category(category: &str) -> Result<()> {
    validate_boundary_hint(
        category,
        &[
            "security_realm",
            "storage_partition",
            "query_prune",
            "placement_affinity",
            "compaction_group",
            "retention_group",
            "observability_group",
        ],
        "category",
    )
}

fn validate_boundary_hint(value: &str, allowed: &[&str], label: &str) -> Result<()> {
    if allowed.contains(&value) {
        Ok(())
    } else {
        bail!("CoreStore boundary {label} {value:?} is not supported")
    }
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

fn parse_stream_cursor(cursor: &str) -> Result<(String, u64)> {
    let (stream_id, sequence) = cursor
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("CoreStore watch cursor is malformed"))?;
    validate_logical_id(stream_id, "watch cursor stream id")?;
    if sequence.len() != 20 || !sequence.as_bytes().iter().all(u8::is_ascii_digit) {
        bail!("CoreStore watch cursor sequence is malformed");
    }
    let sequence = sequence.parse::<u64>()?;
    if sequence == 0 {
        bail!("CoreStore watch cursor sequence must be nonzero");
    }
    Ok((stream_id.to_string(), sequence))
}

fn stream_head_from_records(records: &[StreamRecord]) -> (u64, String) {
    records
        .last()
        .map(|record| (record.sequence, record.event_hash.clone()))
        .unwrap_or_else(|| (0, ZERO_HASH.to_string()))
}

fn is_core_store_temp_entry(name: &std::ffi::OsStr) -> bool {
    name.to_str()
        .is_some_and(|value| value.starts_with('.') || value.ends_with(".tmp"))
}

fn validate_batch_partitions(batch: &CoreMutationBatch) -> Result<()> {
    let mut ref_ops = BTreeSet::new();
    for precondition in &batch.preconditions {
        match precondition {
            CoreMutationPrecondition::Ref { ref_name, .. } => {
                validate_logical_id(ref_name, "precondition ref name")?;
            }
            CoreMutationPrecondition::Fence { fence_name, .. } => {
                validate_logical_id(fence_name, "precondition fence name")?;
            }
            CoreMutationPrecondition::StreamHead {
                stream_id,
                expected_last_event_hash,
                ..
            } => {
                validate_logical_id(stream_id, "precondition stream id")?;
                validate_hash(expected_last_event_hash, "precondition stream head hash")?;
            }
        }
    }
    for operation in &batch.operations {
        match operation {
            CoreMutationOperation::RefUpdate {
                partition_id,
                ref_name,
                ..
            } => {
                validate_logical_id(partition_id, "operation partition id")?;
                validate_logical_id(ref_name, "operation ref name")?;
                if partition_id != &batch.scope_partition {
                    bail!("CrossPartitionAtomicMutationUnsupported");
                }
                if !ref_ops.insert(ref_name) {
                    bail!("CoreStore mutation batch updates ref {ref_name} more than once");
                }
            }
            CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                ..
            } => {
                validate_logical_id(partition_id, "operation partition id")?;
                validate_logical_id(stream_id, "operation stream id")?;
                if partition_id != &batch.scope_partition {
                    bail!("CrossPartitionAtomicMutationUnsupported");
                }
            }
        }
    }
    Ok(())
}

fn ref_precondition_for<'a>(
    preconditions: &'a [CoreMutationPrecondition],
    ref_name: &str,
) -> Option<&'a CoreMutationPrecondition> {
    preconditions.iter().find(|precondition| {
        matches!(
            precondition,
            CoreMutationPrecondition::Ref {
                ref_name: candidate,
                ..
            } if candidate == ref_name
        )
    })
}

fn ref_update_precondition_parts(
    precondition: Option<&CoreMutationPrecondition>,
) -> (
    Option<u64>,
    Option<String>,
    bool,
    bool,
    Option<u64>,
    Option<String>,
    Option<String>,
) {
    match precondition {
        Some(CoreMutationPrecondition::Ref {
            expected_generation,
            expected_target,
            require_absent,
            require_present,
            fence,
            authz_revision,
            source_watch_cursor,
            ..
        }) => (
            *expected_generation,
            expected_target.clone(),
            *require_absent,
            *require_present,
            fence.as_ref().map(|precondition| precondition.fence_token),
            authz_revision.clone(),
            source_watch_cursor.clone(),
        ),
        _ => (None, None, false, false, None, None, None),
    }
}

fn validate_ref_precondition(
    current: Option<&CoreRefValue>,
    ref_name: &str,
    expected_generation: Option<u64>,
    expected_target: Option<&str>,
    require_absent: bool,
    require_present: bool,
) -> Result<()> {
    if require_absent && current.is_some() {
        bail!("CoreStore ref {ref_name} must be absent");
    }
    if require_present && current.is_none() {
        bail!("CoreStore ref {ref_name} must be present");
    }
    if let Some(expected_generation) = expected_generation {
        let actual = current.map(|value| value.generation);
        if actual != Some(expected_generation) {
            bail!(
                "CoreStore ref {ref_name} generation mismatch: expected {}, got {:?}",
                expected_generation,
                actual
            );
        }
    }
    if let Some(expected_target) = expected_target {
        let actual = current.map(|value| value.target.as_str());
        if actual != Some(expected_target) {
            bail!(
                "CoreStore ref {ref_name} target mismatch: expected {}, got {:?}",
                expected_target,
                actual
            );
        }
    }
    Ok(())
}

fn core_fence_ref_name(fence_name: &str) -> String {
    format!("core_fence:{fence_name}")
}

fn root_catalog_ref_name(mesh_id: &str) -> String {
    format!("core_root_catalog:{mesh_id}")
}

fn root_catalog_stream_id(mesh_id: &str) -> String {
    format!("core_root_catalog_history:{mesh_id}")
}

fn quorum_profile_ref_name(placement_group: &str) -> String {
    format!("core_quorum_profile:{placement_group}")
}

fn quorum_profile_stream_id(placement_group: &str) -> String {
    format!("core_quorum_profile_history:{placement_group}")
}

fn ref_update_stream_id(ref_name: &str) -> String {
    format!("core_ref_update:{ref_name}")
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "core-object-ref:{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix("core-object-ref:")
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?)
}

#[cfg(test)]
fn encode_manifest_ref(hash: &str) -> String {
    encode_manifest_ref_with_profile(hash, LOCAL_ERASURE_PROFILE_ID)
}

fn encode_manifest_ref_with_profile(hash: &str, profile_id: &str) -> String {
    format!("core-manifest-sha256:{hash}:profile:{profile_id}")
}

fn decode_manifest_ref(manifest_ref: &str) -> Result<&str> {
    Ok(decode_manifest_ref_parts(manifest_ref)?.0)
}

fn decode_manifest_ref_profile(manifest_ref: &str) -> Result<&str> {
    Ok(decode_manifest_ref_parts(manifest_ref)?.1)
}

fn decode_manifest_ref_parts(manifest_ref: &str) -> Result<(&str, &str)> {
    let raw = manifest_ref
        .strip_prefix("core-manifest-sha256:")
        .ok_or_else(|| anyhow!("CoreStore manifest_ref is not a CoreStore manifest reference"))?;
    let Some((hash, profile)) = raw.split_once(":profile:") else {
        bail!("CoreStore manifest_ref is missing erasure profile");
    };
    if hash.len() != 64 || !hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore manifest_ref hash is invalid");
    }
    validate_logical_id(profile, "manifest erasure profile")?;
    Ok((hash, profile))
}

fn root_catalog_region(catalog: &CoreRootCatalog) -> String {
    catalog
        .root_partitions
        .first()
        .map(|partition| partition.embedded_head_segment_manifest.region_id.clone())
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| "local".to_string())
}

fn validate_root_partition(partition: &CoreRootPartition) -> Result<()> {
    validate_logical_id(&partition.partition_id, "root partition id")?;
    validate_logical_id(&partition.owner_node_id, "root partition owner node id")?;
    validate_logical_id(&partition.placement_group, "root partition placement group")?;
    if partition.fence == 0 {
        bail!("CoreStore root partition fence must be nonzero");
    }
    if partition.embedded_head_segment_manifest.schema != CORE_OBJECT_MANIFEST_SCHEMA {
        bail!("CoreStore root partition embedded manifest has invalid schema");
    }
    if partition
        .embedded_head_segment_manifest
        .placements
        .is_empty()
    {
        bail!("CoreStore root partition embedded manifest must include placements");
    }
    Ok(())
}

fn validate_quorum_profile(profile: &CoreQuorumProfile) -> Result<()> {
    validate_logical_id(&profile.placement_group, "placement group")?;
    if profile.schema != CORE_QUORUM_PROFILE_SCHEMA {
        bail!("CoreStore quorum profile has invalid schema");
    }
    if profile.epoch == 0 {
        bail!("CoreStore quorum profile epoch must be nonzero");
    }
    if profile.replica_count == 0 {
        bail!("CoreStore quorum profile replica_count must be nonzero");
    }
    validate_quorum_member("write_quorum", profile.write_quorum, profile.replica_count)?;
    validate_quorum_member("read_quorum", profile.read_quorum, profile.replica_count)?;
    validate_quorum_member("fence_quorum", profile.fence_quorum, profile.replica_count)?;
    require_quorum_intersection(
        "read_quorum",
        profile.read_quorum,
        "write_quorum",
        profile.write_quorum,
        profile.replica_count,
    )?;
    require_quorum_intersection(
        "fence_quorum",
        profile.fence_quorum,
        "write_quorum",
        profile.write_quorum,
        profile.replica_count,
    )?;
    require_quorum_intersection(
        "fence_quorum",
        profile.fence_quorum,
        "read_quorum",
        profile.read_quorum,
        profile.replica_count,
    )?;
    Ok(())
}

fn validate_quorum_member(label: &str, value: u16, replica_count: u16) -> Result<()> {
    if value == 0 {
        bail!("CoreStore quorum profile {label} must be nonzero");
    }
    if value > replica_count {
        bail!(
            "CoreStore quorum profile {label} {} exceeds replica_count {}",
            value,
            replica_count
        );
    }
    Ok(())
}

fn require_quorum_intersection(
    left_label: &str,
    left: u16,
    right_label: &str,
    right: u16,
    replica_count: u16,
) -> Result<()> {
    if u32::from(left) + u32::from(right) <= u32::from(replica_count) {
        bail!(
            "CoreStore quorum profile {left_label}/{right_label} do not intersect for replica_count {}",
            replica_count
        );
    }
    Ok(())
}

fn hash_root_catalog(catalog: &CoreRootCatalog) -> Result<String> {
    let mut unsigned = catalog.clone();
    unsigned.signature.clear();
    Ok(format!(
        "sha256:{}",
        sha256_hex(&serde_json::to_vec(&unsigned)?)
    ))
}

fn sign_root_catalog(signing_key: &[u8], catalog: &CoreRootCatalog) -> Result<String> {
    if signing_key.is_empty() {
        bail!("CoreStore root catalog signing key must not be empty");
    }
    let hash = hash_root_catalog(catalog)?;
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"core_root_catalog");
    mac.update(catalog.mesh_id.as_bytes());
    mac.update(&catalog.generation.to_le_bytes());
    mac.update(catalog.previous_hash.as_bytes());
    mac.update(catalog.signed_by.as_bytes());
    mac.update(hash.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()))
}

fn verify_root_catalog(catalog: &CoreRootCatalog, signing_key: &[u8]) -> Result<()> {
    if catalog.schema != CORE_ROOT_CATALOG_SCHEMA {
        bail!("CoreStore root catalog has invalid schema");
    }
    if catalog.signature.is_empty() {
        bail!("CoreStore root catalog signature must not be empty");
    }
    if catalog.root_partitions.is_empty() {
        bail!("CoreStore root catalog must include root partitions");
    }
    for partition in &catalog.root_partitions {
        validate_root_partition(partition)?;
    }
    let expected = sign_root_catalog(signing_key, catalog)?;
    if catalog.signature != expected {
        bail!("CoreStore root catalog signature mismatch");
    }
    Ok(())
}

async fn read_core_fence_record(
    store: &CoreStore,
    ref_value: &CoreRefValue,
) -> Result<CoreFenceRecord> {
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    let record: CoreFenceRecord = serde_json::from_slice(&bytes)?;
    if record.schema != CORE_FENCE_SCHEMA {
        bail!("CoreStore fence record has invalid schema");
    }
    Ok(record)
}

fn event_hash_input(record: &StreamRecord) -> Result<Vec<u8>> {
    #[derive(Serialize)]
    struct HashInput<'a> {
        schema: &'a str,
        stream_id: &'a str,
        partition_id: &'a str,
        sequence: u64,
        cursor: &'a str,
        previous_event_hash: &'a str,
        record_kind: &'a str,
        payload_hash: &'a str,
        transaction_id: &'a Option<String>,
        idempotency_key_hash: &'a Option<String>,
        created_at: &'a str,
    }
    Ok(serde_json::to_vec(&HashInput {
        schema: &record.schema,
        stream_id: &record.stream_id,
        partition_id: &record.partition_id,
        sequence: record.sequence,
        cursor: &record.cursor,
        previous_event_hash: &record.previous_event_hash,
        record_kind: &record.record_kind,
        payload_hash: &record.payload_hash,
        transaction_id: &record.transaction_id,
        idempotency_key_hash: &record.idempotency_key_hash,
        created_at: &record.created_at,
    })?)
}

fn verify_stream_record(previous: Option<&StreamRecord>, record: &StreamRecord) -> Result<()> {
    let expected_sequence = previous.map(|prev| prev.sequence + 1).unwrap_or(1);
    if record.sequence != expected_sequence {
        bail!(
            "CoreStore stream {} has sequence gap: expected {}, got {}",
            record.stream_id,
            expected_sequence,
            record.sequence
        );
    }
    let expected_previous = previous
        .map(|prev| prev.event_hash.clone())
        .unwrap_or_else(|| {
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string()
        });
    if record.previous_event_hash != expected_previous {
        bail!("CoreStore stream {} hash chain mismatch", record.stream_id);
    }
    let expected_hash = format!("sha256:{}", sha256_hex(&event_hash_input(record)?));
    if record.event_hash != expected_hash {
        bail!("CoreStore stream {} event hash mismatch", record.stream_id);
    }
    Ok(())
}

fn encode_active_stream_records(stream_id: &str, records: &[StreamRecord]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CORE_ACTIVE_STREAM_MAGIC);
    bytes.extend_from_slice(&CORE_ACTIVE_STREAM_VERSION.to_le_bytes());
    write_u32_le(&mut bytes, stream_id.len())?;
    bytes.extend_from_slice(stream_id.as_bytes());
    bytes.extend_from_slice(&(records.len() as u64).to_le_bytes());

    for record in records {
        if record.stream_id != stream_id {
            bail!("CoreStore active stream record stream_id mismatch");
        }
        let stored = StoredStreamRecord::from(record);
        let record_json = serde_json::to_vec(&stored)?;
        write_u32_le(&mut bytes, record_json.len())?;
        bytes.extend_from_slice(&record_json);
        bytes.extend_from_slice(&crc32c(&record_json).to_le_bytes());
    }

    let stream_hash = Sha256::digest(&bytes);
    bytes.extend_from_slice(&stream_hash);
    Ok(bytes)
}

fn decode_active_stream_id(bytes: &[u8]) -> Result<String> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_ACTIVE_STREAM_MAGIC.len())?;
    if magic != CORE_ACTIVE_STREAM_MAGIC {
        bail!("CoreStore active stream has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_ACTIVE_STREAM_VERSION {
        bail!("CoreStore active stream has unsupported version {version}");
    }
    let encoded_stream_id_len = read_u32_le(bytes, &mut offset)? as usize;
    let encoded_stream_id = read_exact(bytes, &mut offset, encoded_stream_id_len)?;
    Ok(std::str::from_utf8(encoded_stream_id)
        .context("decode CoreStore active stream id as utf-8")?
        .to_string())
}

fn decode_active_stream_records(stream_id: &str, bytes: &[u8]) -> Result<Vec<StreamRecord>> {
    let (mut offset, record_count) = decode_active_stream_header(stream_id, bytes)?;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record = decode_active_stream_record(stream_id, bytes, &mut offset, records.last())?;
        records.push(record);
    }

    let stream_hash_start = offset;
    let stream_hash = read_exact(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        bail!("CoreStore active stream has trailing bytes");
    }
    let actual_stream_hash = Sha256::digest(&bytes[..stream_hash_start]);
    let actual_stream_hash: &[u8] = actual_stream_hash.as_ref();
    if stream_hash != actual_stream_hash {
        bail!("CoreStore active stream hash mismatch");
    }
    Ok(records)
}

fn decode_active_stream_records_page(
    stream_id: &str,
    bytes: &[u8],
    after_sequence: u64,
    limit: usize,
) -> Result<Vec<StreamRecord>> {
    validate_active_stream_hash(bytes)?;
    let (mut offset, record_count) = decode_active_stream_header(stream_id, bytes)?;
    let mut records = Vec::with_capacity(limit.min(record_count as usize));
    let mut previous = None;
    for _ in 0..record_count {
        if limit > 0 && records.len() >= limit {
            break;
        }
        let record = decode_active_stream_record(stream_id, bytes, &mut offset, previous.as_ref())?;
        previous = Some(record.clone());
        if record.sequence > after_sequence {
            records.push(record);
        }
    }
    Ok(records)
}

fn decode_active_stream_header(stream_id: &str, bytes: &[u8]) -> Result<(usize, u64)> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_ACTIVE_STREAM_MAGIC.len())?;
    if magic != CORE_ACTIVE_STREAM_MAGIC {
        bail!("CoreStore active stream has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_ACTIVE_STREAM_VERSION {
        bail!("CoreStore active stream has unsupported version {version}");
    }
    let encoded_stream_id_len = read_u32_le(bytes, &mut offset)? as usize;
    let encoded_stream_id = read_exact(bytes, &mut offset, encoded_stream_id_len)?;
    if encoded_stream_id != stream_id.as_bytes() {
        bail!("CoreStore active stream id mismatch");
    }

    let record_count = read_u64_le(bytes, &mut offset)?;
    Ok((offset, record_count))
}

fn decode_active_stream_record(
    stream_id: &str,
    bytes: &[u8],
    offset: &mut usize,
    previous: Option<&StreamRecord>,
) -> Result<StreamRecord> {
    let record_json_len = read_u32_le(bytes, offset)? as usize;
    let record_json = read_exact(bytes, offset, record_json_len)?;
    let expected_crc = read_u32_le(bytes, offset)?;
    let actual_crc = crc32c(record_json);
    if actual_crc != expected_crc {
        bail!("CoreStore active stream record checksum mismatch");
    }
    let stored: StoredStreamRecord = serde_json::from_slice(record_json)?;
    let record = StreamRecord::from(stored);
    if record.stream_id != stream_id {
        bail!("CoreStore active stream record scope mismatch");
    }
    verify_stream_record(previous, &record)?;
    Ok(record)
}

fn validate_active_stream_hash(bytes: &[u8]) -> Result<()> {
    if bytes.len() < 32 {
        bail!("CoreStore active stream is too short for hash");
    }
    let stream_hash_start = bytes.len() - 32;
    let stream_hash = &bytes[stream_hash_start..];
    let actual_stream_hash = Sha256::digest(&bytes[..stream_hash_start]);
    let actual_stream_hash: &[u8] = actual_stream_hash.as_ref();
    if stream_hash != actual_stream_hash {
        bail!("CoreStore active stream hash mismatch");
    }
    Ok(())
}

fn encode_stream_segment(
    input: &SealStreamSegment,
    records: &[StreamRecord],
    segment_id: &str,
    first_sequence: u64,
    last_sequence: u64,
) -> Result<Vec<u8>> {
    let sealed_at = now_rfc3339();
    let created_at = records
        .first()
        .map(|record| record.created_at.clone())
        .unwrap_or_else(|| sealed_at.clone());
    let header = StoredStreamSegmentHeader {
        schema: CORE_STREAM_SEGMENT_HEADER_SCHEMA.to_string(),
        stream_id: input.stream_id.clone(),
        partition_id: input.partition_id.clone(),
        segment_id: segment_id.to_string(),
        first_sequence,
        last_sequence,
        source_family: input.segment_kind.clone(),
        created_at,
        sealed_at: sealed_at.clone(),
    };

    let mut bytes = Vec::new();
    bytes.extend_from_slice(CORE_STREAM_SEGMENT_MAGIC);
    bytes.extend_from_slice(&CORE_STREAM_SEGMENT_VERSION.to_le_bytes());
    let header_json = serde_json::to_vec(&header)?;
    write_u32_le(&mut bytes, header_json.len())?;
    bytes.extend_from_slice(&header_json);
    bytes.extend_from_slice(&(records.len() as u64).to_le_bytes());

    for record in records {
        let record_header = StoredStreamRecordHeader {
            schema: CORE_STREAM_RECORD_HEADER_SCHEMA.to_string(),
            stream_id: record.stream_id.clone(),
            sequence: record.sequence,
            record_kind: record.record_kind.clone(),
            payload_hash: record.payload_hash.clone(),
            payload_content_type: "application/octet-stream".to_string(),
            mutation_id: record
                .transaction_id
                .clone()
                .unwrap_or_else(|| record.cursor.clone()),
            idempotency_key_hash: record.idempotency_key_hash.clone(),
            previous_event_hash: record.previous_event_hash.clone(),
            event_hash: record.event_hash.clone(),
            transaction_id: record.transaction_id.clone(),
            created_at: record.created_at.clone(),
        };
        let record_header_json = serde_json::to_vec(&record_header)?;
        write_u32_le(&mut bytes, record_header_json.len())?;
        bytes.extend_from_slice(&record_header_json);
        bytes.extend_from_slice(&(record.payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&record.payload);
        let mut checksum_bytes =
            Vec::with_capacity(record_header_json.len() + record.payload.len());
        checksum_bytes.extend_from_slice(&record_header_json);
        checksum_bytes.extend_from_slice(&record.payload);
        bytes.extend_from_slice(&crc32c(&checksum_bytes).to_le_bytes());
    }

    let payload_hash = format!("sha256:{}", sha256_hex(&bytes));
    let trailer = StoredStreamSegmentTrailer {
        schema: CORE_STREAM_SEGMENT_TRAILER_SCHEMA.to_string(),
        stream_id: input.stream_id.clone(),
        partition_id: input.partition_id.clone(),
        segment_id: segment_id.to_string(),
        first_sequence,
        last_sequence,
        record_count: records.len() as u64,
        payload_hash,
        sealed_at,
    };
    let trailer_json = serde_json::to_vec(&trailer)?;
    write_u32_le(&mut bytes, trailer_json.len())?;
    bytes.extend_from_slice(&trailer_json);
    let segment_hash = Sha256::digest(&bytes);
    bytes.extend_from_slice(&segment_hash);
    Ok(bytes)
}

fn decode_stream_segment(bytes: &[u8]) -> Result<Vec<StreamRecord>> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_STREAM_SEGMENT_MAGIC.len())?;
    if magic != CORE_STREAM_SEGMENT_MAGIC {
        bail!("CoreStore stream segment has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_STREAM_SEGMENT_VERSION {
        bail!("CoreStore stream segment has unsupported version {version}");
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let header_json = read_exact(bytes, &mut offset, header_len)?;
    let header: StoredStreamSegmentHeader = serde_json::from_slice(header_json)?;
    if header.schema != CORE_STREAM_SEGMENT_HEADER_SCHEMA {
        bail!("CoreStore stream segment header has invalid schema");
    }
    let record_count = read_u64_le(bytes, &mut offset)?;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record_header_len = read_u32_le(bytes, &mut offset)? as usize;
        let record_header_json = read_exact(bytes, &mut offset, record_header_len)?;
        let record_header: StoredStreamRecordHeader = serde_json::from_slice(record_header_json)?;
        if record_header.schema != CORE_STREAM_RECORD_HEADER_SCHEMA {
            bail!("CoreStore stream segment record header has invalid schema");
        }
        let payload_len = read_u64_le(bytes, &mut offset)? as usize;
        let payload = read_exact(bytes, &mut offset, payload_len)?.to_vec();
        let expected_crc = read_u32_le(bytes, &mut offset)?;
        let mut checksum_bytes = Vec::with_capacity(record_header_json.len() + payload.len());
        checksum_bytes.extend_from_slice(record_header_json);
        checksum_bytes.extend_from_slice(&payload);
        let actual_crc = crc32c(&checksum_bytes);
        if actual_crc != expected_crc {
            bail!("CoreStore stream segment record checksum mismatch");
        }
        if record_header.stream_id != header.stream_id {
            bail!("CoreStore stream segment record stream_id mismatch");
        }
        let record = StreamRecord {
            schema: CORE_WATCH_EVENT_SCHEMA.to_string(),
            stream_id: record_header.stream_id,
            partition_id: header.partition_id.clone(),
            sequence: record_header.sequence,
            cursor: format!("{}:{:020}", header.stream_id, record_header.sequence),
            previous_event_hash: record_header.previous_event_hash,
            event_hash: record_header.event_hash,
            record_kind: record_header.record_kind,
            payload_hash: record_header.payload_hash,
            payload,
            transaction_id: record_header.transaction_id,
            idempotency_key_hash: record_header.idempotency_key_hash,
            created_at: record_header.created_at,
        };
        verify_stream_record(records.last(), &record)?;
        records.push(record);
    }
    let trailer_len_start = offset;
    let trailer_len = read_u32_le(bytes, &mut offset)? as usize;
    let trailer_json = read_exact(bytes, &mut offset, trailer_len)?;
    let trailer: StoredStreamSegmentTrailer = serde_json::from_slice(trailer_json)?;
    if trailer.schema != CORE_STREAM_SEGMENT_TRAILER_SCHEMA {
        bail!("CoreStore stream segment trailer has invalid schema");
    }
    if trailer.stream_id != header.stream_id
        || trailer.partition_id != header.partition_id
        || trailer.segment_id != header.segment_id
        || trailer.first_sequence != header.first_sequence
        || trailer.last_sequence != header.last_sequence
        || trailer.record_count != record_count
    {
        bail!("CoreStore stream segment trailer scope mismatch");
    }
    let segment_hash = read_exact(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        bail!("CoreStore stream segment has trailing bytes");
    }
    let actual_segment_hash = Sha256::digest(&bytes[..trailer_len_start + 4 + trailer_len]);
    let actual_segment_hash: &[u8] = actual_segment_hash.as_ref();
    if segment_hash != actual_segment_hash {
        bail!("CoreStore stream segment hash mismatch");
    }
    if records
        .first()
        .map(|record| record.sequence)
        .unwrap_or_default()
        != header.first_sequence
    {
        bail!("CoreStore stream segment header first_sequence mismatch");
    }
    if records
        .last()
        .map(|record| record.sequence)
        .unwrap_or_default()
        != header.last_sequence
    {
        bail!("CoreStore stream segment header last_sequence mismatch");
    }
    Ok(records)
}

fn write_u32_le(out: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| anyhow!("CoreStore frame length exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn encode_wal_file_header(node_id: &str, wal_epoch: u64, first_sequence: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CORE_WAL_FILE_MAGIC);
    bytes.extend_from_slice(&CORE_WAL_VERSION.to_le_bytes());
    write_u16_len_prefixed_bytes(&mut bytes, node_id.as_bytes(), "wal node id")?;
    bytes.extend_from_slice(&wal_epoch.to_le_bytes());
    bytes.extend_from_slice(&first_sequence.to_le_bytes());
    bytes.extend_from_slice(&unix_timestamp_nanos().to_le_bytes());
    let crc = crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn encode_wal_frame(header_json: &[u8], payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > CORE_WAL_MAX_INLINE_PAYLOAD_BYTES {
        bail!(
            "CoreStore WAL payload exceeds {} bytes",
            CORE_WAL_MAX_INLINE_PAYLOAD_BYTES
        );
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CORE_WAL_FRAME_MAGIC);
    write_u32_le(&mut bytes, header_json.len())?;
    bytes.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    bytes.extend_from_slice(header_json);
    bytes.extend_from_slice(payload);
    let crc = crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn decode_wal_records(bytes: &[u8]) -> Result<Vec<(CoreWalAdmissionRecord, Vec<u8>)>> {
    decode_wal_file(bytes).map(|(_, records)| records)
}

fn decode_wal_file(bytes: &[u8]) -> Result<(u64, Vec<(CoreWalAdmissionRecord, Vec<u8>)>)> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_WAL_FILE_MAGIC.len())?;
    if magic != CORE_WAL_FILE_MAGIC {
        bail!("CoreStore WAL has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_WAL_VERSION {
        bail!("CoreStore WAL has unsupported version {version}");
    }
    let node_id_len = read_u16_le(bytes, &mut offset)? as usize;
    let _node_id = read_exact(bytes, &mut offset, node_id_len)?;
    let _wal_epoch = read_u64_le(bytes, &mut offset)?;
    let first_sequence = read_u64_le(bytes, &mut offset)?;
    let _created_at = read_u64_le(bytes, &mut offset)?;
    let expected_header_crc = read_u32_le(bytes, &mut offset)?;
    let actual_header_crc = crc32c(&bytes[..offset - 4]);
    if expected_header_crc != actual_header_crc {
        bail!("CoreStore WAL header checksum mismatch");
    }

    let mut records = Vec::new();
    while offset < bytes.len() {
        let frame_start = offset;
        if bytes.len().saturating_sub(offset) < CORE_WAL_FRAME_MAGIC.len() {
            break;
        }
        let frame_magic = read_exact(bytes, &mut offset, CORE_WAL_FRAME_MAGIC.len())?;
        if frame_magic != CORE_WAL_FRAME_MAGIC {
            bail!("CoreStore WAL frame has invalid magic");
        }
        if bytes.len().saturating_sub(offset) < 12 {
            break;
        }
        let header_len = read_u32_le(bytes, &mut offset)? as usize;
        let payload_len = read_u64_le(bytes, &mut offset)? as usize;
        if payload_len > CORE_WAL_MAX_INLINE_PAYLOAD_BYTES {
            bail!("CoreStore WAL frame payload exceeds inline limit");
        }
        let required_tail = header_len
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(4))
            .ok_or_else(|| anyhow!("CoreStore WAL frame length overflow"))?;
        if bytes.len().saturating_sub(offset) < required_tail {
            break;
        }
        let header_json = read_exact(bytes, &mut offset, header_len)?;
        let payload = read_exact(bytes, &mut offset, payload_len)?;
        let expected_crc = read_u32_le(bytes, &mut offset)?;
        let actual_crc = crc32c(&bytes[frame_start..offset - 4]);
        if expected_crc != actual_crc {
            bail!("CoreStore WAL frame checksum mismatch");
        }
        let record: CoreWalAdmissionRecord = serde_json::from_slice(header_json)?;
        if record.schema != CORE_WAL_RECORD_SCHEMA {
            bail!("CoreStore WAL record has invalid schema");
        }
        records.push((record, payload.to_vec()));
    }
    Ok((first_sequence, records))
}

fn write_u16_len_prefixed_bytes(out: &mut Vec<u8>, bytes: &[u8], label: &str) -> Result<()> {
    let len = u16::try_from(bytes.len()).map_err(|_| anyhow!("CoreStore {label} exceeds u16"))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn unix_timestamp_nanos() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    u64::try_from(now.as_nanos()).unwrap_or(u64::MAX)
}

fn json_required_string(value: &serde_json::Value, field: &str) -> Result<String> {
    value
        .get(field)
        .and_then(|value| value.as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("CoreStore WAL target is missing string field {field}"))
}

fn json_optional_string(value: &serde_json::Value, field: &str) -> Result<Option<String>> {
    match value.get(field) {
        Some(serde_json::Value::String(value)) => Ok(Some(value.clone())),
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(_) => bail!("CoreStore WAL target field {field} must be a string or null"),
    }
}

fn json_optional_u64(value: &serde_json::Value, field: &str) -> Result<Option<u64>> {
    match value.get(field) {
        Some(serde_json::Value::Number(value)) => value.as_u64().map(Some).ok_or_else(|| {
            anyhow!("CoreStore WAL target field {field} must be an unsigned integer")
        }),
        Some(serde_json::Value::Null) | None => Ok(None),
        Some(_) => bail!("CoreStore WAL target field {field} must be an unsigned integer or null"),
    }
}

fn json_required_bool(value: &serde_json::Value, field: &str) -> Result<bool> {
    value
        .get(field)
        .and_then(|value| value.as_bool())
        .ok_or_else(|| anyhow!("CoreStore WAL target is missing boolean field {field}"))
}

fn read_exact<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow!("CoreStore frame offset overflow"))?;
    if end > bytes.len() {
        bail!("CoreStore frame ended unexpectedly");
    }
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
}

fn read_u16_le(bytes: &[u8], offset: &mut usize) -> Result<u16> {
    let raw = read_exact(bytes, offset, 2)?;
    Ok(u16::from_le_bytes(raw.try_into()?))
}

fn read_u32_le(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let raw = read_exact(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(raw.try_into()?))
}

fn read_u64_le(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let raw = read_exact(bytes, offset, 8)?;
    Ok(u64::from_le_bytes(raw.try_into()?))
}

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

async fn read_file(
    path: &PathBuf,
    component: &'static str,
    operation: &'static str,
) -> std::io::Result<Vec<u8>> {
    let started_at = Instant::now();
    let result = fs::read(path).await;
    let bytes = result.as_ref().map(|bytes| bytes.len() as u64).unwrap_or(0);
    crate::perf::record_io_duration(component, operation, path, bytes, started_at.elapsed());
    result
}

fn is_not_found_error(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<std::io::Error>()
        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
}

async fn write_file_atomic(path: &PathBuf, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        let started_at = Instant::now();
        fs::create_dir_all(parent).await?;
        crate::perf::record_io_duration(
            "core_store",
            "create_dir_all",
            parent,
            0,
            started_at.elapsed(),
        );
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("CoreStore atomic write path has no file name"))?;
    let tmp_path = path.with_file_name(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));
    let started_at = Instant::now();
    let mut file = fs::File::create(&tmp_path).await?;
    crate::perf::record_io_duration(
        "core_store",
        "file_create",
        &tmp_path,
        0,
        started_at.elapsed(),
    );
    let started_at = Instant::now();
    file.write_all(bytes).await?;
    crate::perf::record_io_duration(
        "core_store",
        "write_all",
        &tmp_path,
        bytes.len() as u64,
        started_at.elapsed(),
    );
    let started_at = Instant::now();
    file.sync_all().await?;
    crate::perf::record_io_duration(
        "core_store",
        "sync_all",
        &tmp_path,
        bytes.len() as u64,
        started_at.elapsed(),
    );
    drop(file);
    let started_at = Instant::now();
    let rename_result = fs::rename(&tmp_path, path).await;
    crate::perf::record_io_duration(
        "core_store",
        "rename",
        path,
        bytes.len() as u64,
        started_at.elapsed(),
    );
    if let Err(err) = rename_result {
        let started_at = Instant::now();
        let _ = fs::remove_file(&tmp_path).await;
        crate::perf::record_io_duration(
            "core_store",
            "remove_temp_after_failed_rename",
            &tmp_path,
            bytes.len() as u64,
            started_at.elapsed(),
        );
        return Err(err).with_context(|| {
            format!(
                "commit CoreStore atomic write {} -> {}",
                tmp_path.display(),
                path.display()
            )
        });
    }
    sync_parent_dir(path, "atomic_write_sync_parent_dir").await?;
    Ok(())
}

async fn sync_parent_dir(path: &PathBuf, operation: &'static str) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let parent = parent.to_path_buf();
    let started_at = Instant::now();
    tokio::task::spawn_blocking({
        let parent = parent.clone();
        move || -> std::io::Result<()> {
            let dir = std::fs::File::open(&parent)?;
            dir.sync_all()
        }
    })
    .await
    .map_err(|err| anyhow!("CoreStore directory fsync task failed: {err}"))??;
    crate::perf::record_io_duration("core_store", operation, &parent, 0, started_at.elapsed());
    Ok(())
}

async fn sum_files_with_extension(root: &PathBuf, extensions: &[&str]) -> Result<u64> {
    let mut total = 0_u64;
    let mut pending = vec![root.clone()];

    while let Some(path) = pending.pop() {
        let mut entries = match fs::read_dir(&path).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("read directory {}", path.display()));
            }
        };

        while let Some(entry) = entries.next_entry().await? {
            let entry_path = entry.path();
            let metadata = match entry.metadata().await {
                Ok(metadata) => metadata,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read metadata for {}", entry_path.display()));
                }
            };
            if metadata.is_dir() {
                pending.push(entry_path);
                continue;
            }
            if !metadata.is_file() {
                continue;
            }
            let Some(extension) = entry_path.extension().and_then(|value| value.to_str()) else {
                continue;
            };
            if extensions.contains(&extension) {
                total = total.saturating_add(metadata.len());
            }
        }
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_wal_record(
        mutation_id: &str,
        created_at_unix_nanos: u64,
        sequence: u64,
    ) -> CoreWalAdmissionRecord {
        CoreWalAdmissionRecord {
            schema: CORE_WAL_RECORD_SCHEMA.to_string(),
            node_id: CORE_WAL_NODE_ID.to_string(),
            wal_epoch: CORE_WAL_EPOCH,
            sequence,
            mutation_id: mutation_id.to_string(),
            idempotency_key_hash: None,
            anvil_storage_tenant_id: "local".to_string(),
            authz_scope: serde_json::json!({"realm_id":"system","revision":null}),
            operation_family: "test.operation".to_string(),
            writer_family: "test_writer".to_string(),
            target: serde_json::json!({"target":"test"}),
            preconditions: serde_json::json!([]),
            boundary_values: Vec::new(),
            landed_bytes: Vec::new(),
            created_at_unix_nanos,
        }
    }

    fn test_object_ref_for_payload(bytes: &[u8], profile: LocalErasureProfile) -> CoreObjectRef {
        let hash = sha256_hex(bytes);
        let shards = encode_erasure_shards(bytes, profile).unwrap();
        let placements = plan_local_shard_placements(profile).unwrap();
        let mut object_placements = Vec::new();
        let mut stripe_size = 0u64;
        for (shard_index, shard) in shards.iter().enumerate() {
            let shard_hash = sha256_hex(shard);
            let placement = &placements[shard_index];
            stripe_size =
                stripe_size.max((shard.len() as u64).saturating_mul(profile.data_shards as u64));
            object_placements.push(CoreObjectPlacement {
                shard_index: shard_index as u16,
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_hash: format!("sha256:{shard_hash}"),
                stored_size: shard.len() as u64,
                generation: 1,
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            });
        }
        CoreObjectRef {
            hash: format!("sha256:{hash}"),
            logical_size: bytes.len() as u64,
            manifest_ref: encode_manifest_ref(&hash),
            encoding: CoreObjectEncoding {
                profile_id: profile.id.to_string(),
                data_shards: profile.data_shards as u16,
                parity_shards: profile.parity_shards as u16,
                minimum_read_shards: profile.minimum_read_shards as u16,
                minimum_write_ack_shards: profile.minimum_write_ack_shards as u16,
                stripe_size,
                placement_scope: "region".to_string(),
                repair_priority: "normal".to_string(),
                encryption: "none".to_string(),
            },
            placements: object_placements,
        }
    }

    async fn write_test_wal_records(store: &CoreStore, records: Vec<CoreWalAdmissionRecord>) {
        fs::create_dir_all(store.admission_wal_dir()).await.unwrap();
        let mut bytes = encode_wal_file_header(CORE_WAL_NODE_ID, CORE_WAL_EPOCH, 1).unwrap();
        for record in records {
            let header = serde_json::to_vec(&record).unwrap();
            bytes.extend_from_slice(&encode_wal_frame(&header, &[]).unwrap());
        }
        fs::write(store.active_wal_path(), bytes).await.unwrap();
    }

    fn sample_boundary_schema(bucket: &str, generation: u64) -> CoreBoundarySchema {
        CoreBoundarySchema {
            schema: CORE_BOUNDARY_SCHEMA_SCHEMA.to_string(),
            bucket: bucket.to_string(),
            generation,
            dimensions: vec![CoreBoundaryDimension {
                name: "customer_tenant".to_string(),
                source: CoreBoundarySource::UserMetadataJsonPointer {
                    pointer: "/customer_tenant_id".to_string(),
                },
                value_type: "uuid".to_string(),
                categories: vec![
                    "security_realm".to_string(),
                    "storage_partition".to_string(),
                    "query_prune".to_string(),
                ],
                required: true,
                cardinality: "extreme".to_string(),
                max_values_per_block: 1,
                placement_affinity: "prefer_colocate".to_string(),
                compaction_scope: "require_same_value".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
                deprecated: false,
            }],
            created_at: String::new(),
        }
    }

    #[tokio::test]
    async fn core_store_put_get_blob_verifies_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let object_ref = store
            .put_blob(PutBlob {
                logical_name: "tenant:t/bucket:b/object:a".to_string(),
                bytes: b"hello corestore".to_vec(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "mut-1".to_string(),
            })
            .await
            .unwrap();
        let wal_bytes = tokio::fs::read(store.active_wal_path()).await.unwrap();
        let wal_records = decode_wal_records(&wal_bytes).unwrap();
        assert!(
            wal_records.is_empty(),
            "finalised put_blob records must be checkpointed out of the active WAL"
        );
        assert_eq!(
            store.admission_landed_bytes().await.unwrap(),
            0,
            "finalised put_blob landed bytes must be reclaimed after CoreStore shards are durable"
        );
        let bytes = store.get_blob(GetBlob { object_ref }).await.unwrap();
        assert_eq!(bytes, b"hello corestore");
    }

    #[tokio::test]
    async fn core_store_range_read_does_not_require_unrelated_data_shards() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let payload = b"aaaabbbbccccdddd".to_vec();
        let object_ref = store
            .put_blob(PutBlob {
                logical_name: "tenant:t/bucket:b/object:range".to_string(),
                bytes: payload.clone(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "mut-range-1".to_string(),
            })
            .await
            .unwrap();
        let manifest = store.read_object_manifest(&object_ref).await.unwrap();
        let object_hash = object_ref.hash.strip_prefix("sha256:").unwrap();

        for placement in manifest
            .placements
            .iter()
            .filter(|placement| (1..LOCAL_DATA_SHARDS as u16).contains(&placement.shard_index))
        {
            let shard_hash = placement.shard_hash.strip_prefix("sha256:").unwrap();
            let shard_path = store.shard_path(
                &placement.node_id,
                object_hash,
                placement.shard_index,
                shard_hash,
            );
            fs::write(&shard_path, vec![0xee; placement.stored_size as usize])
                .await
                .unwrap();
        }

        let range = store
            .get_blob_range(GetBlobRange {
                object_ref: object_ref.clone(),
                range: CoreByteRange {
                    start: 1,
                    end_exclusive: 3,
                },
            })
            .await
            .unwrap();
        assert_eq!(range, b"aa");
        assert!(
            store.get_blob(GetBlob { object_ref }).await.is_err(),
            "a full read must fail after unrelated data shards are corrupted; the range read above proves it did not materialise the full object"
        );
    }

    #[tokio::test]
    async fn core_store_logical_file_api_writes_verifies_and_reads_ranges() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let payload = b"alpha beta gamma delta epsilon zeta".to_vec();
        let boundary = CoreBoundaryValue {
            schema_generation: 7,
            name: "customer_tenant".to_string(),
            value_type: "string".to_string(),
            value: "tenant-a".to_string(),
            categories: vec!["query_pruning".to_string()],
            source_kind: "user_metadata".to_string(),
            required: true,
        };
        let manifest = store
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "full_text".to_string(),
                generation: 3,
                logical_file_id: "index/full-text/main/segment-3".to_string(),
                source: payload.clone(),
                range_hints: vec![CoreLogicalRangeHint {
                    range_id: "postings-a".to_string(),
                    byte_start: 6,
                    byte_end: 16,
                    writer_record_kind: "postings".to_string(),
                    boundary_values: vec![boundary.clone()],
                    writer_statistics: Vec::new(),
                    preferred_block_boundary: "preferred".to_string(),
                    boundary_dimension_ids: vec![1],
                    prefetch_next_range_ids: vec!["postings-b".to_string()],
                    shared_range: None,
                }],
                pipeline_policy: CorePipelinePolicy::default(),
                trace_context: CoreTraceContext::default(),
                boundary_values: vec![boundary.clone()],
                mutation_id: "logical-file-api-mut-1".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(manifest.schema, CORE_LOGICAL_FILE_MANIFEST_SCHEMA);
        assert_eq!(manifest.writer_family, "full_text");
        assert_eq!(manifest.writer_generation, 3);
        assert_eq!(manifest.boundary_schema_generation, 7);
        assert_eq!(manifest.blocks.len(), 1);
        assert_eq!(manifest.ranges[0].preferred_block_boundary, "preferred");
        assert_eq!(manifest.ranges[0].boundary_dimension_ids, vec![1]);
        assert_eq!(
            manifest.blocks[0].shards.len(),
            LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS
        );

        let report = store.verify_logical_file_manifest(&manifest).await.unwrap();
        assert!(report.verified);
        assert_eq!(report.checked_blocks, 1);
        assert_eq!(
            report.checked_shards,
            (LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS) as u64
        );

        let slice = store
            .read_logical_range(ReadLogicalRangeRequest {
                manifest,
                ranges: vec![CoreByteRange {
                    start: 6,
                    end_exclusive: 16,
                }],
                authz_scope: AuthzScopeRef {
                    anvil_storage_tenant_id: "local".to_string(),
                    authz_realm_id: "system".to_string(),
                },
                expected_boundary: Some(vec![boundary]),
                prefetch_policy: CorePrefetchPolicy::default(),
                trace_context: CoreTraceContext::default(),
            })
            .await
            .unwrap();
        assert_eq!(slice, payload[6..16].to_vec());
    }

    #[tokio::test]
    async fn core_store_logical_file_pipeline_splits_blocks_and_reads_cross_block_ranges() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let payload = (0..96).map(|value| value as u8).collect::<Vec<_>>();
        let manifest = store
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "typed_index".to_string(),
                generation: 4,
                logical_file_id: "index/typed/split/segment-4".to_string(),
                source: payload.clone(),
                range_hints: vec![CoreLogicalRangeHint {
                    range_id: "cross-block-window".to_string(),
                    byte_start: 24,
                    byte_end: 72,
                    writer_record_kind: "typed_column_page".to_string(),
                    boundary_values: Vec::new(),
                    writer_statistics: Vec::new(),
                    preferred_block_boundary: "preferred".to_string(),
                    boundary_dimension_ids: Vec::new(),
                    prefetch_next_range_ids: Vec::new(),
                    shared_range: None,
                }],
                pipeline_policy: CorePipelinePolicy {
                    target_block_size: 32,
                    ..Default::default()
                },
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "logical-file-split-mut-1".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(manifest.blocks.len(), 3);
        assert_eq!(
            manifest
                .blocks
                .iter()
                .map(|block| (block.logical_offset, block.logical_length))
                .collect::<Vec<_>>(),
            vec![(0, 32), (32, 32), (64, 32)]
        );
        assert_eq!(manifest.ranges[0].block_ids.len(), 3);

        let slice = store
            .read_logical_range(ReadLogicalRangeRequest {
                manifest: manifest.clone(),
                ranges: vec![CoreByteRange {
                    start: 24,
                    end_exclusive: 72,
                }],
                authz_scope: AuthzScopeRef {
                    anvil_storage_tenant_id: "local".to_string(),
                    authz_realm_id: "system".to_string(),
                },
                expected_boundary: None,
                prefetch_policy: CorePrefetchPolicy::default(),
                trace_context: CoreTraceContext::default(),
            })
            .await
            .unwrap();
        assert_eq!(slice, payload[24..72].to_vec());
        store.verify_logical_file_manifest(&manifest).await.unwrap();
    }

    #[tokio::test]
    async fn core_store_logical_file_pipeline_honours_required_writer_boundaries() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let payload = (0..96).map(|value| value as u8).collect::<Vec<_>>();
        let manifest = store
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "stream".to_string(),
                generation: 2,
                logical_file_id: "streams/required-boundary/segment-2".to_string(),
                source: payload,
                range_hints: vec![CoreLogicalRangeHint {
                    range_id: "record-frame-1".to_string(),
                    byte_start: 24,
                    byte_end: 72,
                    writer_record_kind: "record_frame".to_string(),
                    boundary_values: Vec::new(),
                    writer_statistics: Vec::new(),
                    preferred_block_boundary: "required".to_string(),
                    boundary_dimension_ids: Vec::new(),
                    prefetch_next_range_ids: Vec::new(),
                    shared_range: None,
                }],
                pipeline_policy: CorePipelinePolicy {
                    target_block_size: 64,
                    ..Default::default()
                },
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "logical-file-required-boundary-mut-1".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(
            manifest
                .blocks
                .iter()
                .map(|block| (block.logical_offset, block.logical_length))
                .collect::<Vec<_>>(),
            vec![(0, 24), (24, 40), (64, 8), (72, 24)]
        );
        assert_eq!(manifest.ranges[0].block_ids.len(), 2);
    }

    #[tokio::test]
    async fn core_store_logical_range_read_does_not_materialise_unrelated_blocks() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let payload = (0..96).map(|value| value as u8).collect::<Vec<_>>();
        let manifest = store
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "vector".to_string(),
                generation: 5,
                logical_file_id: "index/vector/range/segment-5".to_string(),
                source: payload.clone(),
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy {
                    target_block_size: 32,
                    ..Default::default()
                },
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "logical-file-range-only-mut-1".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();

        let unrelated = manifest
            .blocks
            .iter()
            .find(|block| block.logical_offset == 64)
            .unwrap();
        let unrelated_ref =
            object_ref_from_logical_block_ref(unrelated, &manifest.erasure_profile_id).unwrap();
        let placement = unrelated_ref
            .placements
            .iter()
            .find(|placement| placement.shard_index == 0)
            .unwrap();
        let shard_hash = strip_sha256_prefix(&placement.shard_hash).unwrap();
        let object_hash = strip_sha256_prefix(&unrelated_ref.hash).unwrap();
        let shard_path = store.shard_path(&placement.node_id, object_hash, 0, shard_hash);
        fs::write(&shard_path, vec![0xee; placement.stored_size as usize])
            .await
            .unwrap();

        let slice = store
            .read_logical_range(ReadLogicalRangeRequest {
                manifest: manifest.clone(),
                ranges: vec![CoreByteRange {
                    start: 0,
                    end_exclusive: 16,
                }],
                authz_scope: AuthzScopeRef {
                    anvil_storage_tenant_id: "local".to_string(),
                    authz_realm_id: "system".to_string(),
                },
                expected_boundary: None,
                prefetch_policy: CorePrefetchPolicy::default(),
                trace_context: CoreTraceContext::default(),
            })
            .await
            .unwrap();
        assert_eq!(slice, payload[0..16].to_vec());
        assert!(
            store.verify_logical_file_manifest(&manifest).await.is_err(),
            "full verification must fail after corrupting a block not needed by the range read"
        );
    }

    #[tokio::test]
    async fn core_store_logical_file_api_supports_zstd_compression() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let payload =
            b"alpha alpha alpha alpha beta beta beta beta gamma gamma gamma gamma".repeat(64);
        let manifest = store
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "full_text".to_string(),
                generation: 9,
                logical_file_id: "index/full-text/compressed/segment-9".to_string(),
                source: payload.clone(),
                range_hints: vec![CoreLogicalRangeHint {
                    range_id: "beta-window".to_string(),
                    byte_start: 12,
                    byte_end: 32,
                    writer_record_kind: "postings".to_string(),
                    boundary_values: Vec::new(),
                    writer_statistics: Vec::new(),
                    preferred_block_boundary: "preferred".to_string(),
                    boundary_dimension_ids: Vec::new(),
                    prefetch_next_range_ids: Vec::new(),
                    shared_range: None,
                }],
                pipeline_policy: CorePipelinePolicy {
                    compression: "zstd".to_string(),
                    ..Default::default()
                },
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "logical-file-zstd-mut-1".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(manifest.logical_size, payload.len() as u64);
        assert_eq!(
            manifest.content_hash,
            format!("sha256:{}", sha256_hex(&payload))
        );
        assert_eq!(manifest.compression.algorithm, "zstd");
        assert_eq!(
            manifest.compression.uncompressed_length,
            payload.len() as u64
        );
        assert!(manifest.compression.compressed_length < payload.len() as u64);
        assert_eq!(manifest.blocks[0].logical_length, payload.len() as u64);
        assert_eq!(
            manifest.blocks[0].compressed_length,
            manifest.compression.compressed_length
        );
        assert_ne!(manifest.blocks[0].block_encoded_hash, manifest.content_hash);

        store.verify_logical_file_manifest(&manifest).await.unwrap();
        let slice = store
            .read_logical_range(ReadLogicalRangeRequest {
                manifest,
                ranges: vec![CoreByteRange {
                    start: 12,
                    end_exclusive: 32,
                }],
                authz_scope: AuthzScopeRef {
                    anvil_storage_tenant_id: "local".to_string(),
                    authz_realm_id: "system".to_string(),
                },
                expected_boundary: None,
                prefetch_policy: CorePrefetchPolicy::default(),
                trace_context: CoreTraceContext::default(),
            })
            .await
            .unwrap();
        assert_eq!(slice, payload[12..32].to_vec());
    }

    #[test]
    fn core_store_erasure_codec_matches_rfc_golden_vectors() {
        let ec_4_2_payload =
            hex::decode(concat!("00010203", "10111213", "20212223", "30313233")).unwrap();
        let ec_4_2 = encode_erasure_shards(&ec_4_2_payload, LOCAL_EC_4_2_PROFILE).unwrap();
        assert_eq!(hex::encode(&ec_4_2[0]), "00010203");
        assert_eq!(hex::encode(&ec_4_2[1]), "10111213");
        assert_eq!(hex::encode(&ec_4_2[2]), "20212223");
        assert_eq!(hex::encode(&ec_4_2[3]), "30313233");
        assert_eq!(hex::encode(&ec_4_2[4]), "00000000");
        assert_eq!(hex::encode(&ec_4_2[5]), "8084888c");

        let ec_8_3_payload = hex::decode(concat!(
            "00010203", "10111213", "20212223", "30313233", "40414243", "50515253", "60616263",
            "70717273"
        ))
        .unwrap();
        let ec_8_3 = encode_erasure_shards(&ec_8_3_payload, LOCAL_EC_8_3_PROFILE).unwrap();
        assert_eq!(hex::encode(&ec_8_3[0]), "00010203");
        assert_eq!(hex::encode(&ec_8_3[1]), "10111213");
        assert_eq!(hex::encode(&ec_8_3[2]), "20212223");
        assert_eq!(hex::encode(&ec_8_3[3]), "30313233");
        assert_eq!(hex::encode(&ec_8_3[4]), "40414243");
        assert_eq!(hex::encode(&ec_8_3[5]), "50515253");
        assert_eq!(hex::encode(&ec_8_3[6]), "60616263");
        assert_eq!(hex::encode(&ec_8_3[7]), "70717273");
        assert_eq!(hex::encode(&ec_8_3[8]), "00000000");
        assert_eq!(hex::encode(&ec_8_3[9]), "bab2aaa2");
        assert_eq!(hex::encode(&ec_8_3[10]), "2565a5e5");

        let replicated =
            encode_erasure_shards(b"replicated profile payload", LOCAL_REPLICATED_3_PROFILE)
                .unwrap();
        assert_eq!(replicated[0], replicated[1]);
        assert_eq!(replicated[0], replicated[2]);
    }

    #[test]
    fn core_store_erasure_codec_recovers_every_allowed_missing_shard_set() {
        for profile in [
            LOCAL_EC_4_2_PROFILE,
            LOCAL_EC_8_3_PROFILE,
            LOCAL_REPLICATED_3_PROFILE,
        ] {
            let payload_len = profile.data_shards * 17 + 5;
            let payload = (0..payload_len)
                .map(|index| (index.wrapping_mul(37) % 251) as u8)
                .collect::<Vec<_>>();
            let original = encode_erasure_shards(&payload, profile).unwrap();
            let missing_sets = shard_missing_sets(profile.total_shards(), profile.parity_shards);

            for missing in missing_sets {
                let mut shards = original
                    .iter()
                    .cloned()
                    .map(Some)
                    .collect::<Vec<Option<Vec<u8>>>>();
                for index in &missing {
                    shards[*index] = None;
                }
                reconstruct_data_shards(&mut shards, profile).unwrap_or_else(|error| {
                    panic!(
                        "profile {} failed to recover missing {:?}: {error}",
                        profile.id, missing
                    )
                });
                for shard_index in 0..profile.data_shards {
                    assert_eq!(
                        shards[shard_index].as_ref().unwrap(),
                        &original[shard_index],
                        "profile {} recovered wrong data shard {} with missing {:?}",
                        profile.id,
                        shard_index,
                        missing
                    );
                }
            }
        }
    }

    #[test]
    fn core_store_local_placement_satisfies_profile_failure_domains() {
        let ec_4_2 = plan_local_shard_placements(LOCAL_EC_4_2_PROFILE).unwrap();
        assert_eq!(ec_4_2.len(), 6);
        assert_eq!(
            cell_counts(&ec_4_2),
            BTreeMap::from([
                ("local-cell-1", 2),
                ("local-cell-2", 2),
                ("local-cell-3", 2)
            ])
        );

        let ec_8_3 = plan_local_shard_placements(LOCAL_EC_8_3_PROFILE).unwrap();
        assert_eq!(ec_8_3.len(), 11);
        assert_eq!(
            cell_counts(&ec_8_3),
            BTreeMap::from([
                ("local-cell-1", 3),
                ("local-cell-2", 3),
                ("local-cell-3", 3),
                ("local-cell-4", 2),
            ])
        );

        let replicated = plan_local_shard_placements(LOCAL_REPLICATED_3_PROFILE).unwrap();
        assert_eq!(replicated.len(), 3);
        assert_eq!(
            replicated
                .iter()
                .map(|placement| placement.node_id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
    }

    fn cell_counts(placements: &[LocalShardPlacement]) -> BTreeMap<&str, usize> {
        let mut counts = BTreeMap::new();
        for placement in placements {
            *counts.entry(placement.cell_id.as_str()).or_default() += 1;
        }
        counts
    }

    fn shard_missing_sets(total_shards: usize, max_missing: usize) -> Vec<Vec<usize>> {
        fn visit(
            total_shards: usize,
            remaining: usize,
            start: usize,
            current: &mut Vec<usize>,
            out: &mut Vec<Vec<usize>>,
        ) {
            out.push(current.clone());
            if remaining == 0 {
                return;
            }
            for index in start..total_shards {
                current.push(index);
                visit(total_shards, remaining - 1, index + 1, current, out);
                current.pop();
            }
        }

        let mut out = Vec::new();
        visit(total_shards, max_missing, 0, &mut Vec::new(), &mut out);
        out
    }

    #[tokio::test]
    async fn core_store_logical_file_api_accepts_all_normative_erasure_profiles() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();

        for (profile_id, data_shards, parity_shards, codec_id) in [
            ("ec-4-2", 4, 2, "rs-gf256-vandermonde-0x11d-v1/ec-4-2"),
            ("ec-8-3", 8, 3, "rs-gf256-vandermonde-0x11d-v1/ec-8-3"),
            (
                "replicated-3",
                1,
                2,
                "rs-gf256-vandermonde-0x11d-v1/replicated-3",
            ),
        ] {
            let payload = format!("profile:{profile_id}:logical-file-payload").into_bytes();
            let manifest = store
                .write_logical_file(WriteLogicalFileRequest {
                    writer_family: "profile_test".to_string(),
                    generation: 1,
                    logical_file_id: format!("profile-test/{profile_id}/segment-1"),
                    source: payload.clone(),
                    range_hints: Vec::new(),
                    pipeline_policy: CorePipelinePolicy {
                        erasure_profile_id: profile_id.to_string(),
                        ..Default::default()
                    },
                    trace_context: CoreTraceContext::default(),
                    boundary_values: Vec::new(),
                    mutation_id: format!("profile-test-{profile_id}"),
                    region_id: "local".to_string(),
                })
                .await
                .unwrap();

            assert_eq!(manifest.erasure_profile_id, profile_id);
            assert_eq!(manifest.data_shards, data_shards);
            assert_eq!(manifest.parity_shards, parity_shards);
            assert_eq!(manifest.codec_id, codec_id);
            assert_eq!(manifest.blocks[0].codec_id, codec_id);
            assert_eq!(
                manifest.blocks[0].shards.len(),
                (data_shards + parity_shards) as usize
            );
            assert!(
                core_object_ref_from_logical_file_manifest(&manifest)
                    .manifest_ref
                    .ends_with(&format!(":profile:{profile_id}"))
            );

            store.verify_logical_file_manifest(&manifest).await.unwrap();
            let read_back = store
                .read_logical_range(ReadLogicalRangeRequest {
                    manifest,
                    ranges: vec![CoreByteRange {
                        start: 0,
                        end_exclusive: payload.len() as u64,
                    }],
                    authz_scope: AuthzScopeRef {
                        anvil_storage_tenant_id: "local".to_string(),
                        authz_realm_id: "system".to_string(),
                    },
                    expected_boundary: None,
                    prefetch_policy: CorePrefetchPolicy::default(),
                    trace_context: CoreTraceContext::default(),
                })
                .await
                .unwrap();
            assert_eq!(read_back, payload);
        }
    }

    #[tokio::test]
    async fn core_store_boundary_schema_round_trips_through_corestore() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let receipt = store
            .put_boundary_schema(PutBoundarySchema {
                schema: sample_boundary_schema("customer-documents", 1),
                expected_generation: None,
                mutation_id: "boundary-schema-genesis".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(receipt.bucket, "customer-documents");
        assert_eq!(receipt.generation, 1);
        assert!(receipt.schema_hash.starts_with("sha256:"));

        let schema = store
            .read_boundary_schema("customer-documents")
            .await
            .unwrap()
            .expect("boundary schema");
        assert_eq!(schema.generation, 1);
        assert_eq!(schema.dimensions[0].name, "customer_tenant");
        assert_eq!(schema.dimensions[0].categories[0], "security_realm");
    }

    #[tokio::test]
    async fn core_store_boundary_schema_allows_optional_dimension_evolution() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        store
            .put_boundary_schema(PutBoundarySchema {
                schema: sample_boundary_schema("customer-documents", 1),
                expected_generation: None,
                mutation_id: "boundary-schema-genesis".to_string(),
            })
            .await
            .unwrap();
        let mut next = sample_boundary_schema("customer-documents", 2);
        next.dimensions.push(CoreBoundaryDimension {
            name: "project".to_string(),
            source: CoreBoundarySource::PathTemplate {
                template: "/customers/{customer_tenant}/projects/{project}/**".to_string(),
            },
            value_type: "string".to_string(),
            categories: vec!["storage_partition".to_string(), "query_prune".to_string()],
            required: false,
            cardinality: "high".to_string(),
            max_values_per_block: 8,
            placement_affinity: "prefer_colocate".to_string(),
            compaction_scope: "prefer_same_value".to_string(),
            shared_ranges_allowed: false,
            shared_record_kinds: Vec::new(),
            deprecated: false,
        });

        store
            .put_boundary_schema(PutBoundarySchema {
                schema: next,
                expected_generation: Some(1),
                mutation_id: "boundary-schema-add-project".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(
            store
                .read_boundary_schema("customer-documents")
                .await
                .unwrap()
                .unwrap()
                .dimensions
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn core_store_boundary_schema_rejects_incompatible_evolution() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        store
            .put_boundary_schema(PutBoundarySchema {
                schema: sample_boundary_schema("customer-documents", 1),
                expected_generation: None,
                mutation_id: "boundary-schema-genesis".to_string(),
            })
            .await
            .unwrap();

        let mut required_addition = sample_boundary_schema("customer-documents", 2);
        required_addition.dimensions.push(CoreBoundaryDimension {
            name: "project".to_string(),
            source: CoreBoundarySource::PathTemplate {
                template: "/customers/{customer_tenant}/projects/{project}/**".to_string(),
            },
            value_type: "string".to_string(),
            categories: vec!["query_prune".to_string()],
            required: true,
            cardinality: "high".to_string(),
            max_values_per_block: 8,
            placement_affinity: "prefer_colocate".to_string(),
            compaction_scope: "prefer_same_value".to_string(),
            shared_ranges_allowed: false,
            shared_record_kinds: Vec::new(),
            deprecated: false,
        });
        let err = store
            .put_boundary_schema(PutBoundarySchema {
                schema: required_addition,
                expected_generation: Some(1),
                mutation_id: "boundary-schema-add-required".to_string(),
            })
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains(AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str())
        );

        let mut type_change = sample_boundary_schema("customer-documents", 2);
        type_change.dimensions[0].value_type = "string".to_string();
        let err = store
            .put_boundary_schema(PutBoundarySchema {
                schema: type_change,
                expected_generation: Some(1),
                mutation_id: "boundary-schema-type-change".to_string(),
            })
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains(AnvilErrorCode::BoundarySchemaIncompatibleChange.as_str())
        );
    }

    #[tokio::test]
    async fn core_store_wal_records_never_inline_large_payloads_before_finalisation() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let bytes = vec![b'x'; CORE_WAL_MAX_INLINE_PAYLOAD_BYTES + 1];
        store
            .admit_core_mutation(
                "object.put",
                "object_blob",
                serde_json::json!({
                    "logical_name":"tenant:t/bucket:b/object:large",
                    "erasure_profile_id": LOCAL_ERASURE_PROFILE_ID,
                }),
                "large-payload-admission".to_string(),
                None,
                CoreWalPayload::Landed(&bytes),
                Vec::new(),
            )
            .await
            .unwrap();

        let wal_bytes = tokio::fs::read(store.active_wal_path()).await.unwrap();
        let wal_records = decode_wal_records(&wal_bytes).unwrap();
        assert_eq!(wal_records.len(), 1);
        assert!(
            wal_records[0].1.is_empty(),
            "large payloads must never be embedded in WAL frame payloads"
        );
        let landed = wal_records[0].0.landed_bytes.first().unwrap();
        assert_eq!(landed.length, bytes.len() as u64);
        assert!(
            storage
                .resolve_relative_storage_path(&landed.relative_path)
                .unwrap()
                .exists(),
            "large payload bytes must land outside the WAL and be referenced by hash/length"
        );
    }

    #[tokio::test]
    async fn core_store_wal_records_include_boundary_values() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        store
            .admit_core_mutation(
                "object.put",
                "object_blob",
                serde_json::json!({
                    "logical_name":"tenant:t/bucket:b/object:bounded",
                    "erasure_profile_id": LOCAL_ERASURE_PROFILE_ID,
                }),
                "bounded-payload-admission".to_string(),
                None,
                CoreWalPayload::Landed(b"bounded"),
                vec![CoreBoundaryValue {
                    schema_generation: 2,
                    name: "customer_tenant".to_string(),
                    value_type: "uuid".to_string(),
                    value: "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a".to_string(),
                    categories: vec!["security_realm".to_string()],
                    source_kind: "user_metadata_json_pointer".to_string(),
                    required: true,
                }],
            )
            .await
            .unwrap();

        let wal_bytes = tokio::fs::read(store.active_wal_path()).await.unwrap();
        let wal_records = decode_wal_records(&wal_bytes).unwrap();
        assert_eq!(wal_records.len(), 1);
        assert_eq!(wal_records[0].0.boundary_values.len(), 1);
        assert_eq!(wal_records[0].0.boundary_values[0].name, "customer_tenant");
        assert_eq!(
            wal_records[0].0.boundary_values[0].value,
            "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a"
        );
        let landed = wal_records[0].0.landed_bytes.first().unwrap();
        let meta_path = storage
            .resolve_relative_storage_path(&landed.relative_path)
            .unwrap()
            .with_extension("meta");
        let meta: serde_json::Value =
            serde_json::from_slice(&fs::read(meta_path).await.unwrap()).unwrap();
        assert_eq!(
            meta.pointer("/boundary_values/0/name")
                .and_then(serde_json::Value::as_str),
            Some("customer_tenant")
        );
    }

    #[tokio::test]
    async fn core_store_object_manifest_includes_boundary_values() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let boundary_value = CoreBoundaryValue {
            schema_generation: 2,
            name: "customer_tenant".to_string(),
            value_type: "uuid".to_string(),
            value: "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a".to_string(),
            categories: vec!["security_realm".to_string()],
            source_kind: "user_metadata_json_pointer".to_string(),
            required: true,
        };
        let object_ref = store
            .put_blob(PutBlob {
                logical_name: "tenant:t/bucket:b/object:bounded".to_string(),
                bytes: b"bounded manifest".to_vec(),
                boundary_values: vec![boundary_value.clone()],
                region_id: "local".to_string(),
                mutation_id: "bounded-manifest".to_string(),
            })
            .await
            .unwrap();

        let manifest = store.read_object_manifest(&object_ref).await.unwrap();
        assert_eq!(manifest.boundary_values, vec![boundary_value]);
    }

    #[tokio::test]
    async fn core_store_recovers_unfinalised_put_blob_wal_on_startup() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let bytes = b"recover object from wal".to_vec();
        store
            .admit_core_mutation(
                "object.put",
                "object_blob",
                serde_json::json!({
                    "logical_name": "tenant:t/bucket:b/object:recovered",
                    "region_id": "local",
                    "erasure_profile_id": LOCAL_ERASURE_PROFILE_ID,
                }),
                "recover-object-from-wal".to_string(),
                None,
                CoreWalPayload::Landed(&bytes),
                Vec::new(),
            )
            .await
            .unwrap();
        drop(store);

        let recovered = CoreStore::new(storage).await.unwrap();
        let object_ref = test_object_ref_for_payload(&bytes, LOCAL_EC_4_2_PROFILE);
        assert_eq!(
            recovered
                .get_blob(GetBlob {
                    object_ref: object_ref.clone()
                })
                .await
                .unwrap(),
            bytes
        );
        let wal_bytes = tokio::fs::read(recovered.active_wal_path()).await.unwrap();
        assert!(
            decode_wal_records(&wal_bytes).unwrap().is_empty(),
            "startup recovery must checkpoint recovered object WAL records"
        );
        assert_eq!(recovered.admission_landed_bytes().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn core_store_recovers_unfinalised_stream_append_wal_on_startup() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let payload = br#"{"event":"recover"}"#.to_vec();
        let idempotency_key = "recover-stream-idempotency";
        store
            .admit_core_mutation(
                "stream.append",
                "stream",
                serde_json::json!({
                    "stream_id": "tenant:t/bucket:b/recovered-stream",
                    "partition_id": "tenant:t/bucket:b",
                    "record_kind": "event.recovered",
                    "transaction_id": null,
                }),
                "recover-stream-from-wal".to_string(),
                Some(idempotency_key.to_string()),
                CoreWalPayload::Inline(&payload),
                Vec::new(),
            )
            .await
            .unwrap();
        drop(store);

        let recovered = CoreStore::new(storage).await.unwrap();
        let records = recovered
            .read_stream(ReadStream {
                stream_id: "tenant:t/bucket:b/recovered-stream".to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_kind, "event.recovered");
        assert_eq!(records[0].payload, payload);
        let expected_idempotency_hash =
            format!("sha256:{}", sha256_hex(idempotency_key.as_bytes()));
        assert_eq!(
            records[0].idempotency_key_hash.as_deref(),
            Some(expected_idempotency_hash.as_str())
        );
        let wal_bytes = tokio::fs::read(recovered.active_wal_path()).await.unwrap();
        assert!(
            decode_wal_records(&wal_bytes).unwrap().is_empty(),
            "startup recovery must checkpoint recovered stream WAL records"
        );
    }

    #[tokio::test]
    async fn core_store_recovers_unfinalised_ref_cas_wal_on_startup() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        store
            .admit_core_mutation(
                "ref.compare_and_swap",
                "core-control",
                serde_json::json!({
                    "ref_name": "tenant/t/bucket/b/object/recovered/current",
                    "new_target": "core-object-ref:sha256:aaaaaaaa",
                    "expected_generation": null,
                    "expected_target": null,
                    "require_absent": true,
                    "require_present": false,
                    "transaction_id": null,
                }),
                "recover-ref-cas-from-wal".to_string(),
                None,
                CoreWalPayload::Empty,
                Vec::new(),
            )
            .await
            .unwrap();
        drop(store);

        let recovered = CoreStore::new(storage).await.unwrap();
        let current = recovered
            .read_ref("tenant/t/bucket/b/object/recovered/current")
            .await
            .unwrap()
            .expect("recovered ref");
        assert_eq!(current.generation, 1);
        assert_eq!(current.target, "core-object-ref:sha256:aaaaaaaa");
        let wal_bytes = tokio::fs::read(recovered.active_wal_path()).await.unwrap();
        assert!(
            decode_wal_records(&wal_bytes).unwrap().is_empty(),
            "startup recovery must checkpoint recovered ref CAS WAL records"
        );
    }

    #[tokio::test]
    async fn core_store_recovers_unfinalised_ref_delete_wal_on_startup() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: "tenant/t/bucket/b/object/delete-recovered/current".to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: "core-object-ref:sha256:bbbbbbbb".to_string(),
                transaction_id: None,
            })
            .await
            .unwrap();
        store
            .admit_core_mutation(
                "ref.delete",
                "core-control",
                serde_json::json!({
                    "ref_name": "tenant/t/bucket/b/object/delete-recovered/current",
                    "expected_generation": 1,
                    "expected_target": "core-object-ref:sha256:bbbbbbbb",
                    "require_present": true,
                    "transaction_id": null,
                }),
                "recover-ref-delete-from-wal".to_string(),
                None,
                CoreWalPayload::Empty,
                Vec::new(),
            )
            .await
            .unwrap();
        drop(store);

        let recovered = CoreStore::new(storage).await.unwrap();
        assert!(
            recovered
                .read_ref("tenant/t/bucket/b/object/delete-recovered/current")
                .await
                .unwrap()
                .is_none(),
            "startup recovery must apply admitted ref delete records"
        );
        let wal_bytes = tokio::fs::read(recovered.active_wal_path()).await.unwrap();
        assert!(
            decode_wal_records(&wal_bytes).unwrap().is_empty(),
            "startup recovery must checkpoint recovered ref delete WAL records"
        );
    }

    #[tokio::test]
    async fn core_store_recovers_unfinalised_mutation_batch_wal_on_startup() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let batch = CoreMutationBatch {
            transaction_id: "recover-mutation-batch".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:recovery".to_string(),
            preconditions: vec![CoreMutationPrecondition::Ref {
                ref_name: "tenant/t/bucket/b/object/batch-recovered/current".to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
            }],
            operations: vec![
                CoreMutationOperation::RefUpdate {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    ref_name: "tenant/t/bucket/b/object/batch-recovered/current".to_string(),
                    new_target: "core-object-ref:sha256:cccccccc".to_string(),
                },
                CoreMutationOperation::StreamAppend {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    stream_id: "object_metadata:t:b:batch-recovered".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"batch-recovered"}"#.to_vec(),
                    idempotency_key: Some("batch-recovered-event".to_string()),
                },
            ],
        };
        store
            .admit_core_mutation(
                "mutation.batch",
                "core-control",
                serde_json::json!({
                    "transaction_id": batch.transaction_id.clone(),
                    "scope_partition": batch.scope_partition.clone(),
                    "operation_count": batch.operations.len(),
                }),
                batch.transaction_id.clone(),
                Some(batch.transaction_id.clone()),
                CoreWalPayload::Inline(&serde_json::to_vec(&batch).unwrap()),
                Vec::new(),
            )
            .await
            .unwrap();
        drop(store);

        let recovered = CoreStore::new(storage).await.unwrap();
        let transaction = recovered
            .read_transaction("recover-mutation-batch")
            .await
            .unwrap()
            .expect("recovered transaction");
        assert_eq!(transaction.state, CoreTransactionState::Committed);
        let current = recovered
            .read_ref("tenant/t/bucket/b/object/batch-recovered/current")
            .await
            .unwrap()
            .expect("recovered batch ref");
        assert_eq!(current.target, "core-object-ref:sha256:cccccccc");
        let records = recovered
            .read_stream(ReadStream {
                stream_id: "object_metadata:t:b:batch-recovered".to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_kind, "object.put");
        let wal_bytes = tokio::fs::read(recovered.active_wal_path()).await.unwrap();
        assert!(
            decode_wal_records(&wal_bytes).unwrap().is_empty(),
            "startup recovery must checkpoint recovered mutation batch WAL records"
        );
    }

    #[tokio::test]
    async fn core_store_admission_rejects_when_wal_hard_limit_would_be_exceeded() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        fs::create_dir_all(store.admission_wal_dir()).await.unwrap();
        fs::write(store.active_wal_path(), vec![0_u8; 32])
            .await
            .unwrap();

        let err = store
            .enforce_admission_capacity_with_limits(
                16,
                0,
                CoreAdmissionCapacityLimits {
                    wal_soft_limit_bytes: 32,
                    wal_hard_limit_bytes: 40,
                    wal_soft_lag_seconds: 60,
                    wal_hard_lag_seconds: 300,
                    landed_bytes_soft_limit_bytes: 1024,
                    landed_bytes_hard_limit_bytes: 2048,
                },
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains(AnvilErrorCode::ResourceExhaustedWalBacklog.as_str())
        );
    }

    #[tokio::test]
    async fn core_store_admission_rejects_when_landed_hard_limit_would_be_exceeded() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let landed_dir = store
            .admission_landed_bytes_root()
            .join("sha256")
            .join("aa");
        fs::create_dir_all(&landed_dir).await.unwrap();
        fs::write(landed_dir.join("aa-existing.landed"), vec![0_u8; 64])
            .await
            .unwrap();

        let err = store
            .enforce_admission_capacity_with_limits(
                0,
                64,
                CoreAdmissionCapacityLimits {
                    wal_soft_limit_bytes: 1024,
                    wal_hard_limit_bytes: 2048,
                    wal_soft_lag_seconds: 60,
                    wal_hard_lag_seconds: 300,
                    landed_bytes_soft_limit_bytes: 96,
                    landed_bytes_hard_limit_bytes: 100,
                },
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains(AnvilErrorCode::ResourceExhaustedWalBacklog.as_str())
        );
    }

    #[tokio::test]
    async fn core_store_admission_rejects_when_wal_materialisation_lag_is_too_old() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        write_test_wal_records(
            &store,
            vec![test_wal_record(
                "old-lag-mutation",
                unix_timestamp_nanos().saturating_sub(301_000_000_000),
                1,
            )],
        )
        .await;

        let err = store
            .enforce_admission_capacity_with_limits(
                0,
                0,
                CoreAdmissionCapacityLimits {
                    wal_soft_limit_bytes: 1024 * 1024,
                    wal_hard_limit_bytes: 2 * 1024 * 1024,
                    wal_soft_lag_seconds: 60,
                    wal_hard_lag_seconds: 300,
                    landed_bytes_soft_limit_bytes: 1024 * 1024,
                    landed_bytes_hard_limit_bytes: 2 * 1024 * 1024,
                },
            )
            .await
            .unwrap_err();

        assert!(
            err.to_string()
                .contains(AnvilErrorCode::ResourceExhaustedWalBacklog.as_str())
        );
    }

    #[tokio::test]
    async fn core_store_admission_lag_ignores_finalised_wal_records() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let record = test_wal_record(
            "old-finalised-mutation",
            unix_timestamp_nanos().saturating_sub(301_000_000_000),
            1,
        );
        write_test_wal_records(&store, vec![record.clone()]).await;
        store
            .mark_core_wal_finalised_unlocked(&record, "committed")
            .await
            .unwrap();

        store
            .enforce_admission_capacity_with_limits(
                0,
                0,
                CoreAdmissionCapacityLimits {
                    wal_soft_limit_bytes: 1024 * 1024,
                    wal_hard_limit_bytes: 2 * 1024 * 1024,
                    wal_soft_lag_seconds: 60,
                    wal_hard_lag_seconds: 300,
                    landed_bytes_soft_limit_bytes: 1024 * 1024,
                    landed_bytes_hard_limit_bytes: 2 * 1024 * 1024,
                },
            )
            .await
            .unwrap();
        let wal_bytes = tokio::fs::read(store.active_wal_path()).await.unwrap();
        assert!(
            decode_wal_records(&wal_bytes).unwrap().is_empty(),
            "a fully finalised WAL prefix must be checkpointed out of the active WAL"
        );
        assert_eq!(store.next_core_wal_sequence().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn core_store_wal_finalisation_is_idempotent_for_same_record() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let record = test_wal_record("same-finalisation", unix_timestamp_nanos(), 1);

        store
            .mark_core_wal_finalised_unlocked(&record, "committed")
            .await
            .unwrap();
        store
            .mark_core_wal_finalised_unlocked(&record, "committed")
            .await
            .unwrap();

        let finalisations = store
            .read_all_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await
            .unwrap()
            .into_iter()
            .filter(|record| record.record_kind == CORE_WAL_FINALISATION_RECORD_KIND)
            .count();
        assert_eq!(finalisations, 1);

        let conflicting = test_wal_record("different-finalisation", unix_timestamp_nanos(), 1);
        assert!(
            store
                .mark_core_wal_finalised_unlocked(&conflicting, "committed")
                .await
                .is_err(),
            "same WAL node/epoch/sequence with a different mutation must fail closed"
        );
    }

    #[tokio::test]
    async fn core_store_wal_checkpoint_preserves_high_watermark_when_prefix_is_unfinalised() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let first = test_wal_record("unfinalised-prefix", unix_timestamp_nanos(), 1);
        let second = test_wal_record("finalised-after-gap", unix_timestamp_nanos(), 2);
        write_test_wal_records(&store, vec![first, second.clone()]).await;
        store
            .mark_core_wal_finalised_unlocked(&second, "committed")
            .await
            .unwrap();

        let wal_bytes = tokio::fs::read(store.active_wal_path()).await.unwrap();
        let wal_records = decode_wal_records(&wal_bytes).unwrap();
        assert_eq!(
            wal_records.len(),
            2,
            "checkpointing must not remove finalised records after an unfinalised prefix"
        );
        assert_eq!(
            store.next_core_wal_sequence().await.unwrap(),
            3,
            "WAL sequence allocation must not reuse a finalised sequence that remains after an unfinalised prefix"
        );
    }

    #[tokio::test]
    async fn core_store_put_blob_writes_erasure_shards_and_reconstructs_missing_data() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let payload = b"this payload is long enough to span multiple data shards".to_vec();
        let object_ref = store
            .put_blob(PutBlob {
                logical_name: "mesh:test/tenant:t/bucket:b/object:a".to_string(),
                bytes: payload.clone(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "mut-1".to_string(),
            })
            .await
            .unwrap();
        let manifest = store.read_object_manifest(&object_ref).await.unwrap();
        let object_hash = strip_sha256_prefix(&object_ref.hash).unwrap().to_string();
        assert_eq!(manifest.encoding.profile_id, LOCAL_ERASURE_PROFILE_ID);
        assert_eq!(manifest.encoding.data_shards, LOCAL_DATA_SHARDS as u16);
        assert_eq!(manifest.encoding.parity_shards, LOCAL_PARITY_SHARDS as u16);
        assert_eq!(
            manifest.encoding.minimum_read_shards,
            LOCAL_DATA_SHARDS as u16
        );
        assert_eq!(
            manifest.encoding.minimum_write_ack_shards,
            (LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS) as u16
        );
        assert_eq!(manifest.encoding.placement_scope, "region");
        assert_eq!(manifest.encoding.repair_priority, "normal");
        assert_eq!(
            manifest.placements.len(),
            LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS
        );
        for placement in &manifest.placements {
            assert_eq!(placement.region_id, "local");
            assert!(
                placement.cell_id.starts_with("local-cell-"),
                "placements must carry a cell failure-domain identity"
            );
            assert_eq!(placement.placement_epoch, LOCAL_PLACEMENT_EPOCH);
            assert_eq!(placement.fsync_sequence, LOCAL_SHARD_FSYNC_SEQUENCE);
            let shard_hash = strip_sha256_prefix(&placement.shard_hash).unwrap();
            let path = store.shard_path(
                &placement.node_id,
                &object_hash,
                placement.shard_index,
                shard_hash,
            );
            assert!(
                path.starts_with(storage.core_store_replica_path(&placement.node_id)),
                "shards must be placed under explicit replica node directories"
            );
            assert!(
                path.exists(),
                "replica shard must exist at {}",
                path.display()
            );
            let shard_file = tokio::fs::read(&path).await.unwrap();
            assert!(
                shard_file.starts_with(CORE_BLOCK_SHARD_MAGIC),
                "physical shard files must use the RFC block-shard container"
            );
            let expected_block_id = local_block_id_for_object_hash(&object_hash);
            let payload = read_block_shard_file(
                &path,
                BlockShardExpectation {
                    block_id: &expected_block_id,
                    shard_index: placement.shard_index,
                    erasure_profile_id: LOCAL_ERASURE_PROFILE_ID,
                    placement_epoch: placement.placement_epoch,
                    payload_hash: &placement.shard_hash,
                    payload_len: placement.stored_size,
                },
                "test_read_block_shard",
            )
            .await
            .unwrap();
            assert_eq!(payload.len() as u64, placement.stored_size);
            assert!(
                read_block_shard_file(
                    &path,
                    BlockShardExpectation {
                        block_id: &expected_block_id,
                        shard_index: placement.shard_index,
                        erasure_profile_id: LOCAL_ERASURE_PROFILE_ID,
                        placement_epoch: placement.placement_epoch + 1,
                        payload_hash: &placement.shard_hash,
                        payload_len: placement.stored_size,
                    },
                    "test_read_block_shard_stale_epoch",
                )
                .await
                .is_err(),
                "block shard validation must reject stale placement epochs"
            );
        }

        for placement in manifest.placements.iter().take(LOCAL_PARITY_SHARDS) {
            let shard_hash = strip_sha256_prefix(&placement.shard_hash).unwrap();
            let path = store.shard_path(
                &placement.node_id,
                &object_hash,
                placement.shard_index,
                shard_hash,
            );
            tokio::fs::remove_file(path).await.unwrap();
        }

        let reconstructed = store
            .get_blob(GetBlob {
                object_ref: object_ref.clone(),
            })
            .await
            .unwrap();
        assert_eq!(reconstructed, payload);
    }

    #[tokio::test]
    async fn core_store_get_blob_fails_when_too_many_erasure_shards_are_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let object_ref = store
            .put_blob(PutBlob {
                logical_name: "mesh:test/tenant:t/bucket:b/object:a".to_string(),
                bytes: b"small but durable".to_vec(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "mut-1".to_string(),
            })
            .await
            .unwrap();
        let manifest = store.read_object_manifest(&object_ref).await.unwrap();
        let object_hash = strip_sha256_prefix(&object_ref.hash).unwrap().to_string();
        for placement in manifest.placements.iter().take(LOCAL_PARITY_SHARDS + 1) {
            let shard_hash = strip_sha256_prefix(&placement.shard_hash).unwrap();
            let path = store.shard_path(
                &placement.node_id,
                &object_hash,
                placement.shard_index,
                shard_hash,
            );
            tokio::fs::remove_file(path).await.unwrap();
        }

        let err = store.get_blob(GetBlob { object_ref }).await.unwrap_err();
        assert!(
            err.to_string().contains("has only"),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn core_store_streams_are_gap_free_hash_chained_and_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let first = store
            .append_stream(AppendStreamRecord {
                stream_id: "object_metadata:tenant:b".to_string(),
                partition_id: "partition-1".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"key":"a"}"#.to_vec(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some("idem-1".to_string()),
            })
            .await
            .unwrap();
        let replay = store
            .append_stream(AppendStreamRecord {
                stream_id: "object_metadata:tenant:b".to_string(),
                partition_id: "partition-1".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"key":"a"}"#.to_vec(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some("idem-1".to_string()),
            })
            .await
            .unwrap();
        assert!(replay.idempotent_replay);
        assert_eq!(first.sequence, replay.sequence);

        let second = store
            .append_stream(AppendStreamRecord {
                stream_id: "object_metadata:tenant:b".to_string(),
                partition_id: "partition-1".to_string(),
                record_kind: "object.delete".to_string(),
                payload: br#"{"key":"a"}"#.to_vec(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some("idem-2".to_string()),
            })
            .await
            .unwrap();
        assert_eq!(second.sequence, 2);
        let records = store
            .read_stream(ReadStream {
                stream_id: "object_metadata:tenant:b".to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].previous_event_hash, records[0].event_hash);
        let stream_ids = store
            .list_stream_ids("object_metadata:")
            .await
            .expect("list stream ids");
        assert_eq!(stream_ids, vec!["object_metadata:tenant:b".to_string()]);
        let wal_bytes = tokio::fs::read(store.active_wal_path()).await.unwrap();
        let wal_records = decode_wal_records(&wal_bytes).unwrap();
        assert!(
            wal_records.is_empty(),
            "finalised stream appends must be checkpointed out of the active WAL"
        );
        for node_id in local_control_node_ids() {
            assert!(
                !tmp.path()
                    .join("_core")
                    .join("replicas")
                    .join(&node_id)
                    .join("streams")
                    .join("_names")
                    .exists(),
                "CoreStore stream ids must be reconstructed from stream data, not _names JSON sidecars"
            );
        }
    }

    #[tokio::test]
    async fn core_store_read_stream_page_does_not_decode_unrequested_tail_records() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();

        for sequence in 1..=3 {
            store
                .append_stream(AppendStreamRecord {
                    stream_id: "tenant:t/bucket:b/ranged-stream".to_string(),
                    partition_id: "tenant:t/bucket:b".to_string(),
                    record_kind: format!("event.{sequence}"),
                    payload: format!(r#"{{"sequence":{sequence}}}"#).into_bytes(),
                    fence: None,
                    transaction_id: None,
                    idempotency_key: Some(format!("event-{sequence}")),
                })
                .await
                .unwrap();
        }

        for node_id in local_control_node_ids() {
            let path = store.stream_replica_path(&node_id, "tenant:t/bucket:b/ranged-stream");
            let mut bytes = fs::read(&path).await.unwrap();
            let (mut offset, _record_count) =
                decode_active_stream_header("tenant:t/bucket:b/ranged-stream", &bytes).unwrap();
            for _ in 0..2 {
                let len = read_u32_le(&bytes, &mut offset).unwrap() as usize;
                let _ = read_exact(&bytes, &mut offset, len).unwrap();
                let _ = read_u32_le(&bytes, &mut offset).unwrap();
            }
            let len = read_u32_le(&bytes, &mut offset).unwrap() as usize;
            let _ = read_exact(&bytes, &mut offset, len).unwrap();
            bytes[offset..offset + 4].copy_from_slice(&0u32.to_le_bytes());
            let hash_start = bytes.len() - 32;
            let hash = Sha256::digest(&bytes[..hash_start]);
            bytes[hash_start..].copy_from_slice(hash.as_ref());
            fs::write(path, bytes).await.unwrap();
        }

        let page = store
            .read_stream(ReadStream {
                stream_id: "tenant:t/bucket:b/ranged-stream".to_string(),
                after_sequence: 0,
                limit: 2,
            })
            .await
            .unwrap();
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].record_kind, "event.1");
        assert_eq!(page[1].record_kind, "event.2");

        assert!(
            store
                .read_stream(ReadStream {
                    stream_id: "tenant:t/bucket:b/ranged-stream".to_string(),
                    after_sequence: 0,
                    limit: 0,
                })
                .await
                .is_err(),
            "full stream reads must still validate the corrupted tail record"
        );
    }

    #[tokio::test]
    async fn core_store_refs_are_compare_and_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let created = store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: "tenant/t/bucket/b/object/a/current".to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: "sha256:first".to_string(),
                transaction_id: None,
            })
            .await
            .unwrap();
        assert_eq!(created.generation, 1);

        let conflict = store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: "tenant/t/bucket/b/object/a/current".to_string(),
                expected_generation: Some(99),
                expected_target: None,
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: "sha256:second".to_string(),
                transaction_id: None,
            })
            .await;
        assert!(conflict.is_err());

        let updated = store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: "tenant/t/bucket/b/object/a/current".to_string(),
                expected_generation: Some(1),
                expected_target: Some("sha256:first".to_string()),
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: "sha256:second".to_string(),
                transaction_id: None,
            })
            .await
            .unwrap();
        assert_eq!(updated.generation, 2);
        let updates = store
            .read_ref_updates("tenant/t/bucket/b/object/a/current", 0, 10)
            .await
            .unwrap();
        assert_eq!(updates.len(), 2);
        assert_eq!(updates[0].previous_generation, None);
        assert_eq!(updates[0].new_generation, Some(1));
        assert_eq!(updates[1].previous_generation, Some(1));
        assert_eq!(updates[1].new_generation, Some(2));
        let ref_name = "tenant/t/bucket/b/object/a/current";
        assert_eq!(
            store
                .read_ref(ref_name)
                .await
                .unwrap()
                .expect("read quorum survives minority replica loss")
                .target,
            "sha256:second"
        );
        assert_eq!(
            store.list_ref_names("tenant/t").await.unwrap(),
            vec![ref_name.to_string()]
        );
        for node_id in local_control_node_ids() {
            assert!(
                !tmp.path()
                    .join("_core")
                    .join("replicas")
                    .join(&node_id)
                    .join("refs")
                    .exists(),
                "CoreStore refs must not use final JSON sidecars"
            );
        }
        let recovered = store
            .recover_ref_from_updates(ref_name)
            .await
            .unwrap()
            .expect("recover ref from update stream");
        assert_eq!(recovered.generation, 2);
        assert_eq!(recovered.target, "sha256:second");

        let deleted = store
            .delete_ref(
                "tenant/t/bucket/b/object/a/current",
                Some(2),
                Some("sha256:second"),
                true,
            )
            .await
            .unwrap()
            .expect("deleted ref");
        assert_eq!(deleted.generation, 2);
        let updates = store
            .read_ref_updates("tenant/t/bucket/b/object/a/current", 0, 10)
            .await
            .unwrap();
        assert_eq!(updates.len(), 3);
        assert_eq!(updates[2].previous_generation, Some(2));
        assert_eq!(updates[2].new_generation, None);
        assert!(store.read_ref(ref_name).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn core_store_mutation_batch_commits_refs_streams_and_transaction_record() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let receipt = store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: "txn-batch-1".to_string(),
                scope_partition: "bucket-partition-1".to_string(),
                committed_by_principal: "principal:test".to_string(),
                preconditions: vec![CoreMutationPrecondition::Ref {
                    ref_name: "tenant/t/bucket/b/object/a/current".to_string(),
                    expected_generation: None,
                    expected_target: None,
                    require_absent: true,
                    require_present: false,
                    fence: None,
                    authz_revision: None,
                    source_watch_cursor: None,
                }],
                operations: vec![
                    CoreMutationOperation::RefUpdate {
                        partition_id: "bucket-partition-1".to_string(),
                        ref_name: "tenant/t/bucket/b/object/a/current".to_string(),
                        new_target: "core-object-ref:payload".to_string(),
                    },
                    CoreMutationOperation::StreamAppend {
                        partition_id: "bucket-partition-1".to_string(),
                        stream_id: "object_metadata:t:b".to_string(),
                        record_kind: "object.put".to_string(),
                        payload: br#"{"object":"a"}"#.to_vec(),
                        idempotency_key: Some("object-a-put".to_string()),
                    },
                ],
            })
            .await
            .unwrap();
        assert_eq!(receipt.visible_updates.len(), 2);
        assert_eq!(
            store
                .read_ref("tenant/t/bucket/b/object/a/current")
                .await
                .unwrap()
                .expect("current ref")
                .transaction_id
                .as_deref(),
            Some("txn-batch-1")
        );
        let stream = store
            .read_stream(ReadStream {
                stream_id: "object_metadata:t:b".to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(stream.len(), 1);
        assert_eq!(stream[0].transaction_id.as_deref(), Some("txn-batch-1"));
        let transaction = store
            .read_transaction("txn-batch-1")
            .await
            .unwrap()
            .expect("transaction record");
        assert_eq!(transaction.state, CoreTransactionState::Committed);
        assert_eq!(transaction.visible_updates.len(), 2);
        assert_eq!(receipt.state, CoreTransactionState::Committed);
        assert!(receipt.finalisation_error.is_none());
    }

    #[tokio::test]
    async fn core_store_refs_streams_and_watches_hide_uncommitted_transaction_records() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let transaction_id = "txn-visibility".to_string();
        let ref_name = "tenant/t/bucket/b/object/hidden/current".to_string();
        let stream_id = "object_metadata:t:b:hidden".to_string();

        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: ref_name.clone(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: "core-object-ref:hidden".to_string(),
                transaction_id: Some(transaction_id.clone()),
            })
            .await
            .unwrap();
        store
            .append_stream(AppendStreamRecord {
                stream_id: stream_id.clone(),
                partition_id: "bucket-partition-visibility".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"hidden"}"#.to_vec(),
                fence: None,
                transaction_id: Some(transaction_id.clone()),
                idempotency_key: Some("hidden-object-put".to_string()),
            })
            .await
            .unwrap();

        assert!(store.read_ref(&ref_name).await.unwrap().is_none());
        assert!(
            store
                .read_stream(ReadStream {
                    stream_id: stream_id.clone(),
                    after_sequence: 0,
                    limit: 10,
                })
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .watch(WatchRequest {
                    stream_prefix: "object_metadata:t:b".to_string(),
                    after_cursor: None,
                    limit: 10,
                })
                .await
                .unwrap()
                .is_empty()
        );

        store
            .commit_transaction(CoreTransaction {
                schema: CORE_TRANSACTION_SCHEMA.to_string(),
                transaction_id: transaction_id.clone(),
                scope_partition: "bucket-partition-visibility".to_string(),
                state: CoreTransactionState::Committed,
                preconditions_hash: "sha256:preconditions".to_string(),
                operations_hash: "sha256:operations".to_string(),
                prepared_refs: Vec::new(),
                visible_updates: vec![
                    CoreTransactionUpdate::CoreRefUpdate {
                        ref_name: ref_name.clone(),
                        new_generation: 1,
                    },
                    CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: 1,
                        prepared_record_hash: "sha256:prepared".to_string(),
                    },
                ],
                finalisation_error: None,
                committed_at: now_rfc3339(),
                committed_by_principal: "principal:test".to_string(),
            })
            .await
            .unwrap();

        assert!(store.read_ref(&ref_name).await.unwrap().is_some());
        assert_eq!(
            store
                .read_stream(ReadStream {
                    stream_id: stream_id.clone(),
                    after_sequence: 0,
                    limit: 10,
                })
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .watch(WatchRequest {
                    stream_prefix: "object_metadata:t:b".to_string(),
                    after_cursor: None,
                    limit: 10,
                })
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn core_store_failed_finalisation_is_terminal_and_not_visible() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let batch = CoreMutationBatch {
            transaction_id: "txn-finalisation-fails".to_string(),
            scope_partition: "bucket-partition-finalisation".to_string(),
            committed_by_principal: "principal:test".to_string(),
            preconditions: Vec::new(),
            operations: vec![
                CoreMutationOperation::RefUpdate {
                    partition_id: "bucket-partition-finalisation".to_string(),
                    ref_name: "tenant/t/bucket/b/object/finalisation/current".to_string(),
                    new_target: "core-object-ref:should-not-be-visible".to_string(),
                },
                CoreMutationOperation::StreamAppend {
                    partition_id: "bucket-partition-finalisation".to_string(),
                    stream_id: "object_metadata:t:b:finalisation".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"first"}"#.to_vec(),
                    idempotency_key: Some("same-idempotency-key".to_string()),
                },
                CoreMutationOperation::StreamAppend {
                    partition_id: "bucket-partition-finalisation".to_string(),
                    stream_id: "object_metadata:t:b:finalisation".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"conflict"}"#.to_vec(),
                    idempotency_key: Some("same-idempotency-key".to_string()),
                },
            ],
        };

        let receipt = store.commit_mutation_batch(batch.clone()).await.unwrap();
        assert_eq!(receipt.state, CoreTransactionState::FinalisationFailed);
        assert!(receipt.visible_updates.is_empty());
        assert!(
            receipt
                .finalisation_error
                .as_deref()
                .is_some_and(|error| error.contains("idempotency conflict"))
        );
        let transaction = store
            .read_transaction("txn-finalisation-fails")
            .await
            .unwrap()
            .expect("failed transaction record");
        assert_eq!(transaction.state, CoreTransactionState::FinalisationFailed);
        assert!(transaction.visible_updates.is_empty());
        assert!(transaction.finalisation_error.is_some());
        assert!(
            store
                .read_ref("tenant/t/bucket/b/object/finalisation/current")
                .await
                .unwrap()
                .is_none(),
            "failed finalisation ref updates must not become visible"
        );
        assert!(
            store
                .read_stream(ReadStream {
                    stream_id: "object_metadata:t:b:finalisation".to_string(),
                    after_sequence: 0,
                    limit: 10,
                })
                .await
                .unwrap()
                .is_empty(),
            "failed finalisation stream appends must not become visible"
        );

        let replay = store.commit_mutation_batch(batch).await.unwrap();
        assert_eq!(replay.state, CoreTransactionState::FinalisationFailed);
        assert_eq!(replay.finalisation_error, receipt.finalisation_error);
        assert!(replay.visible_updates.is_empty());
    }

    #[tokio::test]
    async fn core_store_mutation_batch_precondition_failure_leaves_visible_state_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: "tenant/t/bucket/b/object/a/current".to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: "first".to_string(),
                transaction_id: None,
            })
            .await
            .unwrap();

        let err = store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: "txn-batch-fail".to_string(),
                scope_partition: "bucket-partition-1".to_string(),
                committed_by_principal: "principal:test".to_string(),
                preconditions: vec![CoreMutationPrecondition::Ref {
                    ref_name: "tenant/t/bucket/b/object/a/current".to_string(),
                    expected_generation: Some(999),
                    expected_target: None,
                    require_absent: false,
                    require_present: true,
                    fence: None,
                    authz_revision: None,
                    source_watch_cursor: None,
                }],
                operations: vec![
                    CoreMutationOperation::RefUpdate {
                        partition_id: "bucket-partition-1".to_string(),
                        ref_name: "tenant/t/bucket/b/object/a/current".to_string(),
                        new_target: "second".to_string(),
                    },
                    CoreMutationOperation::StreamAppend {
                        partition_id: "bucket-partition-1".to_string(),
                        stream_id: "object_metadata:t:b".to_string(),
                        record_kind: "object.put".to_string(),
                        payload: br#"{"object":"a"}"#.to_vec(),
                        idempotency_key: None,
                    },
                ],
            })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("generation mismatch"));
        assert_eq!(
            store
                .read_ref("tenant/t/bucket/b/object/a/current")
                .await
                .unwrap()
                .expect("current ref")
                .target,
            "first"
        );
        assert!(
            store
                .read_stream(ReadStream {
                    stream_id: "object_metadata:t:b".to_string(),
                    after_sequence: 0,
                    limit: 10,
                })
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .read_transaction("txn-batch-fail")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn core_store_mutation_batch_rejects_cross_partition_atomicity() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let err = store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: "txn-cross-partition".to_string(),
                scope_partition: "bucket-partition-1".to_string(),
                committed_by_principal: "principal:test".to_string(),
                preconditions: Vec::new(),
                operations: vec![CoreMutationOperation::StreamAppend {
                    partition_id: "bucket-partition-2".to_string(),
                    stream_id: "object_metadata:t:b".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"a"}"#.to_vec(),
                    idempotency_key: None,
                }],
            })
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("CrossPartitionAtomicMutationUnsupported")
        );
        assert!(
            store
                .read_stream(ReadStream {
                    stream_id: "object_metadata:t:b".to_string(),
                    after_sequence: 0,
                    limit: 10,
                })
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn core_store_seals_stream_segment_and_exposes_watch_events() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        store
            .append_stream(AppendStreamRecord {
                stream_id: "object_metadata:tenant:b".to_string(),
                partition_id: "partition-1".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"key":"a"}"#.to_vec(),
                fence: None,
                transaction_id: None,
                idempotency_key: None,
            })
            .await
            .unwrap();
        store
            .append_stream(AppendStreamRecord {
                stream_id: "object_metadata:tenant:b".to_string(),
                partition_id: "partition-1".to_string(),
                record_kind: "object.delete".to_string(),
                payload: br#"{"key":"a"}"#.to_vec(),
                fence: None,
                transaction_id: None,
                idempotency_key: None,
            })
            .await
            .unwrap();

        let segment = store
            .seal_stream_segment(SealStreamSegment {
                stream_id: "object_metadata:tenant:b".to_string(),
                partition_id: "partition-1".to_string(),
                through_sequence: None,
                segment_kind: "object_metadata".to_string(),
                mutation_id: "seal-1".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(segment.first_sequence, 1);
        assert_eq!(segment.last_sequence, 2);
        assert_eq!(segment.record_count, 2);
        let segment_bytes = store
            .get_blob(GetBlob {
                object_ref: segment.object_ref.clone(),
            })
            .await
            .unwrap();
        assert!(segment_bytes.starts_with(CORE_STREAM_SEGMENT_MAGIC));
        let decoded = store.read_stream_segment(&segment).await.unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].record_kind, "object.put");
        assert_eq!(decoded[1].record_kind, "object.delete");

        let events = store
            .watch(WatchRequest {
                stream_prefix: "object_metadata:".to_string(),
                after_cursor: Some("object_metadata:tenant:b:00000000000000000001".to_string()),
                limit: 10,
            })
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].sequence, 2);
        assert_eq!(events[0].record_kind, "object.delete");
    }

    #[tokio::test]
    async fn core_store_fences_are_owner_and_token_checked() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let first = store
            .acquire_fence(AcquireFence {
                fence_name: "task:7".to_string(),
                authenticated_principal: "node-a".to_string(),
                ttl_ms: 60_000,
            })
            .await
            .unwrap();
        assert_eq!(first.fence_token, 1);
        assert!(
            store
                .acquire_fence(AcquireFence {
                    fence_name: "task:7".to_string(),
                    authenticated_principal: "node-b".to_string(),
                    ttl_ms: 60_000,
                })
                .await
                .is_err()
        );
        assert!(
            store
                .release_fence(ReleaseFence {
                    fence_name: "task:7".to_string(),
                    authenticated_principal: "node-a".to_string(),
                    fence_token: 99,
                })
                .await
                .is_err()
        );
        store
            .release_fence(ReleaseFence {
                fence_name: "task:7".to_string(),
                authenticated_principal: "node-a".to_string(),
                fence_token: first.fence_token,
            })
            .await
            .unwrap();
        let second = store
            .acquire_fence(AcquireFence {
                fence_name: "task:7".to_string(),
                authenticated_principal: "node-b".to_string(),
                ttl_ms: 60_000,
            })
            .await
            .unwrap();
        assert_eq!(second.fence_token, 2);
        assert!(
            store
                .acquire_fence(AcquireFence {
                    fence_name: "task:ttl".to_string(),
                    authenticated_principal: "node-a".to_string(),
                    ttl_ms: MAX_CORE_FENCE_TTL_MS + 1,
                })
                .await
                .is_err(),
            "CoreStore fences must enforce the maximum TTL policy"
        );
    }

    #[tokio::test]
    async fn core_store_compare_and_swap_ref_enforces_fence_precondition() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let permit = store
            .acquire_fence(AcquireFence {
                fence_name: "ref:guarded".to_string(),
                authenticated_principal: "principal-a".to_string(),
                ttl_ms: 60_000,
            })
            .await
            .unwrap();
        let source = store
            .append_stream(AppendStreamRecord {
                stream_id: "source:stream".to_string(),
                partition_id: "source:partition".to_string(),
                record_kind: "source.event".to_string(),
                payload: br#"{"source":1}"#.to_vec(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some("source-1".to_string()),
            })
            .await
            .unwrap();

        let rejected = store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: "guarded:ref".to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: Some(CoreFencePrecondition {
                    fence_name: "ref:guarded".to_string(),
                    fence_token: permit.fence_token,
                    authenticated_principal: "principal-b".to_string(),
                }),
                authz_revision: Some("azr:12".to_string()),
                source_watch_cursor: Some(source.cursor.clone()),
                new_target: "core-object-ref:test".to_string(),
                transaction_id: None,
            })
            .await;
        assert!(
            rejected.is_err(),
            "CAS must reject stale or impersonated fence preconditions before writing"
        );
        let missing_source = store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: "guarded:ref".to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: Some(CoreFencePrecondition {
                    fence_name: "ref:guarded".to_string(),
                    fence_token: permit.fence_token,
                    authenticated_principal: "principal-a".to_string(),
                }),
                authz_revision: Some("azr:12".to_string()),
                source_watch_cursor: Some("source:stream:00000000000000000099".to_string()),
                new_target: "core-object-ref:test".to_string(),
                transaction_id: None,
            })
            .await;
        assert!(
            missing_source.is_err(),
            "CAS must reject source watch cursors that are not retained"
        );

        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: "guarded:ref".to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: Some(CoreFencePrecondition {
                    fence_name: "ref:guarded".to_string(),
                    fence_token: permit.fence_token,
                    authenticated_principal: "principal-a".to_string(),
                }),
                authz_revision: Some("azr:12".to_string()),
                source_watch_cursor: Some(source.cursor.clone()),
                new_target: "core-object-ref:test".to_string(),
                transaction_id: None,
            })
            .await
            .unwrap();

        let updates = store.read_ref_updates("guarded:ref", 0, 10).await.unwrap();
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].preconditions.fence_token,
            Some(permit.fence_token)
        );
        assert_eq!(
            updates[0].preconditions.authz_revision.as_deref(),
            Some("azr:12")
        );
        assert_eq!(
            updates[0].preconditions.source_watch_cursor.as_deref(),
            Some(source.cursor.as_str())
        );
    }

    #[tokio::test]
    async fn core_store_append_stream_enforces_fence_precondition() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let first = store
            .acquire_fence(AcquireFence {
                fence_name: "stream:guarded".to_string(),
                authenticated_principal: "principal-a".to_string(),
                ttl_ms: 60_000,
            })
            .await
            .unwrap();

        let impersonated = store
            .append_stream(AppendStreamRecord {
                stream_id: "guarded:stream".to_string(),
                partition_id: "guarded:partition".to_string(),
                record_kind: "guarded.event".to_string(),
                payload: br#"{"event":1}"#.to_vec(),
                fence: Some(CoreFencePrecondition {
                    fence_name: "stream:guarded".to_string(),
                    fence_token: first.fence_token,
                    authenticated_principal: "principal-b".to_string(),
                }),
                transaction_id: None,
                idempotency_key: Some("guarded-1".to_string()),
            })
            .await;
        assert!(
            impersonated.is_err(),
            "protected stream append must reject caller-supplied owner impersonation"
        );

        store
            .append_stream(AppendStreamRecord {
                stream_id: "guarded:stream".to_string(),
                partition_id: "guarded:partition".to_string(),
                record_kind: "guarded.event".to_string(),
                payload: br#"{"event":1}"#.to_vec(),
                fence: Some(CoreFencePrecondition {
                    fence_name: "stream:guarded".to_string(),
                    fence_token: first.fence_token,
                    authenticated_principal: "principal-a".to_string(),
                }),
                transaction_id: None,
                idempotency_key: Some("guarded-1".to_string()),
            })
            .await
            .unwrap();

        store
            .release_fence(ReleaseFence {
                fence_name: "stream:guarded".to_string(),
                authenticated_principal: "principal-a".to_string(),
                fence_token: first.fence_token,
            })
            .await
            .unwrap();
        let second = store
            .acquire_fence(AcquireFence {
                fence_name: "stream:guarded".to_string(),
                authenticated_principal: "principal-b".to_string(),
                ttl_ms: 60_000,
            })
            .await
            .unwrap();
        assert_ne!(first.fence_token, second.fence_token);

        let stale = store
            .append_stream(AppendStreamRecord {
                stream_id: "guarded:stream".to_string(),
                partition_id: "guarded:partition".to_string(),
                record_kind: "guarded.event".to_string(),
                payload: br#"{"event":2}"#.to_vec(),
                fence: Some(CoreFencePrecondition {
                    fence_name: "stream:guarded".to_string(),
                    fence_token: first.fence_token,
                    authenticated_principal: "principal-a".to_string(),
                }),
                transaction_id: None,
                idempotency_key: Some("guarded-2".to_string()),
            })
            .await;
        assert!(
            stale.is_err(),
            "protected stream append must reject stale fence tokens"
        );
    }

    #[tokio::test]
    async fn core_store_mutation_batch_fence_precondition_uses_committed_principal() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let permit = store
            .acquire_fence(AcquireFence {
                fence_name: "object:a".to_string(),
                authenticated_principal: "principal-a".to_string(),
                ttl_ms: 60_000,
            })
            .await
            .unwrap();
        let rejected = store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: "txn-fence-rejected".to_string(),
                scope_partition: "tenant:t/bucket:b".to_string(),
                committed_by_principal: "principal-b".to_string(),
                preconditions: vec![CoreMutationPrecondition::Fence {
                    fence_name: "object:a".to_string(),
                    fence_token: permit.fence_token,
                }],
                operations: vec![CoreMutationOperation::StreamAppend {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    stream_id: "object_metadata:t:b".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"a"}"#.to_vec(),
                    idempotency_key: None,
                }],
            })
            .await;
        assert!(
            rejected.is_err(),
            "a caller must not be able to satisfy a fence by supplying another owner's token"
        );

        store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: "txn-fence-accepted".to_string(),
                scope_partition: "tenant:t/bucket:b".to_string(),
                committed_by_principal: "principal-a".to_string(),
                preconditions: vec![CoreMutationPrecondition::Fence {
                    fence_name: "object:a".to_string(),
                    fence_token: permit.fence_token,
                }],
                operations: vec![CoreMutationOperation::StreamAppend {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    stream_id: "object_metadata:t:b".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"a"}"#.to_vec(),
                    idempotency_key: None,
                }],
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn core_store_root_catalog_is_signed_generationed_and_recoverable() {
        const KEY: &[u8] = b"root catalog test signing key";
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let root_segment = store
            .put_blob(PutBlob {
                logical_name: "mesh:test/system/mesh/root-segment/head".to_string(),
                bytes: br#"{"refs":[],"streams":[]}"#.to_vec(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "root-segment-1".to_string(),
            })
            .await
            .unwrap();
        let manifest = store.read_object_manifest(&root_segment).await.unwrap();

        let catalog = CoreRootCatalog {
            schema: CORE_ROOT_CATALOG_SCHEMA.to_string(),
            mesh_id: "mesh-test".to_string(),
            generation: 1,
            previous_hash: ZERO_HASH.to_string(),
            root_partitions: vec![CoreRootPartition {
                partition_id: "core.root.refs.0".to_string(),
                owner_node_id: "node-a".to_string(),
                fence: 1,
                placement_group: "root-pg-0".to_string(),
                embedded_head_segment_manifest: manifest,
            }],
            placement_catalog_ref: "core.ref:/system/placement/current".to_string(),
            stream_directory_ref: "core.ref:/system/streams/current".to_string(),
            ref_directory_ref: "core.ref:/system/refs/current".to_string(),
            authz_system_realm_ref: "core.ref:/system/authz/realm/current".to_string(),
            created_at: now_rfc3339(),
            signed_by: "node-a".to_string(),
            signature: String::new(),
        };
        let receipt = store
            .commit_root_catalog(catalog.clone(), KEY)
            .await
            .expect("commit genesis root catalog");
        assert_eq!(receipt.generation, 1);
        let latest = store
            .read_latest_root_catalog("mesh-test", KEY)
            .await
            .unwrap()
            .expect("latest root catalog");
        assert_eq!(latest.generation, 1);
        assert_eq!(latest.signed_by, "node-a");
        assert!(verify_root_catalog(&latest, KEY).is_ok());
        assert!(verify_root_catalog(&latest, b"wrong-key").is_err());

        let stale = store.commit_root_catalog(catalog, KEY).await;
        assert!(stale.is_err());

        let mut next = latest.clone();
        next.generation = 2;
        next.previous_hash = hash_root_catalog(&latest).unwrap();
        next.signature.clear();
        let next_receipt = store
            .commit_root_catalog(next, KEY)
            .await
            .expect("commit next root catalog");
        assert_eq!(next_receipt.generation, 2);
        let history = store
            .list_root_catalog_history("mesh-test")
            .await
            .expect("root catalog history");
        assert_eq!(history.len(), 2);
    }
}
