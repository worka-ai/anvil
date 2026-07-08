use super::types::*;
use crate::cluster_identity;
use crate::error_codes::AnvilErrorCode;
use crate::storage::Storage;
use aes_gcm_siv::aead::{Aead, AeadCore, OsRng, Payload};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use hmac::{Hmac, Mac};
use libp2p::identity;
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
const LOCAL_CONTROL_NODE_ID_PREFIX: &str = "local-control-node";
const LOCAL_ERASURE_SET_ID: &str = "local-erasure-set";

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
const CORE_STREAM_SEGMENT_MAGIC: &[u8; 8] = b"ANSTRM\n\0";
const CORE_STREAM_SPARSE_INDEX_MAGIC: &[u8; 8] = b"ANSSIX1\0";
const CORE_ACTIVE_STREAM_MAGIC: &[u8; 8] = b"ANASTR1\0";
const CORE_ROOT_ANCHOR_MAGIC: &[u8; 8] = b"ANROOT1\0";
const CORE_ROOT_REGISTER_MAGIC: &[u8; 8] = b"ANREGRT1";
const CORE_TRANSACTION_MANIFEST_MAGIC: &[u8; 8] = b"ANXACT1\0";
const CORE_BLOCK_SHARD_MAGIC: &[u8; 8] = b"ANBLK\n\0\0";
const CORE_WAL_FILE_MAGIC: &[u8; 6] = b"ANWAL\n";
const CORE_WAL_FRAME_MAGIC: &[u8; 4] = b"AWF1";
const CORE_STREAM_SEGMENT_VERSION: u16 = 1;
const CORE_ACTIVE_STREAM_VERSION: u16 = 1;
const CORE_ROOT_ANCHOR_VERSION: u16 = 1;
const CORE_ROOT_REGISTER_VERSION: u16 = 1;
const CORE_TRANSACTION_MANIFEST_VERSION: u16 = 1;
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
const CORE_STREAM_STATE_LOCATOR_SCHEMA: &str = "anvil.core.stream_state_locator.v1";
const CORE_STREAM_STATE_LOCATOR_RECORD_KIND: &str = "core_stream.state_locator";
const CORE_TRANSACTION_STREAM_ID: &str = "core_transactions";
const CORE_TRANSACTION_PARTITION_ID: &str = "core-control";
const CORE_TRANSACTION_ROOT_PARTITION_ID: u64 = 0;
const CORE_TRANSACTION_RECORD_KIND: &str = "core_transaction";
const CORE_PIPELINE_KEY_LEN: usize = 32;
const CORE_PIPELINE_NONCE_LEN: usize = 12;

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
    compressed_length: u64,
    plaintext_hash: String,
    encryption: CoreEncryptionDescriptor,
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
    pipeline_keyring: Option<Arc<CorePipelineKeyring>>,
    node_signing_keypair: Arc<identity::Keypair>,
}

#[derive(Debug, Clone)]
pub struct CorePipelineKeyring {
    active_key_id: String,
    keys: BTreeMap<String, [u8; CORE_PIPELINE_KEY_LEN]>,
}

impl CorePipelineKeyring {
    pub fn new(
        active_key_id: impl Into<String>,
        active_key: [u8; CORE_PIPELINE_KEY_LEN],
    ) -> Result<Self> {
        let active_key_id = validate_pipeline_key_id(active_key_id.into())?;
        let mut keys = BTreeMap::new();
        keys.insert(active_key_id.clone(), active_key);
        Ok(Self {
            active_key_id,
            keys,
        })
    }

    pub fn from_hex_config(
        active_key_id: &str,
        active_key_hex: &str,
        previous_keys: &str,
    ) -> Result<Self> {
        let mut keyring = Self::new(active_key_id, decode_pipeline_key_hex(active_key_hex)?)?;
        for item in previous_keys
            .split(',')
            .map(str::trim)
            .filter(|item| !item.is_empty())
        {
            let (key_id, key_hex) = item
                .split_once(':')
                .ok_or_else(|| anyhow!("previous CoreStore pipeline keys must be key_id:hex"))?;
            keyring.insert_previous_key(key_id, decode_pipeline_key_hex(key_hex)?)?;
        }
        Ok(keyring)
    }

    pub fn active_key_id(&self) -> &str {
        &self.active_key_id
    }

    pub fn insert_previous_key(
        &mut self,
        key_id: &str,
        key: [u8; CORE_PIPELINE_KEY_LEN],
    ) -> Result<()> {
        let key_id = validate_pipeline_key_id(key_id.to_string())?;
        if key_id == self.active_key_id {
            bail!("previous CoreStore pipeline key id must not equal active key id");
        }
        if self.keys.insert(key_id.clone(), key).is_some() {
            bail!("duplicate CoreStore pipeline key id '{key_id}'");
        }
        Ok(())
    }

    fn active_key(&self) -> Result<&[u8; CORE_PIPELINE_KEY_LEN]> {
        self.keys
            .get(&self.active_key_id)
            .ok_or_else(|| anyhow!("active CoreStore pipeline key is not present in keyring"))
    }

    fn key(&self, key_id: &str) -> Result<&[u8; CORE_PIPELINE_KEY_LEN]> {
        self.keys
            .get(key_id)
            .ok_or_else(|| anyhow!("CoreStore pipeline key id '{key_id}' is not configured"))
    }
}

#[derive(Debug, Clone)]
struct PipelineBlockBytes {
    stored: Vec<u8>,
    encryption: CoreEncryptionDescriptor,
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
struct StoredStreamStateLocatorRecord {
    schema: String,
    stream_id: String,
    sequence: u64,
    event_hash: String,
    locator: CoreManifestLocator,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CoreRootAnchorRecord {
    schema: String,
    root_anchor_key: String,
    root_key_hash: String,
    root_generation: u64,
    previous_root_hash: String,
    transaction_manifest: Option<CoreManifestLocator>,
    checkpoint_manifest: Option<CoreManifestLocator>,
    publisher_node_id: String,
    publisher_epoch: u64,
    partition_owner_fence: u64,
    created_at_unix_nanos: u64,
    root_state: String,
    mutation_first: Option<String>,
    mutation_last: Option<String>,
    writer_families: Vec<String>,
    manifest_count: u64,
    final_block_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CoreRootRegisterHeader {
    schema: String,
    root_partition_id: u64,
    root_key_hash: String,
    root_generation: u64,
    shard_index: u16,
    register_cohort_nodes: Vec<String>,
    register_cohort_hash: String,
    placement_epoch: u64,
    created_at_unix_nanos: u64,
    root_anchor_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CoreTransactionManifestRecord {
    schema: String,
    mutation_ids: Vec<String>,
    idempotency_key_hashes: Vec<String>,
    pre_root_generation: u64,
    post_root_generation: u64,
    logical_manifests: Vec<CoreManifestLocator>,
    ref_updates: Vec<serde_json::Value>,
    tombstones: Vec<serde_json::Value>,
    writer_checkpoints: Vec<serde_json::Value>,
    boundary_schema_refs: Vec<serde_json::Value>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamSparseIndexEntry {
    first_sequence: u64,
    first_timestamp_nanos: i64,
    record_ordinal: u32,
    byte_offset: u64,
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
    #[serde(default)]
    result: Option<serde_json::Value>,
    finalised_at_unix_nanos: u64,
}

#[derive(Debug, Clone)]
struct StreamAppendOutcome {
    receipt: StreamAppendReceipt,
    state_locator: Option<CoreManifestLocator>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CoreWalRecordKey {
    node_id: String,
    wal_epoch: u64,
    wal_sequence: u64,
}

#[derive(Debug, Clone)]
struct CoreWalReplayOutcome {
    state: &'static str,
    result: Option<serde_json::Value>,
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
        Self::new_with_optional_pipeline_keyring(storage, None).await
    }

    pub async fn new_with_pipeline_keyring(
        storage: Storage,
        pipeline_keyring: CorePipelineKeyring,
    ) -> Result<Self> {
        Self::new_with_optional_pipeline_keyring(storage, Some(Arc::new(pipeline_keyring))).await
    }

    async fn new_with_optional_pipeline_keyring(
        storage: Storage,
        pipeline_keyring: Option<Arc<CorePipelineKeyring>>,
    ) -> Result<Self> {
        let node_signing_keypair = Arc::new(cluster_identity::load_or_create_cluster_keypair(
            storage
                .core_store_root_path()
                .join("node-signing-keypair.pb"),
        )?);
        let store = Self {
            storage,
            write_lock: Arc::new(Mutex::new(())),
            pipeline_keyring,
            node_signing_keypair,
        };
        store.ensure_layout().await?;
        store.bootstrap_system_root_anchor().await?;
        store.recover_core_wal().await?;
        Ok(store)
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    fn sign_core_receipt(&self, signed_payload_hash: &str) -> Result<Vec<u8>> {
        self.node_signing_keypair
            .sign(signed_payload_hash.as_bytes())
            .map_err(|error| anyhow!("sign CoreStore shard receipt: {error}"))
    }

    fn verify_core_receipt_signature(
        &self,
        node_id: &str,
        signed_payload_hash: &str,
        receipt_signature: &[u8],
    ) -> Result<()> {
        if !is_local_shard_node_id(node_id) {
            bail!("CoreStore shard receipt references unknown node {node_id}");
        }
        if !self
            .node_signing_keypair
            .public()
            .verify(signed_payload_hash.as_bytes(), receipt_signature)
        {
            bail!("CoreStore shard receipt signature verification failed for node {node_id}");
        }
        Ok(())
    }

    fn verify_object_placement_receipt(
        &self,
        block_id: &str,
        profile_id: &str,
        placement: &CoreObjectPlacement,
    ) -> Result<()> {
        let profile = local_erasure_profile(profile_id)?;
        validate_local_shard_receipt_placement(
            profile,
            usize::from(placement.shard_index),
            &placement.node_id,
            &placement.region_id,
            &placement.cell_id,
        )?;
        let expected = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id,
            shard_index: placement.shard_index,
            erasure_profile: profile_id,
            node_id: &placement.node_id,
            region_id: &placement.region_id,
            cell_id: &placement.cell_id,
            placement_epoch: placement.placement_epoch,
            shard_length: placement.stored_size,
            shard_hash: &placement.shard_hash,
            fsync_sequence: placement.fsync_sequence,
            written_at_unix_nanos: placement.written_at_unix_nanos,
        });
        validate_shard_receipt_common(
            &placement.node_id,
            &placement.region_id,
            &placement.cell_id,
            &placement.shard_hash,
            placement.stored_size,
            placement.fsync_sequence,
            placement.written_at_unix_nanos,
            &placement.signed_payload_hash,
            &placement.signature_algorithm,
            &placement.receipt_signature,
            &expected,
        )?;
        self.verify_core_receipt_signature(
            &placement.node_id,
            &placement.signed_payload_hash,
            &placement.receipt_signature,
        )
    }

    fn verify_manifest_locator_receipts(&self, locator: &CoreManifestLocator) -> Result<()> {
        for block in &locator.block_locators {
            for receipt in &block.shard_receipts {
                self.verify_core_receipt_signature(
                    &receipt.node_id,
                    &receipt.signed_payload_hash,
                    &receipt.receipt_signature,
                )?;
            }
        }
        Ok(())
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
        self.put_blob_with_profile_and_encoding(input, profile, "none", "object_blob")
            .await
    }

    async fn put_logical_file_block_with_profile(
        &self,
        request: &WriteLogicalFileRequest,
        block_index: usize,
        bytes: Vec<u8>,
        block_plain_hash: String,
        encryption_algorithm: String,
        profile: LocalErasureProfile,
    ) -> Result<CoreObjectRef> {
        let input = PutBlob {
            logical_name: format!("{}/block-{block_index:06}", request.logical_file_id),
            bytes,
            boundary_values: request.boundary_values.clone(),
            region_id: request.region_id.clone(),
            mutation_id: format!("{}-block-{block_index:06}", request.mutation_id),
        };
        let _perf_guard = crate::perf::guard("anvil_core_store_op", &[("operation", "put_blob")]);
        self.ensure_layout().await?;
        validate_logical_id(&input.logical_name, "blob logical name")?;
        validate_logical_id(&request.writer_family, "blob writer family")?;
        let admission = self
            .admit_core_mutation(
                "object.put",
                &request.writer_family,
                serde_json::json!({
                    "logical_name": input.logical_name.clone(),
                    "region_id": input.region_id.clone(),
                    "erasure_profile_id": profile.id,
                    "encryption": encryption_algorithm.clone(),
                    "block_plain_hash": block_plain_hash.clone(),
                    "writer_generation": request.generation,
                    "block_ordinal": block_index as u64,
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
                &input.logical_name,
                request.generation,
                block_index as u64,
                &block_plain_hash,
                &hash,
                &materialised_bytes,
                &admission.boundary_values,
                &admission.mutation_id,
                profile,
                &encryption_algorithm,
                &request.writer_family,
            )
            .await?;
        self.mark_core_wal_finalised_unlocked(&admission, "committed")
            .await?;
        Ok(object_ref)
    }

    async fn put_blob_with_profile_and_encoding(
        &self,
        input: PutBlob,
        profile: LocalErasureProfile,
        encryption_algorithm: &str,
        writer_family: &str,
    ) -> Result<CoreObjectRef> {
        let _perf_guard = crate::perf::guard("anvil_core_store_op", &[("operation", "put_blob")]);
        self.ensure_layout().await?;
        validate_logical_id(&input.logical_name, "blob logical name")?;
        validate_logical_id(writer_family, "blob writer family")?;
        let admission = self
            .admit_core_mutation(
                "object.put",
                writer_family,
                serde_json::json!({
                    "logical_name": input.logical_name.clone(),
                    "region_id": input.region_id.clone(),
                    "erasure_profile_id": profile.id,
                    "encryption": encryption_algorithm,
                    "block_plain_hash": format!("sha256:{}", sha256_hex(&input.bytes)),
                    "writer_generation": 0_u64,
                    "block_ordinal": 0_u64,
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
                &input.logical_name,
                0,
                0,
                &landed.sha256,
                &hash,
                &materialised_bytes,
                &admission.boundary_values,
                &admission.mutation_id,
                profile,
                encryption_algorithm,
                writer_family,
            )
            .await?;
        self.mark_core_wal_finalised_unlocked(&admission, "committed")
            .await?;
        Ok(object_ref)
    }

    async fn materialise_object_blob_bytes(
        &self,
        logical_file_id: &str,
        writer_generation: u64,
        block_ordinal: u64,
        block_plain_hash: &str,
        hash: &str,
        materialised_bytes: &[u8],
        boundary_values: &[CoreBoundaryValue],
        mutation_id: &str,
        profile: LocalErasureProfile,
        encryption_algorithm: &str,
        writer_family: &str,
    ) -> Result<CoreObjectRef> {
        if sha256_hex(materialised_bytes) != hash {
            bail!("CoreStore object materialisation hash mismatch");
        }
        let block_id = local_block_id_for_logical_block(
            logical_file_id,
            writer_generation,
            block_ordinal,
            block_plain_hash,
        );
        let shards = encode_erasure_shards(materialised_bytes, profile)?;
        let placements = plan_local_shard_placements(profile)?;
        let mut object_placements = Vec::with_capacity(shards.len());
        let mut stripe_size = 0u64;

        for (shard_index, shard) in shards.iter().enumerate() {
            let placement = placements.get(shard_index).ok_or_else(|| {
                anyhow!("CoreStore missing local placement for shard {shard_index}")
            })?;
            let shard_hash = sha256_hex(shard);
            let shard_path = self.shard_path(&placement.node_id, &block_id, shard_index as u16);
            let logical_offset = shard_index as u64 * shard.len() as u64;
            let shard_file = encode_block_shard_file(
                BlockShardHeaderInput {
                    block_id: block_id.clone(),
                    erasure_set_id: LOCAL_ERASURE_SET_ID.to_string(),
                    shard_index: shard_index as u16,
                    erasure_profile_id: profile.id.to_string(),
                    logical_file_id: logical_file_id.to_string(),
                    logical_offset,
                    logical_length: shard.len() as u64,
                    payload_plain_hash: format!("sha256:{shard_hash}"),
                    payload_stored_hash: format!("sha256:{shard_hash}"),
                    compression: "none".to_string(),
                    encryption: encryption_algorithm.to_string(),
                    placement_epoch: LOCAL_PLACEMENT_EPOCH,
                    boundary_summary_hash: boundary_summary_hash(boundary_values)?,
                    boundary_values_b64: URL_SAFE_NO_PAD
                        .encode(serde_json::to_vec(boundary_values)?),
                    writer_family: writer_family.to_string(),
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
            let written_at_unix_nanos = unix_timestamp_nanos();
            let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
                block_id: &block_id,
                shard_index: shard_index as u16,
                erasure_profile: profile.id,
                node_id: &placement.node_id,
                region_id: &placement.region_id,
                cell_id: &placement.cell_id,
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                shard_length: shard.len() as u64,
                shard_hash: &format!("sha256:{shard_hash}"),
                fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                written_at_unix_nanos,
            });
            let receipt_signature = self.sign_core_receipt(&signed_payload_hash)?;
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
                written_at_unix_nanos,
                signed_payload_hash,
                signature_algorithm: "ed25519-libp2p".to_string(),
                receipt_signature,
            });
        }

        Ok(CoreObjectRef {
            hash: format!("sha256:{hash}"),
            logical_size: materialised_bytes.len() as u64,
            manifest_ref: encode_manifest_ref_with_profile(hash, profile.id),
            encoding: CoreObjectEncoding {
                block_id,
                profile_id: profile.id.to_string(),
                data_shards: profile.data_shards as u16,
                parity_shards: profile.parity_shards as u16,
                minimum_read_shards: profile.minimum_read_shards as u16,
                minimum_write_ack_shards: profile.minimum_write_ack_shards as u16,
                stripe_size,
                placement_scope: "region".to_string(),
                repair_priority: "normal".to_string(),
                encryption: encryption_algorithm.to_string(),
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
            let shard_path = self.shard_path(
                &placement.node_id,
                &manifest.encoding.block_id,
                placement.shard_index,
            );
            let shard_bytes = match read_block_shard_file(
                &shard_path,
                BlockShardExpectation {
                    block_id: &manifest.encoding.block_id,
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
            let shard_path = self.shard_path(
                &placement.node_id,
                &manifest.encoding.block_id,
                placement.shard_index,
            );
            let shard_bytes = match read_block_shard_file(
                &shard_path,
                BlockShardExpectation {
                    block_id: &manifest.encoding.block_id,
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
        request: WriteLogicalFileRequest,
    ) -> Result<CoreLogicalFileManifest> {
        Ok(self
            .write_logical_file_with_locator(request)
            .await?
            .manifest)
    }

    pub async fn write_logical_file_with_locator(
        &self,
        mut request: WriteLogicalFileRequest,
    ) -> Result<CoreLogicalFileWrite> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "write_logical_file")],
        );
        validate_logical_id(&request.writer_family, "writer family")?;
        validate_logical_id(&request.logical_file_id, "logical file id")?;
        validate_logical_id(&request.mutation_id, "logical file mutation id")?;
        let profile = local_erasure_profile(&request.pipeline_policy.erasure_profile_id)?;
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
            let block_plain_hash = format!("sha256:{}", sha256_hex(&stored_source));
            let pipeline_block = self.encrypt_pipeline_block(
                &request.pipeline_policy,
                &request.logical_file_id,
                0,
                0,
                plaintext_len,
                &block_plain_hash,
                stored_source,
            )?;
            let object_ref = self
                .put_logical_file_block_with_profile(
                    &request,
                    0,
                    pipeline_block.stored,
                    block_plain_hash.clone(),
                    pipeline_block.encryption.algorithm.clone(),
                    profile,
                )
                .await?;
            let object_manifest = self.read_object_manifest(&object_ref).await?;
            (
                vec![MaterializedLogicalBlock {
                    object_manifest,
                    logical_offset: 0,
                    logical_length: plaintext_len,
                    compressed_length: compression.compressed_length,
                    plaintext_hash: plaintext_hash.clone(),
                    encryption: pipeline_block.encryption,
                }],
                compression,
            )
        };
        let manifest = logical_file_manifest_from_object_manifests(
            &request,
            &blocks,
            plaintext_hash,
            plaintext_len,
            compression,
        )?;
        let locator = self
            .publish_logical_file_manifest(&manifest, &request.pipeline_policy)
            .await?;
        Ok(CoreLogicalFileWrite { manifest, locator })
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
            let empty_hash = format!("sha256:{}", sha256_hex(&[]));
            let pipeline_block = self.encrypt_pipeline_block(
                &request.pipeline_policy,
                &request.logical_file_id,
                0,
                0,
                0,
                &empty_hash,
                Vec::new(),
            )?;
            let object_ref = self
                .put_logical_file_block_with_profile(
                    request,
                    0,
                    pipeline_block.stored,
                    empty_hash.clone(),
                    pipeline_block.encryption.algorithm.clone(),
                    profile,
                )
                .await?;
            let object_manifest = self.read_object_manifest(&object_ref).await?;
            blocks.push(MaterializedLogicalBlock {
                object_manifest,
                logical_offset: 0,
                logical_length: 0,
                compressed_length: 0,
                plaintext_hash: empty_hash,
                encryption: pipeline_block.encryption,
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
            let pipeline_block = self.encrypt_pipeline_block(
                &request.pipeline_policy,
                &request.logical_file_id,
                index,
                logical_offset,
                chunk.len() as u64,
                &chunk_hash,
                chunk_bytes,
            )?;
            let object_ref = self
                .put_logical_file_block_with_profile(
                    request,
                    index,
                    pipeline_block.stored,
                    chunk_hash.clone(),
                    pipeline_block.encryption.algorithm.clone(),
                    profile,
                )
                .await?;
            let object_manifest = self.read_object_manifest(&object_ref).await?;
            blocks.push(MaterializedLogicalBlock {
                object_manifest,
                logical_offset,
                logical_length: chunk.len() as u64,
                compressed_length: chunk.len() as u64,
                plaintext_hash: chunk_hash,
                encryption: pipeline_block.encryption,
            });
        }
        Ok(blocks)
    }

    pub async fn write_logical_file_ref(
        &self,
        request: WriteLogicalFileRequest,
    ) -> Result<CoreObjectRef> {
        let manifest = self
            .write_logical_file_with_locator(request)
            .await?
            .manifest;
        Ok(core_object_ref_from_logical_file_manifest(&manifest))
    }

    async fn publish_logical_file_manifest(
        &self,
        manifest: &CoreLogicalFileManifest,
        policy: &CorePipelinePolicy,
    ) -> Result<CoreManifestLocator> {
        validate_logical_file_manifest_shape(manifest)?;
        let manifest_bytes = serde_json::to_vec(manifest)?;
        let manifest_hash = format!("sha256:{}", sha256_hex(&manifest_bytes));
        let manifest_hash_hex = strip_sha256_prefix(&manifest_hash)?;
        let profile = local_erasure_profile(&policy.erasure_profile_id)?;
        let manifest_block_ref = self
            .materialise_object_blob_bytes(
                &format!("lf_manifest_{manifest_hash_hex}"),
                manifest.writer_generation,
                0,
                &manifest_hash,
                manifest_hash_hex,
                &manifest_bytes,
                &[],
                &format!(
                    "manifest_{}",
                    sha256_hex(manifest.created_by_mutation_id.as_bytes())
                ),
                profile,
                "none",
                "core_control",
            )
            .await?;
        manifest_locator_from_manifest_and_ref(manifest, &manifest_block_ref, &manifest_hash)
    }

    fn encrypt_pipeline_block(
        &self,
        policy: &CorePipelinePolicy,
        logical_file_id: &str,
        _block_index: usize,
        logical_offset: u64,
        logical_length: u64,
        plaintext_hash: &str,
        plaintext: Vec<u8>,
    ) -> Result<PipelineBlockBytes> {
        match policy.encryption.as_str() {
            "none" => {
                let ciphertext_hash = format!("sha256:{}", sha256_hex(&plaintext));
                Ok(PipelineBlockBytes {
                    stored: plaintext,
                    encryption: none_encryption_descriptor(plaintext_hash, &ciphertext_hash),
                })
            }
            "aes_gcm_siv" => {
                let keyring = self.pipeline_keyring.as_ref().ok_or_else(|| {
                    anyhow!(
                        "CoreStore aes_gcm_siv pipeline encryption requires a configured keyring"
                    )
                })?;
                let cipher = <Aes256GcmSiv as aes_gcm_siv::aead::KeyInit>::new_from_slice(
                    keyring.active_key()?,
                )
                .map_err(|err| anyhow!(err.to_string()))?;
                let nonce = Aes256GcmSiv::generate_nonce(&mut OsRng);
                let aad = pipeline_block_aad(
                    logical_file_id,
                    logical_offset,
                    logical_length,
                    plaintext_hash,
                );
                let ciphertext = cipher
                    .encrypt(
                        &nonce,
                        Payload {
                            msg: &plaintext,
                            aad: &aad,
                        },
                    )
                    .map_err(|err| anyhow!(err.to_string()))?;
                let aad_hash = format!("sha256:{}", sha256_hex(&aad));
                let ciphertext_hash = format!("sha256:{}", sha256_hex(&ciphertext));
                #[allow(deprecated)]
                let nonce_bytes = nonce.as_slice().to_vec();
                let descriptor_hash = encryption_descriptor_hash(
                    "aes_gcm_siv",
                    keyring.active_key_id(),
                    &nonce_bytes,
                    &aad_hash,
                    plaintext_hash,
                    &ciphertext_hash,
                );
                Ok(PipelineBlockBytes {
                    stored: ciphertext,
                    encryption: CoreEncryptionDescriptor {
                        algorithm: "aes_gcm_siv".to_string(),
                        key_id: keyring.active_key_id().to_string(),
                        nonce: nonce_bytes,
                        aad_hash,
                        plaintext_hash: plaintext_hash.to_string(),
                        ciphertext_hash,
                        descriptor_hash,
                    },
                })
            }
            other => bail!("CoreStore unsupported logical file encryption policy {other}"),
        }
    }

    fn decrypt_pipeline_block(
        &self,
        logical_file_id: &str,
        block: &CoreLogicalBlockRef,
        stored: Vec<u8>,
    ) -> Result<Vec<u8>> {
        match block.encryption.algorithm.as_str() {
            "none" => {
                let actual_hash = format!("sha256:{}", sha256_hex(&stored));
                if actual_hash != block.encryption.ciphertext_hash {
                    bail!(
                        "CoreStore unencrypted block hash mismatch: expected {}, got {}",
                        block.encryption.ciphertext_hash,
                        actual_hash
                    );
                }
                Ok(stored)
            }
            "aes_gcm_siv" => {
                if block.encryption.nonce.len() != CORE_PIPELINE_NONCE_LEN {
                    bail!("CoreStore aes_gcm_siv block nonce has invalid length");
                }
                let keyring = self.pipeline_keyring.as_ref().ok_or_else(|| {
                    anyhow!(
                        "CoreStore aes_gcm_siv pipeline decryption requires a configured keyring"
                    )
                })?;
                let actual_ciphertext_hash = format!("sha256:{}", sha256_hex(&stored));
                if actual_ciphertext_hash != block.encryption.ciphertext_hash {
                    bail!(
                        "CoreStore encrypted block ciphertext hash mismatch: expected {}, got {}",
                        block.encryption.ciphertext_hash,
                        actual_ciphertext_hash
                    );
                }
                let aad = pipeline_block_aad(
                    logical_file_id,
                    block.logical_offset,
                    block.logical_length,
                    &block.encryption.plaintext_hash,
                );
                let aad_hash = format!("sha256:{}", sha256_hex(&aad));
                if aad_hash != block.encryption.aad_hash {
                    bail!("CoreStore encrypted block AAD hash mismatch");
                }
                let expected_descriptor_hash = encryption_descriptor_hash(
                    "aes_gcm_siv",
                    &block.encryption.key_id,
                    &block.encryption.nonce,
                    &block.encryption.aad_hash,
                    &block.encryption.plaintext_hash,
                    &block.encryption.ciphertext_hash,
                );
                if expected_descriptor_hash != block.encryption.descriptor_hash {
                    bail!("CoreStore encrypted block descriptor hash mismatch");
                }
                let cipher = <Aes256GcmSiv as aes_gcm_siv::aead::KeyInit>::new_from_slice(
                    keyring.key(&block.encryption.key_id)?,
                )
                .map_err(|err| anyhow!(err.to_string()))?;
                #[allow(deprecated)]
                let nonce = Nonce::from_slice(&block.encryption.nonce);
                let plaintext = cipher
                    .decrypt(
                        nonce,
                        Payload {
                            msg: &stored,
                            aad: &aad,
                        },
                    )
                    .map_err(|err| anyhow!(err.to_string()))?;
                let plaintext_hash = format!("sha256:{}", sha256_hex(&plaintext));
                if plaintext_hash != block.encryption.plaintext_hash {
                    bail!(
                        "CoreStore encrypted block plaintext hash mismatch: expected {}, got {}",
                        block.encryption.plaintext_hash,
                        plaintext_hash
                    );
                }
                Ok(plaintext)
            }
            other => bail!("CoreStore unsupported logical file encryption descriptor {other}"),
        }
    }

    async fn write_control_logical_file_ref(
        &self,
        writer_family: &str,
        generation: u64,
        logical_file_id: String,
        bytes: Vec<u8>,
        mutation_id: String,
        region_id: String,
    ) -> Result<CoreObjectRef> {
        self.write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: writer_family.to_string(),
            generation,
            logical_file_id,
            source: bytes,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id,
            region_id,
        })
        .await
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

    pub async fn read_logical_file_manifest(
        &self,
        locator: &CoreManifestLocator,
    ) -> Result<CoreLogicalFileManifest> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "read_logical_file_manifest")],
        );
        validate_manifest_locator(locator)?;
        self.verify_manifest_locator_receipts(locator)?;
        let bytes = self.read_manifest_locator_bytes(locator).await?;
        if bytes.len() as u64 != locator.manifest_length {
            bail!("CoreStore manifest locator length mismatch");
        }
        let actual_hash = format!("sha256:{}", sha256_hex(&bytes));
        if actual_hash != locator.manifest_hash || actual_hash != locator.manifest_ref.manifest_hash
        {
            bail!("CoreStore manifest locator hash mismatch");
        }
        let manifest: CoreLogicalFileManifest = serde_json::from_slice(&bytes)?;
        validate_logical_file_manifest_shape(&manifest)?;
        if manifest.logical_file_id != locator.manifest_ref.logical_file_id
            || manifest.writer_family != locator.manifest_ref.writer_family
            || manifest.writer_generation != locator.manifest_ref.writer_generation
        {
            bail!("CoreStore manifest locator identity mismatch");
        }
        Ok(manifest)
    }

    async fn read_manifest_locator_bytes(&self, locator: &CoreManifestLocator) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(locator.manifest_length as usize);
        for block in &locator.block_locators {
            let object_ref = object_ref_from_manifest_block_locator(block)?;
            let block_bytes = self.get_blob(GetBlob { object_ref }).await?;
            let expected_len = block.logical_end.saturating_sub(block.logical_start);
            if block_bytes.len() as u64 != expected_len {
                bail!("CoreStore manifest locator block length mismatch");
            }
            let block_hash = format!("sha256:{}", sha256_hex(&block_bytes));
            if block_hash != block.block_plain_hash {
                bail!("CoreStore manifest locator block plain hash mismatch");
            }
            out.extend_from_slice(&block_bytes);
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
            if block.encryption.algorithm == "none" {
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
            } else {
                let block_plaintext = self.read_logical_block_plaintext(manifest, block).await?;
                let start = usize::try_from(overlap_start - block_start)
                    .map_err(|_| anyhow!("CoreStore logical block range start exceeds usize"))?;
                let end = usize::try_from(overlap_end - block_start)
                    .map_err(|_| anyhow!("CoreStore logical block range end exceeds usize"))?;
                out.extend_from_slice(&block_plaintext[start..end]);
            }
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
            let block = manifest
                .blocks
                .first()
                .ok_or_else(|| anyhow!("CoreStore compressed logical file has no block"))?;
            self.read_logical_block_plaintext(manifest, block).await?
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

    async fn read_logical_block_plaintext(
        &self,
        manifest: &CoreLogicalFileManifest,
        block: &CoreLogicalBlockRef,
    ) -> Result<Vec<u8>> {
        let object_ref = object_ref_from_logical_block_ref(block, &manifest.erasure_profile_id)?;
        let stored = self.get_blob(GetBlob { object_ref }).await?;
        let plaintext = self.decrypt_pipeline_block(&manifest.logical_file_id, block, stored)?;
        if plaintext.len() as u64 != block.compressed_length {
            bail!(
                "CoreStore decrypted block length mismatch: expected {}, got {}",
                block.compressed_length,
                plaintext.len()
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
            .write_control_logical_file_ref(
                "core_control",
                schema.generation,
                format!(
                    "boundary_schema/bucket:{}/generation:{}",
                    schema.bucket, schema.generation
                ),
                bytes,
                input.mutation_id.clone(),
                "local".to_string(),
            )
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
            Ok(outcome) => {
                let result = outcome
                    .state_locator
                    .as_ref()
                    .map(|locator| serde_json::json!({ "stream_state_locator": locator }));
                self.mark_core_wal_finalised_with_result_unlocked(&admission, "committed", result)
                    .await?;
                Ok(outcome.receipt)
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
    ) -> Result<StreamAppendOutcome> {
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
    ) -> Result<StreamAppendOutcome> {
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
            return Ok(StreamAppendOutcome {
                receipt,
                state_locator: None,
            });
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
        let state_locator = self
            .write_stream_records(&input.stream_id, &records)
            .await?;
        if let Some(locator) = state_locator.as_ref() {
            self.append_stream_state_locator_record(&record, locator)
                .await?;
        }
        Ok(StreamAppendOutcome {
            receipt: StreamAppendReceipt {
                stream_id: record.stream_id,
                sequence: record.sequence,
                cursor: record.cursor,
                event_hash: record.event_hash,
                idempotent_replay: false,
            },
            state_locator,
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
            .write_control_logical_file_ref(
                "core_control",
                next_token,
                ref_name.clone(),
                record_bytes,
                format!(
                    "core-fence:{}:{}:{}",
                    input.fence_name, next_token, record_hash
                ),
                "local".to_string(),
            )
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
            .write_control_logical_file_ref(
                "core_control",
                input.fence_token,
                ref_name.clone(),
                serde_json::to_vec(&released)?,
                format!(
                    "core-fence-release:{}:{}",
                    input.fence_name, input.fence_token
                ),
                "local".to_string(),
            )
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
            .write_control_logical_file_ref(
                "mesh_control",
                catalog.generation,
                format!("mesh:{}/system/mesh/root_catalog", catalog.mesh_id),
                serde_json::to_vec(&catalog)?,
                format!(
                    "root-catalog:{}:{}:{}",
                    catalog.mesh_id, catalog.generation, catalog_hash
                ),
                root_catalog_region(&catalog),
            )
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
            .write_control_logical_file_ref(
                "core_control",
                profile.epoch,
                format!(
                    "mesh:local/system/quorum/{}/epoch:{}",
                    profile.placement_group, profile.epoch
                ),
                serde_json::to_vec(&profile)?,
                format!(
                    "quorum-profile:{}:{}:{profile_hash}",
                    profile.placement_group, profile.epoch
                ),
                "local".to_string(),
            )
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
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
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
        self.mark_core_wal_finalised_with_result_unlocked(admission, state, None)
            .await
    }

    async fn mark_core_wal_finalised_with_result_unlocked(
        &self,
        admission: &CoreWalAdmissionRecord,
        state: &str,
        result: Option<serde_json::Value>,
    ) -> Result<()> {
        let _transaction_guard = self.acquire_stream_lock(CORE_TRANSACTION_STREAM_ID).await?;
        let admission_key = CoreWalRecordKey::from(admission);
        for record in self
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
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
            if existing.mutation_id == admission.mutation_id
                && existing.state == state
                && existing.result == result
            {
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
            result,
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
            let replay = match self
                .replay_core_wal_record_unlocked(&record, &payload)
                .await
            {
                Ok(replay) => replay,
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
            if let Err(error) = self
                .mark_core_wal_finalised_with_result_unlocked(&record, replay.state, replay.result)
                .await
            {
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
    ) -> Result<CoreWalReplayOutcome> {
        match record.operation_family.as_str() {
            "object.put" => {
                let profile_id = json_required_string(&record.target, "erasure_profile_id")?;
                let profile = local_erasure_profile(&profile_id)?;
                let logical_name = json_required_string(&record.target, "logical_name")?;
                let block_plain_hash = json_required_string(&record.target, "block_plain_hash")?;
                let writer_generation = json_required_u64(&record.target, "writer_generation")?;
                let block_ordinal = json_required_u64(&record.target, "block_ordinal")?;
                let encryption = json_optional_string(&record.target, "encryption")?
                    .unwrap_or_else(|| "none".to_string());
                let materialised_bytes = self.core_wal_payload_bytes(record, payload).await?;
                let hash = sha256_hex(&materialised_bytes);
                if let Some(landed) = record.landed_bytes.first() {
                    let landed_hash = strip_sha256_prefix(&landed.sha256)?;
                    if landed_hash != hash {
                        bail!("CoreStore WAL object.put landed hash mismatch");
                    }
                }
                self.materialise_object_blob_bytes(
                    &logical_name,
                    writer_generation,
                    block_ordinal,
                    &block_plain_hash,
                    &hash,
                    &materialised_bytes,
                    &record.boundary_values,
                    &record.mutation_id,
                    profile,
                    &encryption,
                    &record.writer_family,
                )
                .await?;
                Ok(CoreWalReplayOutcome {
                    state: "committed",
                    result: None,
                })
            }
            "stream.append" => {
                let stream_id = json_required_string(&record.target, "stream_id")?;
                let partition_id = json_required_string(&record.target, "partition_id")?;
                let record_kind = json_required_string(&record.target, "record_kind")?;
                let transaction_id = json_optional_string(&record.target, "transaction_id")?;
                let payload = self.core_wal_payload_bytes(record, payload).await?;
                let outcome = self
                    .append_stream_unlocked_with_idempotency_hash(
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
                Ok(CoreWalReplayOutcome {
                    state: "committed",
                    result: outcome
                        .state_locator
                        .as_ref()
                        .map(|locator| serde_json::json!({ "stream_state_locator": locator })),
                })
            }
            "mutation.batch" => {
                let payload = self.core_wal_payload_bytes(record, payload).await?;
                let batch: CoreMutationBatch = serde_json::from_slice(&payload)?;
                let receipt = self.recover_admitted_mutation_batch_unlocked(batch).await?;
                Ok(CoreWalReplayOutcome {
                    state: core_transaction_state_name(receipt.state),
                    result: None,
                })
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
                    return Ok(CoreWalReplayOutcome {
                        state: "committed",
                        result: None,
                    });
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
                Ok(CoreWalReplayOutcome {
                    state: "committed",
                    result: None,
                })
            }
            "ref.delete" => {
                let ref_name = json_required_string(&record.target, "ref_name")?;
                let expected_generation = json_optional_u64(&record.target, "expected_generation")?;
                let expected_target = json_optional_string(&record.target, "expected_target")?;
                let require_present = json_required_bool(&record.target, "require_present")?;
                let transaction_id = json_optional_string(&record.target, "transaction_id")?;
                let current = self.read_ref(&ref_name).await?;
                let Some(previous) = current else {
                    return Ok(CoreWalReplayOutcome {
                        state: "committed",
                        result: None,
                    });
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
                Ok(CoreWalReplayOutcome {
                    state: "committed",
                    result: None,
                })
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
        let mut ids = BTreeSet::new();
        if CORE_TRANSACTION_STREAM_ID.starts_with(prefix)
            && !self
                .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
                .await?
                .is_empty()
        {
            ids.insert(CORE_TRANSACTION_STREAM_ID.to_string());
        }
        for record in self
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await?
        {
            if record.record_kind == CORE_STREAM_STATE_LOCATOR_RECORD_KIND {
                let state: StoredStreamStateLocatorRecord =
                    serde_json::from_slice(&record.payload)?;
                if state.schema == CORE_STREAM_STATE_LOCATOR_SCHEMA
                    && state.stream_id.starts_with(prefix)
                {
                    ids.insert(state.stream_id);
                }
                continue;
            }
            if record.record_kind != CORE_WAL_FINALISATION_RECORD_KIND {
                continue;
            }
            let finalisation: CoreWalFinalisationRecord = serde_json::from_slice(&record.payload)?;
            if finalisation.operation_family != "stream.append" || finalisation.state != "committed"
            {
                continue;
            }
            let stream_id = json_required_string(&finalisation.target, "stream_id")?;
            if stream_id.starts_with(prefix) {
                ids.insert(stream_id);
            }
        }
        Ok(ids.into_iter().collect())
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
                    .map(|outcome| CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: outcome.receipt.sequence,
                        prepared_record_hash: outcome.receipt.event_hash,
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
                    .map(|outcome| CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: outcome.receipt.sequence,
                        prepared_record_hash: outcome.receipt.event_hash,
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
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
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
        self.reconstruct_object_manifest_from_shards(object_ref)
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
    ) -> Result<CoreObjectManifest> {
        self.reconstruct_object_manifest_from_shards_with_required_indices(object_ref, None)
            .await
    }

    async fn reconstruct_object_manifest_from_shards_with_required_indices(
        &self,
        object_ref: &CoreObjectRef,
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
        let mut boundary_values = None::<Vec<CoreBoundaryValue>>;
        let mut stripe_size = 0u64;
        let expected_block_id = object_ref.encoding.block_id.clone();
        validate_logical_id(&expected_block_id, "CoreStore object block id")?;
        for placement in &object_ref.placements {
            if usize::from(placement.shard_index) >= profile.total_shards() {
                bail!("CoreStore object ref contains shard index outside profile");
            }
            self.verify_object_placement_receipt(&expected_block_id, profile.id, placement)?;
            let path = self.shard_path(
                &placement.node_id,
                &expected_block_id,
                placement.shard_index,
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
            if boundary_values.is_none() {
                boundary_values = Some(decoded.boundary_values.clone());
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

        let boundary_values = boundary_values.unwrap_or_default();
        Ok(CoreObjectManifest {
            schema: CORE_OBJECT_MANIFEST_SCHEMA.to_string(),
            mesh_id: "local-mesh".to_string(),
            region_id: "local".to_string(),
            object_hash: object_ref.hash.clone(),
            logical_size: object_ref.logical_size,
            boundary_values,
            encoding: CoreObjectEncoding {
                block_id: object_ref.encoding.block_id.clone(),
                profile_id: profile.id.to_string(),
                data_shards: profile.data_shards as u16,
                parity_shards: profile.parity_shards as u16,
                minimum_read_shards: profile.minimum_read_shards as u16,
                minimum_write_ack_shards: profile.minimum_write_ack_shards as u16,
                stripe_size,
                placement_scope: "region".to_string(),
                repair_priority: "normal".to_string(),
                encryption: object_ref.encoding.encryption.clone(),
            },
            placements,
            created_at: "reconstructed-from-shards".to_string(),
            mutation_id: format!("reconstructed:{}", object_ref.hash),
        })
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
        if stream_id != CORE_TRANSACTION_STREAM_ID {
            let Some(locator) = self.latest_stream_state_locator(stream_id).await? else {
                return Ok(Vec::new());
            };
            let manifest = self.read_logical_file_manifest(&locator).await?;
            let bytes = self.read_logical_file_plaintext(&manifest).await?;
            return decode_active_stream_records(stream_id, &bytes)
                .with_context(|| format!("decode CoreStore stream state {stream_id}"));
        }
        self.read_direct_stream_records(stream_id).await
    }

    async fn read_direct_stream_records(&self, stream_id: &str) -> Result<Vec<StreamRecord>> {
        if stream_id != CORE_TRANSACTION_STREAM_ID {
            bail!(
                "CoreStore direct stream reads are reserved for the root-anchored transaction stream"
            );
        }
        self.read_core_transaction_stream_records_from_root().await
    }

    async fn read_stream_records_after(
        &self,
        stream_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>> {
        if stream_id != CORE_TRANSACTION_STREAM_ID {
            let records = self.read_all_stream_records(stream_id).await?;
            let filtered = records
                .into_iter()
                .filter(|record| record.sequence > after_sequence)
                .collect::<Vec<_>>();
            if limit > 0 {
                return Ok(filtered.into_iter().take(limit).collect());
            }
            return Ok(filtered);
        }
        if stream_id != CORE_TRANSACTION_STREAM_ID {
            bail!(
                "CoreStore direct stream paging is reserved for the root-anchored transaction stream"
            );
        }
        let records = self
            .read_core_transaction_stream_records_from_root()
            .await?;
        let filtered = records
            .into_iter()
            .filter(|record| record.sequence > after_sequence)
            .collect::<Vec<_>>();
        if limit > 0 {
            return Ok(filtered.into_iter().take(limit).collect());
        }
        Ok(filtered)
    }

    async fn latest_stream_state_locator(
        &self,
        stream_id: &str,
    ) -> Result<Option<CoreManifestLocator>> {
        validate_logical_id(stream_id, "stream id")?;
        let mut latest = None::<(u64, CoreManifestLocator)>;
        for record in self
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await?
        {
            if record.record_kind == CORE_STREAM_STATE_LOCATOR_RECORD_KIND {
                let state: StoredStreamStateLocatorRecord =
                    serde_json::from_slice(&record.payload)?;
                if state.schema != CORE_STREAM_STATE_LOCATOR_SCHEMA || state.stream_id != stream_id
                {
                    continue;
                }
                validate_manifest_locator(&state.locator)?;
                if latest
                    .as_ref()
                    .is_none_or(|(sequence, _)| state.sequence > *sequence)
                {
                    latest = Some((state.sequence, state.locator));
                }
                continue;
            }
            if record.record_kind != CORE_WAL_FINALISATION_RECORD_KIND {
                continue;
            }
            let finalisation: CoreWalFinalisationRecord = serde_json::from_slice(&record.payload)?;
            if finalisation.operation_family != "stream.append"
                || finalisation.state != "committed"
                || json_required_string(&finalisation.target, "stream_id")? != stream_id
            {
                continue;
            }
            let Some(result) = finalisation.result else {
                continue;
            };
            let Some(locator_value) = result.get("stream_state_locator") else {
                continue;
            };
            let locator: CoreManifestLocator = serde_json::from_value(locator_value.clone())?;
            validate_manifest_locator(&locator)?;
            if latest
                .as_ref()
                .is_none_or(|(sequence, _)| record.sequence > *sequence)
            {
                latest = Some((record.sequence, locator));
            }
        }
        Ok(latest.map(|(_, locator)| locator))
    }

    async fn append_stream_state_locator_record(
        &self,
        record: &StreamRecord,
        locator: &CoreManifestLocator,
    ) -> Result<()> {
        if record.stream_id == CORE_TRANSACTION_STREAM_ID {
            return Ok(());
        }
        let state = StoredStreamStateLocatorRecord {
            schema: CORE_STREAM_STATE_LOCATOR_SCHEMA.to_string(),
            stream_id: record.stream_id.clone(),
            sequence: record.sequence,
            event_hash: record.event_hash.clone(),
            locator: locator.clone(),
        };
        self.append_transaction_stream_record_direct(
            CORE_STREAM_STATE_LOCATOR_RECORD_KIND,
            serde_json::to_vec(&state)?,
            Some(format!(
                "{}:{}:{}:{}",
                CORE_STREAM_STATE_LOCATOR_RECORD_KIND,
                record.stream_id,
                record.sequence,
                record.event_hash
            )),
        )
        .await?;
        Ok(())
    }

    async fn append_transaction_stream_record_direct(
        &self,
        record_kind: &str,
        payload: Vec<u8>,
        idempotency_key: Option<String>,
    ) -> Result<StreamAppendReceipt> {
        let idempotency_key_hash = idempotency_key
            .as_deref()
            .map(|key| format!("sha256:{}", sha256_hex(key.as_bytes())));
        if let Some(receipt) = self
            .stream_idempotent_replay_by_hash_unlocked(
                CORE_TRANSACTION_STREAM_ID,
                &payload,
                idempotency_key_hash.as_deref(),
            )
            .await?
        {
            return Ok(receipt);
        }

        let mut records = self
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await?;
        let sequence = records
            .last()
            .map(|record| record.sequence + 1)
            .unwrap_or(1);
        let previous_event_hash = records
            .last()
            .map(|record| record.event_hash.clone())
            .unwrap_or_else(|| ZERO_HASH.to_string());
        let cursor = format!("{CORE_TRANSACTION_STREAM_ID}:{sequence:020}");
        let payload_hash = format!("sha256:{}", sha256_hex(&payload));
        let mut record = StreamRecord {
            schema: CORE_WATCH_EVENT_SCHEMA.to_string(),
            stream_id: CORE_TRANSACTION_STREAM_ID.to_string(),
            partition_id: CORE_TRANSACTION_PARTITION_ID.to_string(),
            sequence,
            cursor,
            previous_event_hash,
            event_hash: String::new(),
            record_kind: record_kind.to_string(),
            payload_hash,
            payload,
            transaction_id: None,
            idempotency_key_hash,
            created_at: now_rfc3339(),
        };
        record.event_hash = format!("sha256:{}", sha256_hex(&event_hash_input(&record)?));
        records.push(record.clone());
        self.write_stream_records(CORE_TRANSACTION_STREAM_ID, &records)
            .await?;
        Ok(StreamAppendReceipt {
            stream_id: record.stream_id,
            sequence: record.sequence,
            cursor: record.cursor,
            event_hash: record.event_hash,
            idempotent_replay: false,
        })
    }

    async fn write_stream_records(
        &self,
        stream_id: &str,
        records: &[StreamRecord],
    ) -> Result<Option<CoreManifestLocator>> {
        let bytes = encode_active_stream_records(stream_id, records)?;
        if stream_id == CORE_TRANSACTION_STREAM_ID {
            self.write_core_transaction_stream_records(records, bytes)
                .await?;
            return Ok(None);
        }
        self.write_stream_state_logical_file(stream_id, records, bytes)
            .await
            .map(Some)
    }

    async fn read_core_transaction_stream_records_from_root(&self) -> Result<Vec<StreamRecord>> {
        let Some(anchor) = self
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(transaction_manifest_locator) = anchor.transaction_manifest else {
            if anchor.root_generation == 0 {
                return Ok(Vec::new());
            }
            bail!("CoreStore non-genesis root anchor is missing transaction manifest");
        };
        validate_manifest_locator(&transaction_manifest_locator)?;
        let transaction_manifest = self
            .read_logical_file_manifest(&transaction_manifest_locator)
            .await?;
        let transaction_manifest_bytes = self
            .read_logical_file_plaintext(&transaction_manifest)
            .await?;
        let transaction = decode_transaction_manifest_record(&transaction_manifest_bytes)?;
        validate_transaction_manifest_record(&transaction, anchor.root_generation)?;
        let state_locator = transaction
            .logical_manifests
            .first()
            .ok_or_else(|| anyhow!("CoreStore transaction manifest has no logical manifests"))?;
        validate_manifest_locator(state_locator)?;
        let manifest = self.read_logical_file_manifest(state_locator).await?;
        let bytes = self.read_logical_file_plaintext(&manifest).await?;
        decode_active_stream_records(CORE_TRANSACTION_STREAM_ID, &bytes)
            .with_context(|| "decode root-anchored CoreStore transaction stream")
    }

    async fn write_core_transaction_stream_records(
        &self,
        records: &[StreamRecord],
        bytes: Vec<u8>,
    ) -> Result<()> {
        let root_anchor_key = core_transaction_root_anchor_key();
        let current = self.read_latest_root_anchor(root_anchor_key).await?;
        let pre_root_generation = current
            .as_ref()
            .map(|anchor| anchor.root_generation)
            .unwrap_or(0);
        let post_root_generation = pre_root_generation.saturating_add(1);
        let state_locator = self
            .write_stream_state_logical_file(CORE_TRANSACTION_STREAM_ID, records, bytes)
            .await?;
        let transaction = CoreTransactionManifestRecord {
            schema: "anvil.core.transaction_manifest.v1".to_string(),
            mutation_ids: records.iter().map(|record| record.cursor.clone()).collect(),
            idempotency_key_hashes: records
                .iter()
                .filter_map(|record| record.idempotency_key_hash.clone())
                .collect(),
            pre_root_generation,
            post_root_generation,
            logical_manifests: vec![state_locator],
            ref_updates: Vec::new(),
            tombstones: Vec::new(),
            writer_checkpoints: Vec::new(),
            boundary_schema_refs: Vec::new(),
        };
        let transaction_manifest = self
            .write_logical_bytes_direct(
                "core_control",
                format!("lf_core_transaction_manifest_{post_root_generation:020}"),
                post_root_generation,
                encode_transaction_manifest_record(&transaction)?,
                format!("core_transaction_manifest_{post_root_generation:020}"),
                "local".to_string(),
            )
            .await?;
        self.publish_core_transaction_root_anchor(
            records,
            transaction_manifest,
            current.as_ref(),
            post_root_generation,
        )
        .await
    }

    async fn publish_core_transaction_root_anchor(
        &self,
        records: &[StreamRecord],
        transaction_manifest: CoreManifestLocator,
        current: Option<&CoreRootAnchorRecord>,
        root_generation: u64,
    ) -> Result<()> {
        let root_anchor_key = core_transaction_root_anchor_key();
        let root_key_hash = root_key_hash(root_anchor_key);
        let previous_root_hash = current
            .map(hash_root_anchor_record)
            .transpose()?
            .unwrap_or_else(|| ZERO_HASH.to_string());
        let writer_families = records
            .iter()
            .map(|record| {
                if record.stream_id == CORE_TRANSACTION_STREAM_ID {
                    "core_control".to_string()
                } else {
                    record.partition_id.clone()
                }
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let final_block_count = transaction_manifest
            .block_locators
            .iter()
            .map(|block| block.data_shards + block.parity_shards)
            .sum::<u32>() as u64;
        let anchor = CoreRootAnchorRecord {
            schema: "anvil.core.root_anchor.v1".to_string(),
            root_anchor_key: root_anchor_key.to_string(),
            root_key_hash,
            root_generation,
            previous_root_hash,
            transaction_manifest: Some(transaction_manifest),
            checkpoint_manifest: None,
            publisher_node_id: CORE_WAL_NODE_ID.to_string(),
            publisher_epoch: LOCAL_PLACEMENT_EPOCH,
            partition_owner_fence: LOCAL_PLACEMENT_EPOCH,
            created_at_unix_nanos: unix_timestamp_nanos(),
            root_state: "committed".to_string(),
            mutation_first: records.first().map(|record| record.cursor.clone()),
            mutation_last: records.last().map(|record| record.cursor.clone()),
            writer_families,
            manifest_count: 1,
            final_block_count,
        };
        self.write_root_register_anchor(&anchor).await
    }

    async fn bootstrap_system_root_anchor(&self) -> Result<()> {
        let root_anchor_key = core_transaction_root_anchor_key();
        if self
            .read_latest_root_anchor(root_anchor_key)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let anchor = CoreRootAnchorRecord {
            schema: "anvil.core.root_anchor.v1".to_string(),
            root_anchor_key: root_anchor_key.to_string(),
            root_key_hash: root_key_hash(root_anchor_key),
            root_generation: 0,
            previous_root_hash: ZERO_HASH.to_string(),
            transaction_manifest: None,
            checkpoint_manifest: None,
            publisher_node_id: CORE_WAL_NODE_ID.to_string(),
            publisher_epoch: LOCAL_PLACEMENT_EPOCH,
            partition_owner_fence: LOCAL_PLACEMENT_EPOCH,
            created_at_unix_nanos: unix_timestamp_nanos(),
            root_state: "committed".to_string(),
            mutation_first: None,
            mutation_last: None,
            writer_families: vec!["core_control".to_string()],
            manifest_count: 0,
            final_block_count: 0,
        };
        self.write_root_register_anchor(&anchor).await
    }

    async fn read_latest_root_anchor(
        &self,
        root_anchor_key: &str,
    ) -> Result<Option<CoreRootAnchorRecord>> {
        let root_key_hash = root_key_hash(root_anchor_key);
        let root_hash_hex = strip_sha256_prefix(&root_key_hash)?;
        let root_dir = self
            .root_register_hash_dir(&root_key_hash)?
            .join(root_hash_hex);
        let entries = match std::fs::read_dir(&root_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| format!("read {}", root_dir.display()));
            }
        };
        let mut generations = Vec::new();
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(raw) = name.strip_prefix("generation-") else {
                continue;
            };
            if let Ok(generation) = raw.parse::<u64>() {
                generations.push(generation);
            }
        }
        generations.sort_unstable_by(|left, right| right.cmp(left));
        for generation in generations {
            if let Some(anchor) = self
                .read_committed_root_anchor_generation(&root_key_hash, generation)
                .await?
            {
                if anchor.root_anchor_key != root_anchor_key {
                    bail!("CoreStore root register anchor key mismatch");
                }
                return Ok(Some(anchor));
            }
        }
        Ok(None)
    }

    async fn read_committed_root_anchor_generation(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<CoreRootAnchorRecord>> {
        let generation_dir = self.root_register_generation_dir(root_key_hash, generation)?;
        let entries = match std::fs::read_dir(&generation_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| format!("read {}", generation_dir.display()));
            }
        };
        let mut by_hash = BTreeMap::<String, (CoreRootAnchorRecord, usize)>::new();
        for entry in entries {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_none_or(|extension| extension != "anr")
            {
                continue;
            }
            let bytes = read_file(&entry.path(), "core_store", "read_root_register_shard").await?;
            let (header, anchor) = decode_root_register_shard_file(&bytes)?;
            if header.root_key_hash != root_key_hash || header.root_generation != generation {
                continue;
            }
            let anchor_hash = hash_root_anchor_record(&anchor)?;
            if anchor_hash != header.root_anchor_hash {
                bail!("CoreStore root register shard anchor hash mismatch");
            }
            by_hash
                .entry(anchor_hash)
                .and_modify(|(_, count)| *count += 1)
                .or_insert((anchor, 1));
        }
        Ok(by_hash.into_values().find_map(
            |(anchor, count)| {
                if count >= 2 { Some(anchor) } else { None }
            },
        ))
    }

    async fn write_root_register_anchor(&self, anchor: &CoreRootAnchorRecord) -> Result<()> {
        validate_root_anchor_record(anchor)?;
        let anchor_bytes = encode_root_anchor_record(anchor)?;
        let root_anchor_hash = format!("sha256:{}", sha256_hex(&anchor_bytes));
        match self
            .read_latest_root_anchor(&anchor.root_anchor_key)
            .await?
        {
            Some(current) => {
                let current_hash = hash_root_anchor_record(&current)?;
                if anchor.root_generation < current.root_generation {
                    bail!(
                        "CoreStore root register rejected stale generation {} below current {}",
                        anchor.root_generation,
                        current.root_generation
                    );
                }
                if anchor.root_generation == current.root_generation {
                    if root_anchor_hash == current_hash {
                        return Ok(());
                    }
                    bail!(
                        "CoreStore root register rejected conflicting generation {}",
                        anchor.root_generation
                    );
                }
                if anchor.root_generation != current.root_generation.saturating_add(1) {
                    bail!("CoreStore root register generations must be contiguous");
                }
                if anchor.previous_root_hash != current_hash {
                    bail!("CoreStore root register previous hash mismatch");
                }
            }
            None => {
                if anchor.root_generation != 0 {
                    bail!("CoreStore root register first generation must be zero");
                }
                if anchor.previous_root_hash != ZERO_HASH {
                    bail!("CoreStore root register genesis previous hash must be zero");
                }
            }
        }
        let cohort_nodes = local_control_node_ids()
            .into_iter()
            .take(3)
            .collect::<Vec<_>>();
        let cohort_hash = descriptor_hash(&[
            "anvil.root.cohort.v1",
            &anchor.root_key_hash,
            &anchor.root_generation.to_string(),
            &cohort_nodes.join(","),
        ]);
        let created_at_unix_nanos = unix_timestamp_nanos();
        let generation_dir =
            self.root_register_generation_dir(&anchor.root_key_hash, anchor.root_generation)?;
        for (index, _node_id) in cohort_nodes.iter().enumerate() {
            let header = CoreRootRegisterHeader {
                schema: "anvil.core.root_register_shard.v1".to_string(),
                root_partition_id: CORE_TRANSACTION_ROOT_PARTITION_ID,
                root_key_hash: anchor.root_key_hash.clone(),
                root_generation: anchor.root_generation,
                shard_index: index as u16,
                register_cohort_nodes: cohort_nodes.clone(),
                register_cohort_hash: cohort_hash.clone(),
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                created_at_unix_nanos,
                root_anchor_hash: root_anchor_hash.clone(),
            };
            let bytes = encode_root_register_shard_file(&header, &anchor_bytes)?;
            let shard_path = generation_dir.join(format!("shard-{index}.anr"));
            write_file_create_new_or_same(&shard_path, &bytes).await?;
        }
        Ok(())
    }

    fn root_register_hash_dir(&self, root_key_hash: &str) -> Result<PathBuf> {
        let hash_hex = strip_sha256_prefix(root_key_hash)?;
        Ok(self
            .storage
            .core_store_root_path()
            .join("blocks")
            .join("register")
            .join(CORE_TRANSACTION_ROOT_PARTITION_ID.to_string())
            .join(&hash_hex[0..2]))
    }

    fn root_register_generation_dir(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<PathBuf> {
        let hash_hex = strip_sha256_prefix(root_key_hash)?;
        Ok(self
            .root_register_hash_dir(root_key_hash)?
            .join(hash_hex)
            .join(format!("generation-{generation:020}")))
    }

    async fn write_stream_state_logical_file(
        &self,
        stream_id: &str,
        records: &[StreamRecord],
        bytes: Vec<u8>,
    ) -> Result<CoreManifestLocator> {
        let last_sequence = records.last().map(|record| record.sequence).unwrap_or(0);
        let state_hash = format!("sha256:{}", sha256_hex(&bytes));
        let state_hash_hex = strip_sha256_prefix(&state_hash)?;
        let (writer_family, logical_file_id, mutation_id) =
            if stream_id == CORE_TRANSACTION_STREAM_ID {
                (
                    "core_control",
                    format!("lf_core_transaction_state_{state_hash_hex}"),
                    format!(
                        "core_transaction_state_{}",
                        sha256_hex(stream_id.as_bytes())
                    ),
                )
            } else {
                (
                    "stream",
                    format!("lf_stream_state_{state_hash_hex}"),
                    format!("stream_state_{}", sha256_hex(stream_id.as_bytes())),
                )
            };
        self.write_logical_bytes_direct(
            writer_family,
            logical_file_id,
            last_sequence,
            bytes,
            mutation_id,
            "local".to_string(),
        )
        .await
    }

    async fn write_logical_bytes_direct(
        &self,
        writer_family: &str,
        logical_file_id: String,
        generation: u64,
        bytes: Vec<u8>,
        mutation_id: String,
        region_id: String,
    ) -> Result<CoreManifestLocator> {
        let state_hash = format!("sha256:{}", sha256_hex(&bytes));
        let state_hash_hex = strip_sha256_prefix(&state_hash)?;
        let profile = local_erasure_profile(LOCAL_ERASURE_PROFILE_ID)?;
        let object_ref = self
            .materialise_object_blob_bytes(
                &logical_file_id,
                generation,
                0,
                &state_hash,
                state_hash_hex,
                &bytes,
                &[],
                &mutation_id,
                profile,
                "none",
                writer_family,
            )
            .await?;
        let object_manifest = self.read_object_manifest(&object_ref).await?;
        let block = MaterializedLogicalBlock {
            object_manifest,
            logical_offset: 0,
            logical_length: bytes.len() as u64,
            compressed_length: bytes.len() as u64,
            plaintext_hash: state_hash.clone(),
            encryption: none_encryption_descriptor(&state_hash, &state_hash),
        };
        let request = WriteLogicalFileRequest {
            writer_family: writer_family.to_string(),
            generation,
            logical_file_id,
            source: Vec::new(),
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id,
            region_id,
        };
        let manifest = logical_file_manifest_from_object_manifests(
            &request,
            &[block],
            state_hash,
            bytes.len() as u64,
            none_compression_descriptor(&bytes),
        )?;
        self.publish_logical_file_manifest(&manifest, &request.pipeline_policy)
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

    fn shard_path(&self, node_id: &str, block_id: &str, shard_index: u16) -> PathBuf {
        let block_path_hash = sha256_hex(block_id.as_bytes());
        let prefix = &block_path_hash[0..2];
        self.storage
            .core_store_root_path()
            .join("blocks")
            .join("local-cache")
            .join(LOCAL_ERASURE_SET_ID)
            .join(node_id)
            .join("block-id")
            .join(prefix)
            .join(block_path_hash)
            .join(format!("shard-{shard_index:05}-{block_id}.anb"))
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
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn local_block_id_for_logical_block(
    logical_file_id: &str,
    writer_generation: u64,
    block_ordinal: u64,
    plaintext_hash: &str,
) -> String {
    let mut hasher = Sha256::new();
    for part in [
        "anvil.block.id.v1",
        logical_file_id,
        &writer_generation.to_string(),
        &block_ordinal.to_string(),
        plaintext_hash,
    ] {
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

    let encryption = logical_file_encryption_descriptor(&logical_blocks, &plaintext_hash)?;

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
        encryption,
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
    let shard_payload_len = object_manifest
        .placements
        .iter()
        .map(|placement| placement.stored_size)
        .max()
        .unwrap_or(0);
    let data_shards = u32::from(object_manifest.encoding.data_shards);
    let parity_shards = u32::from(object_manifest.encoding.parity_shards);
    Ok(CoreLogicalBlockRef {
        block_id: object_manifest.encoding.block_id.clone(),
        logical_offset: block.logical_offset,
        logical_length: block.logical_length,
        compressed_length: block.compressed_length,
        encrypted_length: object_manifest.logical_size,
        content_hash: block.plaintext_hash.clone(),
        encryption: block.encryption.clone(),
        erasure_set_id: LOCAL_ERASURE_SET_ID.to_string(),
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
                written_at_unix_nanos: placement.written_at_unix_nanos,
                signed_payload_hash: placement.signed_payload_hash.clone(),
                signature_algorithm: placement.signature_algorithm.clone(),
                receipt_signature: placement.receipt_signature.clone(),
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

fn logical_file_encryption_descriptor(
    blocks: &[CoreLogicalBlockRef],
    file_plaintext_hash: &str,
) -> Result<CoreEncryptionDescriptor> {
    let Some(first) = blocks.first() else {
        bail!("CoreStore logical file encryption descriptor requires at least one block");
    };
    let algorithm = first.encryption.algorithm.clone();
    if blocks
        .iter()
        .any(|block| block.encryption.algorithm != algorithm)
    {
        bail!("CoreStore logical file blocks must use one encryption algorithm");
    }
    if algorithm == "none" {
        let ciphertext_hash = descriptor_hash(
            &blocks
                .iter()
                .map(|block| block.encryption.ciphertext_hash.as_str())
                .collect::<Vec<_>>(),
        );
        return Ok(CoreEncryptionDescriptor {
            algorithm,
            key_id: String::new(),
            nonce: Vec::new(),
            aad_hash: String::new(),
            plaintext_hash: file_plaintext_hash.to_string(),
            ciphertext_hash,
            descriptor_hash: descriptor_hash(&["encryption", "none", file_plaintext_hash]),
        });
    }
    if algorithm != "aes_gcm_siv" {
        bail!("CoreStore unsupported logical file encryption descriptor {algorithm}");
    }
    let key_id = first.encryption.key_id.clone();
    if blocks.iter().any(|block| block.encryption.key_id != key_id) {
        bail!("CoreStore logical file encrypted blocks must use one key id");
    }
    let block_descriptor_hash = descriptor_hash(
        &blocks
            .iter()
            .map(|block| block.encryption.descriptor_hash.as_str())
            .collect::<Vec<_>>(),
    );
    Ok(CoreEncryptionDescriptor {
        algorithm,
        key_id,
        nonce: Vec::new(),
        aad_hash: block_descriptor_hash.clone(),
        plaintext_hash: file_plaintext_hash.to_string(),
        ciphertext_hash: descriptor_hash(
            &blocks
                .iter()
                .map(|block| block.encryption.ciphertext_hash.as_str())
                .collect::<Vec<_>>(),
        ),
        descriptor_hash: descriptor_hash(&[
            "encryption",
            "aes_gcm_siv",
            file_plaintext_hash,
            &block_descriptor_hash,
        ]),
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
        validate_logical_block_encryption(block)?;
        if block.encrypted_length < block.compressed_length {
            bail!(
                "CoreStore logical file encrypted length cannot be smaller than compressed length"
            );
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
    validate_logical_file_encryption_descriptor(manifest)?;
    Ok(())
}

fn validate_logical_block_encryption(block: &CoreLogicalBlockRef) -> Result<()> {
    match block.encryption.algorithm.as_str() {
        "none" => {
            if !block.encryption.key_id.is_empty()
                || !block.encryption.nonce.is_empty()
                || !block.encryption.aad_hash.is_empty()
            {
                bail!(
                    "CoreStore none encrypted block descriptor must not carry key material fields"
                );
            }
            if block.encryption.plaintext_hash.is_empty()
                || block.encryption.ciphertext_hash.is_empty()
            {
                bail!("CoreStore none encrypted block descriptor is incomplete");
            }
            if block.encryption.ciphertext_hash != block.block_encoded_hash {
                bail!(
                    "CoreStore none encrypted block ciphertext hash must match encoded block hash"
                );
            }
        }
        "aes_gcm_siv" => {
            validate_pipeline_key_id(block.encryption.key_id.clone())?;
            if block.encryption.nonce.len() != CORE_PIPELINE_NONCE_LEN {
                bail!("CoreStore aes_gcm_siv block nonce has invalid length");
            }
            if block.encryption.aad_hash.is_empty()
                || block.encryption.plaintext_hash.is_empty()
                || block.encryption.ciphertext_hash.is_empty()
            {
                bail!("CoreStore aes_gcm_siv block descriptor is incomplete");
            }
            if block.encryption.ciphertext_hash != block.block_encoded_hash {
                bail!("CoreStore aes_gcm_siv block ciphertext hash must match encoded block hash");
            }
        }
        other => bail!("CoreStore unsupported logical file encryption descriptor {other}"),
    }
    Ok(())
}

fn validate_logical_file_encryption_descriptor(manifest: &CoreLogicalFileManifest) -> Result<()> {
    let expected = logical_file_encryption_descriptor(&manifest.blocks, &manifest.content_hash)?;
    if expected != manifest.encryption {
        bail!("CoreStore logical file encryption descriptor does not match block descriptors");
    }
    Ok(())
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
            block_id: block.block_id.clone(),
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
            encryption: block.encryption.algorithm.clone(),
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
                written_at_unix_nanos: shard.written_at_unix_nanos,
                signed_payload_hash: shard.signed_payload_hash.clone(),
                signature_algorithm: shard.signature_algorithm.clone(),
                receipt_signature: shard.receipt_signature.clone(),
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

fn manifest_locator_from_manifest_and_ref(
    manifest: &CoreLogicalFileManifest,
    manifest_object_ref: &CoreObjectRef,
    manifest_hash: &str,
) -> Result<CoreManifestLocator> {
    validate_hash(manifest_hash, "logical file manifest hash")?;
    let manifest_bytes_len = manifest_object_ref.logical_size;
    let block_locators = vec![block_locator_from_manifest_object_ref(
        manifest,
        manifest_object_ref,
        manifest_hash,
    )?];

    Ok(CoreManifestLocator {
        manifest_ref: CoreManifestRef {
            logical_file_id: manifest.logical_file_id.clone(),
            writer_family: manifest.writer_family.clone(),
            writer_generation: manifest.writer_generation,
            manifest_hash: manifest_hash.to_string(),
        },
        manifest_encoding: "writer-segment".to_string(),
        manifest_length: manifest_bytes_len,
        manifest_hash: manifest_hash.to_string(),
        block_locators,
    })
}

fn is_local_shard_node_id(node_id: &str) -> bool {
    node_id
        .strip_prefix(LOCAL_NODE_ID_PREFIX)
        .and_then(|suffix| suffix.strip_prefix('-'))
        .is_some_and(|suffix| !suffix.is_empty() && suffix.chars().all(|ch| ch.is_ascii_digit()))
}

fn validate_shard_receipt_common(
    node_id: &str,
    region_id: &str,
    cell_id: &str,
    shard_hash: &str,
    _shard_length: u64,
    fsync_sequence: u64,
    written_at_unix_nanos: u64,
    signed_payload_hash: &str,
    signature_algorithm: &str,
    receipt_signature: &[u8],
    expected_signed_payload_hash: &str,
) -> Result<()> {
    validate_logical_id(node_id, "shard receipt node id")?;
    validate_logical_id(region_id, "shard receipt region id")?;
    validate_logical_id(cell_id, "shard receipt cell id")?;
    validate_hash(shard_hash, "shard receipt hash")?;
    if fsync_sequence == 0 {
        bail!("CoreStore shard receipt fsync sequence must be nonzero");
    }
    if written_at_unix_nanos == 0 {
        bail!("CoreStore shard receipt timestamp must be nonzero");
    }
    validate_hash(signed_payload_hash, "shard receipt payload hash")?;
    if signature_algorithm != "ed25519-libp2p" {
        bail!(
            "CoreStore shard receipt uses unsupported signature algorithm {}",
            signature_algorithm
        );
    }
    if receipt_signature.is_empty() {
        bail!("CoreStore shard receipt signature must not be empty");
    }
    if signed_payload_hash != expected_signed_payload_hash {
        bail!("CoreStore shard receipt signed payload hash mismatch");
    }
    Ok(())
}

fn validate_local_shard_receipt_placement(
    profile: LocalErasureProfile,
    shard_index: usize,
    node_id: &str,
    region_id: &str,
    cell_id: &str,
) -> Result<()> {
    let expected = plan_local_shard_placements(profile)?
        .into_iter()
        .nth(shard_index)
        .ok_or_else(|| anyhow!("CoreStore shard receipt index exceeds placement plan"))?;
    if expected.node_id != node_id || expected.region_id != region_id || expected.cell_id != cell_id
    {
        bail!(
            "CoreStore shard receipt placement mismatch for shard {}: expected {}/{}/{}, got {}/{}/{}",
            shard_index,
            expected.region_id,
            expected.cell_id,
            expected.node_id,
            region_id,
            cell_id,
            node_id
        );
    }
    Ok(())
}

fn validate_manifest_locator(locator: &CoreManifestLocator) -> Result<()> {
    validate_logical_id(
        &locator.manifest_ref.logical_file_id,
        "manifest locator logical file id",
    )?;
    validate_logical_id(
        &locator.manifest_ref.writer_family,
        "manifest locator writer family",
    )?;
    validate_hash(
        &locator.manifest_ref.manifest_hash,
        "manifest locator ref hash",
    )?;
    validate_hash(&locator.manifest_hash, "manifest locator hash")?;
    if locator.manifest_hash != locator.manifest_ref.manifest_hash {
        bail!("CoreStore manifest locator hash must match manifest ref hash");
    }
    match locator.manifest_encoding.as_str() {
        "deterministic-protobuf" | "canonical-cbor" | "writer-segment" => {}
        other => bail!("CoreStore unsupported manifest locator encoding {other}"),
    }
    if locator.manifest_length == 0 {
        bail!("CoreStore manifest locator length must be nonzero");
    }
    if locator.block_locators.is_empty() {
        bail!("CoreStore manifest locator must include block locators");
    }
    let mut expected_start = 0u64;
    for block in &locator.block_locators {
        if block.logical_start != expected_start || block.logical_end <= block.logical_start {
            bail!("CoreStore manifest locator block ranges must be contiguous and non-empty");
        }
        expected_start = block.logical_end;
        validate_hash(&block.block_plain_hash, "manifest locator block plain hash")?;
        validate_hash(
            &block.block_encoded_hash,
            "manifest locator block encoded hash",
        )?;
        if block.data_shards == 0 {
            bail!("CoreStore manifest locator block must include data shards");
        }
        if block.placement_epoch == 0 {
            bail!("CoreStore manifest locator block placement epoch must be nonzero");
        }
        if block.shard_receipts.len() < block.data_shards as usize {
            bail!("CoreStore manifest locator block has too few shard receipts");
        }
        let mut seen_shards = BTreeSet::new();
        for receipt in &block.shard_receipts {
            if !seen_shards.insert(receipt.shard_index) {
                bail!("CoreStore manifest locator shard receipt index is duplicated");
            }
            let shard_index = u16::try_from(receipt.shard_index)
                .map_err(|_| anyhow!("CoreStore manifest locator shard index exceeds u16"))?;
            if receipt.shard_length == 0 && block.logical_end != block.logical_start {
                bail!("CoreStore manifest locator shard receipt length must be nonzero");
            }
            let profile = local_erasure_profile_for_counts(
                &block.erasure_profile_id,
                block.data_shards as usize,
                block.parity_shards as usize,
            )?;
            validate_local_shard_receipt_placement(
                profile,
                usize::from(shard_index),
                &receipt.node_id,
                &receipt.region_id,
                &receipt.cell_id,
            )?;
            let expected_signed_payload_hash =
                shard_receipt_payload_hash(ShardReceiptPayloadInput {
                    block_id: &block.block_id,
                    shard_index,
                    erasure_profile: &block.erasure_profile_id,
                    node_id: &receipt.node_id,
                    region_id: &receipt.region_id,
                    cell_id: &receipt.cell_id,
                    placement_epoch: block.placement_epoch,
                    shard_length: receipt.shard_length,
                    shard_hash: &receipt.shard_hash,
                    fsync_sequence: receipt.fsync_sequence,
                    written_at_unix_nanos: receipt.written_at_unix_nanos,
                });
            validate_shard_receipt_common(
                &receipt.node_id,
                &receipt.region_id,
                &receipt.cell_id,
                &receipt.shard_hash,
                receipt.shard_length,
                receipt.fsync_sequence,
                receipt.written_at_unix_nanos,
                &receipt.signed_payload_hash,
                &receipt.signature_algorithm,
                &receipt.receipt_signature,
                &expected_signed_payload_hash,
            )?;
        }
    }
    if expected_start != locator.manifest_length {
        bail!("CoreStore manifest locator block ranges must cover the manifest bytes exactly");
    }
    Ok(())
}

fn object_ref_from_manifest_block_locator(block: &CoreBlockLocator) -> Result<CoreObjectRef> {
    Ok(CoreObjectRef {
        hash: block.block_encoded_hash.clone(),
        logical_size: block.plaintext_block_len,
        manifest_ref: encode_manifest_ref_with_profile(
            strip_sha256_prefix(&block.block_encoded_hash)?,
            &block.erasure_profile_id,
        ),
        encoding: CoreObjectEncoding {
            block_id: block.block_id.clone(),
            profile_id: block.erasure_profile_id.clone(),
            data_shards: block.data_shards as u16,
            parity_shards: block.parity_shards as u16,
            minimum_read_shards: block.data_shards as u16,
            minimum_write_ack_shards: (block.data_shards + block.parity_shards) as u16,
            stripe_size: block.shard_payload_len * u64::from(block.data_shards),
            placement_scope: "region".to_string(),
            repair_priority: "normal".to_string(),
            encryption: block.encryption.algorithm.clone(),
        },
        placements: block
            .shard_receipts
            .iter()
            .map(|receipt| CoreObjectPlacement {
                shard_index: receipt.shard_index as u16,
                node_id: receipt.node_id.clone(),
                region_id: receipt.region_id.clone(),
                cell_id: receipt.cell_id.clone(),
                shard_hash: receipt.shard_hash.clone(),
                stored_size: receipt.shard_length,
                generation: 1,
                placement_epoch: block.placement_epoch,
                fsync_sequence: receipt.fsync_sequence,
                written_at_unix_nanos: receipt.written_at_unix_nanos,
                signed_payload_hash: receipt.signed_payload_hash.clone(),
                signature_algorithm: receipt.signature_algorithm.clone(),
                receipt_signature: receipt.receipt_signature.clone(),
            })
            .collect(),
    })
}

fn block_locator_from_manifest_object_ref(
    manifest: &CoreLogicalFileManifest,
    manifest_object_ref: &CoreObjectRef,
    manifest_hash: &str,
) -> Result<CoreBlockLocator> {
    validate_hash(manifest_hash, "logical file manifest hash")?;
    validate_hash(&manifest_object_ref.hash, "logical manifest block hash")?;
    Ok(CoreBlockLocator {
        logical_start: 0,
        logical_end: manifest_object_ref.logical_size,
        block_id: manifest_object_ref.encoding.block_id.clone(),
        codec_id: format!(
            "reed-solomon-{}+{}",
            manifest_object_ref.encoding.data_shards, manifest_object_ref.encoding.parity_shards
        ),
        data_shards: u32::from(manifest_object_ref.encoding.data_shards),
        parity_shards: u32::from(manifest_object_ref.encoding.parity_shards),
        plaintext_block_len: manifest_object_ref.logical_size,
        shard_payload_len: manifest_object_ref
            .placements
            .iter()
            .map(|placement| placement.stored_size)
            .max()
            .unwrap_or(0),
        padding_len: manifest_object_ref
            .encoding
            .stripe_size
            .saturating_sub(manifest_object_ref.logical_size),
        block_plain_hash: manifest_hash.to_string(),
        block_encoded_hash: manifest_object_ref.hash.clone(),
        compression: none_compression_descriptor_from_hash(
            manifest_object_ref.logical_size,
            manifest_hash,
        ),
        encryption: none_encryption_descriptor(manifest_hash, &manifest_object_ref.hash),
        erasure_profile_id: manifest_object_ref.encoding.profile_id.clone(),
        placement_epoch: manifest.placement_epoch,
        shard_receipts: manifest_object_ref
            .placements
            .iter()
            .map(shard_receipt_summary_from_object_placement)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn shard_receipt_summary_from_object_placement(
    shard: &CoreObjectPlacement,
) -> Result<CoreShardReceiptSummary> {
    validate_hash(&shard.shard_hash, "logical shard hash")?;
    validate_hash(
        &shard.signed_payload_hash,
        "logical shard receipt payload hash",
    )?;
    if shard.signature_algorithm != "ed25519-libp2p" {
        bail!(
            "CoreStore shard receipt uses unsupported signature algorithm {}",
            shard.signature_algorithm
        );
    }
    if shard.receipt_signature.is_empty() {
        bail!("CoreStore shard receipt signature must not be empty");
    }
    Ok(CoreShardReceiptSummary {
        node_id: shard.node_id.clone(),
        region_id: shard.region_id.clone(),
        cell_id: shard.cell_id.clone(),
        shard_index: u32::from(shard.shard_index),
        shard_hash: shard.shard_hash.clone(),
        shard_length: shard.stored_size,
        fsync_sequence: shard.fsync_sequence,
        written_at_unix_nanos: shard.written_at_unix_nanos,
        signed_payload_hash: shard.signed_payload_hash.clone(),
        signature_algorithm: shard.signature_algorithm.clone(),
        receipt_signature: shard.receipt_signature.clone(),
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
    none_compression_descriptor_from_hash(uncompressed_length, &uncompressed_hash)
}

fn none_compression_descriptor_from_hash(
    uncompressed_length: u64,
    uncompressed_hash: &str,
) -> CoreCompressionDescriptor {
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

fn none_encryption_descriptor(
    plaintext_hash: &str,
    ciphertext_hash: &str,
) -> CoreEncryptionDescriptor {
    CoreEncryptionDescriptor {
        algorithm: "none".to_string(),
        key_id: String::new(),
        nonce: Vec::new(),
        aad_hash: String::new(),
        plaintext_hash: plaintext_hash.to_string(),
        ciphertext_hash: ciphertext_hash.to_string(),
        descriptor_hash: descriptor_hash(&["encryption", "none", plaintext_hash, ciphertext_hash]),
    }
}

fn validate_pipeline_key_id(key_id: String) -> Result<String> {
    if key_id.is_empty()
        || key_id.len() > 128
        || key_id.contains(':')
        || key_id.contains(',')
        || key_id.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        bail!("CoreStore pipeline key id must be 1-128 visible chars excluding ':' and ','");
    }
    Ok(key_id)
}

fn decode_pipeline_key_hex(key_hex: &str) -> Result<[u8; CORE_PIPELINE_KEY_LEN]> {
    let key = hex::decode(key_hex.trim()).context("CoreStore pipeline key must be hex encoded")?;
    if key.len() != CORE_PIPELINE_KEY_LEN {
        bail!("CoreStore pipeline key must be exactly 32 bytes");
    }
    let mut out = [0u8; CORE_PIPELINE_KEY_LEN];
    out.copy_from_slice(&key);
    Ok(out)
}

fn pipeline_block_aad(
    logical_file_id: &str,
    logical_offset: u64,
    logical_length: u64,
    plaintext_hash: &str,
) -> Vec<u8> {
    let mut aad = Vec::new();
    for part in [
        "anvil.core.pipeline_block.v1",
        logical_file_id,
        &logical_offset.to_string(),
        &logical_length.to_string(),
        plaintext_hash,
    ] {
        aad.extend_from_slice(&(part.len() as u64).to_le_bytes());
        aad.extend_from_slice(part.as_bytes());
    }
    aad
}

fn encryption_descriptor_hash(
    algorithm: &str,
    key_id: &str,
    nonce: &[u8],
    aad_hash: &str,
    plaintext_hash: &str,
    ciphertext_hash: &str,
) -> String {
    descriptor_hash(&[
        "encryption",
        algorithm,
        key_id,
        &hex::encode(nonce),
        aad_hash,
        plaintext_hash,
        ciphertext_hash,
    ])
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
    Null,
    Bool(bool),
    U64(u64),
    I64(i64),
    Text(String),
    Array(Vec<MinimalCborValue>),
    Map(BTreeMap<String, MinimalCborValue>),
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
    boundary_values_b64: String,
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
    boundary_values: Vec<CoreBoundaryValue>,
    payload: Vec<u8>,
}

struct ShardReceiptPayloadInput<'a> {
    block_id: &'a str,
    shard_index: u16,
    erasure_profile: &'a str,
    node_id: &'a str,
    region_id: &'a str,
    cell_id: &'a str,
    placement_epoch: u64,
    shard_length: u64,
    shard_hash: &'a str,
    fsync_sequence: u64,
    written_at_unix_nanos: u64,
}

fn shard_receipt_payload_hash(input: ShardReceiptPayloadInput<'_>) -> String {
    let shard_id = format!("{}:{}", input.block_id, input.shard_index);
    descriptor_hash(&[
        "anvil.shard.receipt.v1",
        "anvil.core.shard_receipt.v1",
        input.block_id,
        &shard_id,
        &input.shard_index.to_string(),
        input.erasure_profile,
        input.node_id,
        input.region_id,
        input.cell_id,
        &input.placement_epoch.to_string(),
        &input.shard_length.to_string(),
        input.shard_hash,
        &input.fsync_sequence.to_string(),
        &input.written_at_unix_nanos.to_string(),
    ])
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
    let boundary_values = decode_shard_boundary_values(&header)?;
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
        boundary_values,
        payload,
    })
}

fn decode_shard_boundary_values(
    header: &BTreeMap<String, MinimalCborValue>,
) -> Result<Vec<CoreBoundaryValue>> {
    let encoded = cbor_text_value(header, "boundary_values_b64")?;
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .context("decode CoreStore shard boundary values")?;
    let values: Vec<CoreBoundaryValue> = serde_json::from_slice(&bytes)?;
    let expected_hash = boundary_summary_hash(&values)?;
    expect_cbor_text(header, "boundary_summary_hash", &expected_hash)?;
    Ok(values)
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
        "boundary_values_b64".to_string(),
        MinimalCborValue::Text(header.boundary_values_b64),
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
    push_cbor_map(&mut out, map);
    out
}

fn encode_canonical_cbor_json(value: &serde_json::Value) -> Result<Vec<u8>> {
    let value = minimal_cbor_value_from_json(value)?;
    let mut out = Vec::new();
    push_minimal_cbor_value(&mut out, &value);
    Ok(out)
}

fn decode_canonical_cbor_json(bytes: &[u8]) -> Result<serde_json::Value> {
    let mut offset = 0usize;
    let value = read_minimal_cbor_value(bytes, &mut offset)?;
    if offset != bytes.len() {
        bail!("CoreStore canonical CBOR value has trailing bytes");
    }
    let encoded = {
        let mut out = Vec::new();
        push_minimal_cbor_value(&mut out, &value);
        out
    };
    if encoded != bytes {
        bail!("CoreStore CBOR value is not canonical");
    }
    minimal_cbor_value_to_json(value)
}

fn decode_minimal_cbor_map(bytes: &[u8]) -> Result<BTreeMap<String, MinimalCborValue>> {
    let mut offset = 0usize;
    let value = read_minimal_cbor_value(bytes, &mut offset)?;
    if offset != bytes.len() {
        bail!("CoreStore block shard header CBOR has trailing bytes");
    }
    let MinimalCborValue::Map(map) = value else {
        bail!("CoreStore block shard header CBOR is not a map");
    };
    let encoded = encode_minimal_cbor_map(&map);
    if encoded != bytes {
        bail!("CoreStore block shard header CBOR map keys are not canonical");
    }
    Ok(map)
}

fn minimal_cbor_value_from_json(value: &serde_json::Value) -> Result<MinimalCborValue> {
    Ok(match value {
        serde_json::Value::Null => MinimalCborValue::Null,
        serde_json::Value::Bool(value) => MinimalCborValue::Bool(*value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                MinimalCborValue::U64(value)
            } else if let Some(value) = value.as_i64() {
                MinimalCborValue::I64(value)
            } else {
                bail!("CoreStore canonical CBOR does not support floating point numbers");
            }
        }
        serde_json::Value::String(value) => MinimalCborValue::Text(value.clone()),
        serde_json::Value::Array(values) => MinimalCborValue::Array(
            values
                .iter()
                .map(minimal_cbor_value_from_json)
                .collect::<Result<Vec<_>>>()?,
        ),
        serde_json::Value::Object(values) => {
            let mut map = BTreeMap::new();
            for (key, value) in values {
                map.insert(key.clone(), minimal_cbor_value_from_json(value)?);
            }
            MinimalCborValue::Map(map)
        }
    })
}

fn minimal_cbor_value_to_json(value: MinimalCborValue) -> Result<serde_json::Value> {
    Ok(match value {
        MinimalCborValue::Null => serde_json::Value::Null,
        MinimalCborValue::Bool(value) => serde_json::Value::Bool(value),
        MinimalCborValue::U64(value) => serde_json::Value::Number(value.into()),
        MinimalCborValue::I64(value) => serde_json::Value::Number(value.into()),
        MinimalCborValue::Text(value) => serde_json::Value::String(value),
        MinimalCborValue::Array(values) => serde_json::Value::Array(
            values
                .into_iter()
                .map(minimal_cbor_value_to_json)
                .collect::<Result<Vec<_>>>()?,
        ),
        MinimalCborValue::Map(values) => {
            let mut map = serde_json::Map::new();
            for (key, value) in values {
                map.insert(key, minimal_cbor_value_to_json(value)?);
            }
            serde_json::Value::Object(map)
        }
    })
}

fn push_minimal_cbor_value(out: &mut Vec<u8>, value: &MinimalCborValue) {
    match value {
        MinimalCborValue::Null => out.push(0xf6),
        MinimalCborValue::Bool(false) => out.push(0xf4),
        MinimalCborValue::Bool(true) => out.push(0xf5),
        MinimalCborValue::U64(value) => push_cbor_type_len(out, 0, *value),
        MinimalCborValue::I64(value) => {
            if *value >= 0 {
                push_cbor_type_len(out, 0, *value as u64);
            } else {
                push_cbor_type_len(out, 1, (-1_i128 - i128::from(*value)) as u64);
            }
        }
        MinimalCborValue::Text(value) => push_cbor_text(out, value),
        MinimalCborValue::Array(values) => {
            push_cbor_type_len(out, 4, values.len() as u64);
            for value in values {
                push_minimal_cbor_value(out, value);
            }
        }
        MinimalCborValue::Map(map) => push_cbor_map(out, map),
    }
}

fn push_cbor_map(out: &mut Vec<u8>, map: &BTreeMap<String, MinimalCborValue>) {
    push_cbor_type_len(out, 5, map.len() as u64);
    let mut entries = map
        .iter()
        .map(|(key, value)| (encode_cbor_text_key(key), key, value))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.0.cmp(&right.0));
    for (encoded_key, _key, value) in entries {
        out.extend_from_slice(&encoded_key);
        push_minimal_cbor_value(out, value);
    }
}

fn read_minimal_cbor_value(bytes: &[u8], offset: &mut usize) -> Result<MinimalCborValue> {
    let (major, value_len) = read_cbor_type_len(bytes, offset)?;
    match major {
        0 => Ok(MinimalCborValue::U64(value_len)),
        1 => {
            if value_len > i64::MAX as u64 {
                bail!("CoreStore CBOR negative integer is too small");
            }
            Ok(MinimalCborValue::I64(-1 - value_len as i64))
        }
        3 => {
            let raw = read_exact(bytes, offset, value_len as usize)?;
            Ok(MinimalCborValue::Text(
                std::str::from_utf8(raw)?.to_string(),
            ))
        }
        4 => {
            let mut values = Vec::with_capacity(value_len as usize);
            for _ in 0..value_len {
                values.push(read_minimal_cbor_value(bytes, offset)?);
            }
            Ok(MinimalCborValue::Array(values))
        }
        5 => {
            let mut previous_key = None::<Vec<u8>>;
            let mut map = BTreeMap::new();
            for _ in 0..value_len {
                let key_start = *offset;
                let key = read_cbor_text(bytes, offset)?;
                let key_bytes = bytes[key_start..*offset].to_vec();
                if previous_key
                    .as_ref()
                    .is_some_and(|previous| previous >= &key_bytes)
                {
                    bail!("CoreStore CBOR map keys are not canonical");
                }
                previous_key = Some(key_bytes);
                map.insert(key, read_minimal_cbor_value(bytes, offset)?);
            }
            Ok(MinimalCborValue::Map(map))
        }
        7 => match value_len {
            20 => Ok(MinimalCborValue::Bool(false)),
            21 => Ok(MinimalCborValue::Bool(true)),
            22 => Ok(MinimalCborValue::Null),
            other => bail!("CoreStore CBOR simple value {other} is unsupported"),
        },
        _ => bail!("CoreStore CBOR major type {major} is unsupported"),
    }
}

fn encode_cbor_text_key(value: &str) -> Vec<u8> {
    let mut out = Vec::new();
    push_cbor_text(&mut out, value);
    out
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
    validate_logical_id(&manifest.encoding.block_id, "CoreStore manifest block id")?;
    if manifest.encoding.block_id != object_ref.encoding.block_id {
        bail!(
            "CoreStore manifest block id mismatch: ref {}, manifest {}",
            object_ref.encoding.block_id,
            manifest.encoding.block_id
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

fn core_transaction_root_anchor_key() -> &'static str {
    "system/core-control/0"
}

fn root_key_hash(root_anchor_key: &str) -> String {
    descriptor_hash(&["anvil.root.key.v1", root_anchor_key])
}

fn validate_root_anchor_record(anchor: &CoreRootAnchorRecord) -> Result<()> {
    if anchor.schema != "anvil.core.root_anchor.v1" {
        bail!("CoreStore root anchor has invalid schema");
    }
    if anchor.root_anchor_key != core_transaction_root_anchor_key() {
        bail!(
            "CoreStore unsupported root anchor key {}",
            anchor.root_anchor_key
        );
    }
    let expected_root_key_hash = root_key_hash(&anchor.root_anchor_key);
    if anchor.root_key_hash != expected_root_key_hash {
        bail!("CoreStore root anchor key hash mismatch");
    }
    validate_hash(&anchor.root_key_hash, "root key hash")?;
    validate_hash(&anchor.previous_root_hash, "previous root hash")?;
    if anchor.root_state != "committed" {
        bail!("CoreStore root anchor state must be committed");
    }
    if anchor.publisher_node_id.is_empty() {
        bail!("CoreStore root anchor publisher node id must not be empty");
    }
    if anchor.publisher_epoch == 0 || anchor.partition_owner_fence == 0 {
        bail!("CoreStore root anchor publisher epoch and owner fence must be nonzero");
    }
    if anchor.root_generation > 0 && anchor.transaction_manifest.is_none() {
        bail!("CoreStore non-genesis root anchor must include a transaction manifest");
    }
    if anchor.root_generation == 0 && anchor.transaction_manifest.is_some() {
        bail!("CoreStore genesis root anchor must not include a transaction manifest");
    }
    if let Some(locator) = &anchor.transaction_manifest {
        validate_manifest_locator(locator)?;
    }
    if let Some(locator) = &anchor.checkpoint_manifest {
        validate_manifest_locator(locator)?;
    }
    Ok(())
}

fn validate_transaction_manifest_record(
    transaction: &CoreTransactionManifestRecord,
    expected_root_generation: u64,
) -> Result<()> {
    if transaction.schema != "anvil.core.transaction_manifest.v1" {
        bail!("CoreStore transaction manifest has invalid schema");
    }
    if transaction.post_root_generation != expected_root_generation {
        bail!("CoreStore transaction manifest post_root_generation does not match root anchor");
    }
    if transaction.post_root_generation != transaction.pre_root_generation.saturating_add(1) {
        bail!("CoreStore transaction manifest root generations must be contiguous");
    }
    if transaction.logical_manifests.is_empty() {
        bail!("CoreStore transaction manifest must include logical manifests");
    }
    for locator in &transaction.logical_manifests {
        validate_manifest_locator(locator)?;
    }
    Ok(())
}

fn encode_transaction_manifest_record(
    transaction: &CoreTransactionManifestRecord,
) -> Result<Vec<u8>> {
    let header = serde_json::json!({
        "schema": &transaction.schema,
        "pre_root_generation": transaction.pre_root_generation,
        "post_root_generation": transaction.post_root_generation,
        "mutation_count": transaction.mutation_ids.len(),
        "logical_manifest_count": transaction.logical_manifests.len(),
    });
    let header_cbor = encode_canonical_cbor_json(&header)?;
    let body_cbor = encode_canonical_cbor_json(&serde_json::to_value(transaction)?)?;
    let mut out = Vec::with_capacity(
        CORE_TRANSACTION_MANIFEST_MAGIC.len() + 2 + 4 + 8 + header_cbor.len() + body_cbor.len() + 4,
    );
    out.extend_from_slice(CORE_TRANSACTION_MANIFEST_MAGIC);
    out.extend_from_slice(&CORE_TRANSACTION_MANIFEST_VERSION.to_le_bytes());
    out.extend_from_slice(&(header_cbor.len() as u32).to_le_bytes());
    out.extend_from_slice(&(body_cbor.len() as u64).to_le_bytes());
    out.extend_from_slice(&header_cbor);
    out.extend_from_slice(&body_cbor);
    let mut crc_input = Vec::with_capacity(header_cbor.len() + body_cbor.len());
    crc_input.extend_from_slice(&header_cbor);
    crc_input.extend_from_slice(&body_cbor);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    Ok(out)
}

fn decode_transaction_manifest_record(bytes: &[u8]) -> Result<CoreTransactionManifestRecord> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_TRANSACTION_MANIFEST_MAGIC.len())?;
    if magic != CORE_TRANSACTION_MANIFEST_MAGIC {
        bail!("CoreStore transaction manifest has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_TRANSACTION_MANIFEST_VERSION {
        bail!("CoreStore transaction manifest has unsupported version {version}");
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let body_len = read_u64_le(bytes, &mut offset)? as usize;
    let header_cbor = read_exact(bytes, &mut offset, header_len)?;
    let body_cbor = read_exact(bytes, &mut offset, body_len)?;
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    if offset != bytes.len() {
        bail!("CoreStore transaction manifest has trailing bytes");
    }
    let mut crc_input = Vec::with_capacity(header_cbor.len() + body_cbor.len());
    crc_input.extend_from_slice(header_cbor);
    crc_input.extend_from_slice(body_cbor);
    if crc32c(&crc_input) != expected_crc {
        bail!("CoreStore transaction manifest checksum mismatch");
    }
    let header = decode_canonical_cbor_json(header_cbor)?;
    if json_required_string(&header, "schema")? != "anvil.core.transaction_manifest.v1" {
        bail!("CoreStore transaction manifest header has invalid schema");
    }
    let transaction: CoreTransactionManifestRecord =
        serde_json::from_value(decode_canonical_cbor_json(body_cbor)?)?;
    if json_required_u64(&header, "pre_root_generation")? != transaction.pre_root_generation
        || json_required_u64(&header, "post_root_generation")? != transaction.post_root_generation
        || json_required_u64(&header, "mutation_count")? != transaction.mutation_ids.len() as u64
        || json_required_u64(&header, "logical_manifest_count")?
            != transaction.logical_manifests.len() as u64
    {
        bail!("CoreStore transaction manifest header/body mismatch");
    }
    Ok(transaction)
}

fn hash_root_anchor_record(anchor: &CoreRootAnchorRecord) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        sha256_hex(&encode_root_anchor_record(anchor)?)
    ))
}

fn encode_root_anchor_record(anchor: &CoreRootAnchorRecord) -> Result<Vec<u8>> {
    validate_root_anchor_record(anchor)?;
    let header = serde_json::json!({
        "schema": &anchor.schema,
        "root_anchor_key": &anchor.root_anchor_key,
        "root_key_hash": &anchor.root_key_hash,
        "root_generation": anchor.root_generation,
        "previous_root_hash": &anchor.previous_root_hash,
        "transaction_manifest": &anchor.transaction_manifest,
        "checkpoint_manifest": &anchor.checkpoint_manifest,
        "publisher_node_id": &anchor.publisher_node_id,
        "publisher_epoch": anchor.publisher_epoch,
        "partition_owner_fence": anchor.partition_owner_fence,
        "created_at_unix_nanos": anchor.created_at_unix_nanos,
    });
    let body = serde_json::json!({
        "root_state": &anchor.root_state,
        "mutation_first": &anchor.mutation_first,
        "mutation_last": &anchor.mutation_last,
        "writer_families": &anchor.writer_families,
        "manifest_count": anchor.manifest_count,
        "final_block_count": anchor.final_block_count,
    });
    let header_cbor = encode_canonical_cbor_json(&header)?;
    let body_cbor = encode_canonical_cbor_json(&body)?;
    let mut out = Vec::with_capacity(
        CORE_ROOT_ANCHOR_MAGIC.len() + 2 + 4 + 8 + header_cbor.len() + body_cbor.len() + 4 + 32,
    );
    out.extend_from_slice(CORE_ROOT_ANCHOR_MAGIC);
    out.extend_from_slice(&CORE_ROOT_ANCHOR_VERSION.to_le_bytes());
    out.extend_from_slice(&(header_cbor.len() as u32).to_le_bytes());
    out.extend_from_slice(&(body_cbor.len() as u64).to_le_bytes());
    out.extend_from_slice(&header_cbor);
    out.extend_from_slice(&body_cbor);
    let mut crc_input = Vec::with_capacity(header_cbor.len() + body_cbor.len());
    crc_input.extend_from_slice(&header_cbor);
    crc_input.extend_from_slice(&body_cbor);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    let file_hash = Sha256::digest(&out);
    out.extend_from_slice(&file_hash);
    Ok(out)
}

fn decode_root_anchor_record(bytes: &[u8]) -> Result<CoreRootAnchorRecord> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_ROOT_ANCHOR_MAGIC.len())?;
    if magic != CORE_ROOT_ANCHOR_MAGIC {
        bail!("CoreStore root anchor has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_ROOT_ANCHOR_VERSION {
        bail!("CoreStore root anchor has unsupported version {version}");
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let body_len = read_u64_le(bytes, &mut offset)? as usize;
    let header_cbor = read_exact(bytes, &mut offset, header_len)?;
    let body_cbor = read_exact(bytes, &mut offset, body_len)?;
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    let mut crc_input = Vec::with_capacity(header_cbor.len() + body_cbor.len());
    crc_input.extend_from_slice(header_cbor);
    crc_input.extend_from_slice(body_cbor);
    if crc32c(&crc_input) != expected_crc {
        bail!("CoreStore root anchor checksum mismatch");
    }
    let hash_start = offset;
    let expected_hash = read_exact(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        bail!("CoreStore root anchor has trailing bytes");
    }
    let actual_hash = Sha256::digest(&bytes[..hash_start]);
    let actual_hash: &[u8] = actual_hash.as_ref();
    if expected_hash != actual_hash {
        bail!("CoreStore root anchor file hash mismatch");
    }
    let header = decode_canonical_cbor_json(header_cbor)?;
    let body = decode_canonical_cbor_json(body_cbor)?;
    let anchor = CoreRootAnchorRecord {
        schema: json_required_string(&header, "schema")?,
        root_anchor_key: json_required_string(&header, "root_anchor_key")?,
        root_key_hash: json_required_string(&header, "root_key_hash")?,
        root_generation: json_required_u64(&header, "root_generation")?,
        previous_root_hash: json_required_string(&header, "previous_root_hash")?,
        transaction_manifest: header
            .get("transaction_manifest")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?,
        checkpoint_manifest: header
            .get("checkpoint_manifest")
            .filter(|value| !value.is_null())
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?,
        publisher_node_id: json_required_string(&header, "publisher_node_id")?,
        publisher_epoch: json_required_u64(&header, "publisher_epoch")?,
        partition_owner_fence: json_required_u64(&header, "partition_owner_fence")?,
        created_at_unix_nanos: json_required_u64(&header, "created_at_unix_nanos")?,
        root_state: json_required_string(&body, "root_state")?,
        mutation_first: json_optional_string(&body, "mutation_first")?,
        mutation_last: json_optional_string(&body, "mutation_last")?,
        writer_families: serde_json::from_value(
            body.get("writer_families")
                .cloned()
                .ok_or_else(|| anyhow!("CoreStore root anchor is missing writer_families"))?,
        )?,
        manifest_count: json_required_u64(&body, "manifest_count")?,
        final_block_count: json_required_u64(&body, "final_block_count")?,
    };
    validate_root_anchor_record(&anchor)?;
    Ok(anchor)
}

fn encode_root_register_shard_file(
    header: &CoreRootRegisterHeader,
    root_anchor_record: &[u8],
) -> Result<Vec<u8>> {
    validate_hash(&header.root_key_hash, "root register key hash")?;
    validate_hash(&header.root_anchor_hash, "root register anchor hash")?;
    let root_key_hash = decode_sha256_hash_bytes(&header.root_key_hash)?;
    let header_cbor = encode_canonical_cbor_json(&serde_json::to_value(header)?)?;
    let mut out = Vec::with_capacity(
        CORE_ROOT_REGISTER_MAGIC.len()
            + 2
            + 8
            + 32
            + 8
            + 2
            + 4
            + 8
            + header_cbor.len()
            + root_anchor_record.len()
            + 4
            + 32,
    );
    out.extend_from_slice(CORE_ROOT_REGISTER_MAGIC);
    out.extend_from_slice(&CORE_ROOT_REGISTER_VERSION.to_le_bytes());
    out.extend_from_slice(&header.root_partition_id.to_le_bytes());
    out.extend_from_slice(&root_key_hash);
    out.extend_from_slice(&header.root_generation.to_le_bytes());
    out.extend_from_slice(&header.shard_index.to_le_bytes());
    out.extend_from_slice(&(header_cbor.len() as u32).to_le_bytes());
    out.extend_from_slice(&(root_anchor_record.len() as u64).to_le_bytes());
    out.extend_from_slice(&header_cbor);
    out.extend_from_slice(root_anchor_record);
    let checksum_start = CORE_ROOT_REGISTER_MAGIC.len();
    out.extend_from_slice(&crc32c(&out[checksum_start..]).to_le_bytes());
    let file_hash = Sha256::digest(&out);
    out.extend_from_slice(&file_hash);
    Ok(out)
}

fn decode_root_register_shard_file(
    bytes: &[u8],
) -> Result<(CoreRootRegisterHeader, CoreRootAnchorRecord)> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_ROOT_REGISTER_MAGIC.len())?;
    if magic != CORE_ROOT_REGISTER_MAGIC {
        bail!("CoreStore root register shard has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_ROOT_REGISTER_VERSION {
        bail!("CoreStore root register shard has unsupported version {version}");
    }
    let root_partition_id = read_u64_le(bytes, &mut offset)?;
    let root_key_hash_bytes = read_exact(bytes, &mut offset, 32)?;
    let root_key_hash = format!("sha256:{}", hex::encode(root_key_hash_bytes));
    let root_generation = read_u64_le(bytes, &mut offset)?;
    let shard_index = read_u16_le(bytes, &mut offset)?;
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let root_anchor_len = read_u64_le(bytes, &mut offset)? as usize;
    let header_cbor = read_exact(bytes, &mut offset, header_len)?;
    let root_anchor_record = read_exact(bytes, &mut offset, root_anchor_len)?;
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    let checksum_start = CORE_ROOT_REGISTER_MAGIC.len();
    let checksum_end = offset - 4;
    if crc32c(&bytes[checksum_start..checksum_end]) != expected_crc {
        bail!("CoreStore root register shard checksum mismatch");
    }
    let hash_start = offset;
    let expected_hash = read_exact(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        bail!("CoreStore root register shard has trailing bytes");
    }
    let actual_hash = Sha256::digest(&bytes[..hash_start]);
    let actual_hash: &[u8] = actual_hash.as_ref();
    if expected_hash != actual_hash {
        bail!("CoreStore root register shard file hash mismatch");
    }
    let header: CoreRootRegisterHeader =
        serde_json::from_value(decode_canonical_cbor_json(header_cbor)?)?;
    if header.root_partition_id != root_partition_id
        || header.root_key_hash != root_key_hash
        || header.root_generation != root_generation
        || header.shard_index != shard_index
    {
        bail!("CoreStore root register fixed fields do not match header");
    }
    let anchor = decode_root_anchor_record(root_anchor_record)?;
    Ok((header, anchor))
}

fn decode_sha256_hash_bytes(hash: &str) -> Result<[u8; 32]> {
    let raw = strip_sha256_prefix(hash)?;
    let bytes = hex::decode(raw)?;
    Ok(bytes
        .try_into()
        .map_err(|_| anyhow!("CoreStore hash did not decode to 32 bytes"))?)
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

    let mut index_entries = Vec::with_capacity(records.len());
    for (ordinal, record) in records.iter().enumerate() {
        let record_offset = bytes.len() as u64;
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
        index_entries.push(StreamSparseIndexEntry {
            first_sequence: record.sequence,
            first_timestamp_nanos: parse_stream_record_timestamp_nanos(&record.created_at),
            record_ordinal: ordinal as u32,
            byte_offset: record_offset,
        });
    }

    let index_bytes = encode_stream_sparse_index(&index_entries)?;
    bytes.extend_from_slice(&(index_bytes.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&index_bytes);

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
    let index_len = read_u64_le(bytes, &mut offset)? as usize;
    let index_bytes = read_exact(bytes, &mut offset, index_len)?;
    validate_stream_sparse_index(index_bytes, &records)?;
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

fn encode_stream_sparse_index(entries: &[StreamSparseIndexEntry]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CORE_STREAM_SPARSE_INDEX_MAGIC);
    write_u32_le(&mut bytes, entries.len())?;
    for entry in entries {
        bytes.extend_from_slice(&entry.first_sequence.to_le_bytes());
        bytes.extend_from_slice(&entry.record_ordinal.to_le_bytes());
        bytes.extend_from_slice(&entry.byte_offset.to_le_bytes());
    }
    write_u32_le(&mut bytes, entries.len())?;
    for entry in entries {
        bytes.extend_from_slice(&entry.first_timestamp_nanos.to_le_bytes());
        bytes.extend_from_slice(&entry.record_ordinal.to_le_bytes());
        bytes.extend_from_slice(&entry.byte_offset.to_le_bytes());
    }
    let crc = crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn validate_stream_sparse_index(bytes: &[u8], records: &[StreamRecord]) -> Result<()> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_STREAM_SPARSE_INDEX_MAGIC.len())?;
    if magic != CORE_STREAM_SPARSE_INDEX_MAGIC {
        bail!("CoreStore stream sparse index has invalid magic");
    }
    let sequence_count = read_u32_le(bytes, &mut offset)? as usize;
    if sequence_count != records.len() {
        bail!("CoreStore stream sparse index sequence count mismatch");
    }
    let mut previous_sequence = None;
    for ordinal in 0..sequence_count {
        let first_sequence = read_u64_le(bytes, &mut offset)?;
        let record_ordinal = read_u32_le(bytes, &mut offset)?;
        let _byte_offset = read_u64_le(bytes, &mut offset)?;
        if record_ordinal != ordinal as u32 {
            bail!("CoreStore stream sparse index ordinal mismatch");
        }
        if Some(first_sequence) <= previous_sequence {
            bail!("CoreStore stream sparse index sequence entries are not sorted");
        }
        if records
            .get(ordinal)
            .map(|record| record.sequence)
            .unwrap_or_default()
            != first_sequence
        {
            bail!("CoreStore stream sparse index sequence does not match record");
        }
        previous_sequence = Some(first_sequence);
    }
    let timestamp_count = read_u32_le(bytes, &mut offset)? as usize;
    if timestamp_count != records.len() {
        bail!("CoreStore stream sparse index timestamp count mismatch");
    }
    let mut previous_timestamp = None;
    for ordinal in 0..timestamp_count {
        let timestamp = read_i64_le(bytes, &mut offset)?;
        let record_ordinal = read_u32_le(bytes, &mut offset)?;
        let _byte_offset = read_u64_le(bytes, &mut offset)?;
        if record_ordinal != ordinal as u32 {
            bail!("CoreStore stream sparse timestamp ordinal mismatch");
        }
        if previous_timestamp.is_some_and(|previous| timestamp < previous) {
            bail!("CoreStore stream sparse timestamp entries are not sorted");
        }
        previous_timestamp = Some(timestamp);
    }
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    if offset != bytes.len() {
        bail!("CoreStore stream sparse index has trailing bytes");
    }
    let actual_crc = crc32c(&bytes[..bytes.len() - 4]);
    if actual_crc != expected_crc {
        bail!("CoreStore stream sparse index checksum mismatch");
    }
    Ok(())
}

fn parse_stream_record_timestamp_nanos(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|value| value.timestamp_nanos_opt())
        .unwrap_or_default()
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

fn json_required_u64(value: &serde_json::Value, field: &str) -> Result<u64> {
    json_optional_u64(value, field)?
        .ok_or_else(|| anyhow!("CoreStore WAL target is missing unsigned integer field {field}"))
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

fn read_i64_le(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let raw = read_exact(bytes, offset, 8)?;
    Ok(i64::from_le_bytes(raw.try_into()?))
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

async fn write_file_create_new_or_same(path: &PathBuf, bytes: &[u8]) -> Result<()> {
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

    let started_at = Instant::now();
    let create_result = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await;
    crate::perf::record_io_duration(
        "core_store",
        "create_new",
        path,
        bytes.len() as u64,
        started_at.elapsed(),
    );
    let mut file = match create_result {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let existing = read_file(path, "core_store", "create_new_existing_read").await?;
            if existing == bytes {
                return Ok(());
            }
            bail!(
                "CoreStore create_new detected conflicting existing file {}",
                path.display()
            );
        }
        Err(error) => {
            return Err(error).with_context(|| format!("create CoreStore file {}", path.display()));
        }
    };
    let started_at = Instant::now();
    file.write_all(bytes).await?;
    crate::perf::record_io_duration(
        "core_store",
        "write_all",
        path,
        bytes.len() as u64,
        started_at.elapsed(),
    );
    let started_at = Instant::now();
    file.sync_all().await?;
    crate::perf::record_io_duration(
        "core_store",
        "sync_all",
        path,
        bytes.len() as u64,
        started_at.elapsed(),
    );
    drop(file);
    sync_parent_dir(path, "create_new_sync_parent_dir").await?;
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

    fn count_files_with_extension(root: &std::path::Path, extension: &str) -> usize {
        let Ok(entries) = std::fs::read_dir(root) else {
            return 0;
        };
        let mut count = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                count += count_files_with_extension(&path, extension);
            } else if path.extension().is_some_and(|actual| actual == extension) {
                count += 1;
            }
        }
        count
    }

    fn test_object_ref_for_payload(
        store: &CoreStore,
        logical_file_id: &str,
        bytes: &[u8],
        profile: LocalErasureProfile,
    ) -> CoreObjectRef {
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
            let written_at_unix_nanos = unix_timestamp_nanos();
            let shard_hash = format!("sha256:{shard_hash}");
            let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
                block_id: &local_block_id_for_logical_block(
                    logical_file_id,
                    0,
                    0,
                    &format!("sha256:{hash}"),
                ),
                shard_index: shard_index as u16,
                erasure_profile: profile.id,
                node_id: &placement.node_id,
                region_id: &placement.region_id,
                cell_id: &placement.cell_id,
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                shard_length: shard.len() as u64,
                shard_hash: &shard_hash,
                fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                written_at_unix_nanos,
            });
            object_placements.push(CoreObjectPlacement {
                shard_index: shard_index as u16,
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_hash,
                stored_size: shard.len() as u64,
                generation: 1,
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                written_at_unix_nanos,
                receipt_signature: store.sign_core_receipt(&signed_payload_hash).unwrap(),
                signed_payload_hash,
                signature_algorithm: "ed25519-libp2p".to_string(),
            });
        }
        CoreObjectRef {
            hash: format!("sha256:{hash}"),
            logical_size: bytes.len() as u64,
            manifest_ref: encode_manifest_ref(&hash),
            encoding: CoreObjectEncoding {
                block_id: local_block_id_for_logical_block(
                    logical_file_id,
                    0,
                    0,
                    &format!("sha256:{hash}"),
                ),
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
    async fn core_store_logical_file_aes_gcm_siv_round_trips_without_plaintext_shards() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let keyring = CorePipelineKeyring::from_hex_config(
            "k1",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "",
        )
        .unwrap();
        let store = CoreStore::new_with_pipeline_keyring(storage.clone(), keyring)
            .await
            .unwrap();
        let source = b"alpha tenant boundary data; beta tenant boundary data; gamma".repeat(2);
        let manifest = store
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "object_blob".to_string(),
                generation: 1,
                logical_file_id: "lf_encrypted_object".to_string(),
                source: source.clone(),
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy {
                    encryption: "aes_gcm_siv".to_string(),
                    target_block_size: 24,
                    ..CorePipelinePolicy::default()
                },
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "mut-encrypted-logical-file".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(manifest.encryption.algorithm, "aes_gcm_siv");
        assert!(manifest.blocks.len() > 1);
        assert!(
            manifest
                .blocks
                .iter()
                .all(|block| block.encryption.algorithm == "aes_gcm_siv")
        );
        assert!(
            manifest
                .blocks
                .iter()
                .all(|block| block.encrypted_length > block.compressed_length)
        );

        let first_block = &manifest.blocks[0];
        let first_object_ref =
            object_ref_from_logical_block_ref(first_block, &manifest.erasure_profile_id).unwrap();
        let stored = store
            .get_blob(GetBlob {
                object_ref: first_object_ref,
            })
            .await
            .unwrap();
        assert_ne!(
            &stored[..first_block.compressed_length as usize],
            &source[..first_block.compressed_length as usize]
        );

        let whole = store
            .read_logical_range(ReadLogicalRangeRequest {
                manifest: manifest.clone(),
                ranges: vec![CoreByteRange {
                    start: 0,
                    end_exclusive: source.len() as u64,
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
        assert_eq!(whole, source);

        let slice = store
            .read_logical_range(ReadLogicalRangeRequest {
                manifest,
                ranges: vec![CoreByteRange {
                    start: 7,
                    end_exclusive: 53,
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
        assert_eq!(slice, source[7..53]);
    }

    #[tokio::test]
    async fn core_store_logical_file_aes_gcm_siv_requires_keyring() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let err = store
            .write_logical_file(WriteLogicalFileRequest {
                writer_family: "object_blob".to_string(),
                generation: 1,
                logical_file_id: "lf_encryption_requires_key".to_string(),
                source: b"secret".to_vec(),
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy {
                    encryption: "aes_gcm_siv".to_string(),
                    ..CorePipelinePolicy::default()
                },
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "mut-encryption-requires-key".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("requires a configured keyring"),
            "unexpected error: {err:#}"
        );
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
        for placement in manifest
            .placements
            .iter()
            .filter(|placement| (1..LOCAL_DATA_SHARDS as u16).contains(&placement.shard_index))
        {
            let shard_path = store.shard_path(
                &placement.node_id,
                &manifest.encoding.block_id,
                placement.shard_index,
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
    async fn core_store_logical_file_publish_returns_self_contained_manifest_locator() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let write = store
            .write_logical_file_with_locator(WriteLogicalFileRequest {
                writer_family: "object_blob".to_string(),
                generation: 9,
                logical_file_id: "objects/reports/report-9".to_string(),
                source: b"manifest locator payload".to_vec(),
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy::default(),
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "logical-file-locator-mut-1".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(
            write.locator.manifest_ref.logical_file_id,
            write.manifest.logical_file_id
        );
        assert_eq!(
            write.locator.manifest_ref.writer_family,
            write.manifest.writer_family
        );
        assert_eq!(
            write.locator.manifest_ref.writer_generation,
            write.manifest.writer_generation
        );
        assert_eq!(
            write.locator.manifest_hash,
            write.locator.manifest_ref.manifest_hash
        );
        assert_eq!(write.locator.manifest_encoding, "writer-segment");
        assert_eq!(write.locator.block_locators.len(), 1);
        let block = &write.locator.block_locators[0];
        assert_eq!(block.logical_start, 0);
        assert_eq!(block.logical_end, write.locator.manifest_length);
        assert_eq!(block.block_plain_hash, write.locator.manifest_hash);
        assert_eq!(
            block.shard_receipts.len(),
            LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS
        );
        for receipt in &block.shard_receipts {
            assert_ne!(receipt.written_at_unix_nanos, 0);
            assert!(receipt.signed_payload_hash.starts_with("sha256:"));
            assert_eq!(receipt.signature_algorithm, "ed25519-libp2p");
            assert!(!receipt.receipt_signature.is_empty());
        }
        assert_ne!(
            block.block_id, write.manifest.blocks[0].block_id,
            "manifest locator must point at the published manifest bytes, not the data block"
        );

        let stored_manifest = store
            .read_logical_file_manifest(&write.locator)
            .await
            .unwrap();
        assert_eq!(stored_manifest, write.manifest);
    }

    #[tokio::test]
    async fn core_store_manifest_locator_rejects_invalid_shard_receipts() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let write = store
            .write_logical_file_with_locator(WriteLogicalFileRequest {
                writer_family: "object_blob".to_string(),
                generation: 10,
                logical_file_id: "objects/reports/report-10".to_string(),
                source: b"manifest locator receipt validation".to_vec(),
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy::default(),
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "logical-file-locator-mut-10".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();

        let mut stale_epoch = write.locator.clone();
        stale_epoch.block_locators[0].placement_epoch = 0;
        assert!(
            store
                .read_logical_file_manifest(&stale_epoch)
                .await
                .is_err()
        );

        let mut missing_fsync = write.locator.clone();
        missing_fsync.block_locators[0].shard_receipts[0].fsync_sequence = 0;
        assert!(
            store
                .read_logical_file_manifest(&missing_fsync)
                .await
                .is_err()
        );

        let mut bad_hash = write.locator.clone();
        bad_hash.block_locators[0].shard_receipts[0].shard_hash =
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        assert!(store.read_logical_file_manifest(&bad_hash).await.is_err());

        let mut bad_signature = write.locator.clone();
        bad_signature.block_locators[0].shard_receipts[0].receipt_signature[0] ^= 0x01;
        assert!(
            store
                .read_logical_file_manifest(&bad_signature)
                .await
                .unwrap_err()
                .to_string()
                .contains("signature verification failed")
        );

        let mut wrong_node = write.locator.clone();
        wrong_node.block_locators[0].shard_receipts[0].node_id = "local-node-999".to_string();
        assert!(
            store
                .read_logical_file_manifest(&wrong_node)
                .await
                .unwrap_err()
                .to_string()
                .contains("placement mismatch")
        );

        let mut duplicate = write.locator.clone();
        duplicate.block_locators[0].shard_receipts[1].shard_index =
            duplicate.block_locators[0].shard_receipts[0].shard_index;
        assert!(store.read_logical_file_manifest(&duplicate).await.is_err());
    }

    #[tokio::test]
    async fn core_store_manifest_locator_reads_multiple_contiguous_blocks() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let write = store
            .write_logical_file_with_locator(WriteLogicalFileRequest {
                writer_family: "object_blob".to_string(),
                generation: 11,
                logical_file_id: "objects/reports/report-11".to_string(),
                source: b"manifest locator split block proof".to_vec(),
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy::default(),
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: "logical-file-locator-mut-11".to_string(),
                region_id: "local".to_string(),
            })
            .await
            .unwrap();
        let manifest_bytes = serde_json::to_vec(&write.manifest).unwrap();
        let split_at = manifest_bytes.len() / 2;
        let chunks = [&manifest_bytes[..split_at], &manifest_bytes[split_at..]];
        let profile = local_erasure_profile(LOCAL_ERASURE_PROFILE_ID).unwrap();
        let mut block_locators = Vec::new();
        let mut logical_start = 0u64;
        for (index, chunk) in chunks.iter().enumerate() {
            let chunk_hash = format!("sha256:{}", sha256_hex(chunk));
            let chunk_hash_hex = strip_sha256_prefix(&chunk_hash).unwrap();
            let object_ref = store
                .materialise_object_blob_bytes(
                    &format!("lf_manifest_split_{index}"),
                    write.manifest.writer_generation,
                    index as u64,
                    &chunk_hash,
                    chunk_hash_hex,
                    chunk,
                    &[],
                    &format!("manifest_split_{index}"),
                    profile,
                    "none",
                    "core_control",
                )
                .await
                .unwrap();
            let mut block =
                block_locator_from_manifest_object_ref(&write.manifest, &object_ref, &chunk_hash)
                    .unwrap();
            block.logical_start = logical_start;
            block.logical_end = logical_start + chunk.len() as u64;
            logical_start = block.logical_end;
            block_locators.push(block);
        }
        let split_locator = CoreManifestLocator {
            manifest_ref: write.locator.manifest_ref.clone(),
            manifest_encoding: write.locator.manifest_encoding.clone(),
            manifest_length: manifest_bytes.len() as u64,
            manifest_hash: write.locator.manifest_hash.clone(),
            block_locators,
        };

        let manifest = store
            .read_logical_file_manifest(&split_locator)
            .await
            .unwrap();
        assert_eq!(manifest, write.manifest);

        let mut gap = split_locator.clone();
        gap.block_locators[1].logical_start += 1;
        assert!(store.read_logical_file_manifest(&gap).await.is_err());
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
        let shard_path = store.shard_path(&placement.node_id, &unrelated_ref.encoding.block_id, 0);
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
        for placement in &manifest.placements {
            assert_ne!(placement.written_at_unix_nanos, 0);
            assert!(placement.signed_payload_hash.starts_with("sha256:"));
            assert_eq!(placement.signature_algorithm, "ed25519-libp2p");
            assert!(!placement.receipt_signature.is_empty());
        }

        let mut bad_ref = object_ref.clone();
        bad_ref.placements[0].receipt_signature[0] ^= 0x01;
        assert!(
            store
                .get_blob(GetBlob {
                    object_ref: bad_ref
                })
                .await
                .unwrap_err()
                .to_string()
                .contains("signature verification failed")
        );
    }

    #[tokio::test]
    async fn core_store_recovers_unfinalised_put_blob_wal_on_startup() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let bytes = b"recover object from wal".to_vec();
        let logical_name = "tenant:t/bucket:b/object:recovered";
        let payload_hash = format!("sha256:{}", sha256_hex(&bytes));
        store
            .admit_core_mutation(
                "object.put",
                "object_blob",
                serde_json::json!({
                    "logical_name": logical_name,
                    "region_id": "local",
                    "erasure_profile_id": LOCAL_ERASURE_PROFILE_ID,
                    "encryption": "none",
                    "block_plain_hash": payload_hash,
                    "writer_generation": 0_u64,
                    "block_ordinal": 0_u64,
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
        let object_ref =
            test_object_ref_for_payload(&recovered, logical_name, &bytes, LOCAL_EC_4_2_PROFILE);
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
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
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
            let path = store.shard_path(
                &placement.node_id,
                &object_ref.encoding.block_id,
                placement.shard_index,
            );
            assert!(
                path.starts_with(
                    storage
                        .core_store_root_path()
                        .join("blocks")
                        .join("local-cache")
                        .join(LOCAL_ERASURE_SET_ID)
                        .join(&placement.node_id)
                ),
                "shards must be placed under the RFC CoreStore block cache"
            );
            assert_eq!(
                path.extension().and_then(|value| value.to_str()),
                Some("anb")
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
            let expected_block_id = object_ref.encoding.block_id.clone();
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
            let path = store.shard_path(
                &placement.node_id,
                &object_ref.encoding.block_id,
                placement.shard_index,
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
        for placement in manifest.placements.iter().take(LOCAL_PARITY_SHARDS + 1) {
            let path = store.shard_path(
                &placement.node_id,
                &object_ref.encoding.block_id,
                placement.shard_index,
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
    async fn core_store_read_stream_page_uses_corestore_stream_state_without_replica_files() {
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
            let path = tmp
                .path()
                .join("_core")
                .join("replicas")
                .join(&node_id)
                .join("streams")
                .join("data")
                .join(format!(
                    "{}.anstream",
                    logical_file_name("tenant:t/bucket:b/ranged-stream")
                ));
            assert!(
                !path.exists(),
                "non-transaction stream state must not be stored in direct replica files"
            );
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

        let full = store
            .read_stream(ReadStream {
                stream_id: "tenant:t/bucket:b/ranged-stream".to_string(),
                after_sequence: 0,
                limit: 0,
            })
            .await
            .unwrap();
        assert_eq!(full.len(), 3);
    }

    #[tokio::test]
    async fn core_store_transaction_stream_is_root_anchored_not_direct_replica_file() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();

        store
            .append_stream(AppendStreamRecord {
                stream_id: "tenant:t/bucket:b/root-anchor-proof".to_string(),
                partition_id: "tenant:t/bucket:b".to_string(),
                record_kind: "event.root_anchor_proof".to_string(),
                payload: br#"{"ok":true}"#.to_vec(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some("root-anchor-proof".to_string()),
            })
            .await
            .unwrap();

        let direct_core_stream_file = tmp
            .path()
            .join("_core")
            .join("replicas")
            .join(local_control_node_id(1))
            .join("streams")
            .join("data")
            .join(format!(
                "{}.anstream",
                logical_file_name(CORE_TRANSACTION_STREAM_ID)
            ));
        assert!(
            !direct_core_stream_file.exists(),
            "CoreStore transaction stream must be rooted in register anchors, not direct .anstream replicas"
        );

        let register_root = tmp
            .path()
            .join("_core")
            .join("blocks")
            .join("register")
            .join(CORE_TRANSACTION_ROOT_PARTITION_ID.to_string());
        let register_shard_count = count_files_with_extension(&register_root, "anr");
        assert!(
            register_shard_count >= 3,
            "CoreStore root register must contain root-register-r3 shard files"
        );

        drop(store);
        let recovered = CoreStore::new(storage).await.unwrap();
        let latest_anchor = recovered
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .expect("latest transaction root anchor");
        let transaction_manifest_locator = latest_anchor
            .transaction_manifest
            .clone()
            .expect("root anchor transaction manifest locator");
        let transaction_manifest = recovered
            .read_logical_file_manifest(&transaction_manifest_locator)
            .await
            .unwrap();
        let transaction_manifest_bytes = recovered
            .read_logical_file_plaintext(&transaction_manifest)
            .await
            .unwrap();
        assert!(transaction_manifest_bytes.starts_with(CORE_TRANSACTION_MANIFEST_MAGIC));
        let header_len_offset = CORE_TRANSACTION_MANIFEST_MAGIC.len() + 2;
        let header_len = u32::from_le_bytes(
            transaction_manifest_bytes[header_len_offset..header_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let header_start = header_len_offset + 4 + 8;
        let header_bytes = &transaction_manifest_bytes[header_start..header_start + header_len];
        assert_eq!(
            header_bytes[0] >> 5,
            5,
            "transaction manifest header must be canonical CBOR map"
        );
        assert!(serde_json::from_slice::<serde_json::Value>(header_bytes).is_err());
        let transaction_manifest = decode_transaction_manifest_record(&transaction_manifest_bytes)
            .expect("decode transaction manifest frame");
        assert_eq!(
            transaction_manifest.post_root_generation,
            latest_anchor.root_generation
        );
        assert_eq!(transaction_manifest.logical_manifests.len(), 1);
        let records = recovered
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await
            .unwrap();
        assert!(
            records
                .iter()
                .any(|record| record.record_kind == CORE_WAL_FINALISATION_RECORD_KIND),
            "CoreStore must recover transaction stream records from the latest root anchor"
        );
    }

    #[tokio::test]
    async fn core_store_bootstraps_system_root_anchor_once() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let register_root = tmp
            .path()
            .join("_core")
            .join("blocks")
            .join("register")
            .join(CORE_TRANSACTION_ROOT_PARTITION_ID.to_string());
        assert_eq!(
            count_files_with_extension(&register_root, "anr"),
            3,
            "startup bootstrap must write exactly one root-register-r3 genesis generation"
        );
        let genesis = store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .expect("genesis root anchor");
        assert_eq!(genesis.root_generation, 0);
        assert!(genesis.transaction_manifest.is_none());

        drop(store);
        let reopened = CoreStore::new(storage).await.unwrap();
        assert_eq!(
            count_files_with_extension(&register_root, "anr"),
            3,
            "bootstrap must be idempotent after restart"
        );
        assert!(
            reopened
                .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn core_store_root_register_rejects_conflicting_or_skipped_generations() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let genesis = store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .expect("genesis root anchor");

        store
            .write_root_register_anchor(&genesis)
            .await
            .expect("same root generation and bytes are idempotent");

        let mut conflict = genesis.clone();
        conflict.created_at_unix_nanos = conflict.created_at_unix_nanos.saturating_add(1);
        assert!(
            store.write_root_register_anchor(&conflict).await.is_err(),
            "same root generation with different bytes must fail create-new CAS"
        );

        let mut missing_manifest = genesis.clone();
        missing_manifest.root_generation = 1;
        missing_manifest.previous_root_hash = hash_root_anchor_record(&genesis).unwrap();
        assert!(
            store
                .write_root_register_anchor(&missing_manifest)
                .await
                .unwrap_err()
                .to_string()
                .contains("transaction manifest"),
            "non-genesis roots must not be published without transaction evidence"
        );

        let mut skipped = genesis.clone();
        skipped.root_generation = 2;
        skipped.previous_root_hash = hash_root_anchor_record(&genesis).unwrap();
        assert!(
            store.write_root_register_anchor(&skipped).await.is_err(),
            "root register publication must not skip generations"
        );
    }

    #[tokio::test]
    async fn core_store_root_register_has_single_concurrent_winner() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = Arc::new(CoreStore::new(storage).await.unwrap());
        let genesis = store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .expect("genesis root anchor");
        let previous_root_hash = hash_root_anchor_record(&genesis).unwrap();

        let locator_a = store
            .write_logical_bytes_direct(
                "core_control",
                "lf_root_cas_a".to_string(),
                1,
                b"root cas contender a".to_vec(),
                "root_cas_a".to_string(),
                "local".to_string(),
            )
            .await
            .unwrap();
        let locator_b = store
            .write_logical_bytes_direct(
                "core_control",
                "lf_root_cas_b".to_string(),
                1,
                b"root cas contender b".to_vec(),
                "root_cas_b".to_string(),
                "local".to_string(),
            )
            .await
            .unwrap();

        let anchor = |mutation_id: &str, locator: CoreManifestLocator| CoreRootAnchorRecord {
            schema: "anvil.core.root_anchor.v1".to_string(),
            root_anchor_key: core_transaction_root_anchor_key().to_string(),
            root_key_hash: root_key_hash(core_transaction_root_anchor_key()),
            root_generation: 1,
            previous_root_hash: previous_root_hash.clone(),
            transaction_manifest: Some(locator),
            checkpoint_manifest: None,
            publisher_node_id: CORE_WAL_NODE_ID.to_string(),
            publisher_epoch: LOCAL_PLACEMENT_EPOCH,
            partition_owner_fence: LOCAL_PLACEMENT_EPOCH,
            created_at_unix_nanos: unix_timestamp_nanos(),
            root_state: "committed".to_string(),
            mutation_first: Some(mutation_id.to_string()),
            mutation_last: Some(mutation_id.to_string()),
            writer_families: vec!["core_control".to_string()],
            manifest_count: 1,
            final_block_count: 1,
        };
        let anchor_a = anchor("root-cas-a", locator_a);
        let anchor_b = anchor("root-cas-b", locator_b);
        let barrier = Arc::new(tokio::sync::Barrier::new(2));

        let task_a = {
            let store = store.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                store.write_root_register_anchor(&anchor_a).await
            })
        };
        let task_b = {
            let store = store.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                store.write_root_register_anchor(&anchor_b).await
            })
        };
        let results = vec![task_a.await.unwrap(), task_b.await.unwrap()];
        assert_eq!(
            results.iter().filter(|result| result.is_ok()).count(),
            1,
            "root register create-new CAS must produce exactly one winner"
        );
        assert_eq!(
            results.iter().filter(|result| result.is_err()).count(),
            1,
            "root register create-new CAS must reject the loser"
        );

        let latest = store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .expect("winner root anchor");
        assert_eq!(latest.root_generation, 1);
        assert!(matches!(
            latest.mutation_first.as_deref(),
            Some("root-cas-a") | Some("root-cas-b")
        ));
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
