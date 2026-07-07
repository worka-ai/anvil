use super::types::*;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use hmac::{Hmac, Mac};
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const CORE_REF_LOCK_RETRY_ATTEMPTS: usize = 12_000;
const CORE_REF_LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);
const CORE_CONTROL_READ_RETRY_ATTEMPTS: usize = 400;
const LOCAL_ERASURE_PROFILE_ID: &str = "ec-4-2";
const LOCAL_DATA_SHARDS: usize = 4;
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

const ZERO_HASH: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";
const MAX_CORE_FENCE_TTL_MS: u64 = 120_000;
const CORE_STREAM_SEGMENT_MAGIC: &[u8; 8] = b"ANSEG001";
const CORE_STREAM_SEGMENT_VERSION: u16 = 1;
const CORE_STREAM_SEGMENT_HEADER_SCHEMA: &str = "anvil.core.stream_segment_header.v1";
const CORE_STREAM_RECORD_HEADER_SCHEMA: &str = "anvil.core.stream_record_header.v1";
const CORE_STREAM_SEGMENT_TRAILER_SCHEMA: &str = "anvil.core.stream_segment_trailer.v1";

type HmacSha256 = Hmac<Sha256>;

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
struct StoredStreamDirectoryEntry {
    stream_id: String,
    updated_at: String,
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
struct StoredRefDirectoryEntry {
    ref_name: String,
    updated_at: String,
}

struct CoreStoreLock {
    path: PathBuf,
}

impl Drop for CoreStoreLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
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
        Ok(store)
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    pub async fn put_blob(&self, input: PutBlob) -> Result<CoreObjectRef> {
        self.ensure_layout().await?;
        validate_logical_id(&input.logical_name, "blob logical name")?;
        let hash = sha256_hex(&input.bytes);
        let shards = encode_erasure_shards(&input.bytes)?;
        for (shard_index, shard) in shards.iter().enumerate() {
            let node_id = format!("{LOCAL_NODE_ID_PREFIX}-{}", shard_index + 1);
            let shard_hash = sha256_hex(shard);
            let shard_path = self.shard_path(&node_id, &hash, shard_index as u16, &shard_hash);
            if let Some(parent) = shard_path.parent() {
                fs::create_dir_all(parent).await?;
            }
            write_file_atomic(&shard_path, shard).await?;
        }

        Ok(CoreObjectRef {
            hash: format!("sha256:{hash}"),
            logical_size: input.bytes.len() as u64,
            manifest_ref: encode_manifest_ref(&hash),
        })
    }

    pub async fn get_blob(&self, input: GetBlob) -> Result<Vec<u8>> {
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
        if manifest.encoding.profile_id != LOCAL_ERASURE_PROFILE_ID {
            bail!(
                "CoreStore unsupported erasure profile {}",
                manifest.encoding.profile_id
            );
        }

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
            let shard_bytes = match fs::read(&shard_path).await {
                Ok(bytes) => bytes,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read CoreStore shard {}", shard_path.display()));
                }
            };
            let actual_hash = sha256_hex(&shard_bytes);
            if actual_hash != shard_hash {
                continue;
            }
            if shard_bytes.len() as u64 != placement.stored_size {
                continue;
            }
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
        let reed_solomon = ReedSolomon::new(data_shards, parity_shards)?;
        reed_solomon.reconstruct_data(&mut shards)?;
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

    pub async fn append_stream(&self, input: AppendStreamRecord) -> Result<StreamAppendReceipt> {
        validate_logical_id(&input.stream_id, "stream id")?;
        validate_logical_id(&input.partition_id, "partition id")?;
        let _stream_guard = self.acquire_stream_lock(&input.stream_id).await?;
        let _guard = self.write_lock.lock().await;
        self.append_stream_unlocked(input).await
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
        if let Some(fence) = input.fence.as_ref() {
            self.validate_fence_precondition_unlocked(fence).await?;
        }
        let mut records = self.read_all_stream_records(&input.stream_id).await?;
        let idempotency_key_hash = match input.idempotency_key.as_deref() {
            Some(key) => Some(format!("sha256:{}", sha256_hex(key.as_bytes()))),
            None => None,
        };
        let payload_hash = format!("sha256:{}", sha256_hex(&input.payload));

        if let Some(idempotency_key_hash) = idempotency_key_hash.as_deref() {
            if let Some(existing) = records
                .iter()
                .find(|record| record.idempotency_key_hash.as_deref() == Some(idempotency_key_hash))
            {
                if existing.payload_hash != payload_hash {
                    bail!(
                        "CoreStore stream idempotency conflict for stream {}",
                        input.stream_id
                    );
                }
                return Ok(StreamAppendReceipt {
                    stream_id: existing.stream_id.clone(),
                    sequence: existing.sequence,
                    cursor: existing.cursor.clone(),
                    event_hash: existing.event_hash.clone(),
                    idempotent_replay: true,
                });
            }
        }

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

    pub async fn read_stream(&self, input: ReadStream) -> Result<Vec<StreamRecord>> {
        validate_logical_id(&input.stream_id, "stream id")?;
        let mut records = self.read_all_stream_records(&input.stream_id).await?;
        records = self.filter_committed_stream_records(records).await?;
        records.retain(|record| record.sequence > input.after_sequence);
        if input.limit > 0 && records.len() > input.limit {
            records.truncate(input.limit);
        }
        Ok(records)
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
        let object_ref = self
            .put_blob(PutBlob {
                logical_name: format!(
                    "core_stream_segment:{}:{first_sequence:020}:{last_sequence:020}",
                    input.stream_id
                ),
                bytes: segment_bytes,
                region_id: "local".to_string(),
                mutation_id: input.mutation_id,
            })
            .await?;
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
        let object_ref = self
            .put_blob(PutBlob {
                logical_name: ref_name.clone(),
                bytes: serde_json::to_vec(&record)?,
                region_id: "local".to_string(),
                mutation_id: format!("core-fence:{}:{next_token}", input.fence_name),
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

    pub async fn list_stream_ids(&self, prefix: &str) -> Result<Vec<String>> {
        let mut votes: BTreeMap<String, usize> = BTreeMap::new();
        for node_id in local_control_node_ids() {
            let dir = self.stream_names_replica_dir(&node_id);
            let mut entries = match fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read CoreStore stream directory {node_id}"));
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
                let bytes = match fs::read(entry.path()).await {
                    Ok(bytes) => bytes,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => {
                        return Err(err).with_context(|| "read CoreStore stream name entry");
                    }
                };
                let stored: StoredStreamDirectoryEntry = serde_json::from_slice(&bytes)?;
                if stored.stream_id.starts_with(prefix) {
                    *votes.entry(stored.stream_id).or_default() += 1;
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
        let value = CoreRefValue {
            schema: CORE_REF_SCHEMA.to_string(),
            ref_name: input.ref_name.clone(),
            generation: next_generation,
            target: input.new_target.clone(),
            transaction_id: input.transaction_id,
            updated_at: now_rfc3339(),
        };
        self.write_ref(&value).await?;
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
            committed_at: value.updated_at.clone(),
        };
        self.append_ref_update_unlocked(&update).await?;
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
            self.delete_ref_from_quorum(ref_name).await?;
            self.delete_ref_directory_entry_from_quorum(ref_name)
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
        }
        Ok(current)
    }

    pub async fn read_ref(&self, ref_name: &str) -> Result<Option<CoreRefValue>> {
        validate_logical_id(ref_name, "ref name")?;
        self.read_ref_from_quorum(ref_name).await
    }

    pub async fn list_ref_names(&self, prefix: &str) -> Result<Vec<String>> {
        let mut votes: BTreeMap<String, usize> = BTreeMap::new();
        for node_id in local_control_node_ids() {
            let dir = self.ref_names_replica_dir(&node_id);
            let mut entries = match fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("read CoreStore ref directory {node_id}"));
                }
            };
            while let Some(entry) = entries.next_entry().await? {
                if is_core_store_temp_entry(&entry.file_name()) {
                    continue;
                }
                let file_type = match entry.file_type().await {
                    Ok(file_type) => file_type,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err).with_context(|| "read CoreStore ref entry type"),
                };
                if !file_type.is_file() {
                    continue;
                }
                let bytes = match fs::read(entry.path()).await {
                    Ok(bytes) => bytes,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => return Err(err).with_context(|| "read CoreStore ref name entry"),
                };
                let stored: StoredRefDirectoryEntry = serde_json::from_slice(&bytes)?;
                if stored.ref_name.starts_with(prefix) {
                    *votes.entry(stored.ref_name).or_default() += 1;
                }
            }
        }
        let mut names = votes
            .into_iter()
            .filter_map(|(ref_name, count)| {
                (count >= LOCAL_CONTROL_READ_QUORUM).then_some(ref_name)
            })
            .collect::<Vec<_>>();
        names.sort();
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
        if self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
            .is_some()
        {
            bail!(
                "CoreStore transaction {} already exists",
                batch.transaction_id
            );
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

        let mut visible_updates = Vec::with_capacity(batch.operations.len());
        let step_start = std::time::Instant::now();
        for operation in &batch.operations {
            match operation {
                CoreMutationOperation::RefUpdate {
                    ref_name,
                    new_target,
                    ..
                } => {
                    let update = self
                        .apply_ref_update_unlocked(
                            ref_name,
                            new_target,
                            Some(batch.transaction_id.clone()),
                            ref_precondition_for(&batch.preconditions, ref_name),
                        )
                        .await?;
                    visible_updates.push(CoreTransactionUpdate::CoreRefUpdate {
                        ref_name: ref_name.clone(),
                        new_generation: update.generation,
                    });
                }
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    record_kind,
                    payload,
                    idempotency_key,
                } => {
                    let receipt = self
                        .append_stream_unlocked(AppendStreamRecord {
                            stream_id: stream_id.clone(),
                            partition_id: partition_id.clone(),
                            record_kind: record_kind.clone(),
                            payload: payload.clone(),
                            fence: None,
                            transaction_id: Some(batch.transaction_id.clone()),
                            idempotency_key: idempotency_key.clone(),
                        })
                        .await?;
                    visible_updates.push(CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: receipt.sequence,
                        prepared_record_hash: receipt.event_hash,
                    });
                }
            }
        }
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch operations tx={timing_name}"),
            step_start.elapsed(),
        );

        let transaction = CoreTransaction {
            schema: CORE_TRANSACTION_SCHEMA.to_string(),
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            state: CoreTransactionState::Committed,
            preconditions_hash: format!(
                "sha256:{}",
                sha256_hex(&serde_json::to_vec(&batch.preconditions)?)
            ),
            operations_hash: format!(
                "sha256:{}",
                sha256_hex(&serde_json::to_vec(&batch.operations)?)
            ),
            prepared_refs: Vec::new(),
            visible_updates: visible_updates.clone(),
            committed_at: now_rfc3339(),
            committed_by_principal: batch.committed_by_principal.clone(),
        };
        let step_start = std::time::Instant::now();
        self.write_transaction_unlocked(&transaction).await?;
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
            visible_updates,
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
        self.write_transaction_unlocked(&transaction).await
    }

    async fn write_transaction_unlocked(&self, transaction: &CoreTransaction) -> Result<()> {
        let bytes = serde_json::to_vec(&transaction)?;
        self.write_bytes_to_quorum(
            &format!("CoreStore transaction {}", transaction.transaction_id),
            &bytes,
            |store, node_id| store.transaction_replica_path(node_id, &transaction.transaction_id),
        )
        .await
    }

    async fn read_transaction_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CoreTransaction>> {
        let Some(bytes) = self
            .read_bytes_from_quorum(
                &format!("CoreStore transaction {transaction_id}"),
                |store, node_id| store.transaction_replica_path(node_id, transaction_id),
            )
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    async fn transaction_is_committed(&self, transaction_id: &str) -> Result<bool> {
        match self.read_transaction_unlocked(transaction_id).await {
            Ok(Some(transaction)) => Ok(transaction.state == CoreTransactionState::Committed),
            Ok(None) => Ok(false),
            Err(error) if is_quorum_visibility_gap(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn core_ref_is_visible(&self, value: &CoreRefValue) -> Result<bool> {
        match value.transaction_id.as_deref() {
            Some(transaction_id) => self.transaction_is_committed(transaction_id).await,
            None => Ok(true),
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
        let value = CoreRefValue {
            schema: CORE_REF_SCHEMA.to_string(),
            ref_name: ref_name.to_string(),
            generation: next_generation,
            target: new_target.to_string(),
            transaction_id: transaction_id.clone(),
            updated_at: now_rfc3339(),
        };
        self.write_ref(&value).await?;
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
            committed_at: value.updated_at,
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
        ] {
            fs::create_dir_all(path).await?;
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

    async fn reconstruct_object_manifest_from_shards(
        &self,
        object_ref: &CoreObjectRef,
        object_hash: &str,
    ) -> Result<CoreObjectManifest> {
        let mut placements = Vec::with_capacity(LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS);
        let mut stripe_size = 0u64;
        for node_id in local_shard_node_ids() {
            let prefix = &object_hash[0..2];
            let dir = self
                .storage
                .core_store_replica_path(&node_id)
                .join("blobs")
                .join("sha256")
                .join(prefix)
                .join(object_hash);
            let mut entries = match fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("read CoreStore shard directory {}", dir.display())
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
                    Err(err) => return Err(err).with_context(|| "read CoreStore shard entry type"),
                };
                if !file_type.is_file() {
                    continue;
                }
                let Some(file_name) = entry.file_name().to_str().map(str::to_string) else {
                    continue;
                };
                let Some((shard_index, shard_hash)) = parse_shard_file_name(&file_name) else {
                    continue;
                };
                let metadata = entry.metadata().await?;
                stripe_size =
                    stripe_size.max(metadata.len().saturating_mul(LOCAL_DATA_SHARDS as u64));
                placements.push(CoreObjectPlacement {
                    shard_index,
                    node_id: node_id.clone(),
                    shard_hash: format!("sha256:{shard_hash}"),
                    stored_size: metadata.len(),
                    generation: 1,
                });
            }
        }

        placements.sort_by_key(|placement| placement.shard_index);
        placements.dedup_by_key(|placement| placement.shard_index);
        if placements.len() < LOCAL_DATA_SHARDS {
            bail!(
                "CoreStore manifest {} has only {} shard placements; {} data shards required",
                object_ref.manifest_ref,
                placements.len(),
                LOCAL_DATA_SHARDS
            );
        }

        Ok(CoreObjectManifest {
            schema: CORE_OBJECT_MANIFEST_SCHEMA.to_string(),
            mesh_id: "local-mesh".to_string(),
            region_id: "local".to_string(),
            object_hash: object_ref.hash.clone(),
            logical_size: object_ref.logical_size,
            encoding: CoreObjectEncoding {
                profile_id: LOCAL_ERASURE_PROFILE_ID.to_string(),
                data_shards: LOCAL_DATA_SHARDS as u16,
                parity_shards: LOCAL_PARITY_SHARDS as u16,
                minimum_read_shards: LOCAL_DATA_SHARDS as u16,
                minimum_write_ack_shards: (LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS) as u16,
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

    async fn verify_embedded_manifest_readable(&self, manifest: &CoreObjectManifest) -> Result<()> {
        if manifest.schema != CORE_OBJECT_MANIFEST_SCHEMA {
            bail!("CoreStore embedded root segment manifest has invalid schema");
        }
        let object_ref = CoreObjectRef {
            hash: manifest.object_hash.clone(),
            logical_size: manifest.logical_size,
            manifest_ref: encode_manifest_ref(strip_sha256_prefix(&manifest.object_hash)?),
        };
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
        let Some(bytes) = self
            .read_bytes_from_quorum(
                &format!("CoreStore stream {stream_id}"),
                |store, node_id| store.stream_replica_path(node_id, stream_id),
            )
            .await?
        else {
            return Ok(Vec::new());
        };
        let mut records = Vec::new();
        for (line_index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            let stored: StoredStreamRecord = serde_json::from_slice(line)
                .with_context(|| format!("decode stream {stream_id} line {}", line_index + 1))?;
            let record = StreamRecord::from(stored);
            verify_stream_record(records.last(), &record)?;
            records.push(record);
        }
        Ok(records)
    }

    async fn write_stream_records(&self, stream_id: &str, records: &[StreamRecord]) -> Result<()> {
        let mut bytes = Vec::new();
        for record in records {
            let stored = StoredStreamRecord::from(record);
            bytes.extend_from_slice(&serde_json::to_vec(&stored)?);
            bytes.push(b'\n');
        }
        self.write_bytes_to_quorum(
            &format!("CoreStore stream {stream_id}"),
            &bytes,
            |store, node_id| store.stream_replica_path(node_id, stream_id),
        )
        .await?;
        self.write_stream_directory_entry(stream_id).await
    }

    async fn write_stream_directory_entry(&self, stream_id: &str) -> Result<()> {
        let bytes = serde_json::to_vec(&StoredStreamDirectoryEntry {
            stream_id: stream_id.to_string(),
            updated_at: now_rfc3339(),
        })?;
        self.write_bytes_to_quorum(
            &format!("CoreStore stream directory entry {stream_id}"),
            &bytes,
            |store, node_id| store.stream_name_replica_path(node_id, stream_id),
        )
        .await
    }

    async fn read_ref_from_quorum(&self, ref_name: &str) -> Result<Option<CoreRefValue>> {
        for attempt in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            let mut votes: BTreeMap<String, (CoreRefValue, usize)> = BTreeMap::new();
            let mut found = 0usize;
            for node_id in local_control_node_ids() {
                let path = self.ref_replica_path(&node_id, ref_name);
                let bytes = match fs::read(&path).await {
                    Ok(bytes) => bytes,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => {
                        return Err(err).with_context(|| {
                            format!("read CoreStore ref replica {node_id}/{ref_name}")
                        });
                    }
                };
                let value: CoreRefValue = serde_json::from_slice(&bytes)?;
                if value.schema != CORE_REF_SCHEMA {
                    bail!("CoreStore ref replica {node_id}/{ref_name} has invalid schema");
                }
                if value.ref_name != ref_name {
                    bail!("CoreStore ref replica {node_id}/{ref_name} scope mismatch");
                }
                found += 1;
                let hash = sha256_hex(&bytes);
                let entry = votes.entry(hash).or_insert((value, 0));
                entry.1 += 1;
            }

            if found == 0 {
                return Ok(None);
            }
            let Some((_, (value, count))) = votes.iter().max_by_key(|(_, (_, count))| *count)
            else {
                return Ok(None);
            };
            if *count >= LOCAL_CONTROL_READ_QUORUM {
                if self.core_ref_is_visible(value).await? {
                    return Ok(Some(value.clone()));
                }
                return Ok(None);
            }
            if attempt + 1 == CORE_CONTROL_READ_RETRY_ATTEMPTS {
                bail!(
                    "CoreStore ref {ref_name} did not reach read quorum: {} matching replicas, {} required",
                    count,
                    LOCAL_CONTROL_READ_QUORUM
                );
            }
            tokio::time::sleep(CORE_REF_LOCK_RETRY_DELAY).await;
        }
        unreachable!("CoreStore control read retry loop must return")
    }

    async fn write_ref_to_quorum(&self, value: &CoreRefValue) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        let mut acks = 0usize;
        let mut errors = Vec::new();
        for node_id in local_control_node_ids() {
            let path = self.ref_replica_path(&node_id, &value.ref_name);
            match write_file_atomic(&path, &bytes).await {
                Ok(()) => acks += 1,
                Err(err) => errors.push(format!("{node_id}: {err:#}")),
            }
        }
        if acks < LOCAL_CONTROL_WRITE_QUORUM {
            bail!(
                "CoreStore ref {} write quorum failed: {} acks, {} required; errors={:?}",
                value.ref_name,
                acks,
                LOCAL_CONTROL_WRITE_QUORUM,
                errors
            );
        }
        Ok(())
    }

    async fn delete_ref_from_quorum(&self, ref_name: &str) -> Result<()> {
        let mut acks = 0usize;
        let mut errors = Vec::new();
        for node_id in local_control_node_ids() {
            let path = self.ref_replica_path(&node_id, ref_name);
            match fs::remove_file(&path).await {
                Ok(()) => acks += 1,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => acks += 1,
                Err(err) => errors.push(format!("{node_id}: {err:#}")),
            }
        }
        if acks < LOCAL_CONTROL_WRITE_QUORUM {
            bail!(
                "CoreStore ref {ref_name} delete quorum failed: {} acks, {} required; errors={:?}",
                acks,
                LOCAL_CONTROL_WRITE_QUORUM,
                errors
            );
        }
        Ok(())
    }

    async fn write_ref(&self, value: &CoreRefValue) -> Result<()> {
        self.write_ref_to_quorum(value).await?;
        self.write_ref_directory_entry(&value.ref_name).await
    }

    async fn write_ref_directory_entry(&self, ref_name: &str) -> Result<()> {
        let bytes = serde_json::to_vec(&StoredRefDirectoryEntry {
            ref_name: ref_name.to_string(),
            updated_at: now_rfc3339(),
        })?;
        self.write_bytes_to_quorum(
            &format!("CoreStore ref directory entry {ref_name}"),
            &bytes,
            |store, node_id| store.ref_name_replica_path(node_id, ref_name),
        )
        .await
    }

    async fn delete_ref_directory_entry_from_quorum(&self, ref_name: &str) -> Result<()> {
        self.delete_file_from_quorum(
            &format!("CoreStore ref directory entry {ref_name}"),
            |store, node_id| store.ref_name_replica_path(node_id, ref_name),
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
            fs::create_dir_all(parent).await?;
        }
        for _ in 0..CORE_REF_LOCK_RETRY_ATTEMPTS {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .await
            {
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

    fn stream_replica_path(&self, node_id: &str, stream_id: &str) -> PathBuf {
        self.storage
            .core_store_replica_path(node_id)
            .join("streams")
            .join("data")
            .join(format!("{}.jsonl", logical_file_name(stream_id)))
    }

    fn stream_names_replica_dir(&self, node_id: &str) -> PathBuf {
        self.storage
            .core_store_replica_path(node_id)
            .join("streams")
            .join("_names")
    }

    fn stream_name_replica_path(&self, node_id: &str, stream_id: &str) -> PathBuf {
        self.stream_names_replica_dir(node_id)
            .join(format!("{}.json", logical_file_name(stream_id)))
    }

    fn ref_replica_path(&self, node_id: &str, ref_name: &str) -> PathBuf {
        self.storage
            .core_store_replica_path(node_id)
            .join("refs")
            .join(format!("{}.json", logical_file_name(ref_name)))
    }

    fn ref_names_replica_dir(&self, node_id: &str) -> PathBuf {
        self.storage
            .core_store_replica_path(node_id)
            .join("refs")
            .join("_names")
    }

    fn ref_name_replica_path(&self, node_id: &str, ref_name: &str) -> PathBuf {
        self.ref_names_replica_dir(node_id)
            .join(format!("{}.json", logical_file_name(ref_name)))
    }

    fn transaction_replica_path(&self, node_id: &str, transaction_id: &str) -> PathBuf {
        self.storage
            .core_store_replica_path(node_id)
            .join("transactions")
            .join(format!("{}.json", logical_file_name(transaction_id)))
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
                let bytes = match fs::read(&path).await {
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

    async fn delete_file_from_quorum<F>(&self, label: &str, mut replica_path: F) -> Result<()>
    where
        F: FnMut(&Self, &str) -> PathBuf,
    {
        let mut acks = 0usize;
        let mut errors = Vec::new();
        for node_id in local_control_node_ids() {
            let path = replica_path(self, &node_id);
            match fs::remove_file(&path).await {
                Ok(()) => acks += 1,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => acks += 1,
                Err(err) => errors.push(format!("{node_id}: {err:#}")),
            }
        }
        if acks < LOCAL_CONTROL_WRITE_QUORUM {
            bail!(
                "{label} delete quorum failed: {} acks, {} required; errors={:?}",
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

fn encode_erasure_shards(bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let shard_len = bytes.len().div_ceil(LOCAL_DATA_SHARDS).max(1);
    let total_shards = LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS;
    let mut shards = vec![vec![0u8; shard_len]; total_shards];
    for (index, shard) in shards.iter_mut().take(LOCAL_DATA_SHARDS).enumerate() {
        let start = index.saturating_mul(shard_len);
        if start >= bytes.len() {
            break;
        }
        let end = usize::min(start + shard_len, bytes.len());
        shard[..end - start].copy_from_slice(&bytes[start..end]);
    }
    let reed_solomon = ReedSolomon::new(LOCAL_DATA_SHARDS, LOCAL_PARITY_SHARDS)?;
    reed_solomon.encode(&mut shards)?;
    Ok(shards)
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

fn local_control_node_id(index: usize) -> String {
    format!("{LOCAL_CONTROL_NODE_ID_PREFIX}-{index}")
}

fn local_control_node_ids() -> Vec<String> {
    (1..=LOCAL_CONTROL_REPLICA_COUNT)
        .map(local_control_node_id)
        .collect()
}

fn local_shard_node_ids() -> Vec<String> {
    (1..=(LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS))
        .map(|index| format!("{LOCAL_NODE_ID_PREFIX}-{index}"))
        .collect()
}

fn parse_shard_file_name(file_name: &str) -> Option<(u16, String)> {
    let file_name = file_name.strip_prefix("shard-")?;
    let file_name = file_name.strip_suffix(".bin")?;
    let (index, hash) = file_name.split_once('-')?;
    if index.len() != 5 || !index.as_bytes().iter().all(u8::is_ascii_digit) {
        return None;
    }
    if hash.len() != 64 || !hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        return None;
    }
    Some((index.parse().ok()?, hash.to_string()))
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

fn encode_manifest_ref(hash: &str) -> String {
    format!("core-manifest-sha256:{hash}")
}

fn decode_manifest_ref(manifest_ref: &str) -> Result<&str> {
    let hash = manifest_ref
        .strip_prefix("core-manifest-sha256:")
        .ok_or_else(|| anyhow!("CoreStore manifest_ref is not a CoreStore manifest reference"))?;
    if hash.len() != 64 || !hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore manifest_ref hash is invalid");
    }
    Ok(hash)
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

async fn write_file_atomic(path: &PathBuf, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await?;
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("CoreStore atomic write path has no file name"))?;
    let tmp_path = path.with_file_name(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));
    let mut file = fs::File::create(&tmp_path).await?;
    file.write_all(bytes).await?;
    file.sync_all().await?;
    drop(file);
    if let Err(err) = fs::rename(&tmp_path, path).await {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(err).with_context(|| {
            format!(
                "commit CoreStore atomic write {} -> {}",
                tmp_path.display(),
                path.display()
            )
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn core_store_put_get_blob_verifies_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let object_ref = store
            .put_blob(PutBlob {
                logical_name: "tenant:t/bucket:b/object:a".to_string(),
                bytes: b"hello corestore".to_vec(),
                region_id: "local".to_string(),
                mutation_id: "mut-1".to_string(),
            })
            .await
            .unwrap();
        let bytes = store.get_blob(GetBlob { object_ref }).await.unwrap();
        assert_eq!(bytes, b"hello corestore");
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
        for node_id in local_control_node_ids()
            .into_iter()
            .take(LOCAL_CONTROL_REPLICA_COUNT - LOCAL_CONTROL_READ_QUORUM)
        {
            tokio::fs::remove_file(store.ref_replica_path(&node_id, ref_name))
                .await
                .unwrap();
        }
        assert_eq!(
            store
                .read_ref(ref_name)
                .await
                .unwrap()
                .expect("read quorum survives minority replica loss")
                .target,
            "sha256:second"
        );
        for node_id in local_control_node_ids() {
            match tokio::fs::remove_file(store.ref_replica_path(&node_id, ref_name)).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => panic!("remove ref replica: {err}"),
            }
        }
        assert!(store.read_ref(ref_name).await.unwrap().is_none());
        let recovered = store
            .recover_ref_from_updates(ref_name)
            .await
            .unwrap()
            .expect("recover ref from update stream");
        assert_eq!(recovered.generation, 2);
        assert_eq!(recovered.target, "sha256:second");
        store.write_ref(&recovered).await.unwrap();

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
