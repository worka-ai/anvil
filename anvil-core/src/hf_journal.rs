use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
};
use crate::formats::{Hash32, JournalFrame, JournalRecordKind, hash32, validate_journal_chain};
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
use crate::persistence::{HfIngestion, HfIngestionItem, HfIngestionJob, HfKey};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HfMutationKind {
    KeyUpsert,
    KeyDelete,
    IngestionUpsert,
    ItemUpsert,
}

impl HfMutationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::KeyUpsert => "key_upsert",
            Self::KeyDelete => "key_delete",
            Self::IngestionUpsert => "ingestion_upsert",
            Self::ItemUpsert => "item_upsert",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HfBody {
    event: String,
    key: Option<HfKey>,
    key_name: Option<String>,
    ingestion: Option<HfIngestion>,
    item: Option<HfIngestionItem>,
    emitted_at: String,
}

#[derive(Debug, Clone, Default)]
struct HfState {
    keys: BTreeMap<i64, HfKey>,
    ingestions: BTreeMap<i64, HfIngestion>,
    items: BTreeMap<i64, HfIngestionItem>,
}

#[derive(Debug, Clone, Default)]
struct HfWriteGuard {
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
}

#[cfg(test)]
async fn create_key(
    storage: &Storage,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
) -> Result<()> {
    create_key_inner(
        storage,
        name,
        token_encrypted,
        note,
        HfWriteGuard::default(),
    )
    .await
}

pub(crate) async fn create_key_with_permit(
    storage: &Storage,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    create_key_inner(storage, name, token_encrypted, note, guard).await
}

async fn create_key_inner(
    storage: &Storage,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    let state = read_state(storage).await?;
    if state.keys.values().any(|key| key.name == name) {
        return Err(anyhow!("hugging face key already exists"));
    }
    let now = Utc::now();
    append_body(
        storage,
        HfMutationKind::KeyUpsert,
        Some(HfKey {
            id: next_key_id(&state)?,
            name: name.to_string(),
            token_encrypted: token_encrypted.to_vec(),
            note: note.map(ToOwned::to_owned),
            created_at: now,
            updated_at: now,
        }),
        None,
        None,
        None,
        guard,
    )
    .await
}

#[cfg(test)]
async fn delete_key(storage: &Storage, name: &str) -> Result<u64> {
    delete_key_inner(storage, name, HfWriteGuard::default()).await
}

pub(crate) async fn delete_key_with_permit(
    storage: &Storage,
    name: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    delete_key_inner(storage, name, guard).await
}

async fn delete_key_inner(storage: &Storage, name: &str, guard: HfWriteGuard) -> Result<u64> {
    let state = read_state(storage).await?;
    let deleted = state.keys.values().any(|key| key.name == name);
    if deleted {
        append_body(
            storage,
            HfMutationKind::KeyDelete,
            None,
            Some(name.to_string()),
            None,
            None,
            guard,
        )
        .await?;
    }
    Ok(u64::from(deleted))
}

pub async fn get_key_encrypted(storage: &Storage, name: &str) -> Result<Option<(i64, Vec<u8>)>> {
    Ok(read_state(storage)
        .await?
        .keys
        .into_values()
        .find(|key| key.name == name)
        .map(|key| (key.id, key.token_encrypted)))
}

pub async fn get_key_encrypted_by_id(storage: &Storage, id: i64) -> Result<Option<Vec<u8>>> {
    Ok(read_state(storage)
        .await?
        .keys
        .remove(&id)
        .map(|key| key.token_encrypted))
}

pub(crate) async fn list_encrypted_keys(storage: &Storage) -> Result<Vec<HfKey>> {
    let mut keys = read_state(storage)
        .await?
        .keys
        .into_values()
        .collect::<Vec<_>>();
    keys.sort_by_key(|key| key.id);
    Ok(keys)
}

pub(crate) async fn update_key_encrypted_with_permit(
    storage: &Storage,
    id: i64,
    token_encrypted: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    let state = read_state(storage).await?;
    let mut key = state
        .keys
        .get(&id)
        .cloned()
        .ok_or_else(|| anyhow!("hugging face key not found"))?;
    key.token_encrypted = token_encrypted.to_vec();
    key.updated_at = Utc::now();
    append_body(
        storage,
        HfMutationKind::KeyUpsert,
        Some(key),
        None,
        None,
        None,
        guard,
    )
    .await
}

pub async fn list_keys(
    storage: &Storage,
) -> Result<Vec<(String, Option<String>, DateTime<Utc>, DateTime<Utc>)>> {
    let mut keys = read_state(storage)
        .await?
        .keys
        .into_values()
        .map(|key| (key.name, key.note, key.created_at, key.updated_at))
        .collect::<Vec<_>>();
    keys.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(keys)
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
async fn create_ingestion(
    storage: &Storage,
    key_id: i64,
    tenant_id: i64,
    requester_app_id: i64,
    repo: &str,
    revision: Option<&str>,
    target_bucket: &str,
    target_region: &str,
    target_prefix: Option<&str>,
    include_globs: &[String],
    exclude_globs: &[String],
) -> Result<i64> {
    create_ingestion_inner(
        storage,
        key_id,
        tenant_id,
        requester_app_id,
        repo,
        revision,
        target_bucket,
        target_region,
        target_prefix,
        include_globs,
        exclude_globs,
        HfWriteGuard::default(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_ingestion_with_permit(
    storage: &Storage,
    key_id: i64,
    tenant_id: i64,
    requester_app_id: i64,
    repo: &str,
    revision: Option<&str>,
    target_bucket: &str,
    target_region: &str,
    target_prefix: Option<&str>,
    include_globs: &[String],
    exclude_globs: &[String],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<i64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    create_ingestion_inner(
        storage,
        key_id,
        tenant_id,
        requester_app_id,
        repo,
        revision,
        target_bucket,
        target_region,
        target_prefix,
        include_globs,
        exclude_globs,
        guard,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn create_ingestion_inner(
    storage: &Storage,
    key_id: i64,
    tenant_id: i64,
    requester_app_id: i64,
    repo: &str,
    revision: Option<&str>,
    target_bucket: &str,
    target_region: &str,
    target_prefix: Option<&str>,
    include_globs: &[String],
    exclude_globs: &[String],
    guard: HfWriteGuard,
) -> Result<i64> {
    let state = read_state(storage).await?;
    let id = next_ingestion_id(&state)?;
    append_body(
        storage,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(HfIngestion {
            id,
            key_id,
            tenant_id,
            requester_app_id,
            repo: repo.to_string(),
            revision: revision.unwrap_or("main").to_string(),
            target_bucket: target_bucket.to_string(),
            target_region: target_region.to_string(),
            target_prefix: target_prefix.unwrap_or_default().to_string(),
            include_globs: include_globs.to_vec(),
            exclude_globs: exclude_globs.to_vec(),
            state: crate::tasks::HFIngestionState::Queued,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
        }),
        None,
        guard,
    )
    .await?;
    Ok(id)
}

pub async fn get_ingestion_job(storage: &Storage, id: i64) -> Result<Option<HfIngestionJob>> {
    Ok(read_state(storage)
        .await?
        .ingestions
        .remove(&id)
        .map(|job| HfIngestionJob {
            key_id: job.key_id,
            tenant_id: job.tenant_id,
            requester_app_id: job.requester_app_id,
            repo: job.repo,
            revision: job.revision,
            target_bucket: job.target_bucket,
            target_region: job.target_region,
            target_prefix: job.target_prefix,
            include_globs: job.include_globs,
            exclude_globs: job.exclude_globs,
        }))
}

#[cfg(test)]
async fn update_ingestion_state(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
) -> Result<()> {
    update_ingestion_state_inner(storage, id, state_value, error, HfWriteGuard::default()).await
}

pub(crate) async fn update_ingestion_state_with_permit(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_ingestion_state_inner(storage, id, state_value, error, guard).await
}

async fn update_ingestion_state_inner(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    let Some(mut job) = read_state(storage).await?.ingestions.remove(&id) else {
        return Ok(());
    };
    job.state = state_value;
    job.error = error.map(ToOwned::to_owned);
    if state_value == crate::tasks::HFIngestionState::Running && job.started_at.is_none() {
        job.started_at = Some(Utc::now());
    }
    if matches!(
        state_value,
        crate::tasks::HFIngestionState::Completed
            | crate::tasks::HFIngestionState::Failed
            | crate::tasks::HFIngestionState::Canceled
    ) {
        job.finished_at = Some(Utc::now());
    }
    append_body(
        storage,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(job),
        None,
        guard,
    )
    .await
}

pub(crate) async fn cancel_ingestion_with_permit(
    storage: &Storage,
    id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    cancel_ingestion_inner(storage, id, guard).await
}

async fn cancel_ingestion_inner(storage: &Storage, id: i64, guard: HfWriteGuard) -> Result<u64> {
    let Some(mut job) = read_state(storage).await?.ingestions.remove(&id) else {
        return Ok(0);
    };
    if !matches!(
        job.state,
        crate::tasks::HFIngestionState::Queued | crate::tasks::HFIngestionState::Running
    ) {
        return Ok(0);
    }
    job.state = crate::tasks::HFIngestionState::Canceled;
    job.finished_at = Some(Utc::now());
    append_body(
        storage,
        HfMutationKind::IngestionUpsert,
        None,
        None,
        Some(job),
        None,
        guard,
    )
    .await?;
    Ok(1)
}

#[cfg(test)]
async fn add_item(
    storage: &Storage,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
) -> Result<i64> {
    add_item_inner(
        storage,
        ingestion_id,
        path,
        size,
        etag,
        HfWriteGuard::default(),
    )
    .await
}

pub(crate) async fn add_item_with_permit(
    storage: &Storage,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<i64> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    add_item_inner(storage, ingestion_id, path, size, etag, guard).await
}

async fn add_item_inner(
    storage: &Storage,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
    guard: HfWriteGuard,
) -> Result<i64> {
    let state = read_state(storage).await?;
    let mut item = state
        .items
        .values()
        .find(|item| item.ingestion_id == ingestion_id && item.path == path)
        .cloned()
        .unwrap_or_else(|| HfIngestionItem {
            id: 0,
            ingestion_id,
            path: path.to_string(),
            size,
            etag: etag.map(ToOwned::to_owned),
            state: crate::tasks::HFIngestionItemState::Queued,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
        });
    if item.id == 0 {
        item.id = next_item_id(&state)?;
    }
    item.size = size;
    item.etag = etag.map(ToOwned::to_owned);
    let id = item.id;
    append_body(
        storage,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
    )
    .await?;
    Ok(id)
}

pub(crate) async fn update_item_state_with_permit(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_item_state_inner(storage, id, state_value, error, guard).await
}

async fn update_item_state_inner(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
    guard: HfWriteGuard,
) -> Result<()> {
    let Some(mut item) = read_state(storage).await?.items.remove(&id) else {
        return Ok(());
    };
    item.state = state_value;
    item.error = error.map(ToOwned::to_owned);
    if state_value == crate::tasks::HFIngestionItemState::Downloading && item.started_at.is_none() {
        item.started_at = Some(Utc::now());
    }
    if matches!(
        state_value,
        crate::tasks::HFIngestionItemState::Stored
            | crate::tasks::HFIngestionItemState::Failed
            | crate::tasks::HFIngestionItemState::Skipped
    ) {
        item.finished_at = Some(Utc::now());
    }
    append_body(
        storage,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
    )
    .await
}

#[cfg(test)]
async fn update_item_success(storage: &Storage, id: i64, size: i64, etag: &str) -> Result<()> {
    update_item_success_inner(storage, id, size, etag, HfWriteGuard::default()).await
}

pub(crate) async fn update_item_success_with_permit(
    storage: &Storage,
    id: i64,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let guard = hf_write_guard(storage, permit, partition_owner_signing_key).await?;
    update_item_success_inner(storage, id, size, etag, guard).await
}

async fn update_item_success_inner(
    storage: &Storage,
    id: i64,
    size: i64,
    etag: &str,
    guard: HfWriteGuard,
) -> Result<()> {
    let Some(mut item) = read_state(storage).await?.items.remove(&id) else {
        return Ok(());
    };
    item.state = crate::tasks::HFIngestionItemState::Stored;
    item.size = Some(size);
    item.etag = Some(etag.to_string());
    item.finished_at = Some(Utc::now());
    append_body(
        storage,
        HfMutationKind::ItemUpsert,
        None,
        None,
        None,
        Some(item),
        guard,
    )
    .await
}

pub async fn get_ingestion_items(
    storage: &Storage,
    ingestion_id: i64,
) -> Result<Vec<(String, Option<i64>, Option<String>, Option<DateTime<Utc>>)>> {
    Ok(read_state(storage)
        .await?
        .items
        .into_values()
        .filter(|item| {
            item.ingestion_id == ingestion_id
                && item.state == crate::tasks::HFIngestionItemState::Stored
        })
        .map(|item| (item.path, item.size, item.etag, item.finished_at))
        .collect())
}

pub async fn get_all_items_for_prefix(
    storage: &Storage,
    tenant_id: i64,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<(String, Option<i64>, Option<String>, Option<DateTime<Utc>>)>> {
    let state = read_state(storage).await?;
    let ingestion_ids = state
        .ingestions
        .values()
        .filter(|job| {
            job.tenant_id == tenant_id && job.target_bucket == bucket && job.target_prefix == prefix
        })
        .map(|job| job.id)
        .collect::<HashSet<_>>();
    Ok(state
        .items
        .into_values()
        .filter(|item| {
            ingestion_ids.contains(&item.ingestion_id)
                && item.state == crate::tasks::HFIngestionItemState::Stored
        })
        .map(|item| (item.path, item.size, item.etag, item.finished_at))
        .collect())
}

pub async fn status_summary(
    storage: &Storage,
    id: i64,
) -> Result<(
    String,
    i64,
    i64,
    i64,
    i64,
    Option<String>,
    Option<DateTime<Utc>>,
    Option<DateTime<Utc>>,
    DateTime<Utc>,
)> {
    let state = read_state(storage).await?;
    let job = state
        .ingestions
        .get(&id)
        .ok_or_else(|| anyhow!("ingestion not found"))?;
    let queued = count_items(&state, id, crate::tasks::HFIngestionItemState::Queued);
    let downloading = count_items(&state, id, crate::tasks::HFIngestionItemState::Downloading);
    let stored = count_items(&state, id, crate::tasks::HFIngestionItemState::Stored);
    let failed = count_items(&state, id, crate::tasks::HFIngestionItemState::Failed);
    Ok((
        job.state.as_str().to_string(),
        queued,
        downloading,
        stored,
        failed,
        job.error.clone(),
        job.started_at,
        job.finished_at,
        job.created_at,
    ))
}

fn count_items(state: &HfState, id: i64, item_state: crate::tasks::HFIngestionItemState) -> i64 {
    state
        .items
        .values()
        .filter(|item| item.ingestion_id == id && item.state == item_state)
        .count() as i64
}

async fn read_state(storage: &Storage) -> Result<HfState> {
    let frames = read_hf_journal_frames(storage).await?;
    let mut state = HfState::default();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::HfMetadata {
            continue;
        }
        let body: HfBody = serde_json::from_slice(&frame.body)?;
        match body.event.as_str() {
            "key_upsert" => {
                if let Some(key) = body.key {
                    state.keys.insert(key.id, key);
                }
            }
            "key_delete" => {
                if let Some(name) = body.key_name {
                    state.keys.retain(|_, key| key.name != name);
                }
            }
            "ingestion_upsert" => {
                if let Some(ingestion) = body.ingestion {
                    state.ingestions.insert(ingestion.id, ingestion);
                }
            }
            "item_upsert" => {
                if let Some(item) = body.item {
                    state.items.insert(item.id, item);
                }
            }
            _ => {}
        }
    }
    Ok(state)
}

async fn append_body(
    storage: &Storage,
    event: HfMutationKind,
    key: Option<HfKey>,
    key_name: Option<String>,
    ingestion: Option<HfIngestion>,
    item: Option<HfIngestionItem>,
    guard: HfWriteGuard,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let previous = read_hf_journal_frames_from_store(&core_store)
        .await
        .unwrap_or_default();
    let sequence = previous
        .last()
        .map(|frame| frame.partition_sequence + 1)
        .unwrap_or(1);
    let previous_hash = previous
        .last()
        .map(|frame| frame.record_hash)
        .unwrap_or([0; 32]);
    let mutation_id = uuid::Uuid::new_v4();
    let key_text = key
        .as_ref()
        .map(|key| format!("key/{}", key.id))
        .or_else(|| key_name.as_ref().map(|name| format!("key-name/{name}")))
        .or_else(|| {
            ingestion
                .as_ref()
                .map(|job| format!("ingestion/{}", job.id))
        })
        .or_else(|| item.as_ref().map(|item| format!("item/{}", item.id)))
        .unwrap_or_else(|| event.as_str().to_string());
    let frame = JournalFrame::new(
        JournalRecordKind::HfMetadata,
        sequence,
        guard.fence_token,
        *mutation_id.as_bytes(),
        hash32(format!("hf/{key_text}").as_bytes()),
        previous_hash,
        serde_json::to_vec(&HfBody {
            event: event.as_str().to_string(),
            key,
            key_name,
            ingestion,
            item,
            emitted_at: Utc::now().to_rfc3339(),
        })?,
    );
    let partition_id = hex::encode(hf_partition_id());
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("hf-metadata:{mutation_id}"),
            scope_partition: partition_id.clone(),
            committed_by_principal: hf_partition_principal(),
            preconditions: guard.partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id: hf_metadata_stream_id(),
                record_kind: "hf_metadata".to_string(),
                payload: frame.encode(),
                idempotency_key: Some(format!("hf-metadata:{mutation_id}")),
            }],
        })
        .await?;
    Ok(())
}

async fn read_hf_journal_frames(storage: &Storage) -> Result<Vec<JournalFrame>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_hf_journal_frames_from_store(&core_store).await
}

async fn read_hf_journal_frames_from_store(core_store: &CoreStore) -> Result<Vec<JournalFrame>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: hf_metadata_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut frames = Vec::new();
    for record in records {
        if record.record_kind != "hf_metadata" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

fn next_key_id(state: &HfState) -> Result<i64> {
    state
        .keys
        .keys()
        .copied()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("hf key id overflow"))
}

fn next_ingestion_id(state: &HfState) -> Result<i64> {
    state
        .ingestions
        .keys()
        .copied()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("hf ingestion id overflow"))
}

fn next_item_id(state: &HfState) -> Result<i64> {
    state
        .items
        .keys()
        .copied()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("hf item id overflow"))
}

pub fn hf_partition_id() -> Hash32 {
    hash32(b"hf_metadata/global")
}

fn hf_metadata_stream_id() -> String {
    "hf_metadata:global".to_string()
}

fn hf_partition_principal() -> String {
    "partition-owner:hf_metadata:global".to_string()
}

#[cfg(test)]
pub(crate) async fn read_hf_frame_fences_for_test(storage: &Storage) -> Result<Vec<u64>> {
    Ok(read_hf_journal_frames(storage)
        .await?
        .into_iter()
        .map(|frame| frame.fence_token)
        .collect())
}

async fn hf_write_guard(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<HfWriteGuard> {
    require_hf_permit(permit)?;
    Ok(HfWriteGuard {
        fence_token: permit.fence_token,
        partition_precondition: Some(
            partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?,
        ),
    })
}

fn require_hf_permit(permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "hf_metadata"
        || permit.partition_id != hex::encode(hf_partition_id())
    {
        anyhow::bail!("hf metadata write permit targets a different partition");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use tempfile::tempdir;

    const KEY: &[u8] = b"hf metadata partition owner key";

    #[tokio::test]
    async fn hf_journal_replays_keys_ingestions_and_items() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        create_key(&storage, "primary", b"secret", Some("note"))
            .await
            .unwrap();
        let (key_id, secret) = get_key_encrypted(&storage, "primary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(secret, b"secret");
        let ingestion_id = create_ingestion(
            &storage,
            key_id,
            1,
            2,
            "owner/repo",
            None,
            "bucket",
            "region",
            Some("prefix"),
            &[],
            &[],
        )
        .await
        .unwrap();
        update_ingestion_state(
            &storage,
            ingestion_id,
            crate::tasks::HFIngestionState::Running,
            None,
        )
        .await
        .unwrap();
        let item_id = add_item(&storage, ingestion_id, "a.txt", None, None)
            .await
            .unwrap();
        update_item_success(&storage, item_id, 10, "etag")
            .await
            .unwrap();
        assert_eq!(
            get_ingestion_items(&storage, ingestion_id)
                .await
                .unwrap()
                .len(),
            1
        );
        let summary = status_summary(&storage, ingestion_id).await.unwrap();
        assert_eq!(summary.3, 1);
        assert_eq!(delete_key(&storage, "primary").await.unwrap(), 1);
        assert!(
            get_key_encrypted_by_id(&storage, key_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    pub(crate) async fn hf_journal_with_permit_writes_fenced_frames() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();

        create_key_with_permit(&storage, "primary", b"secret", Some("note"), &permit, KEY)
            .await
            .unwrap();
        let (key_id, _) = get_key_encrypted(&storage, "primary")
            .await
            .unwrap()
            .unwrap();
        let ingestion_id = create_ingestion_with_permit(
            &storage,
            key_id,
            1,
            2,
            "owner/repo",
            None,
            "bucket",
            "region",
            Some("prefix"),
            &[],
            &[],
            &permit,
            KEY,
        )
        .await
        .unwrap();
        update_ingestion_state_with_permit(
            &storage,
            ingestion_id,
            crate::tasks::HFIngestionState::Running,
            None,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        let item_id =
            add_item_with_permit(&storage, ingestion_id, "a.txt", None, None, &permit, KEY)
                .await
                .unwrap();
        update_item_state_with_permit(
            &storage,
            item_id,
            crate::tasks::HFIngestionItemState::Downloading,
            None,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        update_item_success_with_permit(&storage, item_id, 10, "etag", &permit, KEY)
            .await
            .unwrap();
        delete_key_with_permit(&storage, "primary", &permit, KEY)
            .await
            .unwrap();

        let frames = read_hf_journal_frames(&storage).await.unwrap();
        assert_eq!(frames.len(), 7);
        assert!(
            frames
                .iter()
                .all(|frame| frame.fence_token == permit.fence_token)
        );
    }

    #[tokio::test]
    pub(crate) async fn hf_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_key_with_permit(
            &storage,
            "primary",
            b"secret",
            Some("note"),
            &stale_permit,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("write permit owner is not current")
        );
    }

    #[tokio::test]
    pub(crate) async fn hf_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_ref_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_key_inner(
            &storage,
            "primary",
            b"secret",
            Some("note"),
            HfWriteGuard {
                fence_token: stale_permit.fence_token,
                partition_precondition: Some(stale_precondition),
            },
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("generation mismatch") || message.contains("target mismatch"),
            "unexpected stale precondition error: {message}"
        );

        create_key_with_permit(
            &storage,
            "primary",
            b"secret",
            Some("note"),
            &newer.write_permit().unwrap(),
            KEY,
        )
        .await
        .unwrap();
    }

    async fn ready_owner(
        storage: &Storage,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "hf_metadata".to_string();
        let id = hex::encode(hf_partition_id());
        let recovering = acquire_partition_recovery(
            storage,
            PartitionRecoveryAcquire {
                partition_family: family.clone(),
                partition_id: id.clone(),
                owner_node_id: owner_node_id.to_string(),
                recovered_through_sequence: 0,
                recovered_manifest_hash: hex::encode([0; 32]),
                now_nanos: 100,
            },
            KEY,
        )
        .await
        .unwrap();
        publish_partition_ready(
            storage,
            &family,
            &id,
            owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([1; 32]),
            200,
            KEY,
        )
        .await
        .unwrap()
    }
}
