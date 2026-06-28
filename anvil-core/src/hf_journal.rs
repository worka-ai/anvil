use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::partition_fence::{PartitionWritePermit, validate_partition_write};
use crate::persistence::{HfIngestion, HfIngestionItem, HfIngestionJob, HfKey};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use tokio::io::AsyncWriteExt;

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

#[derive(Debug, Serialize)]
struct HfJournalHeader<'a> {
    partition_family: &'static str,
    partition_id: String,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
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

async fn create_key(
    storage: &Storage,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
) -> Result<()> {
    create_key_inner(storage, name, token_encrypted, note, 0).await
}

pub(crate) async fn create_key_with_permit(
    storage: &Storage,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let fence_token = validate_hf_write(storage, permit, partition_owner_signing_key).await?;
    create_key_inner(storage, name, token_encrypted, note, fence_token).await
}

async fn create_key_inner(
    storage: &Storage,
    name: &str,
    token_encrypted: &[u8],
    note: Option<&str>,
    fence_token: u64,
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
        fence_token,
    )
    .await
}

async fn delete_key(storage: &Storage, name: &str) -> Result<u64> {
    delete_key_inner(storage, name, 0).await
}

pub(crate) async fn delete_key_with_permit(
    storage: &Storage,
    name: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let fence_token = validate_hf_write(storage, permit, partition_owner_signing_key).await?;
    delete_key_inner(storage, name, fence_token).await
}

async fn delete_key_inner(storage: &Storage, name: &str, fence_token: u64) -> Result<u64> {
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
            fence_token,
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
        0,
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
    let fence_token = validate_hf_write(storage, permit, partition_owner_signing_key).await?;
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
        fence_token,
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
    fence_token: u64,
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
        fence_token,
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

async fn update_ingestion_state(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
) -> Result<()> {
    update_ingestion_state_inner(storage, id, state_value, error, 0).await
}

pub(crate) async fn update_ingestion_state_with_permit(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let fence_token = validate_hf_write(storage, permit, partition_owner_signing_key).await?;
    update_ingestion_state_inner(storage, id, state_value, error, fence_token).await
}

async fn update_ingestion_state_inner(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionState,
    error: Option<&str>,
    fence_token: u64,
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
        fence_token,
    )
    .await
}

async fn cancel_ingestion(storage: &Storage, id: i64) -> Result<u64> {
    cancel_ingestion_inner(storage, id, 0).await
}

pub(crate) async fn cancel_ingestion_with_permit(
    storage: &Storage,
    id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    let fence_token = validate_hf_write(storage, permit, partition_owner_signing_key).await?;
    cancel_ingestion_inner(storage, id, fence_token).await
}

async fn cancel_ingestion_inner(storage: &Storage, id: i64, fence_token: u64) -> Result<u64> {
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
        fence_token,
    )
    .await?;
    Ok(1)
}

async fn add_item(
    storage: &Storage,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
) -> Result<i64> {
    add_item_inner(storage, ingestion_id, path, size, etag, 0).await
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
    let fence_token = validate_hf_write(storage, permit, partition_owner_signing_key).await?;
    add_item_inner(storage, ingestion_id, path, size, etag, fence_token).await
}

async fn add_item_inner(
    storage: &Storage,
    ingestion_id: i64,
    path: &str,
    size: Option<i64>,
    etag: Option<&str>,
    fence_token: u64,
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
        fence_token,
    )
    .await?;
    Ok(id)
}

async fn update_item_state(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
) -> Result<()> {
    update_item_state_inner(storage, id, state_value, error, 0).await
}

pub(crate) async fn update_item_state_with_permit(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let fence_token = validate_hf_write(storage, permit, partition_owner_signing_key).await?;
    update_item_state_inner(storage, id, state_value, error, fence_token).await
}

async fn update_item_state_inner(
    storage: &Storage,
    id: i64,
    state_value: crate::tasks::HFIngestionItemState,
    error: Option<&str>,
    fence_token: u64,
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
        fence_token,
    )
    .await
}

async fn update_item_success(storage: &Storage, id: i64, size: i64, etag: &str) -> Result<()> {
    update_item_success_inner(storage, id, size, etag, 0).await
}

pub(crate) async fn update_item_success_with_permit(
    storage: &Storage,
    id: i64,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let fence_token = validate_hf_write(storage, permit, partition_owner_signing_key).await?;
    update_item_success_inner(storage, id, size, etag, fence_token).await
}

async fn update_item_success_inner(
    storage: &Storage,
    id: i64,
    size: i64,
    etag: &str,
    fence_token: u64,
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
        fence_token,
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
    let state_text = if job.state == crate::tasks::HFIngestionState::Running
        && queued == 0
        && downloading == 0
        && (stored > 0 || failed > 0)
    {
        "completed".to_string()
    } else {
        job.state.as_str().to_string()
    };
    Ok((
        state_text,
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
    let frames = read_frames(&storage.hf_journal_path()).await?;
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
    fence_token: u64,
) -> Result<()> {
    let path = storage.hf_journal_path();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_header(&path, fence_token).await?;
    let previous = read_frames(&path).await.unwrap_or_default();
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
        fence_token,
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
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn ensure_header(path: &Path, fence_token: u64) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&HfJournalHeader {
        partition_family: "hf_metadata",
        partition_id: hex::encode(hf_partition_id()),
        fence_token,
        first_sequence: 1,
        created_at: &created_at,
        codec: "none",
    })?;
    let header = BinaryEnvelopeHeader::new(FileFamily::MetadataJournal, 0, 0, header_json);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .with_context(|| format!("create hf journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn read_frames(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    decode_journal_file(&tokio::fs::read(path).await?)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("hf journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated hf journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("invalid hf journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated hf journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
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

async fn validate_hf_write(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<u64> {
    require_hf_permit(permit)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    Ok(permit.fence_token)
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
    pub(crate) async fn hf_journal_with_permit_writes_fenced_frames_and_header() {
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

        let journal = tokio::fs::read(storage.hf_journal_path()).await.unwrap();
        let header = BinaryEnvelopeHeader::decode(&journal).unwrap();
        let header_json: serde_json::Value = serde_json::from_slice(&header.header_json).unwrap();
        assert_eq!(header_json["partition_family"], "hf_metadata");
        assert_eq!(header_json["partition_id"], permit.partition_id);
        assert_eq!(header_json["fence_token"], permit.fence_token);

        let frames = decode_journal_file(&journal).unwrap();
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
