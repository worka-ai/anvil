use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::partition_fence::{PartitionWritePermit, validate_partition_write};
use crate::persistence::{AppendStream, AppendStreamRecord};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AppendMutationKind {
    CreateStream,
    AppendRecord,
    SealStream,
}

impl AppendMutationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::CreateStream => "create_stream",
            Self::AppendRecord => "append_record",
            Self::SealStream => "seal_stream",
        }
    }
}

#[derive(Debug, Serialize)]
struct AppendJournalHeader<'a> {
    tenant_id: String,
    bucket_id: String,
    partition_family: &'static str,
    partition_id: String,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendBody {
    event: String,
    stream: Option<AppendStream>,
    record: Option<AppendStreamRecord>,
    emitted_at: String,
}

#[derive(Debug, Clone, Default)]
struct AppendState {
    streams: BTreeMap<i64, AppendStream>,
    records: BTreeMap<(i64, i64), AppendStreamRecord>,
}

pub async fn create_append_stream(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
) -> Result<AppendStream> {
    create_append_stream_inner(storage, tenant_id, bucket_id, bucket_name, stream_key, 0).await
}

pub async fn create_append_stream_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AppendStream> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    create_append_stream_inner(
        storage,
        tenant_id,
        bucket_id,
        bucket_name,
        stream_key,
        permit.fence_token,
    )
    .await
}

async fn create_append_stream_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    fence_token: u64,
) -> Result<AppendStream> {
    let state = read_state(storage, tenant_id, bucket_id).await?;
    let stream = AppendStream {
        id: next_stream_id(&state)?,
        tenant_id,
        bucket_id,
        bucket_name: bucket_name.to_string(),
        stream_key: stream_key.to_string(),
        stream_id: uuid::Uuid::new_v4(),
        created_at: Utc::now(),
        sealed_at: None,
        segment_hash: None,
    };
    append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::CreateStream,
        Some(stream.clone()),
        None,
        fence_token,
    )
    .await?;
    Ok(stream)
}

pub async fn get_active_append_stream(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
) -> Result<Option<AppendStream>> {
    Ok(read_state(storage, tenant_id, bucket_id)
        .await?
        .streams
        .into_values()
        .find(|stream| {
            stream.stream_key == stream_key
                && stream.stream_id == stream_id
                && stream.sealed_at.is_none()
        }))
}

pub async fn append_stream_record(
    storage: &Storage,
    stream_row_id: i64,
    payload_hash: &str,
    payload_size: i64,
) -> Result<AppendStreamRecord> {
    append_stream_record_inner(storage, stream_row_id, payload_hash, payload_size, None).await
}

pub async fn append_stream_record_with_permit(
    storage: &Storage,
    stream_row_id: i64,
    payload_hash: &str,
    payload_size: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AppendStreamRecord> {
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    append_stream_record_inner(
        storage,
        stream_row_id,
        payload_hash,
        payload_size,
        Some(permit),
    )
    .await
}

async fn append_stream_record_inner(
    storage: &Storage,
    stream_row_id: i64,
    payload_hash: &str,
    payload_size: i64,
    permit: Option<&PartitionWritePermit>,
) -> Result<AppendStreamRecord> {
    let (tenant_id, bucket_id, _) = find_stream(storage, stream_row_id)
        .await?
        .ok_or_else(|| anyhow!("append stream not found"))?;
    if let Some(permit) = permit {
        require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    }
    let fence_token = permit.map(|permit| permit.fence_token).unwrap_or(0);
    let state = read_state(storage, tenant_id, bucket_id).await?;
    let next_seq = state
        .records
        .values()
        .filter(|record| record.stream_id == stream_row_id)
        .map(|record| record.record_sequence)
        .max()
        .unwrap_or(0)
        + 1;
    let record = AppendStreamRecord {
        id: next_record_id(&state)?,
        stream_id: stream_row_id,
        record_sequence: next_seq,
        payload_hash: payload_hash.to_string(),
        payload_size,
        created_at: Utc::now(),
    };
    append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::AppendRecord,
        None,
        Some(record.clone()),
        fence_token,
    )
    .await?;
    Ok(record)
}

pub async fn list_append_stream_records(
    storage: &Storage,
    stream_row_id: i64,
) -> Result<Vec<AppendStreamRecord>> {
    let Some((tenant_id, bucket_id, _)) = find_stream(storage, stream_row_id).await? else {
        return Ok(Vec::new());
    };
    let mut records = read_state(storage, tenant_id, bucket_id)
        .await?
        .records
        .into_values()
        .filter(|record| record.stream_id == stream_row_id)
        .collect::<Vec<_>>();
    records.sort_by_key(|record| record.record_sequence);
    Ok(records)
}

pub async fn seal_append_stream(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
) -> Result<bool> {
    seal_append_stream_inner(storage, stream_row_id, segment_hash, None).await
}

pub async fn seal_append_stream_with_permit(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<bool> {
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    seal_append_stream_inner(storage, stream_row_id, segment_hash, Some(permit)).await
}

async fn seal_append_stream_inner(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
    permit: Option<&PartitionWritePermit>,
) -> Result<bool> {
    let Some((tenant_id, bucket_id, mut stream)) = find_stream(storage, stream_row_id).await?
    else {
        return Ok(false);
    };
    if let Some(permit) = permit {
        require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    }
    let fence_token = permit.map(|permit| permit.fence_token).unwrap_or(0);
    if stream.sealed_at.is_some() {
        return Ok(false);
    }
    stream.sealed_at = Some(Utc::now());
    stream.segment_hash = Some(segment_hash.to_string());
    append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::SealStream,
        Some(stream),
        None,
        fence_token,
    )
    .await?;
    Ok(true)
}

async fn find_stream(
    storage: &Storage,
    stream_row_id: i64,
) -> Result<Option<(i64, i64, AppendStream)>> {
    for path in storage.append_journal_paths().await? {
        let state = read_state_from_path(&path).await?;
        if let Some(stream) = state.streams.get(&stream_row_id).cloned() {
            return Ok(Some((stream.tenant_id, stream.bucket_id, stream)));
        }
    }
    Ok(None)
}

async fn read_state(storage: &Storage, tenant_id: i64, bucket_id: i64) -> Result<AppendState> {
    read_state_from_path(&storage.append_journal_path(tenant_id, bucket_id)).await
}

async fn read_state_from_path(path: &Path) -> Result<AppendState> {
    let frames = read_frames(path).await?;
    let mut state = AppendState::default();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::AppendMetadata {
            continue;
        }
        let body: AppendBody = serde_json::from_slice(&frame.body)?;
        match body.event.as_str() {
            "create_stream" | "seal_stream" => {
                if let Some(stream) = body.stream {
                    state.streams.insert(stream.id, stream);
                }
            }
            "append_record" => {
                if let Some(record) = body.record {
                    state
                        .records
                        .insert((record.stream_id, record.record_sequence), record);
                }
            }
            _ => {}
        }
    }
    Ok(state)
}

async fn append_body(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    event: AppendMutationKind,
    stream: Option<AppendStream>,
    record: Option<AppendStreamRecord>,
    fence_token: u64,
) -> Result<()> {
    let path = storage.append_journal_path(tenant_id, bucket_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_header(&path, tenant_id, bucket_id, fence_token).await?;
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
    let key_hash = hash32(
        format!(
            "tenant/{tenant_id}/bucket/{bucket_id}/append/{}/{}",
            stream
                .as_ref()
                .map(|s| s.id)
                .or_else(|| record.as_ref().map(|r| r.stream_id))
                .unwrap_or(0),
            event.as_str()
        )
        .as_bytes(),
    );
    let frame = JournalFrame::new(
        JournalRecordKind::AppendMetadata,
        sequence,
        fence_token,
        *mutation_id.as_bytes(),
        key_hash,
        previous_hash,
        serde_json::to_vec(&AppendBody {
            event: event.as_str().to_string(),
            stream,
            record,
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

async fn ensure_header(
    path: &Path,
    tenant_id: i64,
    bucket_id: i64,
    fence_token: u64,
) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&AppendJournalHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: bucket_id.to_string(),
        partition_family: "append_metadata",
        partition_id: hex::encode(partition_id(tenant_id, bucket_id)),
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
        .with_context(|| format!("create append journal {}", path.display()))?;
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
        anyhow::bail!("append journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated append journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("invalid append journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated append journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

fn next_stream_id(state: &AppendState) -> Result<i64> {
    state
        .streams
        .keys()
        .copied()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("append stream id overflow"))
}

fn next_record_id(state: &AppendState) -> Result<i64> {
    state
        .records
        .values()
        .map(|record| record.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("append record id overflow"))
}

fn partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/append").as_bytes())
}

fn require_append_metadata_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id = hex::encode(partition_id(tenant_id, bucket_id));
    if permit.partition_family != "append_metadata" || permit.partition_id != expected_partition_id
    {
        anyhow::bail!("append metadata write permit targets a different partition");
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

    const KEY: &[u8] = b"append journal partition owner key";

    #[tokio::test]
    async fn append_journal_replays_stream_records_and_seal() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stream = create_append_stream(&storage, 1, 2, "bucket", "stream")
            .await
            .unwrap();
        append_stream_record(&storage, stream.id, "hash-a", 10)
            .await
            .unwrap();
        append_stream_record(&storage, stream.id, "hash-b", 20)
            .await
            .unwrap();
        assert_eq!(
            list_append_stream_records(&storage, stream.id)
                .await
                .unwrap()
                .len(),
            2
        );
        assert!(
            get_active_append_stream(&storage, 1, 2, "stream", stream.stream_id)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            seal_append_stream(&storage, stream.id, "seg")
                .await
                .unwrap()
        );
        assert!(
            get_active_append_stream(&storage, 1, 2, "stream", stream.stream_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn append_journal_with_permit_writes_fenced_frames_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let permit = owner.write_permit().unwrap();

        let stream =
            create_append_stream_with_permit(&storage, 1, 2, "bucket", "stream", &permit, KEY)
                .await
                .unwrap();
        append_stream_record_with_permit(&storage, stream.id, "hash-a", 10, &permit, KEY)
            .await
            .unwrap();
        seal_append_stream_with_permit(&storage, stream.id, "segment-a", &permit, KEY)
            .await
            .unwrap();

        let journal = tokio::fs::read(storage.append_journal_path(1, 2))
            .await
            .unwrap();
        let header = BinaryEnvelopeHeader::decode(&journal).unwrap();
        let header_json: serde_json::Value = serde_json::from_slice(&header.header_json).unwrap();
        assert_eq!(header_json["partition_family"], "append_metadata");
        assert_eq!(header_json["partition_id"], permit.partition_id);
        assert_eq!(header_json["fence_token"], permit.fence_token);

        let frames = decode_journal_file(&journal).unwrap();
        assert_eq!(frames.len(), 3);
        assert!(
            frames
                .iter()
                .all(|frame| frame.fence_token == permit.fence_token)
        );
    }

    #[tokio::test]
    async fn append_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stream = create_append_stream_with_permit(
            &storage,
            1,
            2,
            "bucket",
            "stream",
            &stale_permit,
            KEY,
        )
        .await
        .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err =
            append_stream_record_with_permit(&storage, stream.id, "hash-a", 10, &stale_permit, KEY)
                .await
                .unwrap_err();
        assert!(
            err.to_string()
                .contains("write permit owner is not current")
        );
    }

    async fn ready_owner(
        storage: &Storage,
        tenant_id: i64,
        bucket_id: i64,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "append_metadata".to_string();
        let id = hex::encode(partition_id(tenant_id, bucket_id));
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
