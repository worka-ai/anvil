use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
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
    let (tenant_id, bucket_id, _) = find_stream(storage, stream_row_id)
        .await?
        .ok_or_else(|| anyhow!("append stream not found"))?;
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
    let Some((tenant_id, bucket_id, mut stream)) = find_stream(storage, stream_row_id).await?
    else {
        return Ok(false);
    };
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
) -> Result<()> {
    let path = storage.append_journal_path(tenant_id, bucket_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_header(&path, tenant_id, bucket_id).await?;
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
        0,
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

async fn ensure_header(path: &Path, tenant_id: i64, bucket_id: i64) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&AppendJournalHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: bucket_id.to_string(),
        partition_family: "append_metadata",
        partition_id: hex::encode(partition_id(tenant_id, bucket_id)),
        fence_token: 0,
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
}
