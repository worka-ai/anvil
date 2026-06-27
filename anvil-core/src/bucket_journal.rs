use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::persistence::{Bucket, BucketMetadataEvent};
use crate::storage::Storage;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketJournalMutation {
    Create,
    Update,
    Delete,
}

impl BucketJournalMutation {
    fn event_name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Serialize)]
struct BucketJournalHeader<'a> {
    tenant_id: String,
    partition_family: &'static str,
    partition_id: String,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BucketJournalBody {
    event: String,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    region: String,
    is_public_read: bool,
    mutation_id: String,
    created_at: String,
    #[serde(default)]
    emitted_at: Option<String>,
}

pub async fn append_bucket_mutation(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
) -> Result<()> {
    append_bucket_mutation_to_path(
        storage.bucket_metadata_journal_path(bucket.tenant_id),
        bucket,
        mutation,
        BucketJournalScope::Tenant(bucket.tenant_id),
    )
    .await?;
    append_bucket_mutation_to_path(
        storage.global_bucket_metadata_journal_path(),
        bucket,
        mutation,
        BucketJournalScope::Global,
    )
    .await
}

pub async fn read_public_bucket_by_name(
    storage: &Storage,
    bucket_name: &str,
) -> Result<Option<Bucket>> {
    Ok(
        read_current_buckets_from_path(storage.global_bucket_metadata_journal_path())
            .await?
            .into_iter()
            .find(|bucket| bucket.name == bucket_name && bucket.is_public_read),
    )
}

pub async fn read_current_bucket_by_name(
    storage: &Storage,
    bucket_name: &str,
) -> Result<Option<Bucket>> {
    Ok(
        read_current_buckets_from_path(storage.global_bucket_metadata_journal_path())
            .await?
            .into_iter()
            .find(|bucket| bucket.name == bucket_name),
    )
}

pub async fn read_current_bucket_by_id(
    storage: &Storage,
    bucket_id: i64,
) -> Result<Option<Bucket>> {
    Ok(
        read_current_buckets_from_path(storage.global_bucket_metadata_journal_path())
            .await?
            .into_iter()
            .find(|bucket| bucket.id == bucket_id),
    )
}

pub async fn next_bucket_id(storage: &Storage) -> Result<i64> {
    let frames =
        read_bucket_journal_frames_at_path(&storage.global_bucket_metadata_journal_path()).await?;
    let max_bucket_id = frames
        .into_iter()
        .filter(|frame| frame.record_kind == JournalRecordKind::BucketMetadata)
        .map(|frame| serde_json::from_slice::<BucketJournalBody>(&frame.body))
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .map(|body| body.bucket_id)
        .max()
        .unwrap_or(0);
    max_bucket_id
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("bucket id overflow"))
}

async fn append_bucket_mutation_to_path(
    path: std::path::PathBuf,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    scope: BucketJournalScope,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path, scope).await?;

    let previous = read_bucket_journal_frames_at_path(path.as_path())
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
    let body = serde_json::to_vec(&BucketJournalBody {
        event: mutation.event_name().to_string(),
        tenant_id: bucket.tenant_id,
        bucket_id: bucket.id,
        bucket_name: bucket.name.clone(),
        region: bucket.region.clone(),
        is_public_read: bucket.is_public_read,
        mutation_id: mutation_id.to_string(),
        created_at: bucket.created_at.to_rfc3339(),
        emitted_at: Some(chrono::Utc::now().to_rfc3339()),
    })?;
    let frame = JournalFrame::new(
        JournalRecordKind::BucketMetadata,
        sequence,
        0,
        *mutation_id.as_bytes(),
        bucket_key_hash(bucket.tenant_id, &bucket.name),
        previous_hash,
        body,
    );

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open bucket metadata journal {}", path.display()))?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

pub async fn read_current_bucket(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<Bucket>> {
    Ok(read_current_buckets(storage, tenant_id)
        .await?
        .into_iter()
        .find(|bucket| bucket.name == bucket_name))
}

pub async fn read_current_buckets(storage: &Storage, tenant_id: i64) -> Result<Vec<Bucket>> {
    read_current_buckets_from_path(storage.bucket_metadata_journal_path(tenant_id)).await
}

pub async fn latest_bucket_metadata_event(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<BucketMetadataEvent>> {
    Ok(
        list_bucket_metadata_events(storage, tenant_id, bucket_name, 0, 0)
            .await?
            .into_iter()
            .max_by_key(|event| event.id),
    )
}

pub async fn list_bucket_metadata_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<BucketMetadataEvent>> {
    let path = storage.bucket_metadata_journal_path(tenant_id);
    let frames = read_bucket_journal_frames_at_path(path.as_path()).await?;
    let mut events = Vec::new();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::BucketMetadata {
            continue;
        }
        if frame.partition_sequence <= after_cursor as u64 {
            continue;
        }
        let body: BucketJournalBody = serde_json::from_slice(&frame.body)?;
        if !bucket_name.is_empty() && body.bucket_name != bucket_name {
            continue;
        }
        events.push(bucket_event_from_body(frame.partition_sequence, body)?);
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

pub async fn list_bucket_metadata_events_by_bucket_id(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<BucketMetadataEvent>> {
    let path = storage.bucket_metadata_journal_path(tenant_id);
    let frames = read_bucket_journal_frames_at_path(path.as_path()).await?;
    let mut events = Vec::new();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::BucketMetadata {
            continue;
        }
        if frame.partition_sequence <= after_cursor as u64 {
            continue;
        }
        let body: BucketJournalBody = serde_json::from_slice(&frame.body)?;
        if body.bucket_id != bucket_id {
            continue;
        }
        events.push(bucket_event_from_body(frame.partition_sequence, body)?);
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

async fn read_current_buckets_from_path(path: std::path::PathBuf) -> Result<Vec<Bucket>> {
    let frames = read_bucket_journal_frames_at_path(path.as_path()).await?;
    let mut buckets = std::collections::BTreeMap::<String, Bucket>::new();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::BucketMetadata {
            continue;
        }
        let body: BucketJournalBody = serde_json::from_slice(&frame.body)?;
        if body.event == "delete" {
            buckets.remove(&body.bucket_name);
            continue;
        }
        buckets.insert(
            body.bucket_name.clone(),
            Bucket {
                id: body.bucket_id,
                tenant_id: body.tenant_id,
                name: body.bucket_name,
                region: body.region,
                created_at: chrono::DateTime::parse_from_rfc3339(&body.created_at)?
                    .with_timezone(&chrono::Utc),
                is_public_read: body.is_public_read,
            },
        );
    }
    Ok(buckets.into_values().collect())
}

async fn read_bucket_journal_frames_at_path(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read bucket metadata journal {}", path.display()))?;
    decode_journal_file(&bytes)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("bucket metadata journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated bucket metadata journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow::anyhow!("invalid bucket metadata journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated bucket metadata journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

async fn ensure_journal_header(path: &Path, scope: BucketJournalScope) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = chrono::Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&BucketJournalHeader {
        tenant_id: scope.tenant_label(),
        partition_family: "bucket_metadata",
        partition_id: hex::encode(scope.partition_id()),
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
        .with_context(|| format!("create bucket metadata journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum BucketJournalScope {
    Tenant(i64),
    Global,
}

impl BucketJournalScope {
    fn tenant_label(self) -> String {
        match self {
            Self::Tenant(tenant_id) => tenant_id.to_string(),
            Self::Global => "*".to_string(),
        }
    }

    fn partition_id(self) -> Hash32 {
        match self {
            Self::Tenant(tenant_id) => bucket_partition_id(tenant_id),
            Self::Global => hash32(b"bucket_metadata/global"),
        }
    }
}

fn bucket_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket_metadata").as_bytes())
}

fn bucket_key_hash(tenant_id: i64, bucket_name: &str) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_name}").as_bytes())
}

fn bucket_event_from_body(sequence: u64, body: BucketJournalBody) -> Result<BucketMetadataEvent> {
    let id = i64::try_from(sequence).context("bucket metadata cursor exceeds i64")?;
    let bucket_created_at =
        chrono::DateTime::parse_from_rfc3339(&body.created_at)?.with_timezone(&chrono::Utc);
    let event_created_at = body
        .emitted_at
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()?
        .map(|value| value.with_timezone(&chrono::Utc))
        .unwrap_or(bucket_created_at);
    let deleted = body.event == "delete";
    Ok(BucketMetadataEvent {
        id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        event_type: bucket_event_type(&body.event).to_string(),
        bucket_metadata: bucket_metadata_json(&body, deleted),
        created_at: event_created_at,
    })
}

fn bucket_event_type(event: &str) -> &str {
    match event {
        "update" => "policy_update",
        other => other,
    }
}

fn bucket_metadata_json(body: &BucketJournalBody, deleted: bool) -> JsonValue {
    json!({
        "name": body.bucket_name,
        "creation_date": body.created_at,
        "region": body.region,
        "is_public_read": body.is_public_read,
        "deleted": deleted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::tempdir;

    fn bucket(id: i64, name: &str, is_public_read: bool) -> Bucket {
        Bucket {
            id,
            tenant_id: 42,
            name: name.to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read,
        }
    }

    #[tokio::test]
    async fn bucket_journal_recovers_create_update_delete_state() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let private = bucket(1, "private-bucket", false);
        let public = bucket(1, "private-bucket", true);
        let other = bucket(2, "other-bucket", false);

        append_bucket_mutation(&storage, &private, BucketJournalMutation::Create)
            .await
            .unwrap();
        append_bucket_mutation(&storage, &public, BucketJournalMutation::Update)
            .await
            .unwrap();
        append_bucket_mutation(&storage, &other, BucketJournalMutation::Create)
            .await
            .unwrap();
        append_bucket_mutation(&storage, &other, BucketJournalMutation::Delete)
            .await
            .unwrap();

        let buckets = read_current_buckets(&storage, 42).await.unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, "private-bucket");
        assert!(buckets[0].is_public_read);
        assert!(
            read_current_bucket(&storage, 42, "other-bucket")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            read_public_bucket_by_name(&storage, "private-bucket")
                .await
                .unwrap()
                .unwrap()
                .tenant_id,
            42
        );
        assert!(
            read_public_bucket_by_name(&storage, "other-bucket")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn bucket_journal_lists_watch_events_from_native_log() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let private = bucket(1, "watched-bucket", false);
        let public = bucket(1, "watched-bucket", true);
        append_bucket_mutation(&storage, &private, BucketJournalMutation::Create)
            .await
            .unwrap();
        append_bucket_mutation(&storage, &public, BucketJournalMutation::Update)
            .await
            .unwrap();

        let all = list_bucket_metadata_events(&storage, 42, "", 0, 10)
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].event_type, "create");
        assert_eq!(all[1].event_type, "policy_update");
        assert!(all[1].bucket_metadata["is_public_read"].as_bool().unwrap());

        let after_first = list_bucket_metadata_events(&storage, 42, "", 1, 10)
            .await
            .unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].id, 2);

        let latest = latest_bucket_metadata_event(&storage, 42, "watched-bucket")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.id, 2);
        assert_eq!(latest.bucket_name, "watched-bucket");
    }
}
