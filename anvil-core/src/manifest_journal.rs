use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::persistence::ManifestCasResult;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize)]
struct ManifestJournalHeader<'a> {
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
struct ManifestBody {
    tenant_id: i64,
    bucket_id: i64,
    object_key: String,
    revision: i64,
    manifest_hash: String,
    manifest: JsonValue,
    updated_at: DateTime<Utc>,
}

pub async fn compare_and_swap_manifest(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
) -> Result<Option<ManifestCasResult>> {
    let current = current_revision(storage, tenant_id, bucket_id, object_key).await?;
    if expected_revision != current {
        return Ok(None);
    }
    let revision = current
        .checked_add(1)
        .ok_or_else(|| anyhow!("manifest revision overflow"))?;
    append_manifest(
        storage,
        ManifestBody {
            tenant_id,
            bucket_id,
            object_key: object_key.to_string(),
            revision,
            manifest_hash: manifest_hash.to_string(),
            manifest,
            updated_at: Utc::now(),
        },
    )
    .await?;
    Ok(Some(ManifestCasResult {
        revision,
        manifest_hash: manifest_hash.to_string(),
    }))
}

async fn current_revision(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
) -> Result<i64> {
    Ok(read_manifest_bodies(storage, tenant_id, bucket_id)
        .await?
        .into_iter()
        .filter(|body| body.object_key == object_key)
        .map(|body| body.revision)
        .max()
        .unwrap_or(0))
}

async fn append_manifest(storage: &Storage, body: ManifestBody) -> Result<()> {
    let path = storage.manifest_cas_journal_path(body.tenant_id, body.bucket_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_header(&path, body.tenant_id, body.bucket_id).await?;
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
    let frame = JournalFrame::new(
        JournalRecordKind::ManifestCas,
        sequence,
        0,
        *mutation_id.as_bytes(),
        hash32(
            format!(
                "tenant/{}/bucket/{}/manifest/{}",
                body.tenant_id, body.bucket_id, body.object_key
            )
            .as_bytes(),
        ),
        previous_hash,
        serde_json::to_vec(&body)?,
    );
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn read_manifest_bodies(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<ManifestBody>> {
    let path = storage.manifest_cas_journal_path(tenant_id, bucket_id);
    let frames = read_frames(&path).await?;
    frames
        .into_iter()
        .filter(|frame| frame.record_kind == JournalRecordKind::ManifestCas)
        .map(|frame| serde_json::from_slice(&frame.body).map_err(Into::into))
        .collect()
}

async fn ensure_header(path: &Path, tenant_id: i64, bucket_id: i64) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&ManifestJournalHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: bucket_id.to_string(),
        partition_family: "manifest_cas",
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
        .with_context(|| format!("create manifest journal {}", path.display()))?;
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
        anyhow::bail!("manifest journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated manifest journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("invalid manifest journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated manifest journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

fn partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn manifest_journal_enforces_compare_and_swap() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 1, json!({}), "bad")
                .await
                .unwrap()
                .is_none()
        );
        let first =
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 0, json!({"a":1}), "hash-a")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(first.revision, 1);
        let second =
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 1, json!({"a":2}), "hash-b")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(second.revision, 2);
    }
}
