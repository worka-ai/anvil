use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, FormatError, Hash32, JournalFrame,
    JournalRecordKind, hash32, validate_journal_chain,
};
use crate::persistence::{Bucket, Object};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectJournalMutation {
    Put,
    DeleteMarker,
    DeleteVersion,
}

impl ObjectJournalMutation {
    fn event_name(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::DeleteMarker => "delete_marker",
            Self::DeleteVersion => "delete_version",
        }
    }

    fn object_record_kind(self) -> JournalRecordKind {
        match self {
            Self::Put | Self::DeleteVersion => JournalRecordKind::ObjectVersion,
            Self::DeleteMarker => JournalRecordKind::DeleteMarker,
        }
    }

    fn is_delete_marker(self) -> bool {
        matches!(self, Self::DeleteMarker)
    }
}

#[derive(Debug, Serialize)]
struct MetadataJournalHeader<'a> {
    tenant_id: String,
    bucket_id: String,
    partition_family: &'static str,
    partition_id: String,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

#[derive(Debug, Serialize)]
struct ObjectVersionBody<'a> {
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &'a str,
    object_key: &'a str,
    event: &'a str,
    version_id: String,
    mutation_id: String,
    content_hash: &'a str,
    size: i64,
    etag: &'a str,
    content_type: Option<&'a str>,
    user_metadata_hash: &'a str,
    authz_revision: i64,
    index_policy_snapshot: &'a str,
    record_hash: &'a str,
    storage_class: Option<i16>,
    delete_marker: bool,
    created_at: String,
    deleted_at: Option<String>,
}

#[derive(Debug, Serialize)]
struct DirectoryEntryBody<'a> {
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &'a str,
    object_key: &'a str,
    event: &'a str,
    version_id: String,
    mutation_id: String,
    size: i64,
    etag: &'a str,
    delete_marker: bool,
    created_at: String,
    deleted_at: Option<String>,
}

pub async fn append_object_mutation(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
) -> Result<PathBuf> {
    let path = storage.metadata_journal_path(bucket.tenant_id, bucket.id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    ensure_journal_header(&path, bucket).await?;
    let existing = tokio::fs::read(&path).await?;
    let (header_len, frames) = decode_journal_file(&existing)?;
    let previous_hash = frames
        .last()
        .map(|frame| frame.record_hash)
        .unwrap_or([0; 32]);
    let next_sequence = frames
        .last()
        .map(|frame| frame.partition_sequence + 1)
        .unwrap_or(1);

    let object_body = serde_json::to_vec(&ObjectVersionBody {
        tenant_id: object.tenant_id,
        bucket_id: object.bucket_id,
        bucket_name: &bucket.name,
        object_key: &object.key,
        event: mutation.event_name(),
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        content_hash: &object.content_hash,
        size: object.size,
        etag: &object.etag,
        content_type: object.content_type.as_deref(),
        user_metadata_hash: &object.user_metadata_hash,
        authz_revision: object.authz_revision,
        index_policy_snapshot: &object.index_policy_snapshot,
        record_hash: &object.record_hash,
        storage_class: object.storage_class,
        delete_marker: mutation.is_delete_marker(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object.deleted_at.map(|ts| ts.to_rfc3339()),
    })?;
    let object_frame = JournalFrame::new(
        mutation.object_record_kind(),
        next_sequence,
        0,
        *object.mutation_id.as_bytes(),
        object_version_key_hash(bucket, object),
        previous_hash,
        object_body,
    );

    let directory_body = serde_json::to_vec(&DirectoryEntryBody {
        tenant_id: object.tenant_id,
        bucket_id: object.bucket_id,
        bucket_name: &bucket.name,
        object_key: &object.key,
        event: mutation.event_name(),
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        size: object.size,
        etag: &object.etag,
        delete_marker: mutation.is_delete_marker(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object.deleted_at.map(|ts| ts.to_rfc3339()),
    })?;
    let directory_frame = JournalFrame::new(
        JournalRecordKind::DirectoryEntry,
        next_sequence + 1,
        0,
        *object.mutation_id.as_bytes(),
        directory_key_hash(bucket, object),
        object_frame.record_hash,
        directory_body,
    );

    let mut updated_frames = frames;
    updated_frames.push(object_frame.clone());
    updated_frames.push(directory_frame.clone());
    validate_journal_chain(&updated_frames)?;

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await?;
    file.write_all(&object_frame.encode()).await?;
    file.write_all(&directory_frame.encode()).await?;
    file.sync_data().await?;

    debug_assert!(header_len <= existing.len());
    Ok(path)
}

pub fn decode_journal_file(input: &[u8]) -> Result<(usize, Vec<JournalFrame>)> {
    let header = BinaryEnvelopeHeader::decode(input)?;
    if header.family != FileFamily::MetadataJournal {
        return Err(anyhow!("not a metadata journal file"));
    }
    let header_len = COMMON_HEADER_LEN
        .checked_add(header.header_json.len())
        .ok_or_else(|| anyhow!("metadata journal header length overflow"))?;
    let frames = decode_frames(&input[header_len..])?;
    Ok((header_len, frames))
}

fn decode_frames(mut input: &[u8]) -> Result<Vec<JournalFrame>> {
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            return Err(FormatError::TooShort {
                context: "journal frame length",
                needed: 4,
                actual: input.len(),
            }
            .into());
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("journal frame length overflow"))?;
        if input.len() < frame_end {
            return Err(FormatError::TooShort {
                context: "journal frame",
                needed: frame_end,
                actual: input.len(),
            }
            .into());
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

async fn ensure_journal_header(path: &Path, bucket: &Bucket) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = bucket.created_at.to_rfc3339();
    let header_json = serde_json::to_vec(&MetadataJournalHeader {
        tenant_id: bucket.tenant_id.to_string(),
        bucket_id: bucket.id.to_string(),
        partition_family: "object_metadata",
        partition_id: hex::encode(partition_id(bucket.tenant_id, bucket.id)),
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
        .with_context(|| format!("create metadata journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

fn partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&tenant_id.to_le_bytes());
    bytes.extend_from_slice(&bucket_id.to_le_bytes());
    hash32(&bytes)
}

fn object_version_key_hash(bucket: &Bucket, object: &Object) -> Hash32 {
    hash32(
        format!(
            "tenant/{}/bucket/{}/object/{}/version/{}",
            bucket.tenant_id, bucket.id, object.key, object.version_id
        )
        .as_bytes(),
    )
}

fn directory_key_hash(bucket: &Bucket, object: &Object) -> Hash32 {
    hash32(
        format!(
            "tenant/{}/bucket/{}/directory/{}",
            bucket.tenant_id, bucket.id, object.key
        )
        .as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::tempdir;

    fn sample_bucket() -> Bucket {
        Bucket {
            id: 7,
            tenant_id: 3,
            name: "journal-bucket".to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        }
    }

    fn sample_object(id: i64, key: &str, delete_marker: bool) -> Object {
        Object {
            id,
            tenant_id: 3,
            bucket_id: 7,
            key: key.to_string(),
            content_hash: format!("hash-{id}"),
            size: 42,
            etag: format!("etag-{id}"),
            content_type: Some("text/plain".to_string()),
            version_id: uuid::Uuid::new_v4(),
            mutation_id: uuid::Uuid::new_v4(),
            index_policy_snapshot: "snapshot".to_string(),
            user_metadata_hash: "metadata-hash".to_string(),
            authz_revision: 11,
            record_hash: format!("record-{id}"),
            created_at: Utc::now(),
            deleted_at: delete_marker.then(Utc::now),
            storage_class: None,
            user_meta: None,
            shard_map: None,
            inline_payload: None,
            checksum: None,
        }
    }

    #[tokio::test]
    async fn append_object_mutation_writes_chained_metadata_and_directory_frames() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let first = sample_object(1, "docs/a.txt", false);
        let second = sample_object(2, "docs/b.txt", true);

        let path = append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
            .await
            .unwrap();
        append_object_mutation(
            &storage,
            &bucket,
            &second,
            ObjectJournalMutation::DeleteMarker,
        )
        .await
        .unwrap();

        assert_eq!(
            path,
            storage.metadata_journal_path(bucket.tenant_id, bucket.id)
        );
        let bytes = tokio::fs::read(&path).await.unwrap();
        let (_, frames) = decode_journal_file(&bytes).unwrap();
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].record_kind, JournalRecordKind::ObjectVersion);
        assert_eq!(frames[1].record_kind, JournalRecordKind::DirectoryEntry);
        assert_eq!(frames[2].record_kind, JournalRecordKind::DeleteMarker);
        assert_eq!(frames[3].record_kind, JournalRecordKind::DirectoryEntry);
        assert_eq!(frames[1].previous_record_hash, frames[0].record_hash);
        assert_eq!(frames[2].previous_record_hash, frames[1].record_hash);
        validate_journal_chain(&frames).unwrap();
    }

    #[tokio::test]
    async fn decode_journal_file_rejects_corrupted_appended_frame() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let object = sample_object(1, "docs/a.txt", false);
        let path = append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
            .await
            .unwrap();

        let mut bytes = tokio::fs::read(&path).await.unwrap();
        let last = bytes.len() - 33;
        bytes[last] ^= 1;
        assert!(decode_journal_file(&bytes).is_err());
    }
}
