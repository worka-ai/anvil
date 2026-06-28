use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::partition_fence::{PartitionWritePermit, validate_partition_write};
use crate::persistence::{
    MultipartPartsPage, MultipartUpload, MultipartUploadPart, MultipartUploadsPage,
};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultipartMutationKind {
    CreateUpload,
    UpsertPart,
    CompleteUpload,
    AbortUpload,
}

impl MultipartMutationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::CreateUpload => "create_upload",
            Self::UpsertPart => "upsert_part",
            Self::CompleteUpload => "complete_upload",
            Self::AbortUpload => "abort_upload",
        }
    }
}

#[derive(Debug, Serialize)]
struct MultipartJournalHeader<'a> {
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
struct MultipartBody {
    event: String,
    upload: Option<MultipartUpload>,
    part: Option<MultipartUploadPart>,
    emitted_at: String,
}

#[derive(Debug, Clone, Default)]
struct MultipartState {
    uploads: BTreeMap<i64, MultipartUpload>,
    parts: BTreeMap<(i64, i32), MultipartUploadPart>,
}

pub async fn create_multipart_upload(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
) -> Result<MultipartUpload> {
    create_multipart_upload_inner(storage, tenant_id, bucket_id, key, 0).await
}

pub async fn create_multipart_upload_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartUpload> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    create_multipart_upload_inner(storage, tenant_id, bucket_id, key, permit.fence_token).await
}

async fn create_multipart_upload_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    fence_token: u64,
) -> Result<MultipartUpload> {
    let state = read_state(storage, tenant_id, bucket_id).await?;
    let id = next_upload_id(&state)?;
    let upload = MultipartUpload {
        id,
        tenant_id,
        bucket_id,
        key: key.to_string(),
        upload_id: uuid::Uuid::new_v4(),
        created_at: Utc::now(),
        completed_at: None,
        aborted_at: None,
    };
    append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::CreateUpload,
        Some(upload.clone()),
        None,
        fence_token,
    )
    .await?;
    Ok(upload)
}

pub async fn get_active_multipart_upload(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
) -> Result<Option<MultipartUpload>> {
    Ok(read_state(storage, tenant_id, bucket_id)
        .await?
        .uploads
        .into_values()
        .find(|u| {
            u.key == key
                && u.upload_id == upload_id
                && u.completed_at.is_none()
                && u.aborted_at.is_none()
        }))
}

pub async fn has_active_multipart_upload(storage: &Storage, bucket_id: i64) -> Result<bool> {
    for path in storage.multipart_journal_paths().await? {
        let state = read_state_from_path(&path).await?;
        if state
            .uploads
            .into_values()
            .any(|u| u.bucket_id == bucket_id && u.completed_at.is_none() && u.aborted_at.is_none())
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub async fn upsert_multipart_part(
    storage: &Storage,
    upload_row_id: i64,
    part_number: i32,
    content_hash: &str,
    size: i64,
    etag: &str,
) -> Result<MultipartUploadPart> {
    upsert_multipart_part_inner(
        storage,
        upload_row_id,
        part_number,
        content_hash,
        size,
        etag,
        None,
    )
    .await
}

pub async fn upsert_multipart_part_with_permit(
    storage: &Storage,
    upload_row_id: i64,
    part_number: i32,
    content_hash: &str,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartUploadPart> {
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    upsert_multipart_part_inner(
        storage,
        upload_row_id,
        part_number,
        content_hash,
        size,
        etag,
        Some(permit),
    )
    .await
}

async fn upsert_multipart_part_inner(
    storage: &Storage,
    upload_row_id: i64,
    part_number: i32,
    content_hash: &str,
    size: i64,
    etag: &str,
    permit: Option<&PartitionWritePermit>,
) -> Result<MultipartUploadPart> {
    let (tenant_id, bucket_id, _) = find_upload(storage, upload_row_id)
        .await?
        .ok_or_else(|| anyhow!("multipart upload not found"))?;
    if let Some(permit) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    }
    let fence_token = permit.map(|permit| permit.fence_token).unwrap_or(0);
    let state = read_state(storage, tenant_id, bucket_id).await?;
    let part = MultipartUploadPart {
        id: state
            .parts
            .get(&(upload_row_id, part_number))
            .map(|part| part.id)
            .unwrap_or(next_part_id(&state)?),
        upload_id: upload_row_id,
        part_number,
        content_hash: content_hash.to_string(),
        size,
        etag: etag.to_string(),
        created_at: Utc::now(),
    };
    append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::UpsertPart,
        None,
        Some(part.clone()),
        fence_token,
    )
    .await?;
    Ok(part)
}

pub async fn list_multipart_parts(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<Vec<MultipartUploadPart>> {
    let Some((tenant_id, bucket_id, _)) = find_upload(storage, upload_row_id).await? else {
        return Ok(Vec::new());
    };
    let mut parts = read_state(storage, tenant_id, bucket_id)
        .await?
        .parts
        .into_values()
        .filter(|part| part.upload_id == upload_row_id)
        .collect::<Vec<_>>();
    parts.sort_by_key(|part| part.part_number);
    Ok(parts)
}

pub async fn list_multipart_parts_page(
    storage: &Storage,
    upload_row_id: i64,
    part_number_marker: i32,
    limit: i32,
) -> Result<MultipartPartsPage> {
    let mut parts = list_multipart_parts(storage, upload_row_id)
        .await?
        .into_iter()
        .filter(|part| part.part_number > part_number_marker)
        .collect::<Vec<_>>();
    let limit = if limit == 0 {
        1000
    } else {
        limit.max(1) as usize
    };
    let is_truncated = parts.len() > limit;
    if is_truncated {
        parts.truncate(limit);
    }
    let next_part_number_marker = if is_truncated {
        parts.last().map(|part| part.part_number)
    } else {
        None
    };
    Ok(MultipartPartsPage {
        parts,
        is_truncated,
        next_part_number_marker,
    })
}

pub async fn list_active_multipart_uploads(
    storage: &Storage,
    bucket_id: i64,
    prefix: &str,
    key_marker: &str,
    upload_id_marker: Option<uuid::Uuid>,
    limit: i32,
) -> Result<MultipartUploadsPage> {
    let mut uploads = Vec::new();
    for path in storage.multipart_journal_paths().await? {
        let state = read_state_from_path(&path).await?;
        uploads.extend(state.uploads.into_values().filter(|upload| {
            upload.bucket_id == bucket_id
                && upload.key.starts_with(prefix)
                && upload.completed_at.is_none()
                && upload.aborted_at.is_none()
        }));
    }
    uploads.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| left.created_at.cmp(&right.created_at))
    });
    if !key_marker.is_empty() {
        let mut past_marker = upload_id_marker.is_none();
        uploads.retain(|upload| {
            if upload.key.as_str() < key_marker {
                return false;
            }
            if upload.key.as_str() > key_marker {
                return true;
            }
            if let Some(marker) = upload_id_marker {
                if past_marker {
                    return true;
                }
                if upload.upload_id == marker {
                    past_marker = true;
                }
                return false;
            }
            true
        });
    }
    let limit = if limit == 0 {
        1000
    } else {
        limit.max(1) as usize
    };
    let is_truncated = uploads.len() > limit;
    if is_truncated {
        uploads.truncate(limit);
    }
    let (next_key_marker, next_upload_id_marker) = if is_truncated {
        uploads
            .last()
            .map(|upload| (Some(upload.key.clone()), Some(upload.upload_id)))
            .unwrap_or((None, None))
    } else {
        (None, None)
    };
    Ok(MultipartUploadsPage {
        uploads,
        is_truncated,
        next_key_marker,
        next_upload_id_marker,
    })
}

pub async fn complete_multipart_upload(storage: &Storage, upload_row_id: i64) -> Result<()> {
    complete_multipart_upload_inner(storage, upload_row_id, None).await
}

pub async fn complete_multipart_upload_with_permit(
    storage: &Storage,
    upload_row_id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    complete_multipart_upload_inner(storage, upload_row_id, Some(permit)).await
}

async fn complete_multipart_upload_inner(
    storage: &Storage,
    upload_row_id: i64,
    permit: Option<&PartitionWritePermit>,
) -> Result<()> {
    let Some((tenant_id, bucket_id, mut upload)) = find_upload(storage, upload_row_id).await?
    else {
        return Ok(());
    };
    if let Some(permit) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    }
    let fence_token = permit.map(|permit| permit.fence_token).unwrap_or(0);
    upload.completed_at = Some(Utc::now());
    append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::CompleteUpload,
        Some(upload),
        None,
        fence_token,
    )
    .await
}

pub async fn abort_multipart_upload(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
) -> Result<bool> {
    abort_multipart_upload_inner(storage, tenant_id, bucket_id, key, upload_id, 0).await
}

pub async fn abort_multipart_upload_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<bool> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    abort_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        permit.fence_token,
    )
    .await
}

async fn abort_multipart_upload_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    fence_token: u64,
) -> Result<bool> {
    let Some(mut upload) =
        get_active_multipart_upload(storage, tenant_id, bucket_id, key, upload_id).await?
    else {
        return Ok(false);
    };
    upload.aborted_at = Some(Utc::now());
    append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::AbortUpload,
        Some(upload),
        None,
        fence_token,
    )
    .await?;
    Ok(true)
}

pub async fn find_multipart_upload_partition(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<Option<(i64, i64)>> {
    Ok(find_upload(storage, upload_row_id)
        .await?
        .map(|(tenant_id, bucket_id, _)| (tenant_id, bucket_id)))
}

async fn find_upload(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    for path in storage.multipart_journal_paths().await? {
        let state = read_state_from_path(&path).await?;
        if let Some(upload) = state.uploads.get(&upload_row_id).cloned() {
            return Ok(Some((upload.tenant_id, upload.bucket_id, upload)));
        }
    }
    Ok(None)
}

async fn read_state(storage: &Storage, tenant_id: i64, bucket_id: i64) -> Result<MultipartState> {
    read_state_from_path(&storage.multipart_journal_path(tenant_id, bucket_id)).await
}

async fn read_state_from_path(path: &Path) -> Result<MultipartState> {
    let frames = read_frames(path).await?;
    let mut state = MultipartState::default();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::MultipartMetadata {
            continue;
        }
        let body: MultipartBody = serde_json::from_slice(&frame.body)?;
        match body.event.as_str() {
            "create_upload" | "complete_upload" | "abort_upload" => {
                if let Some(upload) = body.upload {
                    state.uploads.insert(upload.id, upload);
                }
            }
            "upsert_part" => {
                if let Some(part) = body.part {
                    state.parts.insert((part.upload_id, part.part_number), part);
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
    event: MultipartMutationKind,
    upload: Option<MultipartUpload>,
    part: Option<MultipartUploadPart>,
    fence_token: u64,
) -> Result<()> {
    let path = storage.multipart_journal_path(tenant_id, bucket_id);
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
            "tenant/{tenant_id}/bucket/{bucket_id}/multipart/{}/{}",
            upload
                .as_ref()
                .map(|upload| upload.id)
                .or_else(|| part.as_ref().map(|part| part.upload_id))
                .unwrap_or(0),
            event.as_str()
        )
        .as_bytes(),
    );
    let frame = JournalFrame::new(
        JournalRecordKind::MultipartMetadata,
        sequence,
        fence_token,
        *mutation_id.as_bytes(),
        key_hash,
        previous_hash,
        serde_json::to_vec(&MultipartBody {
            event: event.as_str().to_string(),
            upload,
            part,
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
    let header_json = serde_json::to_vec(&MultipartJournalHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: bucket_id.to_string(),
        partition_family: "multipart_metadata",
        partition_id: hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id)),
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
        .with_context(|| format!("create multipart journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn read_frames(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read multipart journal {}", path.display()))?;
    decode_journal_file(&bytes)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("multipart journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated multipart journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("invalid multipart journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated multipart journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

fn next_upload_id(state: &MultipartState) -> Result<i64> {
    state
        .uploads
        .keys()
        .copied()
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("multipart upload id overflow"))
}

fn next_part_id(state: &MultipartState) -> Result<i64> {
    state
        .parts
        .values()
        .map(|part| part.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow!("multipart part id overflow"))
}

pub fn multipart_metadata_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/multipart").as_bytes())
}

fn require_multipart_metadata_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    if permit.partition_family != "multipart_metadata"
        || permit.partition_id != expected_partition_id
    {
        anyhow::bail!("multipart metadata write permit targets a different partition");
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

    const KEY: &[u8] = b"multipart journal partition owner key";

    #[tokio::test]
    async fn multipart_journal_replays_upload_parts_and_state() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let upload = create_multipart_upload(&storage, 1, 2, "obj")
            .await
            .unwrap();
        upsert_multipart_part(&storage, upload.id, 1, "hash-a", 10, "etag-a")
            .await
            .unwrap();
        upsert_multipart_part(&storage, upload.id, 1, "hash-b", 11, "etag-b")
            .await
            .unwrap();
        assert_eq!(
            list_multipart_parts(&storage, upload.id).await.unwrap()[0].etag,
            "etag-b"
        );
        assert!(
            get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
                .await
                .unwrap()
                .is_some()
        );
        complete_multipart_upload(&storage, upload.id)
            .await
            .unwrap();
        assert!(
            get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn multipart_journal_with_permit_writes_fenced_frames_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let permit = owner.write_permit().unwrap();

        let upload = create_multipart_upload_with_permit(&storage, 1, 2, "obj", &permit, KEY)
            .await
            .unwrap();
        upsert_multipart_part_with_permit(
            &storage, upload.id, 1, "hash-a", 10, "etag-a", &permit, KEY,
        )
        .await
        .unwrap();
        complete_multipart_upload_with_permit(&storage, upload.id, &permit, KEY)
            .await
            .unwrap();

        let journal = tokio::fs::read(storage.multipart_journal_path(1, 2))
            .await
            .unwrap();
        let header = BinaryEnvelopeHeader::decode(&journal).unwrap();
        let header_json: serde_json::Value = serde_json::from_slice(&header.header_json).unwrap();
        assert_eq!(header_json["partition_family"], "multipart_metadata");
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
    async fn multipart_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let upload = create_multipart_upload_with_permit(&storage, 1, 2, "obj", &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = upsert_multipart_part_with_permit(
            &storage,
            upload.id,
            1,
            "hash-a",
            10,
            "etag-a",
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
        tenant_id: i64,
        bucket_id: i64,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "multipart_metadata".to_string();
        let id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
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
