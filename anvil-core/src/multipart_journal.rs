use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreObjectRef, CoreStore,
    ReadStream,
};
use crate::formats::{Hash32, JournalFrame, JournalRecordKind, hash32, validate_journal_chain};
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
use crate::persistence::{
    MetadataMutationReceipt, MultipartAbortMutation, MultipartCompletionMutation,
    MultipartPartsPage, MultipartUpload, MultipartUploadMutation, MultipartUploadPart,
    MultipartUploadPartMutation, MultipartUploadsPage,
};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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

#[cfg(test)]
async fn create_multipart_upload(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
) -> Result<MultipartUploadMutation> {
    create_multipart_upload_inner(storage, tenant_id, bucket_id, key, 0, None).await
}

pub(crate) async fn create_multipart_upload_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartUploadMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    create_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn create_multipart_upload_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<MultipartUploadMutation> {
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
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::CreateUpload,
        Some(upload.clone()),
        None,
        fence_token,
        partition_precondition,
    )
    .await?;
    Ok(MultipartUploadMutation { upload, receipt })
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
    let core_store = CoreStore::new(storage.clone()).await?;
    for stream_id in core_store
        .list_stream_ids("multipart_metadata:tenant:")
        .await?
    {
        let state = read_state_from_store(&core_store, &stream_id).await?;
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

#[cfg(test)]
async fn upsert_multipart_part(
    storage: &Storage,
    upload_row_id: i64,
    part_number: i32,
    object_ref: CoreObjectRef,
    size: i64,
    etag: &str,
) -> Result<MultipartUploadPartMutation> {
    upsert_multipart_part_inner(
        storage,
        upload_row_id,
        part_number,
        object_ref,
        size,
        etag,
        None,
    )
    .await
}

pub(crate) async fn upsert_multipart_part_with_permit(
    storage: &Storage,
    upload_row_id: i64,
    part_number: i32,
    object_ref: CoreObjectRef,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartUploadPartMutation> {
    upsert_multipart_part_inner(
        storage,
        upload_row_id,
        part_number,
        object_ref,
        size,
        etag,
        Some((permit, partition_owner_signing_key)),
    )
    .await
}

async fn upsert_multipart_part_inner(
    storage: &Storage,
    upload_row_id: i64,
    part_number: i32,
    object_ref: CoreObjectRef,
    size: i64,
    etag: &str,
    permit: Option<(&PartitionWritePermit, &[u8])>,
) -> Result<MultipartUploadPartMutation> {
    let (tenant_id, bucket_id, _) = find_upload(storage, upload_row_id)
        .await?
        .ok_or_else(|| anyhow!("multipart upload not found"))?;
    let (fence_token, partition_precondition) = if let Some((permit, signing_key)) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
        (
            permit.fence_token,
            Some(partition_write_ref_precondition(storage, permit, signing_key).await?),
        )
    } else {
        (0, None)
    };
    let state = read_state(storage, tenant_id, bucket_id).await?;
    let part = MultipartUploadPart {
        id: state
            .parts
            .get(&(upload_row_id, part_number))
            .map(|part| part.id)
            .unwrap_or(next_part_id(&state)?),
        upload_id: upload_row_id,
        part_number,
        content_hash: object_ref.hash.clone(),
        object_ref,
        size,
        etag: etag.to_string(),
        created_at: Utc::now(),
    };
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::UpsertPart,
        None,
        Some(part.clone()),
        fence_token,
        partition_precondition,
    )
    .await?;
    Ok(MultipartUploadPartMutation { part, receipt })
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
    let core_store = CoreStore::new(storage.clone()).await?;
    for stream_id in core_store
        .list_stream_ids("multipart_metadata:tenant:")
        .await?
    {
        let state = read_state_from_store(&core_store, &stream_id).await?;
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

#[cfg(test)]
async fn complete_multipart_upload(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<MultipartCompletionMutation> {
    complete_multipart_upload_inner(storage, upload_row_id, None).await
}

pub(crate) async fn complete_multipart_upload_with_permit(
    storage: &Storage,
    upload_row_id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartCompletionMutation> {
    complete_multipart_upload_inner(
        storage,
        upload_row_id,
        Some((permit, partition_owner_signing_key)),
    )
    .await
}

async fn complete_multipart_upload_inner(
    storage: &Storage,
    upload_row_id: i64,
    permit: Option<(&PartitionWritePermit, &[u8])>,
) -> Result<MultipartCompletionMutation> {
    let Some((tenant_id, bucket_id, mut upload)) = find_upload(storage, upload_row_id).await?
    else {
        return Ok(MultipartCompletionMutation {
            completed: false,
            receipt: None,
        });
    };
    let (fence_token, partition_precondition) = if let Some((permit, signing_key)) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
        (
            permit.fence_token,
            Some(partition_write_ref_precondition(storage, permit, signing_key).await?),
        )
    } else {
        (0, None)
    };
    upload.completed_at = Some(Utc::now());
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::CompleteUpload,
        Some(upload),
        None,
        fence_token,
        partition_precondition,
    )
    .await?;
    Ok(MultipartCompletionMutation {
        completed: true,
        receipt: Some(receipt),
    })
}

pub(crate) async fn abort_multipart_upload_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<MultipartAbortMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    abort_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        permit.fence_token,
        Some(partition_precondition),
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
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<MultipartAbortMutation> {
    let Some(mut upload) =
        get_active_multipart_upload(storage, tenant_id, bucket_id, key, upload_id).await?
    else {
        return Ok(MultipartAbortMutation {
            aborted: false,
            receipt: None,
        });
    };
    upload.aborted_at = Some(Utc::now());
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        MultipartMutationKind::AbortUpload,
        Some(upload),
        None,
        fence_token,
        partition_precondition,
    )
    .await?;
    Ok(MultipartAbortMutation {
        aborted: true,
        receipt: Some(receipt),
    })
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
    let core_store = CoreStore::new(storage.clone()).await?;
    for stream_id in core_store
        .list_stream_ids("multipart_metadata:tenant:")
        .await?
    {
        let state = read_state_from_store(&core_store, &stream_id).await?;
        if let Some(upload) = state.uploads.get(&upload_row_id).cloned() {
            return Ok(Some((upload.tenant_id, upload.bucket_id, upload)));
        }
    }
    Ok(None)
}

async fn read_state(storage: &Storage, tenant_id: i64, bucket_id: i64) -> Result<MultipartState> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_state_from_store(
        &core_store,
        &multipart_metadata_stream_id(tenant_id, bucket_id),
    )
    .await
}

async fn read_state_from_store(core_store: &CoreStore, stream_id: &str) -> Result<MultipartState> {
    let frames = read_frames_from_store(core_store, stream_id).await?;
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
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = multipart_metadata_stream_id(tenant_id, bucket_id);
    let previous = read_frames_from_store(&core_store, &stream_id)
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
    let body = serde_json::to_vec(&MultipartBody {
        event: event.as_str().to_string(),
        upload,
        part,
        emitted_at: Utc::now().to_rfc3339(),
    })?;
    let payload_hash = hex::encode(hash32(&body));
    let frame = JournalFrame::new(
        JournalRecordKind::MultipartMetadata,
        sequence,
        fence_token,
        *mutation_id.as_bytes(),
        key_hash,
        previous_hash,
        body,
    );
    let receipt = MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: hex::encode(frame.record_hash),
        watch_cursor: frame.partition_sequence,
    };
    let partition_id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("multipart-metadata:{mutation_id}"),
            scope_partition: partition_id.clone(),
            committed_by_principal: multipart_metadata_partition_principal(tenant_id, bucket_id),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind: "multipart_metadata".to_string(),
                payload: frame.encode(),
                idempotency_key: Some(format!("multipart-metadata:{mutation_id}")),
            }],
        })
        .await?;
    Ok(receipt)
}

async fn read_frames_from_store(
    core_store: &CoreStore,
    stream_id: &str,
) -> Result<Vec<JournalFrame>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut frames = Vec::new();
    for record in records {
        if record.record_kind != "multipart_metadata" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
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

fn multipart_metadata_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("multipart_metadata:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn multipart_metadata_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:multipart_metadata:{tenant_id}:{bucket_id}")
}

#[cfg(test)]
pub(crate) async fn read_multipart_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let frames = read_frames_from_store(
        &core_store,
        &multipart_metadata_stream_id(tenant_id, bucket_id),
    )
    .await?;
    Ok(frames.into_iter().map(|frame| frame.fence_token).collect())
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
            .unwrap()
            .upload;
        upsert_multipart_part(
            &storage,
            upload.id,
            1,
            payload_ref("hash-a", 10),
            10,
            "etag-a",
        )
        .await
        .unwrap();
        upsert_multipart_part(
            &storage,
            upload.id,
            1,
            payload_ref("hash-b", 11),
            11,
            "etag-b",
        )
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
        assert!(
            complete_multipart_upload(&storage, upload.id)
                .await
                .unwrap()
                .completed
        );
        assert!(
            get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    pub(crate) async fn multipart_journal_with_permit_writes_fenced_frames() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let permit = owner.write_permit().unwrap();

        let upload = create_multipart_upload_with_permit(&storage, 1, 2, "obj", &permit, KEY)
            .await
            .unwrap();
        upsert_multipart_part_with_permit(
            &storage,
            upload.upload.id,
            1,
            payload_ref("hash-a", 10),
            10,
            "etag-a",
            &permit,
            KEY,
        )
        .await
        .unwrap();
        complete_multipart_upload_with_permit(&storage, upload.upload.id, &permit, KEY)
            .await
            .unwrap();

        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let frames = read_frames_from_store(&core_store, &multipart_metadata_stream_id(1, 2))
            .await
            .unwrap();
        assert_eq!(frames.len(), 3);
        assert!(
            frames
                .iter()
                .all(|frame| frame.fence_token == permit.fence_token)
        );
    }

    #[tokio::test]
    pub(crate) async fn multipart_journal_with_permit_rejects_stale_fence() {
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
            upload.upload.id,
            1,
            payload_ref("hash-a", 10),
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

    #[tokio::test]
    pub(crate) async fn multipart_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_ref_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_multipart_upload_inner(
            &storage,
            1,
            2,
            "obj",
            stale_permit.fence_token,
            Some(stale_precondition),
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("generation mismatch") || message.contains("target mismatch"),
            "unexpected stale precondition error: {message}"
        );

        create_multipart_upload_with_permit(
            &storage,
            1,
            2,
            "obj",
            &newer.write_permit().unwrap(),
            KEY,
        )
        .await
        .unwrap();
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

    fn payload_ref(label: &str, logical_size: u64) -> CoreObjectRef {
        CoreObjectRef::test_unlocated(
            format!(
                "sha256:{}",
                hex::encode(blake3::hash(label.as_bytes()).as_bytes())
            ),
            logical_size,
            format!("manifest:{label}"),
        )
    }
}
