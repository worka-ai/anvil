use crate::core_store::{
    CF_OBJECT_HEADS, CoreCompressionDescriptor, CoreMetaBatchOp, CoreMetaBatchOpKind,
    CoreMetaRecord, CoreMetaStore, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation,
    CoreMutationPrecondition, CoreObjectEncoding, CoreObjectPlacement, CoreObjectRef, CoreStore,
    CoreTransaction, CoreTransactionUpdate, TABLE_MULTIPART_PART_CURRENT_ROW,
    TABLE_MULTIPART_UPLOAD_CURRENT_ROW, canonical_coremeta_cf_name,
    commit_coremeta_batch_for_storage, core_meta_committed_row_common, core_meta_payload_digest,
    core_meta_record_tuple_key, core_meta_root_key_hash, core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{
    MetadataMutationReceipt, MultipartAbortMutation, MultipartCompletionMutation,
    MultipartPartsPage, MultipartUpload, MultipartUploadMutation, MultipartUploadPart,
    MultipartUploadPartMutation, MultipartUploadsPage,
};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use std::collections::BTreeMap;

mod codec;
pub use codec::multipart_metadata_partition_id;
use codec::{
    current_part_payload, current_unix_nanos, current_upload_payload, decode_part_current_record,
    decode_part_current_row, decode_upload_current_record, decode_upload_current_row,
    encode_multipart_event, encode_part_current_row, encode_upload_current_row,
    multipart_all_upload_rows_prefix, multipart_current_root_key,
    multipart_metadata_partition_principal, multipart_metadata_stream_id, multipart_part_row_key,
    multipart_part_rows_prefix, multipart_realm_id, multipart_upload_row_key,
    multipart_upload_rows_prefix, next_part_id, next_upload_id,
};
#[cfg(test)]
use codec::{decode_multipart_event, decode_multipart_event_fence};

const MULTIPART_UPLOAD_SCHEMA: &str = "anvil.multipart.upload.v1";
const MULTIPART_PART_SCHEMA: &str = "anvil.multipart.part.v1";
const MULTIPART_EVENT_SCHEMA: &str = "anvil.multipart.event.v1";
const MULTIPART_UPLOAD_CURRENT_ROW_SCHEMA: &str = "anvil.multipart.upload_current_row.v1";
const MULTIPART_PART_CURRENT_ROW_SCHEMA: &str = "anvil.multipart.part_current_row.v1";
const MULTIPART_CURRENT_ROW_KEY_PREFIX: &str = "multipart_current";
const MULTIPART_MAX_CURRENT_PROTO_BYTES: usize = 16 * 1024;

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

#[derive(Debug, Clone, Default)]
struct MultipartState {
    uploads: BTreeMap<i64, MultipartUpload>,
    parts: BTreeMap<(i64, i32), MultipartUploadPart>,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartUploadProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    id: i64,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    key: String,
    #[prost(bytes, tag = "6")]
    upload_uuid: Vec<u8>,
    #[prost(int64, tag = "7")]
    created_at_unix_nanos: i64,
    #[prost(int64, optional, tag = "8")]
    completed_at_unix_nanos: Option<i64>,
    #[prost(int64, optional, tag = "9")]
    aborted_at_unix_nanos: Option<i64>,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartPartProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    id: i64,
    #[prost(int64, tag = "3")]
    upload_id: i64,
    #[prost(int32, tag = "4")]
    part_number: i32,
    #[prost(string, tag = "5")]
    content_hash: String,
    #[prost(message, optional, tag = "6")]
    object_ref: Option<CoreObjectRefProto>,
    #[prost(int64, tag = "7")]
    size: i64,
    #[prost(string, tag = "8")]
    etag: String,
    #[prost(int64, tag = "9")]
    created_at_unix_nanos: i64,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartEventProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    event: String,
    #[prost(message, optional, tag = "3")]
    upload: Option<MultipartUploadProto>,
    #[prost(message, optional, tag = "4")]
    part: Option<MultipartPartProto>,
    #[prost(int64, tag = "5")]
    emitted_at_unix_nanos: i64,
    #[prost(uint64, tag = "6")]
    fence_token: u64,
    #[prost(string, tag = "7")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartUploadCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    upload: Option<MultipartUploadProto>,
}

#[derive(Clone, PartialEq, Message)]
struct MultipartPartCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(message, optional, tag = "5")]
    part: Option<MultipartPartProto>,
}

#[derive(Debug, Clone)]
struct MultipartUploadCurrentRow {
    upload: MultipartUpload,
    generation: u64,
    transaction_id: String,
    created_at_unix_nanos: u64,
}

#[derive(Debug, Clone)]
struct MultipartPartCurrentRow {
    tenant_id: i64,
    bucket_id: i64,
    part: MultipartUploadPart,
    generation: u64,
    transaction_id: String,
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectRefProto {
    #[prost(string, tag = "1")]
    hash: String,
    #[prost(uint64, tag = "2")]
    logical_size: u64,
    #[prost(string, tag = "3")]
    manifest_ref: String,
    #[prost(message, optional, tag = "4")]
    encoding: Option<CoreObjectEncodingProto>,
    #[prost(message, repeated, tag = "5")]
    placements: Vec<CoreObjectPlacementProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectEncodingProto {
    #[prost(string, tag = "1")]
    block_id: String,
    #[prost(string, tag = "2")]
    profile_id: String,
    #[prost(uint32, tag = "3")]
    data_shards: u32,
    #[prost(uint32, tag = "4")]
    parity_shards: u32,
    #[prost(uint32, tag = "5")]
    minimum_read_shards: u32,
    #[prost(uint32, tag = "6")]
    minimum_write_ack_shards: u32,
    #[prost(uint64, tag = "7")]
    stripe_size: u64,
    #[prost(string, tag = "8")]
    placement_scope: String,
    #[prost(string, tag = "9")]
    repair_priority: String,
    #[prost(string, tag = "10")]
    encryption: String,
    #[prost(string, tag = "11")]
    stored_hash: String,
    #[prost(message, optional, tag = "12")]
    compression: Option<CoreObjectCompressionProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectCompressionProto {
    #[prost(string, tag = "1")]
    algorithm: String,
    #[prost(uint32, tag = "2")]
    level: u32,
    #[prost(uint64, tag = "3")]
    uncompressed_length: u64,
    #[prost(uint64, tag = "4")]
    compressed_length: u64,
    #[prost(string, tag = "5")]
    dictionary_id: String,
    #[prost(string, tag = "6")]
    descriptor_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreObjectPlacementProto {
    #[prost(uint32, tag = "1")]
    shard_index: u32,
    #[prost(string, tag = "2")]
    node_id: String,
    #[prost(string, tag = "3")]
    region_id: String,
    #[prost(string, tag = "4")]
    cell_id: String,
    #[prost(string, tag = "5")]
    shard_hash: String,
    #[prost(uint64, tag = "6")]
    stored_size: u64,
    #[prost(uint64, tag = "7")]
    generation: u64,
    #[prost(uint64, tag = "8")]
    placement_epoch: u64,
    #[prost(uint64, tag = "9")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "10")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "11")]
    signed_payload_hash: String,
    #[prost(string, tag = "12")]
    signature_algorithm: String,
    #[prost(bytes, tag = "13")]
    receipt_signature: Vec<u8>,
}

#[cfg(test)]
async fn create_multipart_upload(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
) -> Result<MultipartUploadMutation> {
    create_multipart_upload_inner(storage, tenant_id, bucket_id, key, 0, None, None).await
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
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        permit.fence_token,
        Some(partition_precondition),
        None,
    )
    .await
}

pub(crate) async fn create_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartUploadMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        permit.fence_token,
        Some(partition_precondition),
        Some((transaction_id, transaction_principal)),
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
    transaction: Option<(&str, &str)>,
) -> Result<MultipartUploadMutation> {
    let state =
        read_state_for_optional_transaction(storage, tenant_id, bucket_id, transaction).await?;
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
        transaction,
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

pub async fn get_active_multipart_upload_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<MultipartUpload>> {
    Ok(read_state_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        Some((transaction_id, transaction_principal)),
    )
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

async fn get_active_multipart_upload_for_optional_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    transaction: Option<(&str, &str)>,
) -> Result<Option<MultipartUpload>> {
    Ok(
        read_state_for_optional_transaction(storage, tenant_id, bucket_id, transaction)
            .await?
            .uploads
            .into_values()
            .find(|u| {
                u.key == key
                    && u.upload_id == upload_id
                    && u.completed_at.is_none()
                    && u.aborted_at.is_none()
            }),
    )
}

pub async fn has_active_multipart_upload(storage: &Storage, bucket_id: i64) -> Result<bool> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    Ok(
        list_uploads_by_prefix(&meta, &multipart_all_upload_rows_prefix()?)?
            .into_iter()
            .any(|u| {
                u.bucket_id == bucket_id && u.completed_at.is_none() && u.aborted_at.is_none()
            }),
    )
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
        None,
    )
    .await
}

pub(crate) async fn upsert_multipart_part_with_permit_in_transaction(
    storage: &Storage,
    upload_row_id: i64,
    part_number: i32,
    object_ref: CoreObjectRef,
    size: i64,
    etag: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartUploadPartMutation> {
    upsert_multipart_part_inner(
        storage,
        upload_row_id,
        part_number,
        object_ref,
        size,
        etag,
        Some((permit, partition_owner_signing_key)),
        Some((transaction_id, transaction_principal)),
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
    transaction: Option<(&str, &str)>,
) -> Result<MultipartUploadPartMutation> {
    let (tenant_id, bucket_id, _) =
        find_upload_for_optional_transaction(storage, upload_row_id, transaction)
            .await?
            .ok_or_else(|| anyhow!("multipart upload not found"))?;
    let (fence_token, partition_precondition) = if let Some((permit, signing_key)) = permit {
        require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
        (
            permit.fence_token,
            Some(partition_write_precondition(storage, permit, signing_key).await?),
        )
    } else {
        (0, None)
    };
    let state =
        read_state_for_optional_transaction(storage, tenant_id, bucket_id, transaction).await?;
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
        transaction,
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

pub async fn list_multipart_parts_in_transaction(
    storage: &Storage,
    upload_row_id: i64,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Vec<MultipartUploadPart>> {
    let transaction = Some((transaction_id, transaction_principal));
    let Some((tenant_id, bucket_id, _)) =
        find_upload_for_optional_transaction(storage, upload_row_id, transaction).await?
    else {
        return Ok(Vec::new());
    };
    let mut parts = read_state_for_optional_transaction(storage, tenant_id, bucket_id, transaction)
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
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let mut uploads = list_uploads_by_prefix(&meta, &multipart_all_upload_rows_prefix()?)?
        .into_iter()
        .filter(|upload| {
            upload.bucket_id == bucket_id
                && upload.key.starts_with(prefix)
                && upload.completed_at.is_none()
                && upload.aborted_at.is_none()
        })
        .collect::<Vec<_>>();
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
    complete_multipart_upload_inner(storage, upload_row_id, None, None).await
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
        None,
    )
    .await
}

pub(crate) async fn complete_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
    upload_row_id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartCompletionMutation> {
    complete_multipart_upload_inner(
        storage,
        upload_row_id,
        Some((permit, partition_owner_signing_key)),
        Some((transaction_id, transaction_principal)),
    )
    .await
}

async fn complete_multipart_upload_inner(
    storage: &Storage,
    upload_row_id: i64,
    permit: Option<(&PartitionWritePermit, &[u8])>,
    transaction: Option<(&str, &str)>,
) -> Result<MultipartCompletionMutation> {
    let Some((tenant_id, bucket_id, mut upload)) =
        find_upload_for_optional_transaction(storage, upload_row_id, transaction).await?
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
            Some(partition_write_precondition(storage, permit, signing_key).await?),
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
        transaction,
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
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    abort_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        permit.fence_token,
        Some(partition_precondition),
        None,
    )
    .await
}

pub(crate) async fn abort_multipart_upload_with_permit_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    key: &str,
    upload_id: uuid::Uuid,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<MultipartAbortMutation> {
    require_multipart_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    abort_multipart_upload_inner(
        storage,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        permit.fence_token,
        Some(partition_precondition),
        Some((transaction_id, transaction_principal)),
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
    transaction: Option<(&str, &str)>,
) -> Result<MultipartAbortMutation> {
    let Some(mut upload) = get_active_multipart_upload_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        key,
        upload_id,
        transaction,
    )
    .await?
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
        transaction,
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

pub async fn find_multipart_upload_partition_in_transaction(
    storage: &Storage,
    upload_row_id: i64,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<(i64, i64)>> {
    Ok(find_upload_for_optional_transaction(
        storage,
        upload_row_id,
        Some((transaction_id, transaction_principal)),
    )
    .await?
    .map(|(tenant_id, bucket_id, _)| (tenant_id, bucket_id)))
}

async fn find_upload(
    storage: &Storage,
    upload_row_id: i64,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    for upload in list_uploads_by_prefix(&meta, &multipart_all_upload_rows_prefix()?)? {
        if upload.id == upload_row_id {
            return Ok(Some((upload.tenant_id, upload.bucket_id, upload)));
        }
    }
    Ok(None)
}

async fn find_upload_for_optional_transaction(
    storage: &Storage,
    upload_row_id: i64,
    transaction: Option<(&str, &str)>,
) -> Result<Option<(i64, i64, MultipartUpload)>> {
    if transaction.is_none() {
        return find_upload(storage, upload_row_id).await;
    }
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let transaction = read_transaction_for_optional_scope(&core_store, transaction).await?;
    for upload in list_uploads_by_prefix(&meta, &multipart_all_upload_rows_prefix()?)? {
        if upload.id == upload_row_id {
            let state = read_state_with_transaction(
                &meta,
                upload.tenant_id,
                upload.bucket_id,
                transaction.as_ref(),
            )?;
            if let Some(upload) = state.uploads.get(&upload_row_id).cloned() {
                return Ok(Some((upload.tenant_id, upload.bucket_id, upload)));
            }
        }
    }
    if let Some(transaction) = transaction.as_ref() {
        for update in &transaction.visible_updates {
            let CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } = update
            else {
                continue;
            };
            if canonical_coremeta_cf_name(cf)? != CF_OBJECT_HEADS
                || *table_id != TABLE_MULTIPART_UPLOAD_CURRENT_ROW
            {
                continue;
            }
            let row = decode_upload_current_row(payload)?;
            if row.upload.id == upload_row_id
                && tuple_key
                    == &multipart_upload_row_key(
                        row.upload.tenant_id,
                        row.upload.bucket_id,
                        row.upload.id,
                    )?
            {
                return Ok(Some((
                    row.upload.tenant_id,
                    row.upload.bucket_id,
                    row.upload,
                )));
            }
        }
    }
    Ok(None)
}

async fn read_state(storage: &Storage, tenant_id: i64, bucket_id: i64) -> Result<MultipartState> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    read_state_from_current_rows(&meta, tenant_id, bucket_id)
}

async fn read_state_for_optional_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    transaction: Option<(&str, &str)>,
) -> Result<MultipartState> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let transaction = read_transaction_for_optional_scope(&core_store, transaction).await?;
    read_state_with_transaction(&meta, tenant_id, bucket_id, transaction.as_ref())
}

async fn read_transaction_for_optional_scope<'a>(
    core_store: &CoreStore,
    transaction: Option<(&'a str, &'a str)>,
) -> Result<Option<CoreTransaction>> {
    let Some((transaction_id, principal)) = transaction else {
        return Ok(None);
    };
    Ok(Some(
        core_store
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?,
    ))
}

fn read_state_with_transaction(
    meta: &CoreMetaStore,
    tenant_id: i64,
    bucket_id: i64,
    transaction: Option<&CoreTransaction>,
) -> Result<MultipartState> {
    let mut state = read_state_from_current_rows(meta, tenant_id, bucket_id)?;
    let Some(transaction) = transaction else {
        return Ok(state);
    };
    for update in &transaction.visible_updates {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } => {
                if canonical_coremeta_cf_name(cf)? != CF_OBJECT_HEADS {
                    continue;
                }
                match *table_id {
                    TABLE_MULTIPART_UPLOAD_CURRENT_ROW => {
                        let row = decode_upload_current_row(payload)?;
                        if row.upload.tenant_id == tenant_id
                            && row.upload.bucket_id == bucket_id
                            && tuple_key
                                == &multipart_upload_row_key(tenant_id, bucket_id, row.upload.id)?
                        {
                            state.uploads.insert(row.upload.id, row.upload);
                        }
                    }
                    TABLE_MULTIPART_PART_CURRENT_ROW => {
                        let row = decode_part_current_row(payload)?;
                        if row.tenant_id == tenant_id
                            && row.bucket_id == bucket_id
                            && tuple_key
                                == &multipart_part_row_key(
                                    tenant_id,
                                    bucket_id,
                                    row.part.upload_id,
                                    row.part.part_number,
                                )?
                        {
                            state
                                .parts
                                .insert((row.part.upload_id, row.part.part_number), row.part);
                        }
                    }
                    _ => {}
                }
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } => {
                if canonical_coremeta_cf_name(cf)? != CF_OBJECT_HEADS {
                    continue;
                }
                match *table_id {
                    TABLE_MULTIPART_UPLOAD_CURRENT_ROW => {
                        for upload in state.uploads.values().cloned().collect::<Vec<_>>() {
                            if tuple_key
                                == &multipart_upload_row_key(tenant_id, bucket_id, upload.id)?
                            {
                                state.uploads.remove(&upload.id);
                            }
                        }
                    }
                    TABLE_MULTIPART_PART_CURRENT_ROW => {
                        for part in state.parts.values().cloned().collect::<Vec<_>>() {
                            if tuple_key
                                == &multipart_part_row_key(
                                    tenant_id,
                                    bucket_id,
                                    part.upload_id,
                                    part.part_number,
                                )?
                            {
                                state.parts.remove(&(part.upload_id, part.part_number));
                            }
                        }
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    Ok(state)
}

fn read_state_from_current_rows(
    meta: &CoreMetaStore,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<MultipartState> {
    let mut state = MultipartState::default();
    for upload in
        list_uploads_by_prefix(meta, &multipart_upload_rows_prefix(tenant_id, bucket_id)?)?
    {
        state.uploads.insert(upload.id, upload);
    }
    for part in list_parts_by_prefix(meta, &multipart_part_rows_prefix(tenant_id, bucket_id)?)? {
        state.parts.insert((part.upload_id, part.part_number), part);
    }
    Ok(state)
}

fn list_uploads_by_prefix(meta: &CoreMetaStore, prefix: &[u8]) -> Result<Vec<MultipartUpload>> {
    let mut uploads = Vec::new();
    for record in meta.scan_prefix(CF_OBJECT_HEADS, TABLE_MULTIPART_UPLOAD_CURRENT_ROW, prefix)? {
        let row = decode_upload_current_record(&record)?;
        uploads.push(row.upload);
    }
    Ok(uploads)
}

fn list_parts_by_prefix(meta: &CoreMetaStore, prefix: &[u8]) -> Result<Vec<MultipartUploadPart>> {
    let mut parts = Vec::new();
    for record in meta.scan_prefix(CF_OBJECT_HEADS, TABLE_MULTIPART_PART_CURRENT_ROW, prefix)? {
        let row = decode_part_current_record(&record)?;
        parts.push(row.part);
    }
    Ok(parts)
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
    transaction: Option<(&str, &str)>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let stream_id = multipart_metadata_stream_id(tenant_id, bucket_id);
    let mutation_id = uuid::Uuid::new_v4();
    let internal_transaction_id = format!("multipart-metadata:{mutation_id}");
    let transaction_record_id = transaction
        .map(|(transaction_id, _)| transaction_id.to_string())
        .unwrap_or_else(|| internal_transaction_id.clone());
    let body = encode_multipart_event(
        event,
        upload.as_ref(),
        part.as_ref(),
        fence_token,
        mutation_id,
    )?;
    let payload_hash = hex::encode(hash32(&body));
    let partition_id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    let transaction_state = read_transaction_for_optional_scope(&core_store, transaction).await?;
    let current_update = if let Some(transaction_state) = transaction_state.as_ref() {
        multipart_current_row_update_with_transaction(
            &meta,
            transaction_state,
            tenant_id,
            bucket_id,
            event,
            upload.as_ref(),
            part.as_ref(),
            &transaction_record_id,
        )?
    } else {
        multipart_current_row_update(
            &meta,
            tenant_id,
            bucket_id,
            event,
            upload.as_ref(),
            part.as_ref(),
            &transaction_record_id,
        )?
    };
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    preconditions.extend(current_update.preconditions.clone());
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id: stream_id.clone(),
        record_kind: "multipart_metadata".to_string(),
        payload: body.clone(),
        idempotency_key: Some(format!("multipart-metadata:{mutation_id}")),
    }];
    operations.extend(multipart_current_row_operations(
        &current_update,
        &partition_id,
    )?);
    let committed_by_principal = transaction
        .map(|(_, principal)| principal.to_string())
        .unwrap_or_else(|| multipart_metadata_partition_principal(tenant_id, bucket_id));
    let batch = CoreMutationBatch {
        transaction_id: transaction_record_id,
        scope_partition: partition_id.clone(),
        committed_by_principal,
        preconditions,
        operations,
    };
    let batch_receipt = if transaction.is_some() {
        core_store.stage_explicit_transaction_batch(batch).await?
    } else {
        core_store.commit_mutation_batch(batch).await?
    };
    let stream_update = batch_receipt
        .visible_updates
        .iter()
        .find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                visible_sequence,
                prepared_record_hash,
                ..
            } => Some((*visible_sequence, prepared_record_hash.clone())),
            CoreTransactionUpdate::CoreMetaPut { .. }
            | CoreTransactionUpdate::CoreMetaDelete { .. } => None,
        })
        .ok_or_else(|| anyhow!("multipart metadata batch did not append stream record"))?;
    Ok(MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: stream_update.1,
        watch_cursor: stream_update.0,
    })
}

#[cfg(test)]
async fn read_events_from_store(
    core_store: &CoreStore,
    stream_id: &str,
) -> Result<Vec<MultipartEventProto>> {
    let records = core_store
        .read_stream(crate::core_store::ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut events = Vec::new();
    for record in records {
        if record.record_kind != "multipart_metadata" {
            continue;
        }
        events.push(decode_multipart_event(&record.payload)?);
    }
    Ok(events)
}

#[derive(Debug, Clone, Default)]
struct MultipartCurrentRowUpdate {
    preconditions: Vec<CoreMutationPrecondition>,
    upload_row: Option<MultipartUploadCurrentRow>,
    part_row: Option<MultipartPartCurrentRow>,
}

fn multipart_current_row_update(
    meta: &CoreMetaStore,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
    upload: Option<&MultipartUpload>,
    part: Option<&MultipartUploadPart>,
    transaction_id: &str,
) -> Result<MultipartCurrentRowUpdate> {
    let mut update = MultipartCurrentRowUpdate::default();
    match event {
        MultipartMutationKind::CreateUpload
        | MultipartMutationKind::CompleteUpload
        | MultipartMutationKind::AbortUpload => {
            let upload = upload.ok_or_else(|| anyhow!("multipart upload event missing upload"))?;
            let (payload, current) = current_upload_payload(meta, tenant_id, bucket_id, upload.id)?;
            let generation = current
                .as_ref()
                .map(|row| row.generation.saturating_add(1))
                .unwrap_or(1);
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                multipart_upload_row_key(tenant_id, bucket_id, upload.id)?,
                payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            ));
            update.upload_row = Some(MultipartUploadCurrentRow {
                upload: upload.clone(),
                generation,
                transaction_id: transaction_id.to_string(),
                created_at_unix_nanos: current_unix_nanos()?,
            });
        }
        MultipartMutationKind::UpsertPart => {
            let part = part.ok_or_else(|| anyhow!("multipart part event missing part"))?;
            let upload_payload = meta.get(
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &multipart_upload_row_key(tenant_id, bucket_id, part.upload_id)?,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                multipart_upload_row_key(tenant_id, bucket_id, part.upload_id)?,
                upload_payload.as_ref(),
                false,
                true,
            ));
            let (payload, current) =
                current_part_payload(meta, tenant_id, bucket_id, part.upload_id, part.part_number)?;
            let generation = current
                .as_ref()
                .map(|row| row.generation.saturating_add(1))
                .unwrap_or(1);
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_PART_CURRENT_ROW,
                multipart_part_row_key(tenant_id, bucket_id, part.upload_id, part.part_number)?,
                payload.as_ref(),
                payload.is_none(),
                payload.is_some(),
            ));
            update.part_row = Some(MultipartPartCurrentRow {
                tenant_id,
                bucket_id,
                part: part.clone(),
                generation,
                transaction_id: transaction_id.to_string(),
                created_at_unix_nanos: current_unix_nanos()?,
            });
        }
    }
    Ok(update)
}

fn multipart_current_row_update_with_transaction(
    meta: &CoreMetaStore,
    transaction: &CoreTransaction,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
    upload: Option<&MultipartUpload>,
    part: Option<&MultipartUploadPart>,
    transaction_id: &str,
) -> Result<MultipartCurrentRowUpdate> {
    let mut update = MultipartCurrentRowUpdate::default();
    match event {
        MultipartMutationKind::CreateUpload
        | MultipartMutationKind::CompleteUpload
        | MultipartMutationKind::AbortUpload => {
            let upload = upload.ok_or_else(|| anyhow!("multipart upload event missing upload"))?;
            let key = multipart_upload_row_key(tenant_id, bucket_id, upload.id)?;
            let payload = coremeta_payload_visible_to_transaction(
                meta,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &key,
            )?;
            let current = payload
                .as_deref()
                .map(decode_upload_current_row)
                .transpose()?;
            let generation = current
                .as_ref()
                .map(|row| row.generation.saturating_add(1))
                .unwrap_or(1);
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                key,
                payload.as_ref(),
                event == MultipartMutationKind::CreateUpload,
                event != MultipartMutationKind::CreateUpload,
            ));
            update.upload_row = Some(MultipartUploadCurrentRow {
                upload: upload.clone(),
                generation,
                transaction_id: transaction_id.to_string(),
                created_at_unix_nanos: current_unix_nanos()?,
            });
        }
        MultipartMutationKind::UpsertPart => {
            let part = part.ok_or_else(|| anyhow!("multipart part event missing part"))?;
            let upload_key = multipart_upload_row_key(tenant_id, bucket_id, part.upload_id)?;
            let upload_payload = coremeta_payload_visible_to_transaction(
                meta,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                &upload_key,
            )?;
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
                upload_key,
                upload_payload.as_ref(),
                false,
                true,
            ));

            let part_key =
                multipart_part_row_key(tenant_id, bucket_id, part.upload_id, part.part_number)?;
            let payload = coremeta_payload_visible_to_transaction(
                meta,
                transaction,
                CF_OBJECT_HEADS,
                TABLE_MULTIPART_PART_CURRENT_ROW,
                &part_key,
            )?;
            let current = payload
                .as_deref()
                .map(decode_part_current_row)
                .transpose()?;
            let generation = current
                .as_ref()
                .map(|row| row.generation.saturating_add(1))
                .unwrap_or(1);
            update.preconditions.push(coremeta_row_precondition(
                TABLE_MULTIPART_PART_CURRENT_ROW,
                part_key,
                payload.as_ref(),
                payload.is_none(),
                payload.is_some(),
            ));
            update.part_row = Some(MultipartPartCurrentRow {
                tenant_id,
                bucket_id,
                part: part.clone(),
                generation,
                transaction_id: transaction_id.to_string(),
                created_at_unix_nanos: current_unix_nanos()?,
            });
        }
    }
    Ok(update)
}

fn coremeta_payload_visible_to_transaction(
    meta: &CoreMetaStore,
    transaction: &CoreTransaction,
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
) -> Result<Option<Vec<u8>>> {
    let cf = canonical_coremeta_cf_name(cf)?;
    let mut current = meta.get_named(cf, table_id, tuple_key)?;
    for update in &transaction.visible_updates {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf: update_cf,
                table_id: update_table_id,
                tuple_key: update_key,
                payload,
                ..
            } => {
                if canonical_coremeta_cf_name(update_cf)? == cf
                    && *update_table_id == table_id
                    && update_key == tuple_key
                {
                    current = Some(payload.clone());
                }
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf: update_cf,
                table_id: update_table_id,
                tuple_key: update_key,
                ..
            } => {
                if canonical_coremeta_cf_name(update_cf)? == cf
                    && *update_table_id == table_id
                    && update_key == tuple_key
                {
                    current = None;
                }
            }
            _ => {}
        }
    }
    Ok(current)
}

fn multipart_current_row_operations(
    update: &MultipartCurrentRowUpdate,
    partition_id: &str,
) -> Result<Vec<CoreMutationOperation>> {
    let mut operations = Vec::new();
    if let Some(row) = update.upload_row.as_ref() {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
            tuple_key: multipart_upload_row_key(
                row.upload.tenant_id,
                row.upload.bucket_id,
                row.upload.id,
            )?,
            payload: encode_upload_current_row(row)?,
        });
    }
    if let Some(row) = update.part_row.as_ref() {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.to_string(),
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MULTIPART_PART_CURRENT_ROW,
            tuple_key: multipart_part_row_key(
                row.tenant_id,
                row.bucket_id,
                row.part.upload_id,
                row.part.part_number,
            )?,
            payload: encode_part_current_row(row)?,
        });
    }
    Ok(operations)
}

fn coremeta_row_precondition(
    table_id: u16,
    tuple_key: Vec<u8>,
    current_payload: Option<&Vec<u8>>,
    require_absent: bool,
    require_present: bool,
) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_OBJECT_HEADS.to_string(),
        table_id,
        tuple_key,
        expected_payload_hash: current_payload
            .map(|payload| core_meta_payload_digest(table_id, payload)),
        require_absent,
        require_present,
    }
}

#[cfg(test)]
pub(crate) async fn read_multipart_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(crate::core_store::ReadStream {
            stream_id: multipart_metadata_stream_id(tenant_id, bucket_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?
        .into_iter()
        .filter(|record| record.record_kind == "multipart_metadata")
        .map(|record| decode_multipart_event_fence(&record.payload))
        .collect::<Result<Vec<_>>>()?)
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
mod tests;
