use crate::core_store::{
    CoreCompressionDescriptor, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreObjectEncoding, CoreObjectPlacement, CoreObjectRef, CoreStore, CoreTransaction,
    CoreTransactionUpdate, ReadStream, StreamRecord,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{
    AppendStream, AppendStreamMutation, AppendStreamRecord, AppendStreamRecordMutation,
    MetadataMutationReceipt, SealAppendStreamMutation,
};
use crate::storage::Storage;
use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use prost::{Message, Oneof};
use std::collections::BTreeMap;

const APPEND_METADATA_BODY_SCHEMA: &str = "anvil.core.append_metadata.v1";

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

#[derive(Debug, Clone)]
struct AppendBody {
    event: String,
    stream: Option<AppendStream>,
    record: Option<AppendStreamRecord>,
    emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct AppendBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    emitted_at: String,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
    #[prost(oneof = "append_body_proto::Event", tags = "10, 11, 12")]
    event: Option<append_body_proto::Event>,
}

mod append_body_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Event {
        #[prost(message, tag = "10")]
        CreateStream(super::AppendStreamProto),
        #[prost(message, tag = "11")]
        AppendRecord(super::AppendStreamRecordProto),
        #[prost(message, tag = "12")]
        SealStream(super::AppendStreamProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct AppendStreamProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(string, tag = "5")]
    stream_key: String,
    #[prost(string, tag = "6")]
    stream_id: String,
    #[prost(string, tag = "7")]
    created_at: String,
    #[prost(string, optional, tag = "8")]
    sealed_at: Option<String>,
    #[prost(string, optional, tag = "9")]
    segment_hash: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct AppendStreamRecordProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    stream_id: i64,
    #[prost(int64, tag = "3")]
    record_sequence: i64,
    #[prost(string, tag = "4")]
    payload_hash: String,
    #[prost(message, optional, tag = "5")]
    payload_object_ref: Option<CoreObjectRefProto>,
    #[prost(int64, tag = "6")]
    payload_size: i64,
    #[prost(string, optional, tag = "7")]
    content_type: Option<String>,
    #[prost(bytes = "vec", tag = "8")]
    user_meta_json: Vec<u8>,
    #[prost(bool, tag = "9")]
    has_user_meta: bool,
    #[prost(string, tag = "10")]
    created_at: String,
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
    #[prost(bytes = "vec", tag = "13")]
    receipt_signature: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
struct AppendState {
    streams: BTreeMap<i64, AppendStream>,
    records: BTreeMap<(i64, i64), AppendStreamRecord>,
}

#[cfg(test)]
async fn create_append_stream(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
) -> Result<AppendStreamMutation> {
    create_append_stream_inner(
        storage,
        tenant_id,
        bucket_id,
        bucket_name,
        stream_key,
        0,
        None,
        None,
        None,
    )
    .await
}

pub(crate) async fn create_append_stream_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_append_stream_inner(
        storage,
        tenant_id,
        bucket_id,
        bucket_name,
        stream_key,
        permit.fence_token,
        Some(partition_precondition),
        None,
        None,
    )
    .await
}

pub(crate) async fn create_append_stream_with_permit_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: &str,
    stream_key: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<AppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_append_stream_inner(
        storage,
        tenant_id,
        bucket_id,
        bucket_name,
        stream_key,
        permit.fence_token,
        Some(partition_precondition),
        Some(transaction_id),
        Some(transaction_principal),
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
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<AppendStreamMutation> {
    let state = read_state_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        transaction_id.zip(transaction_principal),
    )
    .await?;
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
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::CreateStream,
        Some(stream.clone()),
        None,
        fence_token,
        partition_precondition,
        transaction_id,
        transaction_principal,
    )
    .await?;
    Ok(AppendStreamMutation { stream, receipt })
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
        .find(|stream| stream.stream_key == stream_key && stream.stream_id == stream_id))
}

pub async fn get_active_append_stream_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream_key: &str,
    stream_id: uuid::Uuid,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<AppendStream>> {
    Ok(read_state_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        Some((transaction_id, transaction_principal)),
    )
    .await?
    .streams
    .into_values()
    .find(|stream| stream.stream_key == stream_key && stream.stream_id == stream_id))
}

#[cfg(test)]
async fn append_stream_record(
    storage: &Storage,
    stream_row_id: i64,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
) -> Result<AppendStreamRecordMutation> {
    append_stream_record_inner(
        storage,
        stream_row_id,
        payload_object_ref,
        payload_size,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

pub(crate) async fn append_stream_record_with_permit(
    storage: &Storage,
    stream_row_id: i64,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AppendStreamRecordMutation> {
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_stream_record_inner(
        storage,
        stream_row_id,
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        Some(permit),
        Some(partition_precondition),
        None,
        None,
        None,
    )
    .await
}

pub(crate) async fn append_stream_record_with_permit_in_transaction(
    storage: &Storage,
    stream_row_id: i64,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<AppendStreamRecordMutation> {
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_stream_record_inner(
        storage,
        stream_row_id,
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        Some(permit),
        Some(partition_precondition),
        None,
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

pub(crate) async fn append_stream_record_with_permit_in_partition_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream_row_id: i64,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<AppendStreamRecordMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_stream_record_inner(
        storage,
        stream_row_id,
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        Some(permit),
        Some(partition_precondition),
        Some((tenant_id, bucket_id)),
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

async fn append_stream_record_inner(
    storage: &Storage,
    stream_row_id: i64,
    payload_object_ref: CoreObjectRef,
    payload_size: i64,
    content_type: Option<String>,
    user_meta: Option<serde_json::Value>,
    permit: Option<&PartitionWritePermit>,
    partition_precondition: Option<CoreMutationPrecondition>,
    known_partition: Option<(i64, i64)>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<AppendStreamRecordMutation> {
    let (tenant_id, bucket_id) = if let Some((tenant_id, bucket_id)) = known_partition {
        (tenant_id, bucket_id)
    } else {
        let (tenant_id, bucket_id, _) = find_stream(storage, stream_row_id)
            .await?
            .ok_or_else(|| anyhow!("append stream not found"))?;
        (tenant_id, bucket_id)
    };
    if let Some(permit) = permit {
        require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    }
    let fence_token = permit.map(|permit| permit.fence_token).unwrap_or(0);
    let state = read_state_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        transaction_id.zip(transaction_principal),
    )
    .await?;
    if !state.streams.contains_key(&stream_row_id) {
        bail!("append stream not found");
    }
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
        payload_hash: payload_object_ref.hash.clone(),
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        created_at: Utc::now(),
    };
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::AppendRecord,
        None,
        Some(record.clone()),
        fence_token,
        partition_precondition,
        transaction_id,
        transaction_principal,
    )
    .await?;
    Ok(AppendStreamRecordMutation { record, receipt })
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

pub async fn list_append_stream_records_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream_row_id: i64,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Vec<AppendStreamRecord>> {
    let mut records = read_state_for_optional_transaction(
        storage,
        tenant_id,
        bucket_id,
        Some((transaction_id, transaction_principal)),
    )
    .await?
    .records
    .into_values()
    .filter(|record| record.stream_id == stream_row_id)
    .collect::<Vec<_>>();
    records.sort_by_key(|record| record.record_sequence);
    Ok(records)
}

pub async fn list_append_stream_records_for_bucket(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<(AppendStream, AppendStreamRecord)>> {
    let state = read_state(storage, tenant_id, bucket_id).await?;
    let mut records = Vec::new();
    for record in state.records.into_values() {
        if let Some(stream) = state.streams.get(&record.stream_id) {
            records.push((stream.clone(), record));
        }
    }
    records.sort_by(|left, right| {
        left.0
            .stream_key
            .cmp(&right.0.stream_key)
            .then(left.1.record_sequence.cmp(&right.1.record_sequence))
    });
    Ok(records)
}

pub async fn append_record_source_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<u128> {
    Ok(read_state(storage, tenant_id, bucket_id)
        .await?
        .records
        .values()
        .map(|record| record.id.max(0) as u128)
        .max()
        .unwrap_or(0))
}

#[cfg(test)]
async fn seal_append_stream(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
) -> Result<SealAppendStreamMutation> {
    seal_append_stream_inner(
        storage,
        stream_row_id,
        segment_hash,
        None,
        None,
        None,
        None,
        None,
    )
    .await
}

pub(crate) async fn seal_append_stream_with_permit(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<SealAppendStreamMutation> {
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    seal_append_stream_inner(
        storage,
        stream_row_id,
        segment_hash,
        Some(permit),
        Some(partition_precondition),
        None,
        None,
        None,
    )
    .await
}

pub(crate) async fn seal_append_stream_with_permit_in_transaction(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<SealAppendStreamMutation> {
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    seal_append_stream_inner(
        storage,
        stream_row_id,
        segment_hash,
        Some(permit),
        Some(partition_precondition),
        None,
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

pub(crate) async fn seal_append_stream_with_permit_in_partition_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    stream_row_id: i64,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<SealAppendStreamMutation> {
    require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    seal_append_stream_inner(
        storage,
        stream_row_id,
        segment_hash,
        Some(permit),
        Some(partition_precondition),
        Some((tenant_id, bucket_id)),
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

async fn seal_append_stream_inner(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
    permit: Option<&PartitionWritePermit>,
    partition_precondition: Option<CoreMutationPrecondition>,
    known_partition: Option<(i64, i64)>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<SealAppendStreamMutation> {
    let Some((tenant_id, bucket_id, mut stream)) = find_stream_for_optional_transaction(
        storage,
        stream_row_id,
        known_partition,
        transaction_id.zip(transaction_principal),
    )
    .await?
    else {
        return Ok(SealAppendStreamMutation {
            sealed: false,
            receipt: None,
        });
    };
    if let Some(permit) = permit {
        require_append_metadata_permit(tenant_id, bucket_id, permit)?;
    }
    let fence_token = permit.map(|permit| permit.fence_token).unwrap_or(0);
    stream.sealed_at = Some(Utc::now());
    stream.segment_hash = Some(segment_hash.to_string());
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::SealStream,
        Some(stream),
        None,
        fence_token,
        partition_precondition,
        transaction_id,
        transaction_principal,
    )
    .await?;
    Ok(SealAppendStreamMutation {
        sealed: true,
        receipt: Some(receipt),
    })
}

pub async fn find_append_stream_partition(
    storage: &Storage,
    stream_row_id: i64,
) -> Result<Option<(i64, i64)>> {
    Ok(find_stream(storage, stream_row_id)
        .await?
        .map(|(tenant_id, bucket_id, _)| (tenant_id, bucket_id)))
}

async fn find_stream(
    storage: &Storage,
    stream_row_id: i64,
) -> Result<Option<(i64, i64, AppendStream)>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    for stream_id in core_store
        .list_stream_ids("append_metadata:tenant:")
        .await?
    {
        let state = read_state_from_stream(&core_store, &stream_id).await?;
        if let Some(stream) = state.streams.get(&stream_row_id).cloned() {
            return Ok(Some((stream.tenant_id, stream.bucket_id, stream)));
        }
    }
    Ok(None)
}

async fn read_state(storage: &Storage, tenant_id: i64, bucket_id: i64) -> Result<AppendState> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_state_from_stream(
        &core_store,
        &append_metadata_stream_id(tenant_id, bucket_id),
    )
    .await
}

async fn read_state_for_optional_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    transaction: Option<(&str, &str)>,
) -> Result<AppendState> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = append_metadata_stream_id(tenant_id, bucket_id);
    if let Some((transaction_id, transaction_principal)) = transaction {
        return read_state_from_records(
            core_store
                .read_stream_visible_to_transaction(
                    ReadStream {
                        stream_id,
                        after_sequence: 0,
                        limit: 0,
                    },
                    transaction_id,
                    transaction_principal,
                )
                .await?,
        );
    }
    read_state_from_stream(&core_store, &stream_id).await
}

async fn read_state_from_stream(core_store: &CoreStore, stream_id: &str) -> Result<AppendState> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    read_state_from_records(records)
}

fn read_state_from_records(records: Vec<StreamRecord>) -> Result<AppendState> {
    let mut state = AppendState::default();
    for record in records {
        if record.record_kind != "append_metadata" {
            continue;
        }
        let body = decode_append_body(&record.payload)?;
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

pub async fn materialize_committed_append_streams_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<Vec<AppendStream>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut streams = Vec::new();
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
        } = update
        else {
            continue;
        };
        if !stream_id.starts_with("append_metadata:tenant:") {
            continue;
        }
        let after_sequence = visible_sequence.saturating_sub(1);
        let records = core_store
            .read_stream(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence,
                limit: 1,
            })
            .await?;
        for record in records {
            if record.sequence != *visible_sequence || record.event_hash != *prepared_record_hash {
                continue;
            }
            let body = decode_append_body(&record.payload)?;
            if body.event == "create_stream"
                && let Some(stream) = body.stream
            {
                streams.push(stream);
            }
        }
    }
    Ok(streams)
}

async fn find_stream_for_optional_transaction(
    storage: &Storage,
    stream_row_id: i64,
    known_partition: Option<(i64, i64)>,
    transaction: Option<(&str, &str)>,
) -> Result<Option<(i64, i64, AppendStream)>> {
    if let Some((tenant_id, bucket_id)) = known_partition {
        let state =
            read_state_for_optional_transaction(storage, tenant_id, bucket_id, transaction).await?;
        return Ok(state
            .streams
            .get(&stream_row_id)
            .cloned()
            .map(|stream| (tenant_id, bucket_id, stream)));
    }
    find_stream(storage, stream_row_id).await
}

async fn append_body(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    event: AppendMutationKind,
    stream: Option<AppendStream>,
    record: Option<AppendStreamRecord>,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = append_metadata_stream_id(tenant_id, bucket_id);
    let mutation_id = uuid::Uuid::new_v4();
    let body = AppendBody {
        event: event.as_str().to_string(),
        stream,
        record,
        emitted_at: Utc::now().to_rfc3339(),
    };
    let payload = encode_append_body(&body, fence_token, mutation_id)?;
    let payload_hash = hex::encode(hash32(&payload));
    let partition_id = hex::encode(append_metadata_partition_id(tenant_id, bucket_id));
    let batch = CoreMutationBatch {
        transaction_id: transaction_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| format!("append-metadata:{tenant_id}:{bucket_id}:{mutation_id}")),
        scope_partition: partition_id.clone(),
        committed_by_principal: transaction_principal
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| append_metadata_partition_principal(tenant_id, bucket_id)),
        preconditions: partition_precondition.into_iter().collect(),
        operations: vec![CoreMutationOperation::StreamAppend {
            partition_id,
            stream_id,
            record_kind: "append_metadata".to_string(),
            payload,
            idempotency_key: Some(format!(
                "append-metadata:{tenant_id}:{bucket_id}:{mutation_id}"
            )),
        }],
    };
    let batch_receipt = if transaction_id.is_some() {
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
        .ok_or_else(|| anyhow!("append metadata batch did not append stream record"))?;
    Ok(MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: stream_update.1,
        watch_cursor: stream_update.0,
    })
}

fn encode_append_body(
    body: &AppendBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let event = match body.event.as_str() {
        "create_stream" => append_body_proto::Event::CreateStream(stream_to_proto(
            body.stream
                .as_ref()
                .ok_or_else(|| anyhow!("append create body is missing stream"))?,
        )),
        "append_record" => append_body_proto::Event::AppendRecord(record_to_proto(
            body.record
                .as_ref()
                .ok_or_else(|| anyhow!("append record body is missing record"))?,
        )?),
        "seal_stream" => append_body_proto::Event::SealStream(stream_to_proto(
            body.stream
                .as_ref()
                .ok_or_else(|| anyhow!("append seal body is missing stream"))?,
        )),
        other => return Err(anyhow!("unknown append metadata event {other}")),
    };
    encode_deterministic_proto(&AppendBodyProto {
        schema: APPEND_METADATA_BODY_SCHEMA.to_string(),
        emitted_at: body.emitted_at.clone(),
        fence_token,
        mutation_id: mutation_id.to_string(),
        event: Some(event),
    })
}

fn decode_append_body(bytes: &[u8]) -> Result<AppendBody> {
    let proto = AppendBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "append metadata body")?;
    if proto.schema != APPEND_METADATA_BODY_SCHEMA {
        return Err(anyhow!("append metadata body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("append metadata body has invalid mutation id"))?;
    let event = proto
        .event
        .ok_or_else(|| anyhow!("append metadata body is missing event"))?;
    let emitted_at = proto.emitted_at;
    Ok(match event {
        append_body_proto::Event::CreateStream(stream) => AppendBody {
            event: "create_stream".to_string(),
            stream: Some(stream_from_proto(stream)?),
            record: None,
            emitted_at,
        },
        append_body_proto::Event::AppendRecord(record) => AppendBody {
            event: "append_record".to_string(),
            stream: None,
            record: Some(record_from_proto(record)?),
            emitted_at,
        },
        append_body_proto::Event::SealStream(stream) => AppendBody {
            event: "seal_stream".to_string(),
            stream: Some(stream_from_proto(stream)?),
            record: None,
            emitted_at,
        },
    })
}

#[cfg(test)]
fn decode_append_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = AppendBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "append metadata body")?;
    if proto.schema != APPEND_METADATA_BODY_SCHEMA {
        return Err(anyhow!("append metadata body schema mismatch"));
    }
    Ok(proto.fence_token)
}

fn stream_to_proto(stream: &AppendStream) -> AppendStreamProto {
    AppendStreamProto {
        id: stream.id,
        tenant_id: stream.tenant_id,
        bucket_id: stream.bucket_id,
        bucket_name: stream.bucket_name.clone(),
        stream_key: stream.stream_key.clone(),
        stream_id: stream.stream_id.to_string(),
        created_at: stream.created_at.to_rfc3339(),
        sealed_at: stream.sealed_at.as_ref().map(chrono::DateTime::to_rfc3339),
        segment_hash: stream.segment_hash.clone(),
    }
}

fn stream_from_proto(proto: AppendStreamProto) -> Result<AppendStream> {
    Ok(AppendStream {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        bucket_name: proto.bucket_name,
        stream_key: proto.stream_key,
        stream_id: uuid::Uuid::parse_str(&proto.stream_id)?,
        created_at: chrono::DateTime::parse_from_rfc3339(&proto.created_at)?.with_timezone(&Utc),
        sealed_at: proto
            .sealed_at
            .map(|value| chrono::DateTime::parse_from_rfc3339(&value))
            .transpose()?
            .map(|value| value.with_timezone(&Utc)),
        segment_hash: proto.segment_hash,
    })
}

fn record_to_proto(record: &AppendStreamRecord) -> Result<AppendStreamRecordProto> {
    Ok(AppendStreamRecordProto {
        id: record.id,
        stream_id: record.stream_id,
        record_sequence: record.record_sequence,
        payload_hash: record.payload_hash.clone(),
        payload_object_ref: Some(object_ref_to_proto(&record.payload_object_ref)),
        payload_size: record.payload_size,
        content_type: record.content_type.clone(),
        user_meta_json: record
            .user_meta
            .as_ref()
            .map(serde_json::to_vec)
            .transpose()?
            .unwrap_or_default(),
        has_user_meta: record.user_meta.is_some(),
        created_at: record.created_at.to_rfc3339(),
    })
}

fn record_from_proto(proto: AppendStreamRecordProto) -> Result<AppendStreamRecord> {
    Ok(AppendStreamRecord {
        id: proto.id,
        stream_id: proto.stream_id,
        record_sequence: proto.record_sequence,
        payload_hash: proto.payload_hash,
        payload_object_ref: object_ref_from_proto(
            proto
                .payload_object_ref
                .ok_or_else(|| anyhow!("append record body is missing payload object ref"))?,
        )?,
        payload_size: proto.payload_size,
        content_type: proto.content_type,
        user_meta: if proto.has_user_meta {
            Some(serde_json::from_slice(&proto.user_meta_json)?)
        } else {
            None
        },
        created_at: chrono::DateTime::parse_from_rfc3339(&proto.created_at)?.with_timezone(&Utc),
    })
}

fn object_ref_to_proto(value: &CoreObjectRef) -> CoreObjectRefProto {
    CoreObjectRefProto {
        hash: value.hash.clone(),
        logical_size: value.logical_size,
        manifest_ref: value.manifest_ref.clone(),
        encoding: Some(CoreObjectEncodingProto {
            block_id: value.encoding.block_id.clone(),
            profile_id: value.encoding.profile_id.clone(),
            data_shards: value.encoding.data_shards as u32,
            parity_shards: value.encoding.parity_shards as u32,
            minimum_read_shards: value.encoding.minimum_read_shards as u32,
            minimum_write_ack_shards: value.encoding.minimum_write_ack_shards as u32,
            stripe_size: value.encoding.stripe_size,
            placement_scope: value.encoding.placement_scope.clone(),
            repair_priority: value.encoding.repair_priority.clone(),
            stored_hash: value.encoding.stored_hash.clone(),
            compression: Some(object_compression_to_proto(&value.encoding.compression)),
            encryption: value.encoding.encryption.clone(),
        }),
        placements: value
            .placements
            .iter()
            .map(|placement| CoreObjectPlacementProto {
                shard_index: placement.shard_index as u32,
                node_id: placement.node_id.clone(),
                region_id: placement.region_id.clone(),
                cell_id: placement.cell_id.clone(),
                shard_hash: placement.shard_hash.clone(),
                stored_size: placement.stored_size,
                generation: placement.generation,
                placement_epoch: placement.placement_epoch,
                fsync_sequence: placement.fsync_sequence,
                written_at_unix_nanos: placement.written_at_unix_nanos,
                signed_payload_hash: placement.signed_payload_hash.clone(),
                signature_algorithm: placement.signature_algorithm.clone(),
                receipt_signature: placement.receipt_signature.clone(),
            })
            .collect(),
    }
}

fn object_ref_from_proto(value: CoreObjectRefProto) -> Result<CoreObjectRef> {
    let encoding = value
        .encoding
        .ok_or_else(|| anyhow!("append payload object ref is missing encoding"))?;
    Ok(CoreObjectRef {
        hash: value.hash,
        logical_size: value.logical_size,
        manifest_ref: value.manifest_ref,
        encoding: CoreObjectEncoding {
            block_id: encoding.block_id,
            profile_id: encoding.profile_id,
            data_shards: encoding.data_shards as u16,
            parity_shards: encoding.parity_shards as u16,
            minimum_read_shards: encoding.minimum_read_shards as u16,
            minimum_write_ack_shards: encoding.minimum_write_ack_shards as u16,
            stripe_size: encoding.stripe_size,
            placement_scope: encoding.placement_scope,
            repair_priority: encoding.repair_priority,
            stored_hash: encoding.stored_hash,
            compression: object_compression_from_proto(encoding.compression.ok_or_else(|| {
                anyhow!("append payload object ref is missing compression descriptor")
            })?),
            encryption: encoding.encryption,
        },
        placements: value
            .placements
            .into_iter()
            .map(|placement| CoreObjectPlacement {
                shard_index: placement.shard_index as u16,
                node_id: placement.node_id,
                region_id: placement.region_id,
                cell_id: placement.cell_id,
                shard_hash: placement.shard_hash,
                stored_size: placement.stored_size,
                generation: placement.generation,
                placement_epoch: placement.placement_epoch,
                fsync_sequence: placement.fsync_sequence,
                written_at_unix_nanos: placement.written_at_unix_nanos,
                signed_payload_hash: placement.signed_payload_hash,
                signature_algorithm: placement.signature_algorithm,
                receipt_signature: placement.receipt_signature,
            })
            .collect(),
    })
}

fn object_compression_to_proto(value: &CoreCompressionDescriptor) -> CoreObjectCompressionProto {
    CoreObjectCompressionProto {
        algorithm: value.algorithm.clone(),
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

fn object_compression_from_proto(value: CoreObjectCompressionProto) -> CoreCompressionDescriptor {
    CoreCompressionDescriptor {
        algorithm: value.algorithm,
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id,
        descriptor_hash: value.descriptor_hash,
    }
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
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

pub fn append_metadata_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/append").as_bytes())
}

#[cfg(test)]
pub(crate) async fn read_append_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(ReadStream {
            stream_id: append_metadata_stream_id(tenant_id, bucket_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?
        .into_iter()
        .filter(|record| record.record_kind == "append_metadata")
        .map(|record| decode_append_body_fence(&record.payload))
        .collect::<Result<Vec<_>>>()?)
}

fn append_metadata_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("append_metadata:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn append_metadata_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:append_metadata:{tenant_id}:{bucket_id}")
}

fn require_append_metadata_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id = hex::encode(append_metadata_partition_id(tenant_id, bucket_id));
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
            .unwrap()
            .stream;
        append_stream_record(&storage, stream.id, payload_ref("hash-a", 10), 10)
            .await
            .unwrap();
        append_stream_record(&storage, stream.id, payload_ref("hash-b", 20), 20)
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
                .sealed
        );
        assert!(
            get_active_append_stream(&storage, 1, 2, "stream", stream.stream_id)
                .await
                .unwrap()
                .is_some()
        );
        append_stream_record(&storage, stream.id, payload_ref("hash-c", 30), 30)
            .await
            .unwrap();
        assert_eq!(
            list_append_stream_records(&storage, stream.id)
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    pub(crate) async fn append_journal_with_permit_writes_fenced_frames_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let permit = owner.write_permit().unwrap();

        let stream =
            create_append_stream_with_permit(&storage, 1, 2, "bucket", "stream", &permit, KEY)
                .await
                .unwrap();
        append_stream_record_with_permit(
            &storage,
            stream.stream.id,
            payload_ref("hash-a", 10),
            10,
            None,
            None,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        seal_append_stream_with_permit(&storage, stream.stream.id, "segment-a", &permit, KEY)
            .await
            .unwrap();

        let fences = read_append_frame_fences_for_test(&storage, 1, 2)
            .await
            .unwrap();
        assert_eq!(fences.len(), 3);
        assert!(fences.iter().all(|fence| *fence == permit.fence_token));
    }

    #[tokio::test]
    pub(crate) async fn append_journal_with_permit_rejects_stale_fence() {
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

        let err = append_stream_record_with_permit(
            &storage,
            stream.stream.id,
            payload_ref("hash-a", 10),
            10,
            None,
            None,
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
    pub(crate) async fn append_journal_batch_rejects_stale_partition_precondition() {
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
        let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = append_stream_record_inner(
            &storage,
            stream.stream.id,
            payload_ref("hash-a", 10),
            10,
            None,
            None,
            Some(&stale_permit),
            Some(stale_precondition),
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("target mismatch")
                || err.to_string().contains("generation mismatch"),
            "unexpected error: {err:?}"
        );
    }

    async fn ready_owner(
        storage: &Storage,
        tenant_id: i64,
        bucket_id: i64,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "append_metadata".to_string();
        let id = hex::encode(append_metadata_partition_id(tenant_id, bucket_id));
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
