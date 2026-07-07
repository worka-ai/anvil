use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreObjectRef, CoreStore,
    ReadStream,
};
use crate::formats::{Hash32, JournalFrame, JournalRecordKind, hash32, validate_journal_chain};
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
use crate::persistence::{
    AppendStream, AppendStreamMutation, AppendStreamRecord, AppendStreamRecordMutation,
    MetadataMutationReceipt, SealAppendStreamMutation,
};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    create_append_stream_inner(
        storage,
        tenant_id,
        bucket_id,
        bucket_name,
        stream_key,
        permit.fence_token,
        Some(partition_precondition),
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
) -> Result<AppendStreamMutation> {
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
    let receipt = append_body(
        storage,
        tenant_id,
        bucket_id,
        AppendMutationKind::CreateStream,
        Some(stream.clone()),
        None,
        fence_token,
        partition_precondition,
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
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    append_stream_record_inner(
        storage,
        stream_row_id,
        payload_object_ref,
        payload_size,
        content_type,
        user_meta,
        Some(permit),
        Some(partition_precondition),
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
) -> Result<AppendStreamRecordMutation> {
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
    seal_append_stream_inner(storage, stream_row_id, segment_hash, None, None).await
}

pub(crate) async fn seal_append_stream_with_permit(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<SealAppendStreamMutation> {
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    seal_append_stream_inner(
        storage,
        stream_row_id,
        segment_hash,
        Some(permit),
        Some(partition_precondition),
    )
    .await
}

async fn seal_append_stream_inner(
    storage: &Storage,
    stream_row_id: i64,
    segment_hash: &str,
    permit: Option<&PartitionWritePermit>,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<SealAppendStreamMutation> {
    let Some((tenant_id, bucket_id, mut stream)) = find_stream(storage, stream_row_id).await?
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

async fn read_state_from_stream(core_store: &CoreStore, stream_id: &str) -> Result<AppendState> {
    let frames = read_frames(core_store, stream_id).await?;
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
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = append_metadata_stream_id(tenant_id, bucket_id);
    let previous = read_frames(&core_store, &stream_id)
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
    let body = serde_json::to_vec(&AppendBody {
        event: event.as_str().to_string(),
        stream,
        record,
        emitted_at: Utc::now().to_rfc3339(),
    })?;
    let payload_hash = hex::encode(hash32(&body));
    let frame = JournalFrame::new(
        JournalRecordKind::AppendMetadata,
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
    let partition_id = hex::encode(append_metadata_partition_id(tenant_id, bucket_id));
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("append-metadata:{tenant_id}:{bucket_id}:{mutation_id}"),
            scope_partition: partition_id.clone(),
            committed_by_principal: append_metadata_partition_principal(tenant_id, bucket_id),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind: "append_metadata".to_string(),
                payload: frame.encode(),
                idempotency_key: Some(format!(
                    "append-metadata:{tenant_id}:{bucket_id}:{mutation_id}"
                )),
            }],
        })
        .await?;
    Ok(receipt)
}

async fn read_frames(core_store: &CoreStore, stream_id: &str) -> Result<Vec<JournalFrame>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut frames = Vec::new();
    for record in records {
        if record.record_kind != "append_metadata" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
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
    Ok(read_frames(
        &core_store,
        &append_metadata_stream_id(tenant_id, bucket_id),
    )
    .await?
    .into_iter()
    .map(|frame| frame.fence_token)
    .collect())
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

        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let frames = read_frames(&core_store, &append_metadata_stream_id(1, 2))
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
        let stale_precondition = partition_write_ref_precondition(&storage, &stale_permit, KEY)
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
