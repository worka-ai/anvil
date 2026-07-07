use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
};
use crate::formats::{Hash32, JournalFrame, JournalRecordKind, hash32, validate_journal_chain};
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
use crate::persistence::{Bucket, BucketMetadataEvent};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

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

#[cfg(test)]
async fn append_bucket_mutation(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
) -> Result<()> {
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        BucketJournalScope::Tenant(bucket.tenant_id),
        0,
        None,
    )
    .await?;
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        BucketJournalScope::Global,
        0,
        None,
    )
    .await
}

pub(crate) async fn append_bucket_mutation_with_permits(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    tenant_permit: &PartitionWritePermit,
    global_permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let total_start = std::time::Instant::now();
    let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
    let global_scope = BucketJournalScope::Global;
    require_bucket_scope_permit(tenant_scope, tenant_permit)?;
    require_bucket_scope_permit(global_scope, global_permit)?;
    let step_start = std::time::Instant::now();
    let tenant_precondition =
        partition_write_ref_precondition(storage, tenant_permit, partition_owner_signing_key)
            .await?;
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation tenant_precondition",
        step_start.elapsed(),
    );
    let step_start = std::time::Instant::now();
    let global_precondition =
        partition_write_ref_precondition(storage, global_permit, partition_owner_signing_key)
            .await?;
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation global_precondition",
        step_start.elapsed(),
    );
    let step_start = std::time::Instant::now();
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        tenant_scope,
        tenant_permit.fence_token,
        Some(tenant_precondition),
    )
    .await?;
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation tenant_append",
        step_start.elapsed(),
    );
    let step_start = std::time::Instant::now();
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        global_scope,
        global_permit.fence_token,
        Some(global_precondition),
    )
    .await?;
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation global_append",
        step_start.elapsed(),
    );
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation total",
        total_start.elapsed(),
    );
    Ok(())
}

pub async fn read_public_bucket_by_name(
    storage: &Storage,
    bucket_name: &str,
) -> Result<Option<Bucket>> {
    Ok(
        read_current_buckets_from_stream(storage, BucketJournalScope::Global)
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
        read_current_buckets_from_stream(storage, BucketJournalScope::Global)
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
        read_current_buckets_from_stream(storage, BucketJournalScope::Global)
            .await?
            .into_iter()
            .find(|bucket| bucket.id == bucket_id),
    )
}

pub async fn next_bucket_id(storage: &Storage) -> Result<i64> {
    let frames = read_bucket_journal_frames(storage, BucketJournalScope::Global).await?;
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

async fn append_bucket_mutation_to_stream(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    scope: BucketJournalScope,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let previous = read_bucket_journal_frames_from_store(&core_store, scope)
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
        fence_token,
        *mutation_id.as_bytes(),
        bucket_key_hash(bucket.tenant_id, &bucket.name),
        previous_hash,
        body,
    );

    let partition_id = hex::encode(scope.partition_id());
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("bucket-metadata:{}:{}", scope.stream_id(), mutation_id),
            scope_partition: partition_id.clone(),
            committed_by_principal: scope.partition_principal(),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id: scope.stream_id(),
                record_kind: "bucket_metadata".to_string(),
                payload: frame.encode(),
                idempotency_key: Some(format!(
                    "bucket-metadata:{}:{}",
                    scope.stream_id(),
                    mutation_id
                )),
            }],
        })
        .await?;
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
    read_current_buckets_from_stream(storage, BucketJournalScope::Tenant(tenant_id)).await
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
    let frames = read_bucket_journal_frames(storage, BucketJournalScope::Tenant(tenant_id)).await?;
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
    let frames = read_bucket_journal_frames(storage, BucketJournalScope::Tenant(tenant_id)).await?;
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

async fn read_current_buckets_from_stream(
    storage: &Storage,
    scope: BucketJournalScope,
) -> Result<Vec<Bucket>> {
    let frames = read_bucket_journal_frames(storage, scope).await?;
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

async fn read_bucket_journal_frames(
    storage: &Storage,
    scope: BucketJournalScope,
) -> Result<Vec<JournalFrame>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_bucket_journal_frames_from_store(&core_store, scope).await
}

async fn read_bucket_journal_frames_from_store(
    core_store: &CoreStore,
    scope: BucketJournalScope,
) -> Result<Vec<JournalFrame>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: scope.stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut frames = Vec::new();
    for record in records {
        if record.record_kind != "bucket_metadata" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

#[derive(Debug, Clone, Copy)]
enum BucketJournalScope {
    Tenant(i64),
    Global,
}

impl BucketJournalScope {
    fn stream_id(self) -> String {
        match self {
            Self::Tenant(tenant_id) => format!("bucket_metadata:tenant:{tenant_id}"),
            Self::Global => "bucket_metadata:global".to_string(),
        }
    }

    fn partition_id(self) -> Hash32 {
        match self {
            Self::Tenant(tenant_id) => tenant_bucket_partition_id(tenant_id),
            Self::Global => global_bucket_partition_id(),
        }
    }

    fn partition_principal(self) -> String {
        match self {
            Self::Tenant(tenant_id) => {
                format!("partition-owner:bucket_metadata:tenant:{tenant_id}")
            }
            Self::Global => "partition-owner:bucket_metadata:global".to_string(),
        }
    }
}

pub fn tenant_bucket_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket_metadata").as_bytes())
}

pub fn global_bucket_partition_id() -> Hash32 {
    hash32(b"bucket_metadata/global")
}

#[cfg(test)]
pub(crate) async fn read_bucket_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
) -> Result<(Vec<u64>, Vec<u64>)> {
    let tenant = read_bucket_journal_frames(storage, BucketJournalScope::Tenant(tenant_id))
        .await?
        .into_iter()
        .map(|frame| frame.fence_token)
        .collect();
    let global = read_bucket_journal_frames(storage, BucketJournalScope::Global)
        .await?
        .into_iter()
        .map(|frame| frame.fence_token)
        .collect();
    Ok((tenant, global))
}

fn require_bucket_scope_permit(
    scope: BucketJournalScope,
    permit: &PartitionWritePermit,
) -> Result<()> {
    if permit.partition_family != "bucket_metadata"
        || permit.partition_id != hex::encode(scope.partition_id())
    {
        return Err(anyhow!(
            "partition write permit does not target this bucket metadata partition"
        ));
    }
    Ok(())
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
        mutation_id: uuid::Uuid::parse_str(&body.mutation_id)?,
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
        "bucket_id": body.bucket_id,
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
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use chrono::Utc;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"bucket metadata partition owner signing key";

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

    async fn ready_bucket_permit(
        storage: &Storage,
        scope: BucketJournalScope,
        owner_node_id: &str,
    ) -> PartitionWritePermit {
        let request = PartitionRecoveryAcquire {
            partition_family: "bucket_metadata".to_string(),
            partition_id: hex::encode(scope.partition_id()),
            owner_node_id: owner_node_id.to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: 100,
        };
        let recovering = acquire_partition_recovery(storage, request, PARTITION_OWNER_KEY)
            .await
            .unwrap();
        publish_partition_ready(
            storage,
            &recovering.partition_family,
            &recovering.partition_id,
            owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([2; 32]),
            200,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap()
        .write_permit()
        .unwrap()
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

    #[tokio::test]
    async fn bucket_journal_permits_set_tenant_and_global_frame_fences() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = bucket(1, "fenced-bucket", false);
        let tenant_permit = ready_bucket_permit(
            &storage,
            BucketJournalScope::Tenant(bucket.tenant_id),
            "node-a",
        )
        .await;
        let global_permit =
            ready_bucket_permit(&storage, BucketJournalScope::Global, "node-a").await;

        append_bucket_mutation_with_permits(
            &storage,
            &bucket,
            BucketJournalMutation::Create,
            &tenant_permit,
            &global_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();

        let tenant_frames =
            read_bucket_journal_frames(&storage, BucketJournalScope::Tenant(bucket.tenant_id))
                .await
                .unwrap();
        assert_eq!(tenant_frames.len(), 1);
        assert_eq!(tenant_frames[0].fence_token, tenant_permit.fence_token);

        let global_frames = read_bucket_journal_frames(&storage, BucketJournalScope::Global)
            .await
            .unwrap();
        assert_eq!(global_frames.len(), 1);
        assert_eq!(global_frames[0].fence_token, global_permit.fence_token);
    }

    #[tokio::test]
    async fn bucket_journal_rejects_stale_scope_permit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = bucket(1, "stale-bucket", false);
        let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
        let stale_tenant = ready_bucket_permit(&storage, tenant_scope, "node-a").await;
        let fresh_tenant = ready_bucket_permit(&storage, tenant_scope, "node-b").await;
        let global_permit =
            ready_bucket_permit(&storage, BucketJournalScope::Global, "node-b").await;
        assert_eq!(fresh_tenant.fence_token, stale_tenant.fence_token + 1);

        let rejected = append_bucket_mutation_with_permits(
            &storage,
            &bucket,
            BucketJournalMutation::Create,
            &stale_tenant,
            &global_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(rejected.to_string().contains("PartitionNotOwned"));

        append_bucket_mutation_with_permits(
            &storage,
            &bucket,
            BucketJournalMutation::Create,
            &fresh_tenant,
            &global_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn bucket_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = bucket(1, "stale-precondition-bucket", false);
        let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
        let stale_tenant = ready_bucket_permit(&storage, tenant_scope, "node-a").await;
        let stale_precondition =
            partition_write_ref_precondition(&storage, &stale_tenant, PARTITION_OWNER_KEY)
                .await
                .unwrap();
        let fresh_tenant = ready_bucket_permit(&storage, tenant_scope, "node-b").await;
        assert_eq!(fresh_tenant.fence_token, stale_tenant.fence_token + 1);

        let rejected = append_bucket_mutation_to_stream(
            &storage,
            &bucket,
            BucketJournalMutation::Create,
            tenant_scope,
            stale_tenant.fence_token,
            Some(stale_precondition),
        )
        .await
        .unwrap_err();
        assert!(
            rejected.to_string().contains("target mismatch")
                || rejected.to_string().contains("generation mismatch"),
            "unexpected error: {rejected:?}"
        );
    }
}
