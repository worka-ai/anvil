use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
};
use crate::formats::{Hash32, JournalFrame, JournalRecordKind, hash32, validate_journal_chain};
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
use crate::persistence::{ManifestCasResult, MetadataMutationReceipt};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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

#[cfg(test)]
async fn compare_and_swap_manifest(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
) -> Result<Option<ManifestCasResult>> {
    compare_and_swap_manifest_inner(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        0,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compare_and_swap_manifest_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<Option<ManifestCasResult>> {
    require_manifest_cas_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    compare_and_swap_manifest_inner(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn compare_and_swap_manifest_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<Option<ManifestCasResult>> {
    let current = current_revision(storage, tenant_id, bucket_id, object_key).await?;
    if expected_revision != current {
        return Ok(None);
    }
    let revision = current
        .checked_add(1)
        .ok_or_else(|| anyhow!("manifest revision overflow"))?;
    let receipt = append_manifest(
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
        fence_token,
        partition_precondition,
    )
    .await?;
    Ok(Some(ManifestCasResult {
        revision,
        manifest_hash: manifest_hash.to_string(),
        receipt,
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

async fn append_manifest(
    storage: &Storage,
    body: ManifestBody,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = manifest_cas_stream_id(body.tenant_id, body.bucket_id);
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
    let body_bytes = serde_json::to_vec(&body)?;
    let payload_hash = hex::encode(hash32(&body_bytes));
    let frame = JournalFrame::new(
        JournalRecordKind::ManifestCas,
        sequence,
        fence_token,
        *mutation_id.as_bytes(),
        hash32(
            format!(
                "tenant/{}/bucket/{}/manifest/{}",
                body.tenant_id, body.bucket_id, body.object_key
            )
            .as_bytes(),
        ),
        previous_hash,
        body_bytes,
    );
    let receipt = MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: hex::encode(frame.record_hash),
        watch_cursor: frame.partition_sequence,
    };
    let partition_id = hex::encode(manifest_cas_partition_id(body.tenant_id, body.bucket_id));
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "manifest-cas:{}:{}:{mutation_id}",
                body.tenant_id, body.bucket_id
            ),
            scope_partition: partition_id.clone(),
            committed_by_principal: manifest_cas_partition_principal(
                body.tenant_id,
                body.bucket_id,
            ),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind: "manifest_cas".to_string(),
                payload: frame.encode(),
                idempotency_key: Some(format!(
                    "manifest-cas:{}:{}:{mutation_id}",
                    body.tenant_id, body.bucket_id
                )),
            }],
        })
        .await?;
    Ok(receipt)
}

async fn read_manifest_bodies(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<ManifestBody>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let frames = read_frames(&core_store, &manifest_cas_stream_id(tenant_id, bucket_id)).await?;
    frames
        .into_iter()
        .filter(|frame| frame.record_kind == JournalRecordKind::ManifestCas)
        .map(|frame| serde_json::from_slice(&frame.body).map_err(Into::into))
        .collect()
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
        if record.record_kind != "manifest_cas" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

pub fn manifest_cas_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas").as_bytes())
}

fn manifest_cas_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("manifest_cas:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn manifest_cas_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:manifest_cas:{tenant_id}:{bucket_id}")
}

#[cfg(test)]
pub(crate) async fn read_manifest_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(
        read_frames(&core_store, &manifest_cas_stream_id(tenant_id, bucket_id))
            .await?
            .into_iter()
            .map(|frame| frame.fence_token)
            .collect(),
    )
}

fn require_manifest_cas_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id = hex::encode(manifest_cas_partition_id(tenant_id, bucket_id));
    if permit.partition_family != "manifest_cas" || permit.partition_id != expected_partition_id {
        anyhow::bail!("manifest CAS write permit targets a different partition");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use serde_json::json;
    use tempfile::tempdir;

    const KEY: &[u8] = b"manifest journal partition owner key";

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

    #[tokio::test]
    pub(crate) async fn manifest_cas_with_permit_writes_fenced_frame_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let permit = owner.write_permit().unwrap();

        let result = compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            &permit,
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.revision, 1);

        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let frames = read_frames(&core_store, &manifest_cas_stream_id(1, 2))
            .await
            .unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].fence_token, permit.fence_token);
    }

    #[tokio::test]
    pub(crate) async fn manifest_cas_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
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
    pub(crate) async fn manifest_cas_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_ref_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = compare_and_swap_manifest_inner(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
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

        compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            &newer.write_permit().unwrap(),
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn ready_owner(
        storage: &Storage,
        tenant_id: i64,
        bucket_id: i64,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "manifest_cas".to_string();
        let id = hex::encode(manifest_cas_partition_id(tenant_id, bucket_id));
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
