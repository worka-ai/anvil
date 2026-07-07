use crate::anvil_api::{ModelManifest, TensorIndexRow};
use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
};
use crate::formats::{Hash32, JournalFrame, JournalRecordKind, hash32, validate_journal_chain};
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum ModelEventBody {
    ArtifactUpsert {
        artifact_id: String,
        bucket_id: i64,
        key: String,
        manifest: ModelManifest,
    },
    TensorsReplace {
        artifact_id: String,
        tensors: Vec<TensorIndexRow>,
    },
}

#[derive(Debug, Clone, Default)]
struct ModelState {
    artifacts: BTreeMap<String, ModelManifest>,
    tensors: BTreeMap<String, Vec<TensorIndexRow>>,
}

#[cfg(test)]
async fn create_model_artifact(
    storage: &Storage,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
) -> Result<()> {
    create_model_artifact_inner(storage, artifact_id, bucket_id, key, manifest, 0, None).await
}

pub(crate) async fn create_model_artifact_with_permit(
    storage: &Storage,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let partition_precondition =
        model_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_model_artifact_inner(
        storage,
        artifact_id,
        bucket_id,
        key,
        manifest,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn create_model_artifact_inner(
    storage: &Storage,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    require_nonempty(artifact_id, "artifact_id")?;
    require_nonempty(key, "model key")?;
    append_model_event(
        storage,
        ModelEventBody::ArtifactUpsert {
            artifact_id: artifact_id.to_string(),
            bucket_id,
            key: key.to_string(),
            manifest: manifest.clone(),
        },
        fence_token,
        partition_precondition,
    )
    .await
}

#[cfg(test)]
async fn create_model_tensors(
    storage: &Storage,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
) -> Result<()> {
    create_model_tensors_inner(storage, artifact_id, tensors, 0, None).await
}

pub(crate) async fn create_model_tensors_with_permit(
    storage: &Storage,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let partition_precondition =
        model_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_model_tensors_inner(
        storage,
        artifact_id,
        tensors,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn create_model_tensors_inner(
    storage: &Storage,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    require_nonempty(artifact_id, "artifact_id")?;
    append_model_event(
        storage,
        ModelEventBody::TensorsReplace {
            artifact_id: artifact_id.to_string(),
            tensors: tensors.to_vec(),
        },
        fence_token,
        partition_precondition,
    )
    .await
}

pub async fn list_tensors(
    storage: &Storage,
    artifact_id: &str,
    limit: i64,
    offset: i64,
) -> Result<Vec<TensorIndexRow>> {
    let mut tensors = read_model_state(storage)
        .await?
        .tensors
        .remove(artifact_id)
        .unwrap_or_default();
    tensors.sort_by(|a, b| a.tensor_name.cmp(&b.tensor_name));
    Ok(tensors
        .into_iter()
        .skip(offset.max(0) as usize)
        .take(limit.max(0) as usize)
        .collect())
}

pub async fn get_tensor_metadata(
    storage: &Storage,
    artifact_id: &str,
    tensor_name: &str,
) -> Result<Option<TensorIndexRow>> {
    Ok(read_model_state(storage)
        .await?
        .tensors
        .get(artifact_id)
        .and_then(|rows| rows.iter().find(|row| row.tensor_name == tensor_name))
        .cloned())
}

pub async fn get_model_artifact(
    storage: &Storage,
    artifact_id: &str,
) -> Result<Option<ModelManifest>> {
    Ok(read_model_state(storage)
        .await?
        .artifacts
        .get(artifact_id)
        .cloned())
}

async fn read_model_state(storage: &Storage) -> Result<ModelState> {
    let frames = read_model_journal_frames(storage).await?;
    let mut state = ModelState::default();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::ModelMetadata {
            continue;
        }
        let body: ModelEventBody = serde_json::from_slice(&frame.body)?;
        match body {
            ModelEventBody::ArtifactUpsert {
                artifact_id,
                manifest,
                ..
            } => {
                state.artifacts.insert(artifact_id, manifest);
            }
            ModelEventBody::TensorsReplace {
                artifact_id,
                tensors,
            } => {
                state.tensors.insert(artifact_id, tensors);
            }
        }
    }
    Ok(state)
}

async fn append_model_event(
    storage: &Storage,
    event: ModelEventBody,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let previous = read_model_journal_frames_from_store(&core_store)
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
    let key_hash = event_key_hash(&event);
    let frame = JournalFrame::new(
        JournalRecordKind::ModelMetadata,
        sequence,
        fence_token,
        *mutation_id.as_bytes(),
        key_hash,
        previous_hash,
        serde_json::to_vec(&event)?,
    );
    let partition_id = hex::encode(model_partition_id());
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("model-metadata:{mutation_id}"),
            scope_partition: partition_id.clone(),
            committed_by_principal: model_partition_principal(),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id: model_metadata_stream_id(),
                record_kind: "model_metadata".to_string(),
                payload: frame.encode(),
                idempotency_key: Some(format!("model-metadata:{mutation_id}")),
            }],
        })
        .await?;
    Ok(())
}

async fn read_model_journal_frames(storage: &Storage) -> Result<Vec<JournalFrame>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_model_journal_frames_from_store(&core_store).await
}

async fn read_model_journal_frames_from_store(core_store: &CoreStore) -> Result<Vec<JournalFrame>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: model_metadata_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut frames = Vec::new();
    for record in records {
        if record.record_kind != "model_metadata" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

fn event_key_hash(event: &ModelEventBody) -> Hash32 {
    let artifact_id = match event {
        ModelEventBody::ArtifactUpsert { artifact_id, .. }
        | ModelEventBody::TensorsReplace { artifact_id, .. } => artifact_id,
    };
    hash32(format!("model\0{artifact_id}").as_bytes())
}

pub fn model_partition_id() -> Hash32 {
    hash32(b"model_metadata/global")
}

fn model_metadata_stream_id() -> String {
    "model_metadata:global".to_string()
}

fn model_partition_principal() -> String {
    "partition-owner:model_metadata:global".to_string()
}

#[cfg(test)]
pub(crate) async fn read_model_frame_fences_for_test(storage: &Storage) -> Result<Vec<u64>> {
    Ok(read_model_journal_frames(storage)
        .await?
        .into_iter()
        .map(|frame| frame.fence_token)
        .collect())
}

async fn model_write_precondition(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    require_model_permit(permit)?;
    Ok(partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?)
}

fn require_model_permit(permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "model_metadata"
        || permit.partition_id != hex::encode(model_partition_id())
    {
        anyhow::bail!("model metadata write permit targets a different partition");
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
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

    const KEY: &[u8] = b"model metadata partition owner key";

    fn manifest(base: &str) -> ModelManifest {
        ModelManifest {
            schema_version: "1".to_string(),
            artifact_id: "artifact-a".to_string(),
            name: "artifact-a".to_string(),
            format: "test".to_string(),
            components: Vec::new(),
            base_artifact_id: base.to_string(),
            delta_artifact_ids: Vec::new(),
            signatures: Vec::new(),
            merkle_root: "abc".to_string(),
            meta: std::collections::HashMap::new(),
        }
    }

    fn tensor(name: &str) -> TensorIndexRow {
        TensorIndexRow {
            tensor_name: name.to_string(),
            file_path: "weights.bin".to_string(),
            file_offset: 0,
            byte_length: 4,
            dtype: 3,
            shape: vec![1],
            layout: "row_major".to_string(),
            block_bytes: 4,
            blocks: Vec::new(),
        }
    }

    #[tokio::test]
    async fn model_journal_replays_artifacts_and_tensors() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        create_model_artifact(&storage, "artifact-a", 1, "models/a", &manifest(""))
            .await
            .unwrap();
        create_model_tensors(&storage, "artifact-a", &[tensor("z"), tensor("a")])
            .await
            .unwrap();

        assert!(
            get_model_artifact(&storage, "artifact-a")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            list_tensors(&storage, "artifact-a", 10, 0)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.tensor_name)
                .collect::<Vec<_>>(),
            vec!["a".to_string(), "z".to_string()]
        );
        assert_eq!(
            get_tensor_metadata(&storage, "artifact-a", "z")
                .await
                .unwrap()
                .unwrap()
                .tensor_name,
            "z"
        );
    }

    #[tokio::test]
    pub(crate) async fn model_journal_with_permit_writes_fenced_frames_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();

        create_model_artifact_with_permit(
            &storage,
            "artifact-a",
            1,
            "models/a",
            &manifest(""),
            &permit,
            KEY,
        )
        .await
        .unwrap();
        create_model_tensors_with_permit(&storage, "artifact-a", &[tensor("z")], &permit, KEY)
            .await
            .unwrap();

        let frames = read_model_journal_frames(&storage).await.unwrap();
        assert_eq!(frames.len(), 2);
        assert!(
            frames
                .iter()
                .all(|frame| frame.fence_token == permit.fence_token)
        );
    }

    #[tokio::test]
    pub(crate) async fn model_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_model_artifact_with_permit(
            &storage,
            "artifact-a",
            1,
            "models/a",
            &manifest(""),
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
    pub(crate) async fn model_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_ref_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_model_artifact_inner(
            &storage,
            "artifact-a",
            1,
            "models/a",
            &manifest(""),
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

        create_model_artifact_with_permit(
            &storage,
            "artifact-a",
            1,
            "models/a",
            &manifest(""),
            &newer.write_permit().unwrap(),
            KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn model_journal_reader_fails_closed_on_tampered_frame() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        create_model_artifact(&storage, "artifact-a", 1, "models/a", &manifest(""))
            .await
            .unwrap();

        for path in core_stream_paths_for_test(&storage, &model_metadata_stream_id()) {
            let mut bytes = tokio::fs::read(&path).await.unwrap();
            let body_start = bytes
                .iter()
                .position(|byte| *byte != b'\n')
                .expect("stream has bytes");
            bytes[body_start] ^= 0x55;
            tokio::fs::write(&path, bytes).await.unwrap();
        }

        let err = get_model_artifact(&storage, "artifact-a")
            .await
            .expect_err("tampered model journal must not replay partial state");
        assert!(!err.to_string().is_empty());
    }

    async fn ready_owner(
        storage: &Storage,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "model_metadata".to_string();
        let id = hex::encode(model_partition_id());
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

    fn core_stream_paths_for_test(storage: &Storage, stream_id: &str) -> Vec<std::path::PathBuf> {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(stream_id.as_bytes());
        let file_name = format!("{}.anstream", hex::encode(hasher.finalize()));
        (1..=3)
            .map(|index| {
                storage
                    .core_store_replica_path(&format!("local-control-node-{index}"))
                    .join("streams")
                    .join("data")
                    .join(&file_name)
            })
            .collect()
    }
}
