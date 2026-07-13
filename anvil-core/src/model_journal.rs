use crate::anvil_api::{ModelManifest, TensorIndexRow};
use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use prost::{Message, Oneof};
use std::collections::BTreeMap;

const MODEL_METADATA_BODY_SCHEMA: &str = "anvil.core.model_metadata.v1";

#[derive(Debug, Clone)]
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

#[derive(Clone, PartialEq, Message)]
struct ModelEventBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
    #[prost(string, tag = "3")]
    mutation_id: String,
    #[prost(oneof = "model_event_body_proto::Event", tags = "10, 11")]
    event: Option<model_event_body_proto::Event>,
}

mod model_event_body_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Event {
        #[prost(message, tag = "10")]
        ArtifactUpsert(super::ModelArtifactUpsertProto),
        #[prost(message, tag = "11")]
        TensorsReplace(super::ModelTensorsReplaceProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ModelArtifactUpsertProto {
    #[prost(string, tag = "1")]
    artifact_id: String,
    #[prost(int64, tag = "2")]
    bucket_id: i64,
    #[prost(string, tag = "3")]
    key: String,
    #[prost(message, optional, tag = "4")]
    manifest: Option<ModelManifest>,
}

#[derive(Clone, PartialEq, Message)]
struct ModelTensorsReplaceProto {
    #[prost(string, tag = "1")]
    artifact_id: String,
    #[prost(message, repeated, tag = "2")]
    tensors: Vec<TensorIndexRow>,
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
    let events = read_model_events(storage).await?;
    let mut state = ModelState::default();
    for event in events {
        match event {
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
    let mutation_id = uuid::Uuid::new_v4();
    let payload = encode_model_event_body(&event, fence_token, mutation_id)?;
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
                payload,
                idempotency_key: Some(format!("model-metadata:{mutation_id}")),
            }],
        })
        .await?;
    Ok(())
}

async fn read_model_events(storage: &Storage) -> Result<Vec<ModelEventBody>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = core_store
        .read_stream(ReadStream {
            stream_id: model_metadata_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    records
        .into_iter()
        .filter(|record| record.record_kind == "model_metadata")
        .map(|record| decode_model_event_body(&record.payload))
        .collect()
}

pub fn model_partition_id() -> Hash32 {
    hash32(b"model_metadata/global")
}

fn encode_model_event_body(
    event: &ModelEventBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let proto = match event {
        ModelEventBody::ArtifactUpsert {
            artifact_id,
            bucket_id,
            key,
            manifest,
        } => ModelEventBodyProto {
            schema: MODEL_METADATA_BODY_SCHEMA.to_string(),
            fence_token,
            mutation_id: mutation_id.to_string(),
            event: Some(model_event_body_proto::Event::ArtifactUpsert(
                ModelArtifactUpsertProto {
                    artifact_id: artifact_id.clone(),
                    bucket_id: *bucket_id,
                    key: key.clone(),
                    manifest: Some(manifest.clone()),
                },
            )),
        },
        ModelEventBody::TensorsReplace {
            artifact_id,
            tensors,
        } => ModelEventBodyProto {
            schema: MODEL_METADATA_BODY_SCHEMA.to_string(),
            fence_token,
            mutation_id: mutation_id.to_string(),
            event: Some(model_event_body_proto::Event::TensorsReplace(
                ModelTensorsReplaceProto {
                    artifact_id: artifact_id.clone(),
                    tensors: tensors.clone(),
                },
            )),
        },
    };
    encode_deterministic_proto(&proto)
}

fn decode_model_event_body(bytes: &[u8]) -> Result<ModelEventBody> {
    let proto = ModelEventBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "model metadata body")?;
    if proto.schema != MODEL_METADATA_BODY_SCHEMA {
        return Err(anyhow!("model metadata body has invalid schema"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("model metadata body has invalid mutation id"))?;
    Ok(
        match proto
            .event
            .ok_or_else(|| anyhow!("model metadata body is missing event"))?
        {
            model_event_body_proto::Event::ArtifactUpsert(value) => {
                ModelEventBody::ArtifactUpsert {
                    artifact_id: value.artifact_id,
                    bucket_id: value.bucket_id,
                    key: value.key,
                    manifest: value.manifest.ok_or_else(|| {
                        anyhow!("model metadata artifact body is missing manifest")
                    })?,
                }
            }
            model_event_body_proto::Event::TensorsReplace(value) => {
                ModelEventBody::TensorsReplace {
                    artifact_id: value.artifact_id,
                    tensors: value.tensors,
                }
            }
        },
    )
}

#[cfg(test)]
fn decode_model_event_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = ModelEventBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "model metadata body")?;
    if proto.schema != MODEL_METADATA_BODY_SCHEMA {
        return Err(anyhow!("model metadata body has invalid schema"));
    }
    Ok(proto.fence_token)
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

fn model_metadata_stream_id() -> String {
    "model_metadata:global".to_string()
}

fn model_partition_principal() -> String {
    "partition-owner:model_metadata:global".to_string()
}

#[cfg(test)]
pub(crate) async fn read_model_frame_fences_for_test(storage: &Storage) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(ReadStream {
            stream_id: model_metadata_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?
        .into_iter()
        .filter(|record| record.record_kind == "model_metadata")
        .map(|record| decode_model_event_body_fence(&record.payload))
        .collect::<Result<Vec<_>>>()?)
}

async fn model_write_precondition(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    require_model_permit(permit)?;
    Ok(partition_write_precondition(storage, permit, partition_owner_signing_key).await?)
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

        let fences = read_model_frame_fences_for_test(&storage).await.unwrap();
        assert_eq!(fences.len(), 2);
        assert!(fences.iter().all(|fence| *fence == permit.fence_token));
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
        let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
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

        CoreStore::new(storage.clone())
            .await
            .unwrap()
            .corrupt_stream_record_payload_for_test(&model_metadata_stream_id(), 1)
            .unwrap();

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
        crate::partition_fence::ready_partition_owner_for_test(
            storage,
            family,
            id,
            owner_node_id,
            0,
            hex::encode([0; 32]),
            hex::encode([1; 32]),
            KEY,
        )
        .await
    }
}
