use crate::anvil_api::{ModelManifest, TensorIndexRow};
use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize)]
struct ModelJournalHeader<'a> {
    partition_family: &'static str,
    partition_id: &'static str,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

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

pub async fn create_model_artifact(
    storage: &Storage,
    artifact_id: &str,
    bucket_id: i64,
    key: &str,
    manifest: &ModelManifest,
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
    )
    .await
}

pub async fn create_model_tensors(
    storage: &Storage,
    artifact_id: &str,
    tensors: &[TensorIndexRow],
) -> Result<()> {
    require_nonempty(artifact_id, "artifact_id")?;
    append_model_event(
        storage,
        ModelEventBody::TensorsReplace {
            artifact_id: artifact_id.to_string(),
            tensors: tensors.to_vec(),
        },
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
    let frames = read_model_journal_frames_at_path(&storage.model_metadata_journal_path()).await?;
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

async fn append_model_event(storage: &Storage, event: ModelEventBody) -> Result<()> {
    let path = storage.model_metadata_journal_path();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path).await?;
    let previous = read_model_journal_frames_at_path(path.as_path())
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
        0,
        *mutation_id.as_bytes(),
        key_hash,
        previous_hash,
        serde_json::to_vec(&event)?,
    );
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open model metadata journal {}", path.display()))?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn ensure_journal_header(path: &Path) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = chrono::Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&ModelJournalHeader {
        partition_family: "model_metadata",
        partition_id: "global",
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
        .with_context(|| format!("create model metadata journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn read_model_journal_frames_at_path(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read model metadata journal {}", path.display()))?;
    decode_journal_file(&bytes)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("model metadata journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated model metadata journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("invalid model metadata journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated model metadata journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
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
}
