use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::persistence::{Bucket, IndexDefinition, IndexDefinitionEvent};
use crate::storage::Storage;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize)]
struct IndexJournalHeader<'a> {
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
struct IndexEventBody {
    cursor: i64,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    index_id: i64,
    index_name: String,
    event_type: String,
    index_version: i64,
    definition: JsonValue,
    created_at: String,
}

pub async fn append_index_definition_event(
    storage: &Storage,
    event: &IndexDefinitionEvent,
) -> Result<()> {
    let path = storage.index_definition_journal_path(event.tenant_id, event.bucket_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path, event.tenant_id, event.bucket_id).await?;

    let previous = read_index_journal_frames_at_path(path.as_path())
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
    let body = serde_json::to_vec(&IndexEventBody {
        cursor: event.id,
        tenant_id: event.tenant_id,
        bucket_id: event.bucket_id,
        bucket_name: event.bucket_name.clone(),
        index_id: event.index_id,
        index_name: event.index_name.clone(),
        event_type: event.event_type.clone(),
        index_version: event.index_version,
        definition: event.definition.clone(),
        created_at: event.created_at.to_rfc3339(),
    })?;
    let frame = JournalFrame::new(
        JournalRecordKind::IndexDefinition,
        sequence,
        0,
        *mutation_id.as_bytes(),
        index_key_hash(event.tenant_id, event.bucket_id, &event.index_name),
        previous_hash,
        body,
    );

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open index definition journal {}", path.display()))?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

pub async fn write_index_definition_event(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    event_type: &str,
) -> Result<IndexDefinitionEvent> {
    let cursor = read_all_index_definition_events(storage, bucket.tenant_id, bucket.id)
        .await?
        .into_iter()
        .map(|event| event.id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("index definition cursor overflow"))?;
    let event = IndexDefinitionEvent {
        id: cursor,
        tenant_id: bucket.tenant_id,
        bucket_id: bucket.id,
        bucket_name: bucket.name.clone(),
        index_id: index.id,
        index_name: index.name.clone(),
        event_type: event_type.to_string(),
        index_version: index.version,
        definition: index_definition_json(&bucket.name, index),
        created_at: chrono::Utc::now(),
    };
    append_index_definition_event(storage, &event).await?;
    Ok(event)
}

pub async fn read_index_definition_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<IndexDefinitionEvent>> {
    let mut events = read_all_index_definition_events(storage, tenant_id, bucket_id).await?;
    events.retain(|event| event.id > after_cursor);
    events.sort_by_key(|event| event.id);
    if limit > 0 && events.len() > limit {
        events.truncate(limit);
    }
    Ok(events)
}

pub async fn read_current_index_definition_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    include_disabled: bool,
) -> Result<Vec<IndexDefinitionEvent>> {
    let mut current = std::collections::BTreeMap::<String, IndexDefinitionEvent>::new();
    for event in read_all_index_definition_events(storage, tenant_id, bucket_id).await? {
        if event.event_type == "drop" {
            current.remove(&event.index_name);
            continue;
        }
        current.insert(event.index_name.clone(), event);
    }
    let mut events: Vec<_> = current.into_values().collect();
    if !include_disabled {
        events.retain(|event| {
            event
                .definition
                .get("enabled")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false)
        });
    }
    events.sort_by(|left, right| left.index_name.cmp(&right.index_name));
    Ok(events)
}

pub async fn read_current_index_definitions(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    include_disabled: bool,
) -> Result<Vec<IndexDefinition>> {
    read_current_index_definition_events(storage, tenant_id, bucket_id, include_disabled)
        .await?
        .into_iter()
        .map(|event| index_definition_from_event(&event))
        .collect()
}

pub async fn read_current_index_definition(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    name: &str,
) -> Result<Option<IndexDefinition>> {
    Ok(
        read_current_index_definitions(storage, tenant_id, bucket_id, true)
            .await?
            .into_iter()
            .find(|index| index.name == name),
    )
}

pub async fn next_index_definition_id(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    read_all_index_definition_events(storage, tenant_id, bucket_id)
        .await?
        .into_iter()
        .map(|event| event.index_id)
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("index definition id overflow"))
}

pub fn index_storage_id(tenant_id: i64, bucket_id: i64, index_id: i64) -> String {
    format!("tenant-{tenant_id}-bucket-{bucket_id}-index-{index_id}")
}

async fn read_all_index_definition_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<IndexDefinitionEvent>> {
    let frames = read_index_journal_frames_at_path(
        &storage.index_definition_journal_path(tenant_id, bucket_id),
    )
    .await?;
    let mut events = Vec::new();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::IndexDefinition {
            continue;
        }
        let body: IndexEventBody = serde_json::from_slice(&frame.body)?;
        events.push(IndexDefinitionEvent {
            id: body.cursor,
            tenant_id: body.tenant_id,
            bucket_id: body.bucket_id,
            bucket_name: body.bucket_name,
            index_id: body.index_id,
            index_name: body.index_name,
            event_type: body.event_type,
            index_version: body.index_version,
            definition: body.definition,
            created_at: chrono::DateTime::parse_from_rfc3339(&body.created_at)?
                .with_timezone(&chrono::Utc),
        });
    }
    Ok(events)
}

async fn read_index_journal_frames_at_path(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read index definition journal {}", path.display()))?;
    decode_journal_file(&bytes)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("index definition journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated index definition journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow::anyhow!("invalid index definition journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated index definition journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

async fn ensure_journal_header(path: &Path, tenant_id: i64, bucket_id: i64) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = chrono::Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&IndexJournalHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: bucket_id.to_string(),
        partition_family: "index_definition",
        partition_id: hex::encode(index_partition_id(tenant_id, bucket_id)),
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
        .with_context(|| format!("create index definition journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

fn index_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition").as_bytes())
}

fn index_key_hash(tenant_id: i64, bucket_id: i64, index_name: &str) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index/{index_name}").as_bytes())
}

fn index_definition_json(bucket_name: &str, index: &IndexDefinition) -> JsonValue {
    json!({
        "index_id": index.id,
        "bucket_name": bucket_name,
        "name": index.name,
        "kind": index.kind,
        "selector_json": index.selector.to_string(),
        "extractor_json": index.extractor.to_string(),
        "authorization_mode": index.authorization_mode,
        "build_policy_json": index.build_policy.to_string(),
        "enabled": index.enabled,
        "version": index.version,
        "created_at": index.created_at.to_rfc3339(),
        "updated_at": index.updated_at.to_rfc3339(),
    })
}

fn index_definition_from_event(event: &IndexDefinitionEvent) -> Result<IndexDefinition> {
    let definition = &event.definition;
    let field = |name: &'static str| -> Result<&JsonValue> {
        definition
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("index definition missing {name}"))
    };
    let string_field = |name: &'static str| -> Result<String> {
        field(name)?
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("index definition field {name} is not a string"))
    };
    let json_string_field = |name: &'static str| -> Result<JsonValue> {
        let raw = string_field(name)?;
        serde_json::from_str(&raw)
            .with_context(|| format!("parse index definition JSON field {name}"))
    };
    Ok(IndexDefinition {
        id: field("index_id")?
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("index_id is not an integer"))?,
        tenant_id: event.tenant_id,
        bucket_id: event.bucket_id,
        name: string_field("name")?,
        kind: string_field("kind")?,
        selector: json_string_field("selector_json")?,
        extractor: json_string_field("extractor_json")?,
        authorization_mode: string_field("authorization_mode")?,
        build_policy: json_string_field("build_policy_json")?,
        enabled: field("enabled")?
            .as_bool()
            .ok_or_else(|| anyhow::anyhow!("enabled is not a bool"))?,
        version: field("version")?
            .as_i64()
            .ok_or_else(|| anyhow::anyhow!("version is not an integer"))?,
        created_at: parse_definition_time(definition.get("created_at"), event.created_at)?,
        updated_at: parse_definition_time(definition.get("updated_at"), event.created_at)?,
    })
}

fn parse_definition_time(
    value: Option<&JsonValue>,
    fallback: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>> {
    let Some(value) = value.and_then(JsonValue::as_str) else {
        return Ok(fallback);
    };
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&chrono::Utc))
        .or_else(|_| {
            chrono::DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S %Z")
                .map(|value| value.with_timezone(&chrono::Utc))
        })
        .or(Ok(fallback))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use tempfile::tempdir;

    fn event(cursor: i64, name: &str, event_type: &str, enabled: bool) -> IndexDefinitionEvent {
        IndexDefinitionEvent {
            id: cursor,
            tenant_id: 42,
            bucket_id: 7,
            bucket_name: "docs".to_string(),
            index_id: 100,
            index_name: name.to_string(),
            event_type: event_type.to_string(),
            index_version: cursor,
            definition: json!({
                "index_id": 100,
                "bucket_name": "docs",
                "name": name,
                "kind": "full_text",
                "selector_json": "{}",
                "extractor_json": "{}",
                "authorization_mode": "object_acl",
                "build_policy_json": "{}",
                "enabled": enabled,
                "version": cursor,
                "created_at": "2026-01-01 00:00:00 UTC",
                "updated_at": "2026-01-01 00:00:00 UTC",
            }),
            created_at: Utc::now(),
        }
    }

    fn bucket() -> Bucket {
        Bucket {
            id: 7,
            tenant_id: 42,
            name: "docs".to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        }
    }

    fn index(version: i64, enabled: bool) -> IndexDefinition {
        IndexDefinition {
            id: 100,
            tenant_id: 42,
            bucket_id: 7,
            name: "body".to_string(),
            kind: "full_text".to_string(),
            selector: json!({"prefix": "docs/"}),
            extractor: json!({"field": "body"}),
            authorization_mode: "inherit_object".to_string(),
            build_policy: json!({}),
            enabled,
            version,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn index_journal_recovers_events_and_current_definitions() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        append_index_definition_event(&storage, &event(1, "body", "create", true))
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(2, "body", "update", true))
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(3, "title", "create", true))
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(4, "body", "disable", false))
            .await
            .unwrap();
        append_index_definition_event(&storage, &event(5, "title", "drop", true))
            .await
            .unwrap();

        let events = read_index_definition_events(&storage, 42, 7, 2, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].id, 3);

        let active = read_current_index_definition_events(&storage, 42, 7, false)
            .await
            .unwrap();
        assert!(active.is_empty());

        let with_disabled = read_current_index_definition_events(&storage, 42, 7, true)
            .await
            .unwrap();
        assert_eq!(with_disabled.len(), 1);
        assert_eq!(with_disabled[0].index_name, "body");
        assert_eq!(with_disabled[0].event_type, "disable");
    }

    #[tokio::test]
    async fn index_journal_allocates_native_event_cursors() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = bucket();
        let first = write_index_definition_event(&storage, &bucket, &index(1, true), "create")
            .await
            .unwrap();
        let second = write_index_definition_event(&storage, &bucket, &index(2, false), "disable")
            .await
            .unwrap();

        assert_eq!(first.id, 1);
        assert_eq!(second.id, 2);
        let events = read_index_definition_events(&storage, 42, 7, 0, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[1].event_type, "disable");
        assert_eq!(events[1].definition["enabled"].as_bool(), Some(false));
    }
}
