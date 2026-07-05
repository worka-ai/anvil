use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
};
use crate::formats::{Hash32, JournalFrame, JournalRecordKind, hash32, validate_journal_chain};
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
#[cfg(test)]
use crate::persistence::Bucket;
use crate::persistence::{IndexDefinition, IndexDefinitionEvent};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
#[cfg(test)]
use serde_json::json;

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

#[cfg(test)]
async fn append_index_definition_event(
    storage: &Storage,
    event: &IndexDefinitionEvent,
) -> Result<()> {
    append_index_definition_event_inner(storage, event, 0, None).await
}

pub(crate) async fn append_index_definition_event_with_permit(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_index_definition_permit(event.tenant_id, event.bucket_id, permit)?;
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    append_index_definition_event_inner(
        storage,
        event,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn append_index_definition_event_inner(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = index_definition_stream_id(event.tenant_id, event.bucket_id);
    let previous = read_index_journal_frames(&core_store, &stream_id)
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
        fence_token,
        *event.mutation_id.as_bytes(),
        index_key_hash(event.tenant_id, event.bucket_id, &event.index_name),
        previous_hash,
        body,
    );

    let partition_id = hex::encode(index_definition_partition_id(
        event.tenant_id,
        event.bucket_id,
    ));
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "index-definition:{}:{}:{}",
                event.tenant_id, event.bucket_id, event.mutation_id
            ),
            scope_partition: partition_id.clone(),
            committed_by_principal: index_definition_partition_principal(
                event.tenant_id,
                event.bucket_id,
            ),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind: "index_definition".to_string(),
                payload: frame.encode(),
                idempotency_key: Some(format!(
                    "index-definition:{}:{}:{}",
                    event.tenant_id, event.bucket_id, event.mutation_id
                )),
            }],
        })
        .await?;
    Ok(())
}

#[cfg(test)]
async fn write_index_definition_event(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    event_type: &str,
) -> Result<IndexDefinitionEvent> {
    write_index_definition_event_inner(storage, bucket, index, event_type, 0, None).await
}

#[cfg(test)]
pub(crate) async fn write_index_definition_event_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    event_type: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<IndexDefinitionEvent> {
    require_index_definition_permit(bucket.tenant_id, bucket.id, permit)?;
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    write_index_definition_event_inner(
        storage,
        bucket,
        index,
        event_type,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

#[cfg(test)]
async fn write_index_definition_event_inner(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    event_type: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
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
        mutation_id: uuid::Uuid::new_v4(),
        definition: index_definition_json(&bucket.name, index),
        created_at: chrono::Utc::now(),
    };
    append_index_definition_event_inner(storage, &event, fence_token, partition_precondition)
        .await?;
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
    let core_store = CoreStore::new(storage.clone()).await?;
    let frames = read_index_journal_frames(
        &core_store,
        &index_definition_stream_id(tenant_id, bucket_id),
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
            mutation_id: uuid::Uuid::from_bytes(frame.mutation_id),
            definition: body.definition,
            created_at: chrono::DateTime::parse_from_rfc3339(&body.created_at)?
                .with_timezone(&chrono::Utc),
        });
    }
    Ok(events)
}

async fn read_index_journal_frames(
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
        if record.record_kind != "index_definition" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

pub fn index_definition_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition").as_bytes())
}

fn index_definition_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("index_definition:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn index_definition_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:index_definition:{tenant_id}:{bucket_id}")
}

#[cfg(test)]
pub(crate) async fn read_index_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(read_index_journal_frames(
        &core_store,
        &index_definition_stream_id(tenant_id, bucket_id),
    )
    .await?
    .into_iter()
    .map(|frame| frame.fence_token)
    .collect())
}

fn require_index_definition_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    if permit.partition_family != "index_definition"
        || permit.partition_id != hex::encode(index_definition_partition_id(tenant_id, bucket_id))
    {
        return Err(anyhow!(
            "partition write permit does not target this index definition partition"
        ));
    }
    Ok(())
}

fn index_key_hash(tenant_id: i64, bucket_id: i64, index_name: &str) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index/{index_name}").as_bytes())
}

#[cfg(test)]
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
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use chrono::Utc;
    use serde_json::json;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"index definition partition owner signing key";

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
            mutation_id: uuid::Uuid::new_v4(),
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

    async fn ready_index_permit(storage: &Storage, owner_node_id: &str) -> PartitionWritePermit {
        let request = PartitionRecoveryAcquire {
            partition_family: "index_definition".to_string(),
            partition_id: hex::encode(index_definition_partition_id(42, 7)),
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
            &hex::encode([4; 32]),
            200,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap()
        .write_permit()
        .unwrap()
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

    #[tokio::test]
    async fn index_journal_permit_sets_frame_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_index_permit(&storage, "node-a").await;

        append_index_definition_event_with_permit(
            &storage,
            &event(1, "body", "create", true),
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();

        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let frames = read_index_journal_frames(&core_store, &index_definition_stream_id(42, 7))
            .await
            .unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].fence_token, permit.fence_token);
    }

    #[tokio::test]
    async fn index_journal_rejects_stale_partition_permit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_index_permit(&storage, "node-a").await;
        let fresh = ready_index_permit(&storage, "node-b").await;
        assert_eq!(fresh.fence_token, stale.fence_token + 1);

        let rejected = append_index_definition_event_with_permit(
            &storage,
            &event(1, "body", "create", true),
            &stale,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(rejected.to_string().contains("PartitionNotOwned"));

        append_index_definition_event_with_permit(
            &storage,
            &event(1, "body", "create", true),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn index_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_index_permit(&storage, "node-a").await;
        let stale_precondition =
            partition_write_ref_precondition(&storage, &stale, PARTITION_OWNER_KEY)
                .await
                .unwrap();
        let fresh = ready_index_permit(&storage, "node-b").await;
        assert_eq!(fresh.fence_token, stale.fence_token + 1);

        let rejected = append_index_definition_event_inner(
            &storage,
            &event(1, "body", "create", true),
            stale.fence_token,
            Some(stale_precondition),
        )
        .await
        .unwrap_err();
        let message = rejected.to_string();
        assert!(
            message.contains("generation mismatch") || message.contains("target mismatch"),
            "unexpected stale precondition error: {message}"
        );

        append_index_definition_event_with_permit(
            &storage,
            &event(1, "body", "create", true),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    pub(crate) async fn index_write_with_permit_allocates_cursor_under_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_index_permit(&storage, "node-a").await;
        let written = write_index_definition_event_with_permit(
            &storage,
            &bucket(),
            &index(1, true),
            "create",
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();

        assert_eq!(written.id, 1);
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let frames = read_index_journal_frames(&core_store, &index_definition_stream_id(42, 7))
            .await
            .unwrap();
        assert_eq!(frames[0].fence_token, permit.fence_token);
    }
}
