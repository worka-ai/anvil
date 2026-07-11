use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, CoreTransaction,
    CoreTransactionUpdate, ReadStream,
};
use crate::formats::{Hash32, hash32};
use crate::index_coremeta::{
    self, IndexDefinitionCurrentCoreMetaRecord, IndexDefinitionStateCoreMetaRecord,
};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
#[cfg(test)]
use crate::persistence::Bucket;
use crate::persistence::{IndexDefinition, IndexDefinitionEvent};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use prost::Message;
use serde_json::Value as JsonValue;
use serde_json::json;

const INDEX_EVENT_BODY_SCHEMA: &str = "anvil.core.index_definition_event.v1";
const INDEX_DEFINITION_RECORD_KIND: &str = "index_definition";

#[derive(Clone, PartialEq, Message)]
struct IndexDefinitionFieldsProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    name: String,
    #[prost(string, tag = "5")]
    kind: String,
    #[prost(string, tag = "6")]
    selector_json: String,
    #[prost(string, tag = "7")]
    extractor_json: String,
    #[prost(string, tag = "8")]
    authorization_mode: String,
    #[prost(string, tag = "9")]
    build_policy_json: String,
    #[prost(bool, tag = "10")]
    enabled: bool,
    #[prost(int64, tag = "11")]
    version: i64,
    #[prost(string, tag = "12")]
    created_at: String,
    #[prost(string, tag = "13")]
    updated_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct IndexEventBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    cursor: i64,
    #[prost(string, tag = "3")]
    bucket_name: String,
    #[prost(string, tag = "4")]
    event_type: String,
    #[prost(int64, tag = "5")]
    index_version: i64,
    #[prost(string, tag = "6")]
    event_created_at: String,
    #[prost(message, optional, tag = "7")]
    definition: Option<IndexDefinitionFieldsProto>,
    #[prost(uint64, tag = "8")]
    fence_token: u64,
    #[prost(string, tag = "9")]
    mutation_id: String,
}

#[derive(Debug, Clone)]
struct IndexCurrentRef {
    deleted: bool,
    event: IndexDefinitionEvent,
}

#[derive(Debug, Clone, Copy)]
struct IndexCurrentState {
    latest_cursor: i64,
    max_index_id: i64,
}

#[cfg(test)]
async fn append_index_definition_event(
    storage: &Storage,
    event: &IndexDefinitionEvent,
) -> Result<()> {
    append_index_definition_event_inner(storage, event, 0, None, None, None).await
}

pub(crate) async fn append_index_definition_event_with_permit(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    append_index_definition_event_with_permit_in_transaction(
        storage,
        event,
        permit,
        partition_owner_signing_key,
        None,
        None,
    )
    .await
}

pub(crate) async fn append_index_definition_event_with_permit_in_transaction(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    require_index_definition_permit(event.tenant_id, event.bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_index_definition_event_inner(
        storage,
        event,
        permit.fence_token,
        Some(partition_precondition),
        transaction_id,
        transaction_principal,
    )
    .await
}

async fn append_index_definition_event_inner(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = index_definition_stream_id(event.tenant_id, event.bucket_id);
    let payload = encode_index_event_body(event, fence_token)?;
    let partition_id = hex::encode(index_definition_partition_id(
        event.tenant_id,
        event.bucket_id,
    ));
    let batch =
        CoreMutationBatch {
            transaction_id: transaction_id.map(ToOwned::to_owned).unwrap_or_else(|| {
                format!(
                    "index-definition:{}:{}:{}",
                    event.tenant_id, event.bucket_id, event.mutation_id
                )
            }),
            scope_partition: partition_id.clone(),
            committed_by_principal: transaction_principal.map(ToOwned::to_owned).unwrap_or_else(
                || index_definition_partition_principal(event.tenant_id, event.bucket_id),
            ),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: partition_id.clone(),
                stream_id,
                record_kind: INDEX_DEFINITION_RECORD_KIND.to_string(),
                payload: payload.clone(),
                idempotency_key: Some(format!(
                    "index-definition:{}:{}:{}",
                    event.tenant_id, event.bucket_id, event.mutation_id
                )),
            }],
        };
    if transaction_id.is_some() {
        core_store.stage_explicit_transaction_batch(batch).await?;
    } else {
        core_store.commit_mutation_batch(batch).await?;
        write_index_current_coremeta_rows(storage, event, &payload).await?;
    }
    Ok(())
}

pub async fn materialize_committed_index_definition_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<Vec<IndexDefinitionEvent>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut materialized = Vec::new();
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
        } = update
        else {
            continue;
        };
        let Some((tenant_id, bucket_id)) = parse_index_definition_stream_id(stream_id) else {
            continue;
        };
        let records = core_store
            .read_stream(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence: visible_sequence.saturating_sub(1),
                limit: 1,
            })
            .await?;
        let Some(record) = records.into_iter().find(|record| {
            record.sequence == *visible_sequence && &record.event_hash == prepared_record_hash
        }) else {
            return Err(anyhow!(
                "index definition transaction {} committed stream record {stream_id}:{visible_sequence} is not readable",
                transaction.transaction_id
            ));
        };
        if record.record_kind != INDEX_DEFINITION_RECORD_KIND {
            continue;
        }
        let event = index_event_body_from_proto(decode_index_event_body(&record.payload)?)?;
        if event.tenant_id != tenant_id || event.bucket_id != bucket_id {
            return Err(anyhow!(
                "index definition transaction {} stream scope does not match payload",
                transaction.transaction_id
            ));
        }
        write_index_current_coremeta_rows(storage, &event, &record.payload).await?;
        materialized.push(event);
    }
    Ok(materialized)
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
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
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
    let cursor = read_index_current_state(storage, bucket.tenant_id, bucket.id)
        .await?
        .map(|state| state.latest_cursor)
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
    append_index_definition_event_inner(
        storage,
        &event,
        fence_token,
        partition_precondition,
        None,
        None,
    )
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
    let mut events = Vec::new();
    for row in index_coremeta::list_index_definition_current_coremeta_records(
        storage, tenant_id, bucket_id,
    )? {
        let current = index_current_from_coremeta_row(row)?;
        ensure_index_event_scope_matches(&current.event, tenant_id, bucket_id)?;
        if current.deleted {
            continue;
        }
        events.push(current.event);
    }
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
    let current = read_index_current_row(storage, tenant_id, bucket_id, name).await?;
    let Some(current) = current else {
        return Ok(None);
    };
    ensure_index_event_name_matches(&current.event, tenant_id, bucket_id, name)?;
    if current.deleted {
        return Ok(None);
    }
    index_definition_from_event(&current.event).map(Some)
}

pub async fn next_index_definition_id(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<i64> {
    read_index_current_state(storage, tenant_id, bucket_id)
        .await?
        .map(|state| state.max_index_id)
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
    let bodies = read_index_journal_bodies(
        &core_store,
        &index_definition_stream_id(tenant_id, bucket_id),
    )
    .await?;
    let mut events = Vec::new();
    for body in bodies {
        events.push(index_event_body_from_proto(body)?);
    }
    Ok(events)
}

async fn read_index_journal_bodies(
    core_store: &CoreStore,
    stream_id: &str,
) -> Result<Vec<IndexEventBodyProto>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut bodies = Vec::new();
    for record in records {
        if record.record_kind != INDEX_DEFINITION_RECORD_KIND {
            continue;
        }
        bodies.push(decode_index_event_body(&record.payload)?);
    }
    Ok(bodies)
}

pub fn index_definition_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition").as_bytes())
}

fn index_definition_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("index_definition:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn parse_index_definition_stream_id(stream_id: &str) -> Option<(i64, i64)> {
    let rest = stream_id.strip_prefix("index_definition:tenant:")?;
    let (tenant, bucket_part) = rest.split_once(":bucket:")?;
    Some((tenant.parse().ok()?, bucket_part.parse().ok()?))
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
    Ok(read_index_journal_bodies(
        &core_store,
        &index_definition_stream_id(tenant_id, bucket_id),
    )
    .await?
    .into_iter()
    .map(|body| body.fence_token)
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

async fn write_index_current_coremeta_rows(
    storage: &Storage,
    event: &IndexDefinitionEvent,
    event_payload: &[u8],
) -> Result<()> {
    let existing = read_index_current_state(storage, event.tenant_id, event.bucket_id).await?;
    let state = IndexCurrentState {
        latest_cursor: existing
            .map(|state| state.latest_cursor)
            .unwrap_or(0)
            .max(event.id),
        max_index_id: existing
            .map(|state| state.max_index_id)
            .unwrap_or(0)
            .max(event.index_id),
    };
    let updated_at_unix_nanos = event_time_unix_nanos(event.created_at)?;
    index_coremeta::write_index_definition_current_coremeta_record(
        storage,
        &IndexDefinitionCurrentCoreMetaRecord {
            tenant_id: event.tenant_id,
            bucket_id: event.bucket_id,
            index_name: event.index_name.clone(),
            deleted: event.event_type == "drop",
            cursor: event.id,
            index_version: event.index_version,
            event_payload: event_payload.to_vec(),
            updated_at_unix_nanos,
        },
    )
    .await?;
    index_coremeta::write_index_definition_state_coremeta_record(
        storage,
        &IndexDefinitionStateCoreMetaRecord {
            tenant_id: event.tenant_id,
            bucket_id: event.bucket_id,
            latest_cursor: state.latest_cursor,
            max_index_id: state.max_index_id,
            updated_at_unix_nanos,
        },
    )
    .await?;
    Ok(())
}

async fn read_index_current_row(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<Option<IndexCurrentRef>> {
    let Some(row) = index_coremeta::read_index_definition_current_coremeta_record(
        storage, tenant_id, bucket_id, index_name,
    )?
    else {
        return Ok(None);
    };
    index_current_from_coremeta_row(row).map(Some)
}

async fn read_index_current_state(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Option<IndexCurrentState>> {
    Ok(
        index_coremeta::read_index_definition_state_coremeta_record(storage, tenant_id, bucket_id)?
            .map(index_state_from_coremeta_row),
    )
}

fn encode_index_event_body(event: &IndexDefinitionEvent, fence_token: u64) -> Result<Vec<u8>> {
    let definition = index_definition_from_event(event)?;
    let proto = IndexEventBodyProto {
        schema: INDEX_EVENT_BODY_SCHEMA.to_string(),
        cursor: event.id,
        bucket_name: event.bucket_name.clone(),
        event_type: event.event_type.clone(),
        index_version: event.index_version,
        event_created_at: event.created_at.to_rfc3339(),
        definition: Some(index_definition_to_proto(&definition)?),
        fence_token,
        mutation_id: event.mutation_id.to_string(),
    };
    encode_deterministic_proto(&proto)
}

fn decode_index_event_body(bytes: &[u8]) -> Result<IndexEventBodyProto> {
    let proto = IndexEventBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "index definition event body")?;
    if proto.schema != INDEX_EVENT_BODY_SCHEMA {
        return Err(anyhow!("index definition event body has invalid schema"));
    }
    uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("index definition event body has invalid mutation id"))?;
    Ok(proto)
}

fn index_event_body_from_proto(proto: IndexEventBodyProto) -> Result<IndexDefinitionEvent> {
    let definition = index_definition_from_proto(
        proto
            .definition
            .ok_or_else(|| anyhow!("index definition event body missing definition"))?,
    )?;
    let mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)?;
    index_event_from_parts(
        proto.cursor,
        proto.bucket_name,
        proto.event_type,
        proto.index_version,
        mutation_id,
        proto.event_created_at,
        definition,
    )
}

fn index_current_from_coremeta_row(
    row: IndexDefinitionCurrentCoreMetaRecord,
) -> Result<IndexCurrentRef> {
    let event = index_event_body_from_proto(decode_index_event_body(&row.event_payload)?)?;
    if event.tenant_id != row.tenant_id
        || event.bucket_id != row.bucket_id
        || event.index_name != row.index_name
        || event.id != row.cursor
        || event.index_version != row.index_version
    {
        return Err(anyhow!(
            "CoreMeta index definition current row payload scope mismatch"
        ));
    }
    if row.deleted != (event.event_type == "drop") {
        return Err(anyhow!(
            "CoreMeta index definition current row deletion marker mismatch"
        ));
    }
    Ok(IndexCurrentRef {
        deleted: row.deleted,
        event,
    })
}

fn index_state_from_coremeta_row(row: IndexDefinitionStateCoreMetaRecord) -> IndexCurrentState {
    IndexCurrentState {
        latest_cursor: row.latest_cursor,
        max_index_id: row.max_index_id,
    }
}

fn index_definition_to_proto(index: &IndexDefinition) -> Result<IndexDefinitionFieldsProto> {
    Ok(IndexDefinitionFieldsProto {
        id: index.id,
        tenant_id: index.tenant_id,
        bucket_id: index.bucket_id,
        name: index.name.clone(),
        kind: index.kind.clone(),
        selector_json: serde_json::to_string(&index.selector)?,
        extractor_json: serde_json::to_string(&index.extractor)?,
        authorization_mode: index.authorization_mode.clone(),
        build_policy_json: serde_json::to_string(&index.build_policy)?,
        enabled: index.enabled,
        version: index.version,
        created_at: index.created_at.to_rfc3339(),
        updated_at: index.updated_at.to_rfc3339(),
    })
}

fn index_definition_from_proto(proto: IndexDefinitionFieldsProto) -> Result<IndexDefinition> {
    Ok(IndexDefinition {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        name: proto.name,
        kind: proto.kind,
        selector: serde_json::from_str(&proto.selector_json)
            .context("parse index selector from current row")?,
        extractor: serde_json::from_str(&proto.extractor_json)
            .context("parse index extractor from current row")?,
        authorization_mode: proto.authorization_mode,
        build_policy: serde_json::from_str(&proto.build_policy_json)
            .context("parse index build policy from current row")?,
        enabled: proto.enabled,
        version: proto.version,
        created_at: chrono::DateTime::parse_from_rfc3339(&proto.created_at)?
            .with_timezone(&chrono::Utc),
        updated_at: chrono::DateTime::parse_from_rfc3339(&proto.updated_at)?
            .with_timezone(&chrono::Utc),
    })
}

fn index_event_from_parts(
    cursor: i64,
    bucket_name: String,
    event_type: String,
    index_version: i64,
    mutation_id: uuid::Uuid,
    event_created_at: String,
    definition: IndexDefinition,
) -> Result<IndexDefinitionEvent> {
    Ok(IndexDefinitionEvent {
        id: cursor,
        tenant_id: definition.tenant_id,
        bucket_id: definition.bucket_id,
        bucket_name: bucket_name.clone(),
        index_id: definition.id,
        index_name: definition.name.clone(),
        event_type,
        index_version,
        mutation_id,
        definition: index_definition_json(&bucket_name, &definition),
        created_at: chrono::DateTime::parse_from_rfc3339(&event_created_at)?
            .with_timezone(&chrono::Utc),
    })
}

fn ensure_index_event_scope_matches(
    event: &IndexDefinitionEvent,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<()> {
    if event.tenant_id != tenant_id || event.bucket_id != bucket_id {
        return Err(anyhow!("CoreMeta index current list row scope mismatch"));
    }
    Ok(())
}

fn ensure_index_event_name_matches(
    event: &IndexDefinitionEvent,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<()> {
    ensure_index_event_scope_matches(event, tenant_id, bucket_id)?;
    if event.index_name != index_name {
        return Err(anyhow!("CoreMeta index current name row scope mismatch"));
    }
    Ok(())
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

fn event_time_unix_nanos(event_time: chrono::DateTime<chrono::Utc>) -> Result<u64> {
    let nanos = event_time
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("index definition timestamp cannot be represented as nanos"))?;
    u64::try_from(nanos).map_err(|_| anyhow!("index definition timestamp is before unix epoch"))
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

pub fn index_definition_from_event_for_projection(
    event: &IndexDefinitionEvent,
) -> Result<IndexDefinition> {
    index_definition_from_event(event)
}

fn parse_definition_time(
    value: Option<&JsonValue>,
    default_time: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>> {
    let Some(value) = value.and_then(JsonValue::as_str) else {
        return Ok(default_time);
    };
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&chrono::Utc))
        .or_else(|_| {
            chrono::DateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S %Z")
                .map(|value| value.with_timezone(&chrono::Utc))
        })
        .or(Ok(default_time))
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
    async fn index_current_rows_and_id_allocation_do_not_replay_history_stream() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        let create = event(7, "body", "create", true);
        let create_payload = encode_index_event_body(&create, 0).unwrap();
        write_index_current_coremeta_rows(&storage, &create, &create_payload)
            .await
            .unwrap();

        assert!(
            read_index_definition_events(&storage, 42, 7, 0, 10)
                .await
                .unwrap()
                .is_empty(),
            "test setup must not write index history records"
        );
        let current = read_current_index_definition(&storage, 42, 7, "body")
            .await
            .unwrap()
            .expect("current definition should come from CoreMeta current row");
        assert_eq!(current.name, "body");
        assert_eq!(
            next_index_definition_id(&storage, 42, 7).await.unwrap(),
            101
        );

        let row =
            index_coremeta::read_index_definition_current_coremeta_record(&storage, 42, 7, "body")
                .unwrap()
                .expect("current index row should exist");
        assert_eq!(row.event_payload, create_payload);
        assert!(!row.deleted);

        let drop = event(8, "body", "drop", true);
        let drop_payload = encode_index_event_body(&drop, 0).unwrap();
        write_index_current_coremeta_rows(&storage, &drop, &drop_payload)
            .await
            .unwrap();

        assert!(
            read_current_index_definition(&storage, 42, 7, "body")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            next_index_definition_id(&storage, 42, 7).await.unwrap(),
            101
        );
    }

    #[tokio::test]
    async fn index_journal_permit_sets_protobuf_fence() {
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
        let bodies = read_index_journal_bodies(&core_store, &index_definition_stream_id(42, 7))
            .await
            .unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0].fence_token, permit.fence_token);
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
            partition_write_precondition(&storage, &stale, PARTITION_OWNER_KEY)
                .await
                .unwrap();
        let fresh = ready_index_permit(&storage, "node-b").await;
        assert_eq!(fresh.fence_token, stale.fence_token + 1);

        let rejected = append_index_definition_event_inner(
            &storage,
            &event(1, "body", "create", true),
            stale.fence_token,
            Some(stale_precondition),
            None,
            None,
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
        let bodies = read_index_journal_bodies(&core_store, &index_definition_stream_id(42, 7))
            .await
            .unwrap();
        assert_eq!(bodies[0].fence_token, permit.fence_token);
    }
}
