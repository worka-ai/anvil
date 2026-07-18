#[cfg(test)]
use crate::core_store::ReadStream;
use crate::core_store::{
    CF_LEASES_FENCES, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, CoreMetaTuplePart,
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore,
    TABLE_TASK_CURRENT_ROW, commit_coremeta_batch_for_storage, core_meta_committed_row_common,
    core_meta_payload_digest, core_meta_record_tuple_key, core_meta_root_key_hash,
    core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::TaskRecord;
use crate::storage::Storage;
use crate::tasks::{TaskStatus, TaskType};
use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use prost::{Message, Oneof};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};

const TASK_CURRENT_ROW_SCHEMA: &str = "anvil.core.task_current.v1";
const TASK_JOURNAL_BODY_SCHEMA: &str = "anvil.core.task_audit.v1";
const TASK_CURRENT_ROW_KEY_PREFIX: &str = "task_queue_current/by_id";
const TASK_CURRENT_ROW_ROOT_KEY: &str = "task_queue_current:global";
const TASK_CURRENT_ROW_MAX_PROTO_BYTES: usize = 16 * 1024;
const TASK_QUEUE_AUDIT_RECORD_KIND: &str = "task_queue_audit";

#[derive(Debug, Clone)]
enum TaskJournalBody {
    Enqueued {
        task: TaskRecord,
    },
    Claimed {
        task_id: i64,
        updated_at: DateTime<Utc>,
    },
    StatusUpdated {
        task_id: i64,
        status: TaskStatus,
        updated_at: DateTime<Utc>,
    },
    Failed {
        task_id: i64,
        error: String,
        attempts: i32,
        scheduled_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Default)]
struct TaskQueueState {
    tasks: BTreeMap<i64, TaskRecord>,
}

#[derive(Clone, PartialEq, Message)]
struct TaskJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(enumeration = "TaskJournalEventKindProto", tag = "2")]
    event: i32,
    #[prost(message, optional, tag = "3")]
    task: Option<TaskRecordProto>,
    #[prost(int64, optional, tag = "4")]
    task_id: Option<i64>,
    #[prost(enumeration = "TaskStatusProto", optional, tag = "5")]
    status: Option<i32>,
    #[prost(string, optional, tag = "6")]
    error: Option<String>,
    #[prost(int32, optional, tag = "7")]
    attempts: Option<i32>,
    #[prost(string, optional, tag = "8")]
    scheduled_at: Option<String>,
    #[prost(string, optional, tag = "9")]
    updated_at: Option<String>,
    #[prost(uint64, tag = "10")]
    fence_token: u64,
    #[prost(string, tag = "11")]
    mutation_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum TaskJournalEventKindProto {
    Unspecified = 0,
    Enqueued = 1,
    Claimed = 2,
    StatusUpdated = 3,
    Failed = 4,
}

#[derive(Clone, PartialEq, Message)]
struct TaskCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    task: Option<TaskRecordProto>,
}

#[derive(Debug, Clone)]
struct TaskCurrentCoreMetaRow {
    task: TaskRecord,
    generation: u64,
    transaction_id: String,
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct TaskRecordProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(enumeration = "TaskTypeProto", tag = "2")]
    task_type: i32,
    #[prost(message, optional, tag = "3")]
    payload: Option<JsonValueProto>,
    #[prost(int32, tag = "4")]
    priority: i32,
    #[prost(enumeration = "TaskStatusProto", tag = "5")]
    status: i32,
    #[prost(int32, tag = "6")]
    attempts: i32,
    #[prost(string, optional, tag = "7")]
    last_error: Option<String>,
    #[prost(string, tag = "8")]
    scheduled_at: String,
    #[prost(string, tag = "9")]
    created_at: String,
    #[prost(string, tag = "10")]
    updated_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum TaskTypeProto {
    Unspecified = 0,
    DeleteObject = 1,
    DeleteBucket = 2,
    ObjectMetadataCompaction = 3,
    IndexBuild = 4,
    RebalanceShard = 5,
    HfIngestion = 6,
    AuthzMaterialization = 7,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum TaskStatusProto {
    Unspecified = 0,
    Pending = 1,
    Running = 2,
    Completed = 3,
    Failed = 4,
}

#[derive(Clone, PartialEq, Message)]
struct JsonValueProto {
    #[prost(oneof = "json_value_proto::Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    kind: Option<json_value_proto::Kind>,
}

mod json_value_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(bool, tag = "1")]
        Null(bool),
        #[prost(bool, tag = "2")]
        Bool(bool),
        #[prost(int64, tag = "3")]
        I64(i64),
        #[prost(uint64, tag = "4")]
        U64(u64),
        #[prost(double, tag = "5")]
        F64(f64),
        #[prost(string, tag = "6")]
        String(String),
        #[prost(message, tag = "7")]
        Array(super::JsonArrayProto),
        #[prost(message, tag = "8")]
        Object(super::JsonObjectProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct JsonArrayProto {
    #[prost(message, repeated, tag = "1")]
    values: Vec<JsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct JsonObjectProto {
    #[prost(message, repeated, tag = "1")]
    entries: Vec<JsonObjectEntryProto>,
}

#[derive(Clone, PartialEq, Message)]
struct JsonObjectEntryProto {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<JsonValueProto>,
}

#[cfg(test)]
async fn enqueue_task(
    storage: &Storage,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
) -> Result<()> {
    enqueue_task_inner(storage, task_type, payload, priority, 0, None).await
}

pub(crate) async fn enqueue_task_with_permit(
    storage: &Storage,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_task_queue_permit(permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    enqueue_task_inner(
        storage,
        task_type,
        payload,
        priority,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

pub(crate) async fn enqueue_task_if_absent_with_permit(
    storage: &Storage,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<bool> {
    require_task_queue_permit(permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    let state = read_task_queue_state(storage).await?;
    if state.has_live_task(&task_type, &payload) {
        return Ok(false);
    }
    enqueue_task_inner(
        storage,
        task_type,
        payload,
        priority,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
    .map(|_| true)
}

pub(crate) async fn enqueue_index_build_task_with_permit(
    storage: &Storage,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<bool> {
    require_task_queue_permit(permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    validate_index_build_payload(&payload)?;
    let state = read_task_queue_state(storage).await?;
    let now = Utc::now();
    let requested_cursor = json_u128(&payload, "source_cursor")
        .ok_or_else(|| anyhow!("index build source_cursor must be a nonnegative integer"))?;
    let existing = state.index_build_tasks_for_payload(&payload);
    if existing.iter().any(|task| {
        matches!(task.status, TaskStatus::Pending | TaskStatus::Failed)
            && task.priority == priority
            && json_u128(&task.payload, "source_cursor").unwrap_or(0) >= requested_cursor
    }) {
        return Ok(false);
    }
    for task in existing {
        if matches!(task.status, TaskStatus::Pending | TaskStatus::Failed) {
            append_task_event(
                storage,
                TaskJournalBody::StatusUpdated {
                    task_id: task.id,
                    status: TaskStatus::Completed,
                    updated_at: now,
                },
                permit.fence_token,
                Some(partition_precondition.clone()),
            )
            .await?;
        }
    }
    enqueue_task_inner(
        storage,
        TaskType::IndexBuild,
        payload,
        priority,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
    .map(|_| true)
}

pub(crate) async fn enqueue_authz_materialization_task_with_permit(
    storage: &Storage,
    payload: JsonValue,
    priority: i32,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<bool> {
    require_task_queue_permit(permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    validate_authz_materialization_payload(&payload)?;
    let state = read_task_queue_state(storage).await?;
    let now = Utc::now();
    let requested_revision = json_u128(&payload, "target_revision")
        .ok_or_else(|| anyhow!("authz materialization target_revision must be nonnegative"))?;
    let existing = state.authz_materialization_tasks_for_payload(&payload);
    if existing.iter().any(|task| {
        matches!(task.status, TaskStatus::Pending | TaskStatus::Failed)
            && task.priority == priority
            && json_u128(&task.payload, "target_revision").unwrap_or(0) >= requested_revision
    }) {
        return Ok(false);
    }
    for task in existing {
        if matches!(task.status, TaskStatus::Pending | TaskStatus::Failed) {
            append_task_event(
                storage,
                TaskJournalBody::StatusUpdated {
                    task_id: task.id,
                    status: TaskStatus::Completed,
                    updated_at: now,
                },
                permit.fence_token,
                Some(partition_precondition.clone()),
            )
            .await?;
        }
    }
    enqueue_task_inner(
        storage,
        TaskType::AuthzMaterialization,
        payload,
        priority,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
    .map(|_| true)
}

async fn enqueue_task_inner(
    storage: &Storage,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let mut attempts = 0_u8;
    loop {
        attempts += 1;
        let result = enqueue_task_inner_once(
            storage,
            task_type,
            payload.clone(),
            priority,
            fence_token,
            partition_precondition.clone(),
        )
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(error) if attempts < 5 && is_retryable_task_id_collision(&error) => {
                tokio::task::yield_now().await;
                continue;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn enqueue_task_inner_once(
    storage: &Storage,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let state = read_task_queue_state(storage).await?;
    let now = Utc::now();
    let task = TaskRecord {
        id: state.next_task_id()?,
        task_type,
        payload,
        priority,
        status: TaskStatus::Pending,
        attempts: 0,
        last_error: None,
        scheduled_at: now,
        created_at: now,
        updated_at: now,
    };
    append_task_event(
        storage,
        TaskJournalBody::Enqueued { task },
        fence_token,
        partition_precondition,
    )
    .await
}

fn is_retryable_task_id_collision(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}");
    message.contains("CoreMeta row")
        && message.contains(&format!("{TABLE_TASK_CURRENT_ROW:#06x}"))
        && message.contains("must be absent")
}

#[cfg(test)]
async fn claim_pending_tasks(storage: &Storage, limit: i64) -> Result<Vec<TaskRecord>> {
    claim_pending_tasks_inner(storage, limit, 0, None).await
}

pub(crate) async fn claim_pending_tasks_with_permit(
    storage: &Storage,
    limit: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<Vec<TaskRecord>> {
    require_task_queue_permit(permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    claim_pending_tasks_inner(
        storage,
        limit,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn claim_pending_tasks_inner(
    storage: &Storage,
    limit: i64,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<Vec<TaskRecord>> {
    let state = read_task_queue_state(storage).await?;
    let now = Utc::now();
    let running_index_build_keys = state.running_index_build_keys();
    let running_authz_materialization_keys = state.running_authz_materialization_keys();
    let mut selected_index_build_keys = BTreeSet::new();
    let mut selected_authz_materialization_keys = BTreeSet::new();
    let mut tasks = state
        .tasks
        .values()
        .filter(|task| {
            matches!(task.status, TaskStatus::Pending | TaskStatus::Failed)
                && task.scheduled_at <= now
        })
        .cloned()
        .collect::<Vec<_>>();
    tasks.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| a.created_at.cmp(&b.created_at))
            .then_with(|| a.id.cmp(&b.id))
    });
    let limit = limit.max(0) as usize;
    let mut selected = Vec::new();
    for task in tasks {
        if selected.len() >= limit {
            break;
        }
        if task.task_type == TaskType::IndexBuild
            && let Some(key) = index_build_key(&task.payload)
        {
            if running_index_build_keys.contains(&key) || selected_index_build_keys.contains(&key) {
                continue;
            }
            selected_index_build_keys.insert(key);
        }
        if task.task_type == TaskType::AuthzMaterialization
            && let Some(key) = authz_materialization_key(&task.payload)
        {
            if running_authz_materialization_keys.contains(&key)
                || selected_authz_materialization_keys.contains(&key)
            {
                continue;
            }
            selected_authz_materialization_keys.insert(key);
        }
        selected.push(task);
    }
    let tasks = selected;
    for task in &tasks {
        append_task_event(
            storage,
            TaskJournalBody::Claimed {
                task_id: task.id,
                updated_at: now,
            },
            fence_token,
            partition_precondition.clone(),
        )
        .await?;
    }
    Ok(tasks
        .into_iter()
        .map(|mut task| {
            task.status = TaskStatus::Running;
            task.updated_at = now;
            task
        })
        .collect())
}

pub async fn list_tasks(storage: &Storage) -> Result<Vec<TaskRecord>> {
    Ok(read_task_queue_state(storage).await?.tasks())
}

pub(crate) async fn has_due_tasks(storage: &Storage) -> Result<bool> {
    Ok(read_task_queue_state(storage)
        .await?
        .has_due_tasks(Utc::now()))
}

#[cfg(test)]
async fn update_task_status(storage: &Storage, task_id: i64, status: TaskStatus) -> Result<()> {
    update_task_status_inner(storage, task_id, status, 0, None).await
}

pub(crate) async fn update_task_status_with_permit(
    storage: &Storage,
    task_id: i64,
    status: TaskStatus,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_task_queue_permit(permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    update_task_status_inner(
        storage,
        task_id,
        status,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn update_task_status_inner(
    storage: &Storage,
    task_id: i64,
    status: TaskStatus,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    // Task status is an event-sourced transition. Do not pre-read and silently
    // drop it: the writer already holds the task-queue fence, and stale local
    // reads can otherwise lose a valid completion event.
    append_task_event(
        storage,
        TaskJournalBody::StatusUpdated {
            task_id,
            status,
            updated_at: Utc::now(),
        },
        fence_token,
        partition_precondition,
    )
    .await
}

#[cfg(test)]
async fn fail_task(storage: &Storage, task_id: i64, error: &str) -> Result<()> {
    fail_task_inner(storage, task_id, error, 0, None).await
}

pub(crate) async fn fail_task_with_permit(
    storage: &Storage,
    task_id: i64,
    error: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_task_queue_permit(permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    fail_task_inner(
        storage,
        task_id,
        error,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn fail_task_inner(
    storage: &Storage,
    task_id: i64,
    error: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let Some(task) = read_task_queue_state(storage)
        .await?
        .tasks
        .get(&task_id)
        .cloned()
    else {
        return Ok(());
    };
    let attempts = task.attempts.saturating_add(1);
    let now = Utc::now();
    let retry_delay = i64::from(attempts.saturating_mul(attempts).saturating_mul(10));
    append_task_event(
        storage,
        TaskJournalBody::Failed {
            task_id,
            error: error.to_string(),
            attempts,
            scheduled_at: now + chrono::Duration::seconds(retry_delay),
            updated_at: now,
        },
        fence_token,
        partition_precondition,
    )
    .await
}

async fn read_task_queue_state(storage: &Storage) -> Result<TaskQueueState> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let mut state = TaskQueueState::default();
    for record in meta.scan_prefix(
        CF_LEASES_FENCES,
        TABLE_TASK_CURRENT_ROW,
        &task_current_row_prefix()?,
    )? {
        let row =
            decode_task_current_row(&record.payload).context("decode task current CoreMeta row")?;
        ensure_task_row_key_matches(&record.key, row.task.id)?;
        state.tasks.insert(row.task.id, row.task);
    }
    Ok(state)
}

async fn append_task_event(
    storage: &Storage,
    event: TaskJournalBody,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let stream_id = task_queue_stream_id();
    let mutation_id = uuid::Uuid::new_v4();
    let partition_id = hex::encode(task_queue_partition_id());
    let transaction_id = format!("task-queue:{mutation_id}");
    let payload = encode_task_journal_body(&event, fence_token, mutation_id)?;
    let current_update = task_current_row_update(&meta, &event, &transaction_id)?;
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    if let Some(precondition) = current_update.precondition.as_ref() {
        preconditions.push(precondition.clone());
    }
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id,
        record_kind: TASK_QUEUE_AUDIT_RECORD_KIND.to_string(),
        payload,
        idempotency_key: None,
    }];
    if let Some(row) = current_update.row.as_ref() {
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: partition_id.clone(),
            cf: CF_LEASES_FENCES.to_string(),
            table_id: TABLE_TASK_CURRENT_ROW,
            tuple_key: task_current_row_key(row.task.id)?,
            payload: encode_task_current_row(row)?,
        });
    }
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id,
            committed_by_principal: task_queue_partition_principal(),
            preconditions,
            operations,
        })
        .await?;
    Ok(())
}

#[cfg(test)]
async fn read_task_journal_bodies(storage: &Storage) -> Result<Vec<TaskJournalBody>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_task_journal_bodies_from_store(&core_store).await
}

#[cfg(test)]
async fn read_task_journal_bodies_from_store(
    core_store: &CoreStore,
) -> Result<Vec<TaskJournalBody>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: task_queue_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut bodies = Vec::new();
    for record in records {
        if record.record_kind != TASK_QUEUE_AUDIT_RECORD_KIND {
            continue;
        }
        bodies.push(decode_task_journal_body(&record.payload)?);
    }
    Ok(bodies)
}

#[derive(Debug, Clone)]
struct TaskCurrentRowUpdate {
    precondition: Option<CoreMutationPrecondition>,
    row: Option<TaskCurrentCoreMetaRow>,
}

fn task_current_row_update(
    meta: &CoreMetaStore,
    event: &TaskJournalBody,
    transaction_id: &str,
) -> Result<TaskCurrentRowUpdate> {
    let Some(task) = task_after_event(meta, event)? else {
        return Ok(TaskCurrentRowUpdate {
            precondition: None,
            row: None,
        });
    };
    let key = task_current_row_key(task.id)?;
    let current = meta.get(CF_LEASES_FENCES, TABLE_TASK_CURRENT_ROW, &key)?;
    let current_row = current
        .as_deref()
        .map(decode_task_current_row)
        .transpose()?;
    if let Some(current_row) = current_row.as_ref() {
        if current_row.task.id != task.id {
            bail!("CoreStore task current CoreMeta row scope mismatch");
        }
    }
    let generation = current_row
        .as_ref()
        .map(|row| row.generation.saturating_add(1))
        .unwrap_or(1);
    let precondition = Some(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_LEASES_FENCES.to_string(),
        table_id: TABLE_TASK_CURRENT_ROW,
        tuple_key: key,
        expected_payload_hash: current
            .as_ref()
            .map(|payload| core_meta_payload_digest(TABLE_TASK_CURRENT_ROW, payload)),
        require_absent: current.is_none(),
        require_present: current.is_some(),
    });
    Ok(TaskCurrentRowUpdate {
        precondition,
        row: Some(TaskCurrentCoreMetaRow {
            task,
            generation,
            transaction_id: transaction_id.to_string(),
            created_at_unix_nanos: current_unix_nanos()?,
        }),
    })
}

fn task_after_event(meta: &CoreMetaStore, event: &TaskJournalBody) -> Result<Option<TaskRecord>> {
    match event {
        TaskJournalBody::Enqueued { task } => Ok(Some(task.clone())),
        TaskJournalBody::Claimed {
            task_id,
            updated_at,
        } => {
            let Some(mut task) = read_current_task(meta, *task_id)? else {
                return Ok(None);
            };
            task.status = TaskStatus::Running;
            task.updated_at = *updated_at;
            Ok(Some(task))
        }
        TaskJournalBody::StatusUpdated {
            task_id,
            status,
            updated_at,
        } => {
            let Some(mut task) = read_current_task(meta, *task_id)? else {
                return Ok(None);
            };
            task.status = *status;
            task.updated_at = *updated_at;
            Ok(Some(task))
        }
        TaskJournalBody::Failed {
            task_id,
            error,
            attempts,
            scheduled_at,
            updated_at,
        } => {
            let Some(mut task) = read_current_task(meta, *task_id)? else {
                return Ok(None);
            };
            task.status = TaskStatus::Failed;
            task.last_error = Some(error.clone());
            task.attempts = *attempts;
            task.scheduled_at = *scheduled_at;
            task.updated_at = *updated_at;
            Ok(Some(task))
        }
    }
}

fn read_current_task(meta: &CoreMetaStore, task_id: i64) -> Result<Option<TaskRecord>> {
    let key = task_current_row_key(task_id)?;
    let Some(payload) = meta.get(CF_LEASES_FENCES, TABLE_TASK_CURRENT_ROW, &key)? else {
        return Ok(None);
    };
    let row = decode_task_current_row(&payload).context("decode task current CoreMeta row")?;
    if row.task.id != task_id {
        bail!("CoreStore task current CoreMeta row scope mismatch");
    }
    Ok(Some(row.task))
}

async fn write_task_current_row(
    storage: &Storage,
    meta: &CoreMetaStore,
    row: &TaskCurrentCoreMetaRow,
    precondition: Option<&CoreMutationPrecondition>,
) -> Result<()> {
    validate_task_current_precondition(meta, precondition)?;
    let key = task_current_row_key(row.task.id)?;
    let payload = encode_task_current_row(row)?;
    let op = CoreMetaBatchOp {
        cf: CF_LEASES_FENCES,
        table_id: TABLE_TASK_CURRENT_ROW,
        tuple_key: &key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!("task-current:{}:{}", row.task.id, row.generation),
        &[op],
    )
    .await?;
    Ok(())
}

fn validate_task_current_precondition(
    meta: &CoreMetaStore,
    precondition: Option<&CoreMutationPrecondition>,
) -> Result<()> {
    let Some(precondition) = precondition else {
        return Ok(());
    };
    let CoreMutationPrecondition::CoreMetaRow {
        cf,
        table_id,
        tuple_key,
        expected_payload_hash,
        require_absent,
        require_present,
    } = precondition
    else {
        bail!("task current writer received unsupported non-CoreMeta precondition");
    };
    let current = meta.get_named(cf, *table_id, tuple_key)?;
    if *require_absent && current.is_some() {
        bail!("task current CoreMeta precondition failed: row must be absent");
    }
    if *require_present && current.is_none() {
        bail!("task current CoreMeta precondition failed: row must be present");
    }
    if let (Some(expected), Some(current)) = (expected_payload_hash.as_ref(), current.as_ref()) {
        let actual = core_meta_payload_digest(*table_id, current);
        if actual != *expected {
            bail!("task current CoreMeta precondition failed: payload hash mismatch");
        }
    }
    Ok(())
}

fn encode_task_current_row(row: &TaskCurrentCoreMetaRow) -> Result<Vec<u8>> {
    let target = TaskCurrentRowProto {
        schema: TASK_CURRENT_ROW_SCHEMA.to_string(),
        common: Some(core_meta_committed_row_common(
            task_queue_realm_id(),
            core_meta_root_key_hash(TASK_CURRENT_ROW_ROOT_KEY),
            row.generation,
            &row.transaction_id,
            row.created_at_unix_nanos,
        )),
        task: Some(task_record_to_proto(&row.task)?),
    };
    let bytes = encode_deterministic_proto(&target, "task current CoreMeta row")?;
    if bytes.len() > TASK_CURRENT_ROW_MAX_PROTO_BYTES {
        bail!(
            "CoreStore task current CoreMeta row is {} bytes, exceeding {} bytes",
            bytes.len(),
            TASK_CURRENT_ROW_MAX_PROTO_BYTES
        );
    }
    Ok(bytes)
}

fn decode_task_current_row(bytes: &[u8]) -> Result<TaskCurrentCoreMetaRow> {
    if bytes.len() > TASK_CURRENT_ROW_MAX_PROTO_BYTES {
        bail!(
            "CoreStore task current CoreMeta row is {} bytes, exceeding {} bytes",
            bytes.len(),
            TASK_CURRENT_ROW_MAX_PROTO_BYTES
        );
    }
    let target = TaskCurrentRowProto::decode(bytes)?;
    let mut canonical = Vec::with_capacity(target.encoded_len());
    target.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore task current CoreMeta row is not deterministic protobuf");
    }
    if target.schema != TASK_CURRENT_ROW_SCHEMA {
        bail!("CoreStore task current CoreMeta row has invalid schema");
    }
    let common = target
        .common
        .ok_or_else(|| anyhow!("CoreStore task current CoreMeta row is missing common metadata"))?;
    if common.realm_id != task_queue_realm_id() {
        bail!("CoreStore task current CoreMeta row has invalid realm");
    }
    if common.root_key_hash != core_meta_root_key_hash(TASK_CURRENT_ROW_ROOT_KEY) {
        bail!("CoreStore task current CoreMeta row has invalid root key hash");
    }
    if common.visibility_state != crate::core_store::CoreMetaVisibilityState::Committed as i32 {
        bail!("CoreStore task current CoreMeta row is not committed");
    }
    let task = target
        .task
        .ok_or_else(|| anyhow!("CoreStore task current CoreMeta row is missing task payload"))?;
    Ok(TaskCurrentCoreMetaRow {
        task: task_record_from_proto(task)?,
        generation: common.root_generation,
        transaction_id: common.transaction_id,
        created_at_unix_nanos: common.created_at_unix_nanos,
    })
}

fn encode_deterministic_proto<M>(message: &M, label: &str) -> Result<Vec<u8>>
where
    M: Message + Default,
{
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    let decoded = M::decode(bytes.as_slice())?;
    let mut canonical = Vec::with_capacity(decoded.encoded_len());
    decoded.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} is not deterministic protobuf");
    }
    Ok(bytes)
}

#[cfg(test)]
fn decode_deterministic_proto<M>(bytes: &[u8], label: &str) -> Result<M>
where
    M: Message + Default,
{
    let value = M::decode(bytes)?;
    let mut canonical = Vec::with_capacity(value.encoded_len());
    value.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} is not deterministic protobuf");
    }
    Ok(value)
}

fn encode_task_journal_body(
    event: &TaskJournalBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let body = task_journal_body_to_proto(event, fence_token, mutation_id)?;
    encode_deterministic_proto(&body, "task audit body")
}

#[cfg(test)]
fn decode_task_journal_body(bytes: &[u8]) -> Result<TaskJournalBody> {
    task_journal_body_from_proto(decode_deterministic_proto(bytes, "task audit body")?)
}

#[cfg(test)]
fn decode_task_journal_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = decode_deterministic_proto::<TaskJournalBodyProto>(bytes, "task audit body")?;
    if proto.schema != TASK_JOURNAL_BODY_SCHEMA {
        bail!("CoreStore task audit body has invalid schema");
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("CoreStore task audit body has invalid mutation id"))?;
    Ok(proto.fence_token)
}

fn task_current_row_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(TASK_CURRENT_ROW_KEY_PREFIX)])
}

fn task_current_row_key(task_id: i64) -> Result<Vec<u8>> {
    if task_id < 0 {
        bail!("task current CoreMeta row task id must be nonnegative");
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(TASK_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::I64(task_id),
    ])
}

fn ensure_task_row_key_matches(encoded_core_meta_key: &[u8], task_id: i64) -> Result<()> {
    let tuple_key = core_meta_record_tuple_key(encoded_core_meta_key)?;
    let expected = task_current_row_key(task_id)?;
    if tuple_key != expected.as_slice() {
        bail!("CoreStore task current CoreMeta row scope mismatch");
    }
    Ok(())
}

fn task_queue_realm_id() -> &'static str {
    "anvil.system.task_queue"
}

fn current_unix_nanos() -> Result<u64> {
    let nanos = Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("current timestamp cannot be represented as unix nanoseconds"))?;
    u64::try_from(nanos).context("current unix timestamp is negative")
}

fn task_journal_body_to_proto(
    event: &TaskJournalBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<TaskJournalBodyProto> {
    let mut body = TaskJournalBodyProto {
        schema: TASK_JOURNAL_BODY_SCHEMA.to_string(),
        event: TaskJournalEventKindProto::Unspecified as i32,
        task: None,
        task_id: None,
        status: None,
        error: None,
        attempts: None,
        scheduled_at: None,
        updated_at: None,
        fence_token,
        mutation_id: mutation_id.to_string(),
    };
    match event {
        TaskJournalBody::Enqueued { task } => {
            body.event = TaskJournalEventKindProto::Enqueued as i32;
            body.task = Some(task_record_to_proto(task)?);
        }
        TaskJournalBody::Claimed {
            task_id,
            updated_at,
        } => {
            body.event = TaskJournalEventKindProto::Claimed as i32;
            body.task_id = Some(*task_id);
            body.updated_at = Some(updated_at.to_rfc3339());
        }
        TaskJournalBody::StatusUpdated {
            task_id,
            status,
            updated_at,
        } => {
            body.event = TaskJournalEventKindProto::StatusUpdated as i32;
            body.task_id = Some(*task_id);
            body.status = Some(task_status_to_proto(*status) as i32);
            body.updated_at = Some(updated_at.to_rfc3339());
        }
        TaskJournalBody::Failed {
            task_id,
            error,
            attempts,
            scheduled_at,
            updated_at,
        } => {
            body.event = TaskJournalEventKindProto::Failed as i32;
            body.task_id = Some(*task_id);
            body.error = Some(error.clone());
            body.attempts = Some(*attempts);
            body.scheduled_at = Some(scheduled_at.to_rfc3339());
            body.updated_at = Some(updated_at.to_rfc3339());
        }
    }
    Ok(body)
}

#[cfg(test)]
fn task_journal_body_from_proto(proto: TaskJournalBodyProto) -> Result<TaskJournalBody> {
    if proto.schema != TASK_JOURNAL_BODY_SCHEMA {
        bail!("CoreStore task audit body has invalid schema");
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("CoreStore task audit body has invalid mutation id"))?;
    let event = TaskJournalEventKindProto::try_from(proto.event)
        .map_err(|_| anyhow!("CoreStore task audit body has invalid event kind"))?;
    match event {
        TaskJournalEventKindProto::Unspecified => {
            bail!("CoreStore task audit body has unspecified event kind")
        }
        TaskJournalEventKindProto::Enqueued => {
            let task = proto
                .task
                .ok_or_else(|| anyhow!("CoreStore task enqueue audit body is missing task"))?;
            Ok(TaskJournalBody::Enqueued {
                task: task_record_from_proto(task)?,
            })
        }
        TaskJournalEventKindProto::Claimed => Ok(TaskJournalBody::Claimed {
            task_id: require_task_id(proto.task_id)?,
            updated_at: parse_task_time(proto.updated_at.as_deref(), "updated_at")?,
        }),
        TaskJournalEventKindProto::StatusUpdated => {
            Ok(TaskJournalBody::StatusUpdated {
                task_id: require_task_id(proto.task_id)?,
                status: task_status_from_proto_i32(proto.status.ok_or_else(|| {
                    anyhow!("CoreStore task status audit body is missing status")
                })?)?,
                updated_at: parse_task_time(proto.updated_at.as_deref(), "updated_at")?,
            })
        }
        TaskJournalEventKindProto::Failed => Ok(TaskJournalBody::Failed {
            task_id: require_task_id(proto.task_id)?,
            error: proto
                .error
                .ok_or_else(|| anyhow!("CoreStore task failure audit body is missing error"))?,
            attempts: proto
                .attempts
                .ok_or_else(|| anyhow!("CoreStore task failure audit body is missing attempts"))?,
            scheduled_at: parse_task_time(proto.scheduled_at.as_deref(), "scheduled_at")?,
            updated_at: parse_task_time(proto.updated_at.as_deref(), "updated_at")?,
        }),
    }
}

#[cfg(test)]
fn require_task_id(task_id: Option<i64>) -> Result<i64> {
    task_id.ok_or_else(|| anyhow!("CoreStore task audit body is missing task_id"))
}

fn task_record_to_proto(task: &TaskRecord) -> Result<TaskRecordProto> {
    Ok(TaskRecordProto {
        id: task.id,
        task_type: task_type_to_proto(task.task_type) as i32,
        payload: Some(json_value_to_proto(&task.payload)?),
        priority: task.priority,
        status: task_status_to_proto(task.status) as i32,
        attempts: task.attempts,
        last_error: task.last_error.clone(),
        scheduled_at: task.scheduled_at.to_rfc3339(),
        created_at: task.created_at.to_rfc3339(),
        updated_at: task.updated_at.to_rfc3339(),
    })
}

fn task_record_from_proto(proto: TaskRecordProto) -> Result<TaskRecord> {
    Ok(TaskRecord {
        id: proto.id,
        task_type: task_type_from_proto_i32(proto.task_type)?,
        payload: json_value_from_proto(
            proto
                .payload
                .ok_or_else(|| anyhow!("CoreStore task record is missing payload"))?,
        )?,
        priority: proto.priority,
        status: task_status_from_proto_i32(proto.status)?,
        attempts: proto.attempts,
        last_error: proto.last_error,
        scheduled_at: parse_required_task_time(&proto.scheduled_at, "scheduled_at")?,
        created_at: parse_required_task_time(&proto.created_at, "created_at")?,
        updated_at: parse_required_task_time(&proto.updated_at, "updated_at")?,
    })
}

fn task_type_to_proto(task_type: TaskType) -> TaskTypeProto {
    match task_type {
        TaskType::DeleteObject => TaskTypeProto::DeleteObject,
        TaskType::DeleteBucket => TaskTypeProto::DeleteBucket,
        TaskType::ObjectMetadataCompaction => TaskTypeProto::ObjectMetadataCompaction,
        TaskType::IndexBuild => TaskTypeProto::IndexBuild,
        TaskType::RebalanceShard => TaskTypeProto::RebalanceShard,
        TaskType::HFIngestion => TaskTypeProto::HfIngestion,
        TaskType::AuthzMaterialization => TaskTypeProto::AuthzMaterialization,
    }
}

fn task_type_from_proto_i32(value: i32) -> Result<TaskType> {
    Ok(
        match TaskTypeProto::try_from(value)
            .map_err(|_| anyhow!("CoreStore task record has invalid task type"))?
        {
            TaskTypeProto::Unspecified => bail!("CoreStore task record has unspecified task type"),
            TaskTypeProto::DeleteObject => TaskType::DeleteObject,
            TaskTypeProto::DeleteBucket => TaskType::DeleteBucket,
            TaskTypeProto::ObjectMetadataCompaction => TaskType::ObjectMetadataCompaction,
            TaskTypeProto::IndexBuild => TaskType::IndexBuild,
            TaskTypeProto::RebalanceShard => TaskType::RebalanceShard,
            TaskTypeProto::HfIngestion => TaskType::HFIngestion,
            TaskTypeProto::AuthzMaterialization => TaskType::AuthzMaterialization,
        },
    )
}

fn task_status_to_proto(status: TaskStatus) -> TaskStatusProto {
    match status {
        TaskStatus::Pending => TaskStatusProto::Pending,
        TaskStatus::Running => TaskStatusProto::Running,
        TaskStatus::Completed => TaskStatusProto::Completed,
        TaskStatus::Failed => TaskStatusProto::Failed,
    }
}

fn task_status_from_proto_i32(value: i32) -> Result<TaskStatus> {
    Ok(
        match TaskStatusProto::try_from(value)
            .map_err(|_| anyhow!("CoreStore task record has invalid status"))?
        {
            TaskStatusProto::Unspecified => bail!("CoreStore task record has unspecified status"),
            TaskStatusProto::Pending => TaskStatus::Pending,
            TaskStatusProto::Running => TaskStatus::Running,
            TaskStatusProto::Completed => TaskStatus::Completed,
            TaskStatusProto::Failed => TaskStatus::Failed,
        },
    )
}

fn json_value_to_proto(value: &JsonValue) -> Result<JsonValueProto> {
    let kind = match value {
        JsonValue::Null => json_value_proto::Kind::Null(true),
        JsonValue::Bool(value) => json_value_proto::Kind::Bool(*value),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                json_value_proto::Kind::I64(value)
            } else if let Some(value) = number.as_u64() {
                json_value_proto::Kind::U64(value)
            } else {
                json_value_proto::Kind::F64(number.as_f64().ok_or_else(|| {
                    anyhow!("CoreStore task JSON number cannot be represented deterministically")
                })?)
            }
        }
        JsonValue::String(value) => json_value_proto::Kind::String(value.clone()),
        JsonValue::Array(values) => json_value_proto::Kind::Array(JsonArrayProto {
            values: values
                .iter()
                .map(json_value_to_proto)
                .collect::<Result<Vec<_>>>()?,
        }),
        JsonValue::Object(map) => {
            let mut entries = map
                .iter()
                .map(|(key, value)| {
                    Ok(JsonObjectEntryProto {
                        key: key.clone(),
                        value: Some(json_value_to_proto(value)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            entries.sort_by(|left, right| left.key.cmp(&right.key));
            json_value_proto::Kind::Object(JsonObjectProto { entries })
        }
    };
    Ok(JsonValueProto { kind: Some(kind) })
}

fn json_value_from_proto(proto: JsonValueProto) -> Result<JsonValue> {
    let kind = proto
        .kind
        .ok_or_else(|| anyhow!("CoreStore task JSON value is missing kind"))?;
    Ok(match kind {
        json_value_proto::Kind::Null(_) => JsonValue::Null,
        json_value_proto::Kind::Bool(value) => JsonValue::Bool(value),
        json_value_proto::Kind::I64(value) => JsonValue::Number(value.into()),
        json_value_proto::Kind::U64(value) => JsonValue::Number(value.into()),
        json_value_proto::Kind::F64(value) => JsonValue::Number(
            serde_json::Number::from_f64(value)
                .ok_or_else(|| anyhow!("CoreStore task JSON f64 is not finite"))?,
        ),
        json_value_proto::Kind::String(value) => JsonValue::String(value),
        json_value_proto::Kind::Array(array) => JsonValue::Array(
            array
                .values
                .into_iter()
                .map(json_value_from_proto)
                .collect::<Result<Vec<_>>>()?,
        ),
        json_value_proto::Kind::Object(object) => {
            let mut map = serde_json::Map::new();
            for entry in object.entries {
                let value = entry
                    .value
                    .ok_or_else(|| anyhow!("CoreStore task JSON object entry is missing value"))?;
                map.insert(entry.key, json_value_from_proto(value)?);
            }
            JsonValue::Object(map)
        }
    })
}

#[cfg(test)]
fn parse_task_time(value: Option<&str>, field: &str) -> Result<DateTime<Utc>> {
    let value = value.ok_or_else(|| anyhow!("CoreStore task timestamp {field} is missing"))?;
    parse_required_task_time(value, field)
}

fn parse_required_task_time(value: &str, field: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("parse CoreStore task timestamp {field}"))?
        .with_timezone(&Utc))
}

impl TaskQueueState {
    fn next_task_id(&self) -> Result<i64> {
        self.tasks
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow!("task id overflow"))
    }

    fn tasks(&self) -> Vec<TaskRecord> {
        self.tasks.values().cloned().collect()
    }

    fn has_due_tasks(&self, now: DateTime<Utc>) -> bool {
        self.tasks.values().any(|task| {
            matches!(task.status, TaskStatus::Pending | TaskStatus::Failed)
                && task.scheduled_at <= now
        })
    }

    fn has_live_task(&self, task_type: &TaskType, payload: &JsonValue) -> bool {
        self.tasks.values().any(|task| {
            &task.task_type == task_type
                && &task.payload == payload
                && matches!(task.status, TaskStatus::Pending | TaskStatus::Running)
        })
    }

    fn index_build_tasks_for_payload(&self, payload: &JsonValue) -> Vec<TaskRecord> {
        let Some(key) = index_build_key(payload) else {
            return Vec::new();
        };
        let mut tasks = self
            .tasks
            .values()
            .filter(|task| {
                task.task_type == TaskType::IndexBuild
                    && matches!(task.status, TaskStatus::Pending | TaskStatus::Failed)
                    && index_build_key(&task.payload).as_ref() == Some(&key)
            })
            .cloned()
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| {
            json_u128(&left.payload, "source_cursor")
                .unwrap_or(0)
                .cmp(&json_u128(&right.payload, "source_cursor").unwrap_or(0))
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        tasks
    }

    fn authz_materialization_tasks_for_payload(&self, payload: &JsonValue) -> Vec<TaskRecord> {
        let Some(key) = authz_materialization_key(payload) else {
            return Vec::new();
        };
        let mut tasks = self
            .tasks
            .values()
            .filter(|task| {
                task.task_type == TaskType::AuthzMaterialization
                    && matches!(task.status, TaskStatus::Pending | TaskStatus::Failed)
                    && authz_materialization_key(&task.payload).as_ref() == Some(&key)
            })
            .cloned()
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| {
            json_u128(&left.payload, "target_revision")
                .unwrap_or(0)
                .cmp(&json_u128(&right.payload, "target_revision").unwrap_or(0))
                .then_with(|| left.created_at.cmp(&right.created_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        tasks
    }

    fn running_index_build_keys(&self) -> BTreeSet<IndexBuildKey> {
        self.tasks
            .values()
            .filter(|task| {
                task.task_type == TaskType::IndexBuild && task.status == TaskStatus::Running
            })
            .filter_map(|task| index_build_key(&task.payload))
            .collect()
    }

    fn running_authz_materialization_keys(&self) -> BTreeSet<AuthzMaterializationKey> {
        self.tasks
            .values()
            .filter(|task| {
                task.task_type == TaskType::AuthzMaterialization
                    && task.status == TaskStatus::Running
            })
            .filter_map(|task| authz_materialization_key(&task.payload))
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexBuildKey {
    tenant_id: i64,
    bucket_id: i64,
    index_id: i64,
    index_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AuthzMaterializationKey {
    tenant_id: i64,
}

fn validate_index_build_payload(payload: &JsonValue) -> Result<()> {
    index_build_key(payload).ok_or_else(|| {
        anyhow!(
            "index build payload must include tenant_id, bucket_id, index_id, and index_version"
        )
    })?;
    json_u128(payload, "source_cursor")
        .ok_or_else(|| anyhow!("index build source_cursor must be a nonnegative integer"))?;
    Ok(())
}

fn index_build_key(payload: &JsonValue) -> Option<IndexBuildKey> {
    Some(IndexBuildKey {
        tenant_id: json_i64(payload, "tenant_id")?,
        bucket_id: json_i64(payload, "bucket_id")?,
        index_id: json_i64(payload, "index_id")?,
        index_version: json_i64(payload, "index_version")?,
    })
}

fn validate_authz_materialization_payload(payload: &JsonValue) -> Result<()> {
    authz_materialization_key(payload)
        .ok_or_else(|| anyhow!("authz materialization payload must include tenant_id"))?;
    json_u128(payload, "target_revision").ok_or_else(|| {
        anyhow!("authz materialization payload must include nonnegative target_revision")
    })?;
    Ok(())
}

fn authz_materialization_key(payload: &JsonValue) -> Option<AuthzMaterializationKey> {
    Some(AuthzMaterializationKey {
        tenant_id: json_i64(payload, "tenant_id")?,
    })
}

fn json_i64(payload: &JsonValue, field: &str) -> Option<i64> {
    payload.get(field)?.as_i64()
}

fn json_u128(payload: &JsonValue, field: &str) -> Option<u128> {
    if let Some(value) = payload.get(field)?.as_u64() {
        return Some(u128::from(value));
    }
    payload.get(field)?.as_str()?.parse().ok()
}

pub fn task_queue_partition_id() -> Hash32 {
    hash32(b"task_queue/global")
}

fn task_queue_stream_id() -> String {
    "task_queue:global".to_string()
}

fn task_queue_partition_principal() -> String {
    "partition-owner:task_queue:global".to_string()
}

#[cfg(test)]
pub(crate) async fn read_task_frame_fences_for_test(storage: &Storage) -> Result<Vec<u64>> {
    read_task_journal_payload_fences(storage).await
}

#[cfg(test)]
async fn read_task_journal_payload_fences(storage: &Storage) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(ReadStream {
            stream_id: task_queue_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?
        .into_iter()
        .filter(|record| record.record_kind == TASK_QUEUE_AUDIT_RECORD_KIND)
        .map(|record| decode_task_journal_body_fence(&record.payload))
        .collect::<Result<Vec<_>>>()?)
}

fn require_task_queue_permit(permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "task_queue"
        || permit.partition_id != hex::encode(task_queue_partition_id())
    {
        anyhow::bail!("task queue write permit targets a different partition");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
