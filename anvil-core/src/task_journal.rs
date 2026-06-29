use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::partition_fence::{PartitionWritePermit, validate_partition_write};
use crate::persistence::TaskRecord;
use crate::storage::Storage;
use crate::tasks::{TaskStatus, TaskType};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize)]
struct TaskJournalHeader<'a> {
    partition_family: &'static str,
    partition_id: String,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
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

#[cfg(test)]
async fn enqueue_task(
    storage: &Storage,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
) -> Result<()> {
    enqueue_task_inner(storage, task_type, payload, priority, 0).await
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
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    enqueue_task_inner(storage, task_type, payload, priority, permit.fence_token).await
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
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    let state = read_task_queue_state(storage).await?;
    if state.has_live_task(&task_type, &payload) {
        return Ok(false);
    }
    enqueue_task_inner(storage, task_type, payload, priority, permit.fence_token)
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
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
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
    append_task_event(storage, TaskJournalBody::Enqueued { task }, fence_token).await
}

#[cfg(test)]
async fn claim_pending_tasks(storage: &Storage, limit: i64) -> Result<Vec<TaskRecord>> {
    claim_pending_tasks_inner(storage, limit, 0).await
}

pub(crate) async fn claim_pending_tasks_with_permit(
    storage: &Storage,
    limit: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<Vec<TaskRecord>> {
    require_task_queue_permit(permit)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    claim_pending_tasks_inner(storage, limit, permit.fence_token).await
}

async fn claim_pending_tasks_inner(
    storage: &Storage,
    limit: i64,
    fence_token: u64,
) -> Result<Vec<TaskRecord>> {
    let state = read_task_queue_state(storage).await?;
    let now = Utc::now();
    let running_index_build_keys = state.running_index_build_keys();
    let mut selected_index_build_keys = BTreeSet::new();
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
        if let Some(key) = index_build_key(&task.payload) {
            if running_index_build_keys.contains(&key) || selected_index_build_keys.contains(&key) {
                continue;
            }
            selected_index_build_keys.insert(key);
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

#[cfg(test)]
async fn update_task_status(storage: &Storage, task_id: i64, status: TaskStatus) -> Result<()> {
    update_task_status_inner(storage, task_id, status, 0).await
}

pub(crate) async fn update_task_status_with_permit(
    storage: &Storage,
    task_id: i64,
    status: TaskStatus,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_task_queue_permit(permit)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    update_task_status_inner(storage, task_id, status, permit.fence_token).await
}

async fn update_task_status_inner(
    storage: &Storage,
    task_id: i64,
    status: TaskStatus,
    fence_token: u64,
) -> Result<()> {
    if !read_task_queue_state(storage)
        .await?
        .tasks
        .contains_key(&task_id)
    {
        return Ok(());
    }
    append_task_event(
        storage,
        TaskJournalBody::StatusUpdated {
            task_id,
            status,
            updated_at: Utc::now(),
        },
        fence_token,
    )
    .await
}

#[cfg(test)]
async fn fail_task(storage: &Storage, task_id: i64, error: &str) -> Result<()> {
    fail_task_inner(storage, task_id, error, 0).await
}

pub(crate) async fn fail_task_with_permit(
    storage: &Storage,
    task_id: i64,
    error: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_task_queue_permit(permit)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    fail_task_inner(storage, task_id, error, permit.fence_token).await
}

async fn fail_task_inner(
    storage: &Storage,
    task_id: i64,
    error: &str,
    fence_token: u64,
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
    )
    .await
}

async fn read_task_queue_state(storage: &Storage) -> Result<TaskQueueState> {
    let frames = read_task_journal_frames_at_path(&storage.task_queue_journal_path()).await?;
    let mut state = TaskQueueState::default();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::TaskQueue {
            continue;
        }
        let body: TaskJournalBody = serde_json::from_slice(&frame.body)?;
        state.apply(body);
    }
    Ok(state)
}

async fn append_task_event(
    storage: &Storage,
    event: TaskJournalBody,
    fence_token: u64,
) -> Result<()> {
    let path = storage.task_queue_journal_path();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path, fence_token).await?;
    let previous = read_task_journal_frames_at_path(path.as_path())
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
        JournalRecordKind::TaskQueue,
        sequence,
        fence_token,
        *mutation_id.as_bytes(),
        key_hash,
        previous_hash,
        serde_json::to_vec(&event)?,
    );
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open task queue journal {}", path.display()))?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn ensure_journal_header(path: &Path, fence_token: u64) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&TaskJournalHeader {
        partition_family: "task_queue",
        partition_id: hex::encode(task_queue_partition_id()),
        fence_token,
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
        .with_context(|| format!("create task queue journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn read_task_journal_frames_at_path(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read task queue journal {}", path.display()))?;
    decode_journal_file(&bytes)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("task queue journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated task queue journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("invalid task queue journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated task queue journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

impl TaskQueueState {
    fn apply(&mut self, event: TaskJournalBody) {
        match event {
            TaskJournalBody::Enqueued { task } => {
                self.tasks.insert(task.id, task);
            }
            TaskJournalBody::Claimed {
                task_id,
                updated_at,
            } => {
                if let Some(task) = self.tasks.get_mut(&task_id) {
                    task.status = TaskStatus::Running;
                    task.updated_at = updated_at;
                }
            }
            TaskJournalBody::StatusUpdated {
                task_id,
                status,
                updated_at,
            } => {
                if let Some(task) = self.tasks.get_mut(&task_id) {
                    task.status = status;
                    task.updated_at = updated_at;
                }
            }
            TaskJournalBody::Failed {
                task_id,
                error,
                attempts,
                scheduled_at,
                updated_at,
            } => {
                if let Some(task) = self.tasks.get_mut(&task_id) {
                    task.status = TaskStatus::Failed;
                    task.last_error = Some(error);
                    task.attempts = attempts;
                    task.scheduled_at = scheduled_at;
                    task.updated_at = updated_at;
                }
            }
        }
    }

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

    fn running_index_build_keys(&self) -> BTreeSet<IndexBuildKey> {
        self.tasks
            .values()
            .filter(|task| {
                task.task_type == TaskType::IndexBuild && task.status == TaskStatus::Running
            })
            .filter_map(|task| index_build_key(&task.payload))
            .collect()
    }
}

fn event_key_hash(event: &TaskJournalBody) -> Hash32 {
    match event {
        TaskJournalBody::Enqueued { task } => hash32(format!("task/{}", task.id).as_bytes()),
        TaskJournalBody::Claimed { task_id, .. }
        | TaskJournalBody::StatusUpdated { task_id, .. }
        | TaskJournalBody::Failed { task_id, .. } => hash32(format!("task/{task_id}").as_bytes()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IndexBuildKey {
    tenant_id: i64,
    bucket_id: i64,
    index_id: i64,
    index_version: i64,
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

fn require_task_queue_permit(permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "task_queue"
        || permit.partition_id != hex::encode(task_queue_partition_id())
    {
        anyhow::bail!("task queue write permit targets a different partition");
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

    const KEY: &[u8] = b"task queue partition owner key";

    #[tokio::test]
    async fn task_journal_claims_and_replays_queue_state() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        enqueue_task(
            &storage,
            TaskType::DeleteBucket,
            json!({"bucket_id": 7}),
            100,
        )
        .await
        .unwrap();
        enqueue_task(
            &storage,
            TaskType::DeleteObject,
            json!({"object_id": 9}),
            10,
        )
        .await
        .unwrap();

        let claimed = claim_pending_tasks(&storage, 1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, 2);
        assert_eq!(claimed[0].status, TaskStatus::Running);

        fail_task(&storage, claimed[0].id, "boom").await.unwrap();
        update_task_status(&storage, 1, TaskStatus::Completed)
            .await
            .unwrap();

        let tasks = list_tasks(&storage).await.unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].status, TaskStatus::Completed);
        assert_eq!(tasks[1].status, TaskStatus::Failed);
        assert_eq!(tasks[1].attempts, 1);
        assert_eq!(tasks[1].last_error.as_deref(), Some("boom"));
    }

    #[tokio::test]
    async fn task_journal_reader_fails_closed_on_tampered_frame() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        enqueue_task(
            &storage,
            TaskType::DeleteBucket,
            json!({"bucket_id": 7}),
            100,
        )
        .await
        .unwrap();

        let path = storage.task_queue_journal_path();
        let mut bytes = tokio::fs::read(&path).await.unwrap();
        let header = BinaryEnvelopeHeader::decode(&bytes).unwrap();
        let body_start = COMMON_HEADER_LEN
            .checked_add(header.header_json.len())
            .and_then(|offset| offset.checked_add(4))
            .and_then(|offset| offset.checked_add(140))
            .expect("frame body offset");
        bytes[body_start] ^= 0x55;
        tokio::fs::write(&path, bytes).await.unwrap();

        let err = list_tasks(&storage)
            .await
            .expect_err("tampered task queue journal must not replay partial state");
        assert!(
            err.to_string().contains("hash mismatch"),
            "unexpected tamper error: {err}"
        );
    }

    #[tokio::test]
    pub(crate) async fn task_journal_with_permit_writes_fenced_frames_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();

        enqueue_task_with_permit(
            &storage,
            TaskType::DeleteBucket,
            json!({"bucket_id": 7}),
            100,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        let claimed = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
            .await
            .unwrap();
        update_task_status_with_permit(
            &storage,
            claimed[0].id,
            TaskStatus::Completed,
            &permit,
            KEY,
        )
        .await
        .unwrap();

        let journal = tokio::fs::read(storage.task_queue_journal_path())
            .await
            .unwrap();
        let header = BinaryEnvelopeHeader::decode(&journal).unwrap();
        let header_json: serde_json::Value = serde_json::from_slice(&header.header_json).unwrap();
        assert_eq!(header_json["partition_family"], "task_queue");
        assert_eq!(header_json["partition_id"], permit.partition_id);
        assert_eq!(header_json["fence_token"], permit.fence_token);

        let frames = decode_journal_file(&journal).unwrap();
        assert_eq!(frames.len(), 3);
        assert!(
            frames
                .iter()
                .all(|frame| frame.fence_token == permit.fence_token)
        );
    }

    #[tokio::test]
    pub(crate) async fn task_journal_deduplicates_live_tasks_but_allows_new_after_completion() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();
        let payload = json!({"bucket_id": 7});

        assert!(
            enqueue_task_if_absent_with_permit(
                &storage,
                TaskType::ObjectMetadataCompaction,
                payload.clone(),
                50,
                &permit,
                KEY,
            )
            .await
            .unwrap()
        );
        assert!(
            !enqueue_task_if_absent_with_permit(
                &storage,
                TaskType::ObjectMetadataCompaction,
                payload.clone(),
                50,
                &permit,
                KEY,
            )
            .await
            .unwrap()
        );
        assert_eq!(list_tasks(&storage).await.unwrap().len(), 1);

        let claimed = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
            .await
            .unwrap();
        update_task_status_with_permit(
            &storage,
            claimed[0].id,
            TaskStatus::Completed,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        assert!(
            enqueue_task_if_absent_with_permit(
                &storage,
                TaskType::ObjectMetadataCompaction,
                payload,
                50,
                &permit,
                KEY,
            )
            .await
            .unwrap()
        );
        assert_eq!(list_tasks(&storage).await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn task_journal_supersedes_pending_index_build_tasks_by_index_identity() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();
        let payload_cursor_2 = json!({
            "tenant_id": 1,
            "bucket_id": 7,
            "index_id": 9,
            "index_version": 3,
            "source_cursor": 2,
        });
        let payload_cursor_5 = json!({
            "tenant_id": 1,
            "bucket_id": 7,
            "index_id": 9,
            "index_version": 3,
            "source_cursor": 5,
        });

        assert!(
            enqueue_index_build_task_with_permit(&storage, payload_cursor_2, 40, &permit, KEY,)
                .await
                .unwrap()
        );
        assert!(
            enqueue_index_build_task_with_permit(&storage, payload_cursor_5, 40, &permit, KEY,)
                .await
                .unwrap(),
            "newer cursor should supersede the pending task with admitted task events"
        );
        let tasks = list_tasks(&storage).await.unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].status, TaskStatus::Completed);
        assert_eq!(tasks[0].payload["source_cursor"], json!(2));
        assert_eq!(tasks[1].status, TaskStatus::Pending);
        assert_eq!(tasks[1].payload["source_cursor"], json!(5));

        assert!(
            !enqueue_index_build_task_with_permit(
                &storage,
                json!({
                    "tenant_id": 1,
                    "bucket_id": 7,
                    "index_id": 9,
                    "index_version": 3,
                    "source_cursor": 4,
                }),
                40,
                &permit,
                KEY,
            )
            .await
            .unwrap(),
            "older cursor is already covered by the pending build"
        );

        let claimed = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].payload["source_cursor"], json!(5));

        let journal = tokio::fs::read(storage.task_queue_journal_path())
            .await
            .unwrap();
        for frame in decode_journal_file(&journal).unwrap() {
            let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
            assert!(
                matches!(
                    body["event"].as_str(),
                    Some("enqueued" | "claimed" | "status_updated" | "failed")
                ),
                "unexpected task queue event: {body:?}"
            );
        }
    }

    #[tokio::test]
    async fn task_journal_serializes_running_and_followup_index_builds() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();
        let payload_cursor_2 = json!({
            "tenant_id": 1,
            "bucket_id": 7,
            "index_id": 9,
            "index_version": 3,
            "source_cursor": 2,
        });
        let payload_cursor_8 = json!({
            "tenant_id": 1,
            "bucket_id": 7,
            "index_id": 9,
            "index_version": 3,
            "source_cursor": 8,
        });

        enqueue_index_build_task_with_permit(&storage, payload_cursor_2, 40, &permit, KEY)
            .await
            .unwrap();
        let first_claim = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
            .await
            .unwrap();
        assert_eq!(first_claim.len(), 1);
        assert_eq!(first_claim[0].payload["source_cursor"], json!(2));

        enqueue_index_build_task_with_permit(&storage, payload_cursor_8, 40, &permit, KEY)
            .await
            .unwrap();
        let blocked_by_running = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
            .await
            .unwrap();
        assert!(
            blocked_by_running.is_empty(),
            "pending follow-up for the same index must not run beside an active build"
        );

        update_task_status_with_permit(
            &storage,
            first_claim[0].id,
            TaskStatus::Completed,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        let followup = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
            .await
            .unwrap();
        assert_eq!(followup.len(), 1);
        assert_eq!(followup[0].payload["source_cursor"], json!(8));
    }

    #[tokio::test]
    pub(crate) async fn task_journal_reclaims_failed_tasks_after_retry_delay() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();

        enqueue_task_with_permit(
            &storage,
            TaskType::DeleteBucket,
            json!({"bucket_id": 7}),
            100,
            &permit,
            KEY,
        )
        .await
        .unwrap();
        let first_claim = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
            .await
            .unwrap();
        fail_task_with_permit(&storage, first_claim[0].id, "try again", &permit, KEY)
            .await
            .unwrap();
        let not_ready = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
            .await
            .unwrap();
        assert!(not_ready.is_empty());

        let mut state = read_task_queue_state(&storage).await.unwrap();
        let task = state.tasks.get_mut(&first_claim[0].id).unwrap();
        task.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
        append_task_event(
            &storage,
            TaskJournalBody::Failed {
                task_id: task.id,
                error: task.last_error.clone().unwrap(),
                attempts: task.attempts,
                scheduled_at: task.scheduled_at,
                updated_at: Utc::now(),
            },
            permit.fence_token,
        )
        .await
        .unwrap();

        let retried = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
            .await
            .unwrap();
        assert_eq!(retried.len(), 1);
        assert_eq!(retried[0].id, first_claim[0].id);
        assert_eq!(retried[0].status, TaskStatus::Running);
        assert_eq!(retried[0].attempts, 1);
    }

    #[tokio::test]
    pub(crate) async fn task_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = enqueue_task_with_permit(
            &storage,
            TaskType::DeleteBucket,
            json!({"bucket_id": 7}),
            100,
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

    async fn ready_owner(
        storage: &Storage,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "task_queue".to_string();
        let id = hex::encode(task_queue_partition_id());
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
