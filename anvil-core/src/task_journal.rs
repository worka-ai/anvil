use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::persistence::TaskRecord;
use crate::storage::Storage;
use crate::tasks::{TaskStatus, TaskType};
use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize)]
struct TaskJournalHeader<'a> {
    partition_family: &'static str,
    partition_id: &'static str,
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

pub async fn enqueue_task(
    storage: &Storage,
    task_type: TaskType,
    payload: JsonValue,
    priority: i32,
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
    append_task_event(storage, TaskJournalBody::Enqueued { task }).await
}

pub async fn claim_pending_tasks(storage: &Storage, limit: i64) -> Result<Vec<TaskRecord>> {
    let state = read_task_queue_state(storage).await?;
    let now = Utc::now();
    let mut tasks = state
        .tasks
        .values()
        .filter(|task| task.status == TaskStatus::Pending && task.scheduled_at <= now)
        .cloned()
        .collect::<Vec<_>>();
    tasks.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| a.created_at.cmp(&b.created_at))
            .then_with(|| a.id.cmp(&b.id))
    });
    tasks.truncate(limit.max(0) as usize);
    for task in &tasks {
        append_task_event(
            storage,
            TaskJournalBody::Claimed {
                task_id: task.id,
                updated_at: now,
            },
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

pub async fn update_task_status(storage: &Storage, task_id: i64, status: TaskStatus) -> Result<()> {
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
    )
    .await
}

pub async fn fail_task(storage: &Storage, task_id: i64, error: &str) -> Result<()> {
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

async fn append_task_event(storage: &Storage, event: TaskJournalBody) -> Result<()> {
    let path = storage.task_queue_journal_path();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path).await?;
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
        .with_context(|| format!("open task queue journal {}", path.display()))?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn ensure_journal_header(path: &Path) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&TaskJournalHeader {
        partition_family: "task_queue",
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
}

fn event_key_hash(event: &TaskJournalBody) -> Hash32 {
    match event {
        TaskJournalBody::Enqueued { task } => hash32(format!("task/{}", task.id).as_bytes()),
        TaskJournalBody::Claimed { task_id, .. }
        | TaskJournalBody::StatusUpdated { task_id, .. }
        | TaskJournalBody::Failed { task_id, .. } => hash32(format!("task/{task_id}").as_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

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
}
