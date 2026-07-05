use crate::{
    core_store::{AppendStreamRecord, CoreStore, ReadStream},
    formats::{Hash32, hash32, watch::WatchRecord},
    storage::Storage,
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

const GIT_SOURCE_PARTITION_FAMILY: u16 = 6;
const GIT_SOURCE_RECORD_KIND: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitSourceWatchPayload {
    pub repository_id: String,
    pub event_type: String,
    pub generation: u64,
    pub source_hash: String,
    pub index_path: String,
    pub pack_object_version_id: Option<String>,
    pub emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitSourceWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub index_generation: u64,
    pub payload: GitSourceWatchPayload,
}

pub async fn append_git_source_watch_record(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
    cursor: u128,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: GitSourceWatchPayload,
) -> Result<()> {
    validate_payload(repository_id, &payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = git_source_watch_stream_id(tenant_id, repository_id);
    ensure_cursor_is_monotonic(&core_store, &stream_id, cursor).await?;

    let record = WatchRecord::new(
        cursor,
        GIT_SOURCE_PARTITION_FAMILY,
        partition_id(tenant_id, repository_id),
        mutation_id,
        GIT_SOURCE_RECORD_KIND,
        authz_revision,
        payload.generation,
        0,
        serde_json::to_vec(&payload)?,
    );
    core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(partition_id(tenant_id, repository_id)),
            record_kind: "git_source_watch".to_string(),
            payload: record.encode(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "git-source-watch:{tenant_id}:{repository_id}:{cursor}"
            )),
        })
        .await?;
    Ok(())
}

pub async fn list_git_source_watch_events(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<GitSourceWatchEvent>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = read_watch_or_empty(
        &core_store,
        &git_source_watch_stream_id(tenant_id, repository_id),
    )
    .await?;
    let mut events = Vec::new();
    for record in records {
        if record.cursor <= after_cursor {
            continue;
        }
        if record.partition_family != GIT_SOURCE_PARTITION_FAMILY
            || record.record_kind != GIT_SOURCE_RECORD_KIND
            || record.partition_id != partition_id(tenant_id, repository_id)
        {
            continue;
        }
        let payload: GitSourceWatchPayload = serde_json::from_slice(&record.payload)?;
        validate_payload(repository_id, &payload)?;
        events.push(GitSourceWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            index_generation: record.index_generation,
            payload,
        });
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

pub async fn latest_git_source_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<u128>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = read_watch_or_empty(
        &core_store,
        &git_source_watch_stream_id(tenant_id, repository_id),
    )
    .await?;
    Ok(records
        .into_iter()
        .filter(|record| {
            record.partition_family == GIT_SOURCE_PARTITION_FAMILY
                && record.record_kind == GIT_SOURCE_RECORD_KIND
                && record.partition_id == partition_id(tenant_id, repository_id)
        })
        .map(|record| record.cursor)
        .max())
}

async fn ensure_cursor_is_monotonic(
    core_store: &CoreStore,
    stream_id: &str,
    cursor: u128,
) -> Result<()> {
    let records = read_watch_or_empty(core_store, stream_id).await?;
    if let Some(latest) = records.iter().map(|record| record.cursor).max()
        && cursor <= latest
    {
        return Err(anyhow!("git source watch cursor must be monotonic"));
    }
    Ok(())
}

async fn read_watch_or_empty(core_store: &CoreStore, stream_id: &str) -> Result<Vec<WatchRecord>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    records
        .into_iter()
        .filter(|record| record.record_kind == "git_source_watch")
        .map(|record| {
            WatchRecord::decode(&record.payload)
                .map(|(record, _)| record)
                .map_err(Into::into)
        })
        .collect()
}

fn validate_payload(repository_id: &str, payload: &GitSourceWatchPayload) -> Result<()> {
    if payload.repository_id != repository_id {
        return Err(anyhow!("git source watch payload repository mismatch"));
    }
    if payload.event_type.is_empty()
        || payload.index_path.is_empty()
        || payload.emitted_at.is_empty()
    {
        return Err(anyhow!("git source watch payload is incomplete"));
    }
    validate_hex32(&payload.source_hash, "source_hash")?;
    if payload
        .pack_object_version_id
        .as_deref()
        .is_some_and(str::is_empty)
    {
        return Err(anyhow!("pack_object_version_id must not be empty"));
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn partition_id(tenant_id: i64, repository_id: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:git:{repository_id}:watch:source").as_bytes())
}

fn git_source_watch_stream_id(tenant_id: i64, repository_id: &str) -> String {
    format!("watch:git_source:tenant:{tenant_id}:repository:{repository_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn git_source_watch_appends_lists_and_tracks_latest_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_git_source_watch_record(&storage, 9, "repo-alpha", 5, [1; 16], 2, payload(1))
            .await
            .unwrap();
        append_git_source_watch_record(&storage, 9, "repo-alpha", 6, [2; 16], 3, payload(2))
            .await
            .unwrap();

        assert_eq!(
            git_source_watch_stream_id(9, "repo-alpha"),
            "watch:git_source:tenant:9:repository:repo-alpha"
        );
        let events = list_git_source_watch_events(&storage, 9, "repo-alpha", 5, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 6);
        assert_eq!(events[0].index_generation, 2);
        assert_eq!(events[0].payload.generation, 2);
        assert_eq!(
            latest_git_source_watch_cursor(&storage, 9, "repo-alpha")
                .await
                .unwrap(),
            Some(6)
        );
    }

    #[tokio::test]
    async fn git_source_watch_rejects_non_monotonic_cursor_and_bad_payload() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_git_source_watch_record(&storage, 9, "repo-alpha", 5, [1; 16], 2, payload(1))
            .await
            .unwrap();
        assert!(
            append_git_source_watch_record(&storage, 9, "repo-alpha", 5, [2; 16], 2, payload(2))
                .await
                .is_err()
        );

        let mut bad = payload(3);
        bad.repository_id = "repo-beta".to_string();
        assert!(
            append_git_source_watch_record(&storage, 9, "repo-alpha", 6, [3; 16], 2, bad)
                .await
                .is_err()
        );
    }

    fn payload(generation: u64) -> GitSourceWatchPayload {
        GitSourceWatchPayload {
            repository_id: "repo-alpha".to_string(),
            event_type: "index_published".to_string(),
            generation,
            source_hash: hex::encode([generation as u8; 32]),
            index_path: format!(
                "_anvil/git/tenants/tenant-9/repositories/repo-alpha/indexes/generation-{generation:020}-source.angit"
            ),
            pack_object_version_id: Some("00000000-0000-0000-0000-000000000001".to_string()),
            emitted_at: "2026-06-27T00:00:00.000000000Z".to_string(),
        }
    }
}
