use crate::{
    core_store::{AppendStreamRecord, CoreStore, ReadStream},
    formats::{Hash32, hash32, watch::WatchRecord},
    storage::Storage,
};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

const PERSONALDB_GROUP_PARTITION_FAMILY: u16 = 4;
const PERSONALDB_GROUP_RECORD_KIND: u16 = 1;
const PERSONALDB_PROJECTION_PARTITION_FAMILY: u16 = 5;
const PERSONALDB_PROJECTION_RECORD_KIND: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbGroupWatchPayload {
    pub database_id: String,
    pub event_type: String,
    pub log_index: u64,
    pub log_hash: String,
    pub changeset_payload_hash: String,
    pub certificate_hash: String,
    pub committed_head_hash: String,
    pub emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbGroupWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub payload: PersonalDbGroupWatchPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbProjectionWatchPayload {
    pub database_id: String,
    pub projection_id: String,
    pub event_type: String,
    pub source_database_id: String,
    pub source_log_index: u64,
    pub source_log_hash: String,
    pub projection_log_index: u64,
    pub projection_log_hash: String,
    pub definition_hash: String,
    pub emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbProjectionWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub payload: PersonalDbProjectionWatchPayload,
}

pub async fn append_personaldb_group_watch_record(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    cursor: u128,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: PersonalDbGroupWatchPayload,
) -> Result<()> {
    validate_payload(database_id, &payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = personaldb_group_watch_stream_id(tenant_id, database_id);
    ensure_cursor_is_monotonic(&core_store, &stream_id, "personaldb_group_watch", cursor).await?;

    let record = WatchRecord::new(
        cursor,
        PERSONALDB_GROUP_PARTITION_FAMILY,
        partition_id(tenant_id, database_id),
        mutation_id,
        PERSONALDB_GROUP_RECORD_KIND,
        authz_revision,
        0,
        payload.log_index,
        serde_json::to_vec(&payload)?,
    );
    core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(partition_id(tenant_id, database_id)),
            record_kind: "personaldb_group_watch".to_string(),
            payload: record.encode(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "personaldb-group-watch:{tenant_id}:{database_id}:{cursor}"
            )),
        })
        .await?;
    Ok(())
}

pub async fn append_personaldb_projection_watch_record(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    cursor: u128,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: PersonalDbProjectionWatchPayload,
) -> Result<()> {
    validate_projection_payload(database_id, projection_id, &payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = personaldb_projection_watch_stream_id(tenant_id, database_id, projection_id);
    ensure_cursor_is_monotonic(
        &core_store,
        &stream_id,
        "personaldb_projection_watch",
        cursor,
    )
    .await?;

    let record = WatchRecord::new(
        cursor,
        PERSONALDB_PROJECTION_PARTITION_FAMILY,
        projection_partition_id(tenant_id, database_id, projection_id),
        mutation_id,
        PERSONALDB_PROJECTION_RECORD_KIND,
        authz_revision,
        0,
        payload.projection_log_index,
        serde_json::to_vec(&payload)?,
    );
    core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(projection_partition_id(
                tenant_id,
                database_id,
                projection_id,
            )),
            record_kind: "personaldb_projection_watch".to_string(),
            payload: record.encode(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "personaldb-projection-watch:{tenant_id}:{database_id}:{projection_id}:{cursor}"
            )),
        })
        .await?;
    Ok(())
}

pub async fn list_personaldb_group_watch_events(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<PersonalDbGroupWatchEvent>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = read_watch_or_empty(
        &core_store,
        &personaldb_group_watch_stream_id(tenant_id, database_id),
        "personaldb_group_watch",
    )
    .await?;
    let mut events = Vec::new();
    for record in records {
        if record.cursor <= after_cursor {
            continue;
        }
        if record.partition_family != PERSONALDB_GROUP_PARTITION_FAMILY
            || record.record_kind != PERSONALDB_GROUP_RECORD_KIND
            || record.partition_id != partition_id(tenant_id, database_id)
        {
            continue;
        }
        let payload: PersonalDbGroupWatchPayload = serde_json::from_slice(&record.payload)?;
        validate_payload(database_id, &payload)?;
        events.push(PersonalDbGroupWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            payload,
        });
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

pub async fn list_personaldb_projection_watch_events(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<PersonalDbProjectionWatchEvent>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = read_watch_or_empty(
        &core_store,
        &personaldb_projection_watch_stream_id(tenant_id, database_id, projection_id),
        "personaldb_projection_watch",
    )
    .await?;
    let mut events = Vec::new();
    for record in records {
        if record.cursor <= after_cursor {
            continue;
        }
        if record.partition_family != PERSONALDB_PROJECTION_PARTITION_FAMILY
            || record.record_kind != PERSONALDB_PROJECTION_RECORD_KIND
            || record.partition_id != projection_partition_id(tenant_id, database_id, projection_id)
        {
            continue;
        }
        let payload: PersonalDbProjectionWatchPayload = serde_json::from_slice(&record.payload)?;
        validate_projection_payload(database_id, projection_id, &payload)?;
        events.push(PersonalDbProjectionWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            payload,
        });
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

pub async fn latest_personaldb_group_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<Option<u128>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = read_watch_or_empty(
        &core_store,
        &personaldb_group_watch_stream_id(tenant_id, database_id),
        "personaldb_group_watch",
    )
    .await?;
    Ok(records
        .into_iter()
        .filter(|record| {
            record.partition_family == PERSONALDB_GROUP_PARTITION_FAMILY
                && record.record_kind == PERSONALDB_GROUP_RECORD_KIND
                && record.partition_id == partition_id(tenant_id, database_id)
        })
        .map(|record| record.cursor)
        .max())
}

pub async fn latest_personaldb_projection_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> Result<Option<u128>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = read_watch_or_empty(
        &core_store,
        &personaldb_projection_watch_stream_id(tenant_id, database_id, projection_id),
        "personaldb_projection_watch",
    )
    .await?;
    Ok(records
        .into_iter()
        .filter(|record| {
            record.partition_family == PERSONALDB_PROJECTION_PARTITION_FAMILY
                && record.record_kind == PERSONALDB_PROJECTION_RECORD_KIND
                && record.partition_id
                    == projection_partition_id(tenant_id, database_id, projection_id)
        })
        .map(|record| record.cursor)
        .max())
}

async fn ensure_cursor_is_monotonic(
    core_store: &CoreStore,
    stream_id: &str,
    record_kind: &str,
    cursor: u128,
) -> Result<()> {
    let records = read_watch_or_empty(core_store, stream_id, record_kind).await?;
    if let Some(latest) = records.iter().map(|record| record.cursor).max()
        && cursor <= latest
    {
        return Err(anyhow!("personaldb watch cursor must be monotonic"));
    }
    Ok(())
}

async fn read_watch_or_empty(
    core_store: &CoreStore,
    stream_id: &str,
    record_kind: &str,
) -> Result<Vec<WatchRecord>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    records
        .into_iter()
        .filter(|record| record.record_kind == record_kind)
        .map(|record| {
            WatchRecord::decode(&record.payload)
                .map(|(record, _)| record)
                .map_err(Into::into)
        })
        .collect()
}

fn validate_payload(database_id: &str, payload: &PersonalDbGroupWatchPayload) -> Result<()> {
    if payload.database_id != database_id {
        return Err(anyhow!("personaldb watch payload database mismatch"));
    }
    if payload.event_type.is_empty() || payload.emitted_at.is_empty() {
        return Err(anyhow!("personaldb watch payload is incomplete"));
    }
    validate_hex32(&payload.log_hash, "log_hash")?;
    validate_hex32(&payload.changeset_payload_hash, "changeset_payload_hash")?;
    validate_hex32(&payload.certificate_hash, "certificate_hash")?;
    validate_hex32(&payload.committed_head_hash, "committed_head_hash")?;
    Ok(())
}

fn validate_projection_payload(
    database_id: &str,
    projection_id: &str,
    payload: &PersonalDbProjectionWatchPayload,
) -> Result<()> {
    if payload.database_id != database_id || payload.projection_id != projection_id {
        return Err(anyhow!(
            "personaldb projection watch payload scope mismatch"
        ));
    }
    if payload.event_type.is_empty()
        || payload.source_database_id.is_empty()
        || payload.emitted_at.is_empty()
    {
        return Err(anyhow!("personaldb projection watch payload is incomplete"));
    }
    validate_hex32(&payload.source_log_hash, "source_log_hash")?;
    validate_hex32(&payload.projection_log_hash, "projection_log_hash")?;
    validate_hex32(&payload.definition_hash, "definition_hash")?;
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn partition_id(tenant_id: i64, database_id: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:personaldb:{database_id}:watch:group").as_bytes())
}

fn projection_partition_id(tenant_id: i64, database_id: &str, projection_id: &str) -> Hash32 {
    hash32(
        format!("tenant:{tenant_id}:personaldb:{database_id}:projection:{projection_id}:watch")
            .as_bytes(),
    )
}

fn personaldb_group_watch_stream_id(tenant_id: i64, database_id: &str) -> String {
    format!("watch:personaldb_group:tenant:{tenant_id}:database:{database_id}")
}

fn personaldb_projection_watch_stream_id(
    tenant_id: i64,
    database_id: &str,
    projection_id: &str,
) -> String {
    format!(
        "watch:personaldb_projection:tenant:{tenant_id}:database:{database_id}:projection:{projection_id}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn personaldb_group_watch_appends_lists_and_tracks_latest_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_personaldb_group_watch_record(&storage, 4, "db-alpha", 10, [1; 16], 7, payload(1))
            .await
            .unwrap();
        append_personaldb_group_watch_record(&storage, 4, "db-alpha", 11, [2; 16], 8, payload(2))
            .await
            .unwrap();

        assert_eq!(
            personaldb_group_watch_stream_id(4, "db-alpha"),
            "watch:personaldb_group:tenant:4:database:db-alpha"
        );
        let events = list_personaldb_group_watch_events(&storage, 4, "db-alpha", 10, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 11);
        assert_eq!(events[0].authz_revision, 8);
        assert_eq!(events[0].payload.log_index, 2);
        assert_eq!(
            latest_personaldb_group_watch_cursor(&storage, 4, "db-alpha")
                .await
                .unwrap(),
            Some(11)
        );
    }

    #[tokio::test]
    async fn personaldb_group_watch_rejects_non_monotonic_cursor_and_bad_payload() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_personaldb_group_watch_record(&storage, 4, "db-alpha", 10, [1; 16], 7, payload(1))
            .await
            .unwrap();
        assert!(
            append_personaldb_group_watch_record(
                &storage,
                4,
                "db-alpha",
                10,
                [2; 16],
                7,
                payload(2),
            )
            .await
            .is_err()
        );

        let mut bad = payload(3);
        bad.database_id = "db-beta".to_string();
        assert!(
            append_personaldb_group_watch_record(&storage, 4, "db-alpha", 11, [3; 16], 7, bad)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn personaldb_projection_watch_appends_lists_and_tracks_latest_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_personaldb_projection_watch_record(
            &storage,
            4,
            "projection-db",
            "projection-a",
            20,
            [1; 16],
            9,
            projection_payload(1),
        )
        .await
        .unwrap();
        append_personaldb_projection_watch_record(
            &storage,
            4,
            "projection-db",
            "projection-a",
            21,
            [2; 16],
            10,
            projection_payload(2),
        )
        .await
        .unwrap();

        assert_eq!(
            personaldb_projection_watch_stream_id(4, "projection-db", "projection-a"),
            "watch:personaldb_projection:tenant:4:database:projection-db:projection:projection-a"
        );
        let events = list_personaldb_projection_watch_events(
            &storage,
            4,
            "projection-db",
            "projection-a",
            20,
            10,
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 21);
        assert_eq!(events[0].authz_revision, 10);
        assert_eq!(events[0].payload.projection_log_index, 2);
        assert_eq!(
            latest_personaldb_projection_watch_cursor(&storage, 4, "projection-db", "projection-a")
                .await
                .unwrap(),
            Some(21)
        );
    }

    #[tokio::test]
    async fn personaldb_projection_watch_rejects_non_monotonic_cursor_and_bad_payload() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_personaldb_projection_watch_record(
            &storage,
            4,
            "projection-db",
            "projection-a",
            20,
            [1; 16],
            9,
            projection_payload(1),
        )
        .await
        .unwrap();
        assert!(
            append_personaldb_projection_watch_record(
                &storage,
                4,
                "projection-db",
                "projection-a",
                20,
                [2; 16],
                9,
                projection_payload(2),
            )
            .await
            .is_err()
        );

        let mut bad = projection_payload(3);
        bad.projection_id = "projection-b".to_string();
        assert!(
            append_personaldb_projection_watch_record(
                &storage,
                4,
                "projection-db",
                "projection-a",
                21,
                [3; 16],
                9,
                bad,
            )
            .await
            .is_err()
        );
    }

    fn payload(log_index: u64) -> PersonalDbGroupWatchPayload {
        PersonalDbGroupWatchPayload {
            database_id: "db-alpha".to_string(),
            event_type: "committed".to_string(),
            log_index,
            log_hash: hex::encode([log_index as u8; 32]),
            changeset_payload_hash: hex::encode([2; 32]),
            certificate_hash: hex::encode([3; 32]),
            committed_head_hash: hex::encode([4; 32]),
            emitted_at: "2026-06-27T00:00:00.000000000Z".to_string(),
        }
    }

    fn projection_payload(log_index: u64) -> PersonalDbProjectionWatchPayload {
        PersonalDbProjectionWatchPayload {
            database_id: "projection-db".to_string(),
            projection_id: "projection-a".to_string(),
            event_type: "projection_committed".to_string(),
            source_database_id: "source-db".to_string(),
            source_log_index: log_index + 10,
            source_log_hash: hex::encode([5; 32]),
            projection_log_index: log_index,
            projection_log_hash: hex::encode([log_index as u8; 32]),
            definition_hash: hex::encode([6; 32]),
            emitted_at: "2026-06-27T00:00:00.000000000Z".to_string(),
        }
    }
}
