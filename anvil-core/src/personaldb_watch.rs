use crate::{
    formats::{FileFamily, Hash32, hash32, watch::WatchRecord},
    storage::Storage,
    watch_log::{DecodedWatchLog, WatchLogHeader, decode_watch_log},
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

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
) -> Result<PathBuf> {
    validate_payload(database_id, &payload)?;
    let path = storage.personaldb_group_watch_path(tenant_id, database_id)?;
    ensure_watch_header(
        tenant_id,
        database_id,
        "personaldb_group",
        "personaldb_group",
        partition_id(tenant_id, database_id),
        &path,
    )
    .await?;
    ensure_cursor_is_monotonic(&path, cursor).await?;

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
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await?;
    file.write_all(&record.encode()).await?;
    file.sync_data().await?;
    Ok(path)
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
) -> Result<PathBuf> {
    validate_projection_payload(database_id, projection_id, &payload)?;
    let path = storage.personaldb_projection_watch_path(tenant_id, database_id, projection_id)?;
    ensure_watch_header(
        tenant_id,
        &format!("{database_id}/{projection_id}"),
        "personaldb_projection",
        "personaldb_projection",
        projection_partition_id(tenant_id, database_id, projection_id),
        &path,
    )
    .await?;
    ensure_cursor_is_monotonic(&path, cursor).await?;

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
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await?;
    file.write_all(&record.encode()).await?;
    file.sync_data().await?;
    Ok(path)
}

pub async fn list_personaldb_group_watch_events(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<PersonalDbGroupWatchEvent>> {
    let path = storage.personaldb_group_watch_path(tenant_id, database_id)?;
    let decoded = read_watch_or_empty(&path).await?;
    let mut events = Vec::new();
    for record in decoded.records {
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
    let path = storage.personaldb_projection_watch_path(tenant_id, database_id, projection_id)?;
    let decoded = read_watch_or_empty(&path).await?;
    let mut events = Vec::new();
    for record in decoded.records {
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
    let path = storage.personaldb_group_watch_path(tenant_id, database_id)?;
    let decoded = read_watch_or_empty(&path).await?;
    Ok(decoded
        .records
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
    let path = storage.personaldb_projection_watch_path(tenant_id, database_id, projection_id)?;
    let decoded = read_watch_or_empty(&path).await?;
    Ok(decoded
        .records
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

async fn ensure_watch_header(
    tenant_id: i64,
    stream_key: &str,
    watch_stream: &str,
    partition_family: &str,
    partition_id: Hash32,
    path: &PathBuf,
) -> Result<()> {
    if tokio::fs::metadata(path).await.is_ok() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let header = WatchLogHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: stream_key.to_string(),
        watch_stream: watch_stream.to_string(),
        partition_family: partition_family.to_string(),
        partition_id: hex::encode(partition_id),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        codec: "none".to_string(),
    };
    let envelope = crate::formats::BinaryEnvelopeHeader::new(
        FileFamily::WatchSegment,
        0,
        0,
        serde_json::to_vec(&header)?,
    );
    match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
    {
        Ok(mut file) => {
            file.write_all(&envelope.encode()).await?;
            file.sync_data().await?;
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => {
            Err(err).with_context(|| format!("create personaldb watch file {}", path.display()))
        }
    }
}

async fn ensure_cursor_is_monotonic(path: &PathBuf, cursor: u128) -> Result<()> {
    let decoded = read_watch_or_empty(path).await?;
    if let Some(latest) = decoded.records.iter().map(|record| record.cursor).max() {
        if cursor <= latest {
            return Err(anyhow!("personaldb watch cursor must be monotonic"));
        }
    }
    Ok(())
}

async fn read_watch_or_empty(path: &PathBuf) -> Result<DecodedWatchLog> {
    match tokio::fs::read(path).await {
        Ok(bytes) => decode_watch_log(&bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(DecodedWatchLog {
            header: WatchLogHeader {
                tenant_id: String::new(),
                bucket_id: String::new(),
                watch_stream: "personaldb_group".to_string(),
                partition_family: "personaldb_group".to_string(),
                partition_id: String::new(),
                created_at: String::new(),
                codec: "none".to_string(),
            },
            records: Vec::new(),
        }),
        Err(err) => {
            Err(err).with_context(|| format!("read personaldb watch file {}", path.display()))
        }
    }
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

        let path = storage.personaldb_group_watch_path(4, "db-alpha").unwrap();
        assert!(path.ends_with("_anvil/watch/personaldb/tenant-4/groups/db-alpha.anwatch"));
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

        let path = storage
            .personaldb_projection_watch_path(4, "projection-db", "projection-a")
            .unwrap();
        assert!(path.ends_with(
            "_anvil/watch/personaldb/tenant-4/groups/projection-db/projections/projection-a.anwatch"
        ));
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
