use crate::{
    formats::{FileFamily, Hash32, hash32, watch::WatchRecord},
    storage::Storage,
    watch_log::{DecodedWatchLog, WatchLogHeader, decode_watch_log},
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

const AUTHZ_DERIVED_LAG_PARTITION_FAMILY: u16 = 8;
const AUTHZ_DERIVED_LAG_RECORD_KIND: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzDerivedLagWatchPayload {
    pub derived_index_id: String,
    pub derived_index_kind: String,
    pub processed_revision: u64,
    pub latest_revision: u64,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub emitted_at: String,
}

impl AuthzDerivedLagWatchPayload {
    pub fn revision_lag(&self) -> u64 {
        self.latest_revision.saturating_sub(self.processed_revision)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzDerivedLagWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub index_generation: u64,
    pub payload: AuthzDerivedLagWatchPayload,
}

pub async fn append_authz_derived_lag_watch_record(
    storage: &Storage,
    tenant_id: i64,
    cursor: u128,
    mutation_id: [u8; 16],
    payload: AuthzDerivedLagWatchPayload,
) -> Result<PathBuf> {
    validate_payload(&payload)?;
    let path = storage.authz_derived_lag_watch_path(tenant_id, &payload.derived_index_id)?;
    ensure_watch_header(tenant_id, &payload.derived_index_id, &path).await?;
    ensure_cursor_is_monotonic(&path, cursor).await?;

    let record = WatchRecord::new(
        cursor,
        AUTHZ_DERIVED_LAG_PARTITION_FAMILY,
        partition_id(tenant_id, &payload.derived_index_id),
        mutation_id,
        AUTHZ_DERIVED_LAG_RECORD_KIND,
        payload.latest_revision,
        payload.generation,
        0,
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

pub async fn list_authz_derived_lag_watch_events(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<AuthzDerivedLagWatchEvent>> {
    let path = storage.authz_derived_lag_watch_path(tenant_id, derived_index_id)?;
    let decoded = read_watch_or_empty(&path).await?;
    let expected_partition = partition_id(tenant_id, derived_index_id);
    let mut events = Vec::new();
    for record in decoded.records {
        if record.cursor <= after_cursor {
            continue;
        }
        if record.partition_family != AUTHZ_DERIVED_LAG_PARTITION_FAMILY
            || record.record_kind != AUTHZ_DERIVED_LAG_RECORD_KIND
            || record.partition_id != expected_partition
        {
            continue;
        }
        let payload: AuthzDerivedLagWatchPayload = serde_json::from_slice(&record.payload)?;
        if payload.derived_index_id != derived_index_id {
            return Err(anyhow!("authz derived lag watch payload scope mismatch"));
        }
        validate_payload(&payload)?;
        events.push(AuthzDerivedLagWatchEvent {
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

pub async fn latest_authz_derived_lag_watch_event(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
) -> Result<Option<AuthzDerivedLagWatchEvent>> {
    Ok(
        list_authz_derived_lag_watch_events(storage, tenant_id, derived_index_id, 0, 0)
            .await?
            .into_iter()
            .max_by_key(|event| event.cursor),
    )
}

async fn ensure_watch_header(tenant_id: i64, derived_index_id: &str, path: &PathBuf) -> Result<()> {
    if tokio::fs::metadata(path).await.is_ok() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let header = WatchLogHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: derived_index_id.to_string(),
        watch_stream: "authz_derived_lag".to_string(),
        partition_family: "authz_derived_lag".to_string(),
        partition_id: hex::encode(partition_id(tenant_id, derived_index_id)),
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
        Err(err) => Err(err).with_context(|| {
            format!(
                "create authorization derived lag watch file {}",
                path.display()
            )
        }),
    }
}

async fn ensure_cursor_is_monotonic(path: &PathBuf, cursor: u128) -> Result<()> {
    let decoded = read_watch_or_empty(path).await?;
    if let Some(latest) = decoded.records.iter().map(|record| record.cursor).max() {
        if cursor <= latest {
            return Err(anyhow!(
                "authorization derived lag watch cursor must be monotonic"
            ));
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
                watch_stream: "authz_derived_lag".to_string(),
                partition_family: "authz_derived_lag".to_string(),
                partition_id: String::new(),
                created_at: String::new(),
                codec: "none".to_string(),
            },
            records: Vec::new(),
        }),
        Err(err) => Err(err).with_context(|| {
            format!(
                "read authorization derived lag watch file {}",
                path.display()
            )
        }),
    }
}

fn validate_payload(payload: &AuthzDerivedLagWatchPayload) -> Result<()> {
    require_nonempty(&payload.derived_index_id, "derived_index_id")?;
    require_nonempty(&payload.derived_index_kind, "derived_index_kind")?;
    validate_hex32(&payload.source_manifest_hash, "source_manifest_hash")?;
    if payload.generation == 0 {
        return Err(anyhow!(
            "authorization derived lag generation must be nonzero"
        ));
    }
    if payload.processed_revision > payload.latest_revision {
        return Err(anyhow!(
            "authorization derived lag processed revision is after latest revision"
        ));
    }
    require_nonempty(&payload.emitted_at, "emitted_at")?;
    Ok(())
}

fn partition_id(tenant_id: i64, derived_index_id: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:authz-derived-lag:{derived_index_id}").as_bytes())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn authz_derived_lag_watch_appends_lists_and_reports_latest() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first =
            append_authz_derived_lag_watch_record(&storage, 11, 1, [1; 16], payload(90, 100, 1))
                .await
                .unwrap();
        let second =
            append_authz_derived_lag_watch_record(&storage, 11, 2, [2; 16], payload(100, 100, 2))
                .await
                .unwrap();
        assert_eq!(first, second);
        assert!(
            first.ends_with(
                "_anvil/watch/authz-derived-lag/tenant-11/derived-userset-primary.anwatch"
            )
        );

        let events =
            list_authz_derived_lag_watch_events(&storage, 11, "derived-userset-primary", 1, 10)
                .await
                .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        assert_eq!(events[0].authz_revision, 100);
        assert_eq!(events[0].index_generation, 2);
        assert_eq!(events[0].payload.revision_lag(), 0);

        let latest = latest_authz_derived_lag_watch_event(&storage, 11, "derived-userset-primary")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.cursor, 2);
        assert_eq!(latest.payload.processed_revision, 100);
    }

    #[tokio::test]
    async fn authz_derived_lag_watch_rejects_invalid_payloads_and_non_monotonic_cursors() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_authz_derived_lag_watch_record(&storage, 11, 1, [1; 16], payload(90, 100, 1))
            .await
            .unwrap();
        assert!(
            append_authz_derived_lag_watch_record(&storage, 11, 1, [2; 16], payload(91, 100, 2))
                .await
                .is_err()
        );
        let mut invalid = payload(101, 100, 3);
        assert!(
            append_authz_derived_lag_watch_record(&storage, 11, 2, [3; 16], invalid.clone())
                .await
                .is_err()
        );
        invalid.processed_revision = 99;
        invalid.source_manifest_hash = "not-hex".to_string();
        assert!(
            append_authz_derived_lag_watch_record(&storage, 11, 2, [4; 16], invalid)
                .await
                .is_err()
        );
        assert!(
            storage
                .authz_derived_lag_watch_path(11, "../escape")
                .is_err()
        );
    }

    #[tokio::test]
    async fn authz_derived_lag_watch_limit_zero_means_unlimited() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for cursor in 1..=3 {
            append_authz_derived_lag_watch_record(
                &storage,
                11,
                cursor,
                [cursor as u8; 16],
                payload(90 + cursor as u64, 100, cursor as u64),
            )
            .await
            .unwrap();
        }
        let events =
            list_authz_derived_lag_watch_events(&storage, 11, "derived-userset-primary", 0, 0)
                .await
                .unwrap();
        assert_eq!(events.len(), 3);
    }

    fn payload(
        processed_revision: u64,
        latest_revision: u64,
        generation: u64,
    ) -> AuthzDerivedLagWatchPayload {
        AuthzDerivedLagWatchPayload {
            derived_index_id: "derived-userset-primary".to_string(),
            derived_index_kind: "userset".to_string(),
            processed_revision,
            latest_revision,
            source_cursor: u128::from(latest_revision),
            source_manifest_hash: hex::encode([9; 32]),
            generation,
            emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        }
    }
}
