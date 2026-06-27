use crate::{
    formats::{FileFamily, Hash32, hash32, watch::WatchRecord},
    storage::Storage,
    watch_log::{DecodedWatchLog, WatchLogHeader, decode_watch_log},
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

const AUTHZ_NAMESPACE_PARTITION_FAMILY: u16 = 9;
const AUTHZ_NAMESPACE_RECORD_KIND: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzNamespaceWatchPayload {
    pub namespace: String,
    pub event_type: String,
    pub authz_revision: u64,
    pub schema_hash: String,
    pub invalidates_derived_usersets: bool,
    pub emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzNamespaceWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub payload: AuthzNamespaceWatchPayload,
}

pub async fn append_authz_namespace_watch_record(
    storage: &Storage,
    tenant_id: i64,
    cursor: u128,
    mutation_id: [u8; 16],
    payload: AuthzNamespaceWatchPayload,
) -> Result<PathBuf> {
    validate_payload(&payload)?;
    let path = storage.authz_namespace_watch_path(tenant_id, &payload.namespace)?;
    ensure_watch_header(tenant_id, &payload.namespace, &path).await?;
    ensure_cursor_is_monotonic(&path, cursor).await?;

    let record = WatchRecord::new(
        cursor,
        AUTHZ_NAMESPACE_PARTITION_FAMILY,
        partition_id(tenant_id, &payload.namespace),
        mutation_id,
        AUTHZ_NAMESPACE_RECORD_KIND,
        payload.authz_revision,
        0,
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

pub async fn list_authz_namespace_watch_events(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<AuthzNamespaceWatchEvent>> {
    let path = storage.authz_namespace_watch_path(tenant_id, namespace)?;
    let decoded = read_watch_or_empty(&path).await?;
    let expected_partition = partition_id(tenant_id, namespace);
    let mut events = Vec::new();
    for record in decoded.records {
        if record.cursor <= after_cursor {
            continue;
        }
        if record.partition_family != AUTHZ_NAMESPACE_PARTITION_FAMILY
            || record.record_kind != AUTHZ_NAMESPACE_RECORD_KIND
            || record.partition_id != expected_partition
        {
            continue;
        }
        let payload: AuthzNamespaceWatchPayload = serde_json::from_slice(&record.payload)?;
        if payload.namespace != namespace {
            return Err(anyhow!(
                "authorization namespace watch payload scope mismatch"
            ));
        }
        validate_payload(&payload)?;
        events.push(AuthzNamespaceWatchEvent {
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

pub async fn latest_authz_namespace_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
) -> Result<Option<u128>> {
    Ok(
        list_authz_namespace_watch_events(storage, tenant_id, namespace, 0, 0)
            .await?
            .into_iter()
            .map(|event| event.cursor)
            .max(),
    )
}

async fn ensure_watch_header(tenant_id: i64, namespace: &str, path: &PathBuf) -> Result<()> {
    if tokio::fs::metadata(path).await.is_ok() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let header = WatchLogHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: namespace.to_string(),
        watch_stream: "authz_namespace".to_string(),
        partition_family: "authz_namespace".to_string(),
        partition_id: hex::encode(partition_id(tenant_id, namespace)),
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
                "create authorization namespace watch file {}",
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
                "authorization namespace watch cursor must be monotonic"
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
                watch_stream: "authz_namespace".to_string(),
                partition_family: "authz_namespace".to_string(),
                partition_id: String::new(),
                created_at: String::new(),
                codec: "none".to_string(),
            },
            records: Vec::new(),
        }),
        Err(err) => Err(err)
            .with_context(|| format!("read authorization namespace watch file {}", path.display())),
    }
}

fn validate_payload(payload: &AuthzNamespaceWatchPayload) -> Result<()> {
    require_nonempty(&payload.namespace, "namespace")?;
    require_nonempty(&payload.event_type, "event_type")?;
    if payload.authz_revision == 0 {
        return Err(anyhow!(
            "authorization namespace watch revision must be nonzero"
        ));
    }
    validate_hex32(&payload.schema_hash, "schema_hash")?;
    require_nonempty(&payload.emitted_at, "emitted_at")?;
    Ok(())
}

fn partition_id(tenant_id: i64, namespace: &str) -> Hash32 {
    hash32(format!("tenant:{tenant_id}:authz-namespace:{namespace}").as_bytes())
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
    async fn authz_namespace_watch_appends_lists_and_tracks_latest() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = append_authz_namespace_watch_record(&storage, 5, 1, [1; 16], payload(10))
            .await
            .unwrap();
        let second = append_authz_namespace_watch_record(&storage, 5, 2, [2; 16], payload(11))
            .await
            .unwrap();
        assert_eq!(first, second);
        assert!(first.ends_with("_anvil/watch/authz-namespace/tenant-5/document.anwatch"));

        let events = list_authz_namespace_watch_events(&storage, 5, "document", 1, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 2);
        assert_eq!(events[0].authz_revision, 11);
        assert!(events[0].payload.invalidates_derived_usersets);
        assert_eq!(
            latest_authz_namespace_watch_cursor(&storage, 5, "document")
                .await
                .unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn authz_namespace_watch_rejects_invalid_payloads_and_non_monotonic_cursors() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_authz_namespace_watch_record(&storage, 5, 1, [1; 16], payload(10))
            .await
            .unwrap();
        assert!(
            append_authz_namespace_watch_record(&storage, 5, 1, [2; 16], payload(11))
                .await
                .is_err()
        );
        let mut invalid = payload(12);
        invalid.schema_hash = "not-hex".to_string();
        assert!(
            append_authz_namespace_watch_record(&storage, 5, 2, [3; 16], invalid)
                .await
                .is_err()
        );
        assert!(storage.authz_namespace_watch_path(5, "../escape").is_err());
    }

    #[tokio::test]
    async fn authz_namespace_watch_limit_zero_means_unlimited() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for cursor in 1..=3 {
            append_authz_namespace_watch_record(
                &storage,
                5,
                cursor,
                [cursor as u8; 16],
                payload(9 + cursor as u64),
            )
            .await
            .unwrap();
        }
        let events = list_authz_namespace_watch_events(&storage, 5, "document", 0, 0)
            .await
            .unwrap();
        assert_eq!(events.len(), 3);
    }

    fn payload(authz_revision: u64) -> AuthzNamespaceWatchPayload {
        AuthzNamespaceWatchPayload {
            namespace: "document".to_string(),
            event_type: "schema_changed".to_string(),
            authz_revision,
            schema_hash: hex::encode([4; 32]),
            invalidates_derived_usersets: true,
            emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        }
    }
}
