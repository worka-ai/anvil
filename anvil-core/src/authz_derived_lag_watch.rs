use crate::{
    core_store::{
        AppendStreamRecord, CoreStore, ReadStream, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::{Hash32, hash32, watch::WatchRecord},
    storage::Storage,
};
use anyhow::{Result, anyhow};
use prost::Message;
use serde::{Deserialize, Serialize};

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

#[derive(Clone, PartialEq, Message)]
struct AuthzDerivedLagWatchPayloadProto {
    #[prost(string, tag = "1")]
    derived_index_id: String,
    #[prost(string, tag = "2")]
    derived_index_kind: String,
    #[prost(uint64, tag = "3")]
    processed_revision: u64,
    #[prost(uint64, tag = "4")]
    latest_revision: u64,
    #[prost(string, tag = "5")]
    source_cursor: String,
    #[prost(string, tag = "6")]
    source_manifest_hash: String,
    #[prost(uint64, tag = "7")]
    generation: u64,
    #[prost(string, tag = "8")]
    emitted_at: String,
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
) -> Result<()> {
    validate_payload(&payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = authz_derived_lag_watch_stream_id(tenant_id, &payload.derived_index_id);
    ensure_cursor_is_monotonic(&core_store, &stream_id, cursor).await?;

    let record = WatchRecord::new(
        cursor,
        AUTHZ_DERIVED_LAG_PARTITION_FAMILY,
        partition_id(tenant_id, &payload.derived_index_id),
        mutation_id,
        AUTHZ_DERIVED_LAG_RECORD_KIND,
        payload.latest_revision,
        payload.generation,
        0,
        encode_lag_watch_payload(&payload),
    );
    core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(partition_id(tenant_id, &payload.derived_index_id)),
            record_kind: "authz_derived_lag_watch".to_string(),
            payload: record.encode(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "authz-derived-lag-watch:{tenant_id}:{}:{cursor}",
                payload.derived_index_id
            )),
        })
        .await?;
    Ok(())
}

pub async fn list_authz_derived_lag_watch_events(
    storage: &Storage,
    tenant_id: i64,
    derived_index_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<AuthzDerivedLagWatchEvent>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = read_watch_or_empty(
        &core_store,
        &authz_derived_lag_watch_stream_id(tenant_id, derived_index_id),
    )
    .await?;
    let expected_partition = partition_id(tenant_id, derived_index_id);
    let mut events = Vec::new();
    for record in records {
        if record.cursor <= after_cursor {
            continue;
        }
        if record.partition_family != AUTHZ_DERIVED_LAG_PARTITION_FAMILY
            || record.record_kind != AUTHZ_DERIVED_LAG_RECORD_KIND
            || record.partition_id != expected_partition
        {
            continue;
        }
        let payload = decode_lag_watch_payload(&record.payload)?;
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

async fn ensure_cursor_is_monotonic(
    core_store: &CoreStore,
    stream_id: &str,
    cursor: u128,
) -> Result<()> {
    let records = read_watch_or_empty(core_store, stream_id).await?;
    if let Some(latest) = records.iter().map(|record| record.cursor).max()
        && cursor <= latest
    {
        return Err(anyhow!(
            "authorization derived lag watch cursor must be monotonic"
        ));
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
        .filter(|record| record.record_kind == "authz_derived_lag_watch")
        .map(|record| {
            let (watch_record, used) = WatchRecord::decode(&record.payload)?;
            if used != record.payload.len() {
                return Err(anyhow!(
                    "authz derived lag watch CoreStore record has trailing bytes"
                ));
            }
            Ok(watch_record)
        })
        .collect()
}

fn encode_lag_watch_payload(payload: &AuthzDerivedLagWatchPayload) -> Vec<u8> {
    encode_deterministic_proto(&AuthzDerivedLagWatchPayloadProto {
        derived_index_id: payload.derived_index_id.clone(),
        derived_index_kind: payload.derived_index_kind.clone(),
        processed_revision: payload.processed_revision,
        latest_revision: payload.latest_revision,
        source_cursor: payload.source_cursor.to_string(),
        source_manifest_hash: payload.source_manifest_hash.clone(),
        generation: payload.generation,
        emitted_at: payload.emitted_at.clone(),
    })
}

fn decode_lag_watch_payload(bytes: &[u8]) -> Result<AuthzDerivedLagWatchPayload> {
    let proto = decode_deterministic_proto::<AuthzDerivedLagWatchPayloadProto>(
        bytes,
        "authorization derived lag watch payload",
    )?;
    Ok(AuthzDerivedLagWatchPayload {
        derived_index_id: proto.derived_index_id,
        derived_index_kind: proto.derived_index_kind,
        processed_revision: proto.processed_revision,
        latest_revision: proto.latest_revision,
        source_cursor: proto
            .source_cursor
            .parse()
            .map_err(|_| anyhow!("authorization derived lag source_cursor is not u128"))?,
        source_manifest_hash: proto.source_manifest_hash,
        generation: proto.generation,
        emitted_at: proto.emitted_at,
    })
}

fn validate_payload(payload: &AuthzDerivedLagWatchPayload) -> Result<()> {
    require_safe_component(&payload.derived_index_id, "derived_index_id")?;
    require_safe_component(&payload.derived_index_kind, "derived_index_kind")?;
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

fn authz_derived_lag_watch_stream_id(tenant_id: i64, derived_index_id: &str) -> String {
    format!("watch:authz_derived_lag:tenant:{tenant_id}:derived:{derived_index_id}")
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

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe component"));
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
        append_authz_derived_lag_watch_record(&storage, 11, 1, [1; 16], payload(90, 100, 1))
            .await
            .unwrap();
        append_authz_derived_lag_watch_record(&storage, 11, 2, [2; 16], payload(100, 100, 2))
            .await
            .unwrap();
        assert_eq!(
            authz_derived_lag_watch_stream_id(11, "derived-userset-primary"),
            "watch:authz_derived_lag:tenant:11:derived:derived-userset-primary"
        );
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let raw = read_watch_or_empty(
            &core_store,
            &authz_derived_lag_watch_stream_id(11, "derived-userset-primary"),
        )
        .await
        .unwrap();
        assert_ne!(raw[0].payload.first().copied(), Some(b'{'));
        assert!(decode_lag_watch_payload(&raw[0].payload).is_ok());

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
        let mut invalid = payload(99, 100, 4);
        invalid.derived_index_id = "../escape".to_string();
        assert!(
            append_authz_derived_lag_watch_record(&storage, 11, 3, [5; 16], invalid)
                .await
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
