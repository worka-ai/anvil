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

const AUTHZ_NAMESPACE_PARTITION_FAMILY: u16 = 9;
const AUTHZ_NAMESPACE_RECORD_KIND: u16 = 1;

#[derive(Clone, PartialEq, Message)]
struct AuthzNamespaceWatchPayloadProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    event_type: String,
    #[prost(uint64, tag = "3")]
    authz_revision: u64,
    #[prost(string, tag = "4")]
    schema_hash: String,
    #[prost(bool, tag = "5")]
    invalidates_derived_usersets: bool,
    #[prost(string, tag = "6")]
    emitted_at: String,
}

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
) -> Result<()> {
    validate_payload(&payload)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = authz_namespace_watch_stream_id(tenant_id, &payload.namespace);
    ensure_cursor_is_monotonic(&core_store, &stream_id, cursor).await?;

    let record = WatchRecord::new(
        cursor,
        AUTHZ_NAMESPACE_PARTITION_FAMILY,
        partition_id(tenant_id, &payload.namespace),
        mutation_id,
        AUTHZ_NAMESPACE_RECORD_KIND,
        payload.authz_revision,
        0,
        0,
        encode_authz_namespace_watch_payload(&payload)?,
    );
    core_store
        .append_stream(AppendStreamRecord {
            stream_id,
            partition_id: hex::encode(partition_id(tenant_id, &payload.namespace)),
            record_kind: "authz_namespace_watch".to_string(),
            payload: record.encode(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "authz-namespace-watch:{tenant_id}:{}:{cursor}",
                payload.namespace
            )),
        })
        .await?;
    Ok(())
}

pub async fn list_authz_namespace_watch_events(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<AuthzNamespaceWatchEvent>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = read_watch_or_empty(
        &core_store,
        &authz_namespace_watch_stream_id(tenant_id, namespace),
    )
    .await?;
    let expected_partition = partition_id(tenant_id, namespace);
    let mut events = Vec::new();
    for record in records {
        if record.cursor <= after_cursor {
            continue;
        }
        if record.partition_family != AUTHZ_NAMESPACE_PARTITION_FAMILY
            || record.record_kind != AUTHZ_NAMESPACE_RECORD_KIND
            || record.partition_id != expected_partition
        {
            continue;
        }
        let payload: AuthzNamespaceWatchPayload =
            decode_authz_namespace_watch_payload(&record.payload)?;
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

fn encode_authz_namespace_watch_payload(payload: &AuthzNamespaceWatchPayload) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(
        &AuthzNamespaceWatchPayloadProto {
            namespace: payload.namespace.clone(),
            event_type: payload.event_type.clone(),
            authz_revision: payload.authz_revision,
            schema_hash: payload.schema_hash.clone(),
            invalidates_derived_usersets: payload.invalidates_derived_usersets,
            emitted_at: payload.emitted_at.clone(),
        },
    ))
}

fn decode_authz_namespace_watch_payload(bytes: &[u8]) -> Result<AuthzNamespaceWatchPayload> {
    let proto = decode_deterministic_proto::<AuthzNamespaceWatchPayloadProto>(
        bytes,
        "AuthzNamespaceWatchPayload payload",
    )?;
    Ok(AuthzNamespaceWatchPayload {
        namespace: proto.namespace,
        event_type: proto.event_type,
        authz_revision: proto.authz_revision,
        schema_hash: proto.schema_hash,
        invalidates_derived_usersets: proto.invalidates_derived_usersets,
        emitted_at: proto.emitted_at,
    })
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
            "authorization namespace watch cursor must be monotonic"
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
        .filter(|record| record.record_kind == "authz_namespace_watch")
        .map(|record| {
            WatchRecord::decode(&record.payload)
                .map(|(record, _)| record)
                .map_err(Into::into)
        })
        .collect()
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

fn authz_namespace_watch_stream_id(tenant_id: i64, namespace: &str) -> String {
    format!("watch:authz_namespace:tenant:{tenant_id}:namespace:{namespace}")
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
        append_authz_namespace_watch_record(&storage, 5, 1, [1; 16], payload(10))
            .await
            .unwrap();
        append_authz_namespace_watch_record(&storage, 5, 2, [2; 16], payload(11))
            .await
            .unwrap();
        assert_eq!(
            authz_namespace_watch_stream_id(5, "document"),
            "watch:authz_namespace:tenant:5:namespace:document"
        );

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
        let mut invalid = payload(13);
        invalid.namespace = "../escape".to_string();
        assert!(
            append_authz_namespace_watch_record(&storage, 5, 3, [4; 16], invalid)
                .await
                .is_err()
        );
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
