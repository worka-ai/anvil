use crate::core_store::{
    AppendStreamRecord, CoreStore, ReadStream, StreamAppendReceipt, decode_deterministic_proto,
    encode_deterministic_proto,
};
use crate::formats::{hash32, watch::WatchRecord};
use crate::persistence::{Bucket, Object, ObjectWatchEvent};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use prost::Message;

const OBJECT_WATCH_PARTITION_FAMILY: u16 = 1;
const OBJECT_WATCH_RECORD_KIND: u16 = 1;

#[derive(Debug, Clone)]
struct ObjectWatchPayload {
    bucket_name: String,
    key: String,
    event_type: String,
    version_id: Option<String>,
    mutation_id: Option<String>,
    payload_hash: Option<String>,
    etag: Option<String>,
    size: i64,
    is_delete_marker: bool,
    emitted_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectWatchPayloadProto {
    #[prost(string, tag = "1")]
    bucket_name: String,
    #[prost(string, tag = "2")]
    key: String,
    #[prost(string, tag = "3")]
    event_type: String,
    #[prost(string, optional, tag = "4")]
    version_id: Option<String>,
    #[prost(string, optional, tag = "5")]
    mutation_id: Option<String>,
    #[prost(string, optional, tag = "6")]
    payload_hash: Option<String>,
    #[prost(string, optional, tag = "7")]
    etag: Option<String>,
    #[prost(int64, tag = "8")]
    size: i64,
    #[prost(bool, tag = "9")]
    is_delete_marker: bool,
    #[prost(string, tag = "10")]
    emitted_at: String,
}

pub async fn append_object_watch_record(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
) -> Result<StreamAppendReceipt> {
    let payload = encode_object_watch_payload(&ObjectWatchPayload {
        bucket_name: event.bucket_name.clone(),
        key: event.key.clone(),
        event_type: event.event_type.clone(),
        version_id: event.version_id.map(|id| id.to_string()),
        mutation_id: Some(event.mutation_id.to_string()),
        payload_hash: Some(event.payload_hash.clone()),
        etag: event.etag.clone(),
        size: event.size,
        is_delete_marker: event.is_delete_marker,
        emitted_at: event
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    });
    let record = WatchRecord::new(
        event.id as u128,
        OBJECT_WATCH_PARTITION_FAMILY,
        partition_id(bucket.tenant_id, bucket.id),
        *object.mutation_id.as_bytes(),
        OBJECT_WATCH_RECORD_KIND,
        object.authz_revision as u64,
        0,
        0,
        payload,
    );
    let core_store = CoreStore::new(storage.clone()).await?;
    core_store
        .append_stream(AppendStreamRecord {
            stream_id: object_watch_stream_id(bucket.tenant_id, bucket.id),
            partition_id: hex::encode(partition_id(bucket.tenant_id, bucket.id)),
            record_kind: "object_watch".to_string(),
            payload: record.encode(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some(format!(
                "object-watch:{}:{}:{}",
                bucket.tenant_id, bucket.id, event.mutation_id
            )),
        })
        .await
}

pub async fn latest_object_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    version_id: uuid::Uuid,
) -> Result<Option<u128>> {
    let records = read_object_watch_records(storage, tenant_id, bucket_id).await?;
    let expected = version_id.to_string();
    let mut latest = None;
    for record in records {
        let payload = decode_object_watch_payload(&record.payload)?;
        if payload.version_id.as_deref() == Some(expected.as_str()) {
            latest = latest.max(Some(record.cursor));
        }
    }
    Ok(latest)
}

pub async fn list_object_watch_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    prefix: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<ObjectWatchEvent>> {
    let records = read_object_watch_records(storage, tenant_id, bucket_id).await?;

    let mut events = Vec::new();
    for record in records {
        if record.cursor <= after_cursor as u128 {
            continue;
        }
        let payload = decode_object_watch_payload(&record.payload)?;
        if !payload.key.starts_with(prefix) {
            continue;
        }
        let id = i64::try_from(record.cursor).map_err(|_| anyhow!("watch cursor exceeds i64"))?;
        let version_id = payload
            .version_id
            .as_deref()
            .map(uuid::Uuid::parse_str)
            .transpose()?;
        let mutation_id = payload
            .mutation_id
            .as_deref()
            .map(uuid::Uuid::parse_str)
            .transpose()?
            .unwrap_or_else(uuid::Uuid::nil);
        let created_at =
            chrono::DateTime::parse_from_rfc3339(&payload.emitted_at)?.with_timezone(&chrono::Utc);
        events.push(ObjectWatchEvent {
            id,
            tenant_id,
            bucket_id,
            bucket_name: payload.bucket_name,
            key: payload.key,
            event_type: payload.event_type,
            version_id,
            mutation_id,
            payload_hash: payload.payload_hash.unwrap_or_default(),
            etag: payload.etag,
            size: payload.size,
            is_delete_marker: payload.is_delete_marker,
            created_at,
        });
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

fn encode_object_watch_payload(payload: &ObjectWatchPayload) -> Vec<u8> {
    encode_deterministic_proto(&ObjectWatchPayloadProto {
        bucket_name: payload.bucket_name.clone(),
        key: payload.key.clone(),
        event_type: payload.event_type.clone(),
        version_id: payload.version_id.clone(),
        mutation_id: payload.mutation_id.clone(),
        payload_hash: payload.payload_hash.clone(),
        etag: payload.etag.clone(),
        size: payload.size,
        is_delete_marker: payload.is_delete_marker,
        emitted_at: payload.emitted_at.clone(),
    })
}

fn decode_object_watch_payload(bytes: &[u8]) -> Result<ObjectWatchPayload> {
    let proto =
        decode_deterministic_proto::<ObjectWatchPayloadProto>(bytes, "object watch payload")?;
    Ok(ObjectWatchPayload {
        bucket_name: proto.bucket_name,
        key: proto.key,
        event_type: proto.event_type,
        version_id: proto.version_id,
        mutation_id: proto.mutation_id,
        payload_hash: proto.payload_hash,
        etag: proto.etag,
        size: proto.size,
        is_delete_marker: proto.is_delete_marker,
        emitted_at: proto.emitted_at,
    })
}

async fn read_object_watch_records(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<WatchRecord>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = core_store
        .read_stream(ReadStream {
            stream_id: object_watch_stream_id(tenant_id, bucket_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut decoded = Vec::new();
    for record in records {
        if record.record_kind != "object_watch" {
            continue;
        }
        let (watch_record, used) = WatchRecord::decode(&record.payload)?;
        if used != record.payload.len() {
            return Err(anyhow!("object watch CoreStore record has trailing bytes"));
        }
        decoded.push(watch_record);
    }
    Ok(decoded)
}

fn partition_id(tenant_id: i64, bucket_id: i64) -> [u8; 32] {
    hash32(format!("tenant:{tenant_id}:bucket:{bucket_id}:watch:object").as_bytes())
}

fn object_watch_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("object_watch:tenant:{tenant_id}:bucket:{bucket_id}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{Bucket, Object, ObjectWatchEvent};
    use crate::storage::Storage;
    use chrono::Utc;
    use tempfile::tempdir;

    fn sample_bucket() -> Bucket {
        Bucket {
            id: 77,
            tenant_id: 12,
            name: "watch-bucket".to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        }
    }

    fn sample_object(id: i64, key: &str) -> Object {
        Object {
            id,
            tenant_id: 12,
            bucket_id: 77,
            key: key.to_string(),
            kind: crate::object_links::ObjectEntryKind::Blob,
            content_hash: format!("hash-{id}"),
            size: 100 + id,
            etag: format!("etag-{id}"),
            content_type: Some("text/plain".to_string()),
            version_id: uuid::Uuid::new_v4(),
            mutation_id: uuid::Uuid::new_v4(),
            index_policy_snapshot: "snapshot".to_string(),
            user_metadata_hash: "metadata-hash".to_string(),
            authz_revision: 3,
            record_hash: format!("record-{id}"),
            created_at: Utc::now(),
            deleted_at: None,
            storage_class: None,
            user_meta: None,
            shard_map: None,
            checksum: None,
            link: None,
        }
    }

    fn sample_event(
        id: i64,
        bucket: &Bucket,
        object: &Object,
        event_type: &str,
    ) -> ObjectWatchEvent {
        ObjectWatchEvent {
            id,
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            bucket_name: bucket.name.clone(),
            key: object.key.clone(),
            event_type: event_type.to_string(),
            version_id: Some(object.version_id),
            mutation_id: object.mutation_id,
            payload_hash: object.content_hash.clone(),
            etag: Some(object.etag.clone()),
            size: object.size,
            is_delete_marker: false,
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn append_object_watch_record_writes_core_store_watch_stream() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let first = sample_object(1, "docs/a.txt");
        let second = sample_object(2, "docs/b.txt");
        append_object_watch_record(
            &storage,
            &bucket,
            &first,
            &sample_event(1, &bucket, &first, "put"),
        )
        .await
        .unwrap();
        let receipt = append_object_watch_record(
            &storage,
            &bucket,
            &second,
            &sample_event(2, &bucket, &second, "put"),
        )
        .await
        .unwrap();
        assert_eq!(receipt.sequence, 2);

        let decoded = read_object_watch_records(&storage, bucket.tenant_id, bucket.id)
            .await
            .unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].cursor, 1);
        assert_eq!(decoded[1].cursor, 2);
        assert_ne!(decoded[0].payload.first().copied(), Some(b'{'));
        assert!(decode_object_watch_payload(&decoded[0].payload).is_ok());
    }

    #[tokio::test]
    async fn object_watch_queries_recover_from_native_log() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let first = sample_object(1, "docs/a.txt");
        let second = sample_object(2, "images/b.png");
        append_object_watch_record(
            &storage,
            &bucket,
            &first,
            &sample_event(10, &bucket, &first, "put"),
        )
        .await
        .unwrap();
        append_object_watch_record(
            &storage,
            &bucket,
            &second,
            &sample_event(11, &bucket, &second, "delete"),
        )
        .await
        .unwrap();

        assert_eq!(
            latest_object_watch_cursor(&storage, bucket.tenant_id, bucket.id, first.version_id)
                .await
                .unwrap(),
            Some(10)
        );
        assert_eq!(
            latest_object_watch_cursor(&storage, bucket.tenant_id, bucket.id, uuid::Uuid::new_v4())
                .await
                .unwrap(),
            None
        );

        let docs = list_object_watch_events(&storage, bucket.tenant_id, bucket.id, "docs/", 0, 10)
            .await
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].key, "docs/a.txt");

        let after_first =
            list_object_watch_events(&storage, bucket.tenant_id, bucket.id, "", 10, 10)
                .await
                .unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].id, 11);
        assert_eq!(after_first[0].event_type, "delete");
    }
}
