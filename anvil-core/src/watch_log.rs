use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, FormatError, hash32, watch::WatchRecord,
};
use crate::persistence::{Bucket, Object, ObjectWatchEvent};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

const OBJECT_WATCH_PARTITION_FAMILY: u16 = 1;
const OBJECT_WATCH_RECORD_KIND: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WatchLogHeader {
    pub tenant_id: String,
    pub bucket_id: String,
    pub watch_stream: String,
    pub partition_family: String,
    pub partition_id: String,
    pub created_at: String,
    pub codec: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedWatchLog {
    pub header: WatchLogHeader,
    pub records: Vec<WatchRecord>,
}

pub async fn append_object_watch_record(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    event: &ObjectWatchEvent,
) -> Result<PathBuf> {
    let path = storage.object_watch_path(bucket.tenant_id, bucket.id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if tokio::fs::metadata(&path).await.is_err() {
        let header = WatchLogHeader {
            tenant_id: bucket.tenant_id.to_string(),
            bucket_id: bucket.id.to_string(),
            watch_stream: "object_prefix".to_string(),
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode(partition_id(bucket.tenant_id, bucket.id)),
            created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            codec: "none".to_string(),
        };
        let header_json = serde_json::to_vec(&header)?;
        let envelope = BinaryEnvelopeHeader::new(FileFamily::WatchSegment, 0, 0, header_json);
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        file.write_all(&envelope.encode()).await?;
        file.sync_data().await?;
    }

    let payload = serde_json::to_vec(&serde_json::json!({
        "bucket_name": event.bucket_name,
        "key": event.key,
        "event_type": event.event_type,
        "version_id": event.version_id.map(|id| id.to_string()),
        "etag": event.etag,
        "size": event.size,
        "is_delete_marker": event.is_delete_marker,
        "emitted_at": event.created_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    }))?;
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
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await?;
    file.write_all(&record.encode()).await?;
    file.sync_data().await?;
    Ok(path)
}

pub fn decode_watch_log(input: &[u8]) -> Result<DecodedWatchLog> {
    let envelope = BinaryEnvelopeHeader::decode(input)?;
    if envelope.family != FileFamily::WatchSegment {
        return Err(anyhow!("watch log file family mismatch"));
    }
    let header: WatchLogHeader = serde_json::from_slice(&envelope.header_json)?;
    let header_len = COMMON_HEADER_LEN
        .checked_add(envelope.header_json.len())
        .ok_or_else(|| anyhow!("watch log header length overflow"))?;
    let mut body = &input[header_len..];
    let mut records = Vec::new();
    while !body.is_empty() {
        match WatchRecord::decode(body) {
            Ok((record, used)) => {
                records.push(record);
                body = &body[used..];
            }
            Err(FormatError::TooShort { .. }) | Err(FormatError::HashMismatch { .. }) => break,
            Err(err) => return Err(err.into()),
        }
    }
    Ok(DecodedWatchLog { header, records })
}

fn partition_id(tenant_id: i64, bucket_id: i64) -> [u8; 32] {
    hash32(format!("tenant:{tenant_id}:bucket:{bucket_id}:watch:object").as_bytes())
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
            inline_payload: None,
            checksum: None,
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
            etag: Some(object.etag.clone()),
            size: object.size,
            is_delete_marker: false,
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn append_object_watch_record_writes_active_watch_log() {
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
        let path = append_object_watch_record(
            &storage,
            &bucket,
            &second,
            &sample_event(2, &bucket, &second, "put"),
        )
        .await
        .unwrap();
        assert_eq!(path, storage.object_watch_path(bucket.tenant_id, bucket.id));

        let decoded = decode_watch_log(&tokio::fs::read(path).await.unwrap()).unwrap();
        assert_eq!(decoded.header.partition_family, "object_metadata");
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].cursor, 1);
        assert_eq!(decoded.records[1].cursor, 2);

        let mut corrupted = tokio::fs::read(storage.object_watch_path(bucket.tenant_id, bucket.id))
            .await
            .unwrap();
        let last = corrupted.len() - 1;
        corrupted[last] ^= 1;
        let decoded = decode_watch_log(&corrupted).unwrap();
        assert_eq!(decoded.records.len(), 1);
    }
}
