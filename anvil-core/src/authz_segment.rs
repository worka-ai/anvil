use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    Hash32,
    authz::{TupleKey, TupleOperation, TupleValue},
    hash32,
    segment::{SegmentBody, SegmentRecord},
};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzSegmentHeader {
    pub tenant_id: String,
    pub partition_family: String,
    pub partition_id: String,
    pub generation: u64,
    #[serde(default)]
    pub source_fence_token: u64,
    pub key_order: String,
    pub created_at: String,
    pub codec: String,
}

#[derive(Debug, Clone)]
pub struct DecodedAuthzSegment {
    pub header: AuthzSegmentHeader,
    pub records: Vec<AuthzTupleRecord>,
}

async fn write_authz_tuple_segment(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
) -> Result<PathBuf> {
    write_authz_tuple_segment_inner(storage, tenant_id, records, 0).await
}

pub(crate) async fn write_authz_tuple_segment_with_fence(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    source_fence_token: u64,
) -> Result<PathBuf> {
    write_authz_tuple_segment_inner(storage, tenant_id, records, source_fence_token).await
}

async fn write_authz_tuple_segment_inner(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    source_fence_token: u64,
) -> Result<PathBuf> {
    let generation = records
        .iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or(0);
    let generation = u64::try_from(generation).context("authz segment generation is negative")?;
    let path = storage.authz_tuple_segment_path(tenant_id, generation);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let header = AuthzSegmentHeader {
        tenant_id: tenant_id.to_string(),
        partition_family: "authz_tuple".to_string(),
        partition_id: hex::encode(partition_id(tenant_id)),
        generation,
        source_fence_token,
        key_order: "tuple_key_revision".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        codec: "none".to_string(),
    };
    let header_json = serde_json::to_vec(&header)?;
    let envelope = BinaryEnvelopeHeader::new(FileFamily::AuthzTupleSegment, 0, 0, header_json);
    let encoded_header = envelope.encode();
    let segment_records = segment_records_from_authz_records(records)?;
    let body = SegmentBody::from_uncompressed_records(&segment_records)?.encode();
    let (first_hash, last_hash) = segment_record_hash_bounds(&segment_records);
    let footer = BinaryFileFooter::new(
        &encoded_header,
        &body,
        segment_records.len() as u64,
        first_hash,
        last_hash,
    );

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await
        .with_context(|| format!("create authz tuple segment {}", path.display()))?;
    file.write_all(&encoded_header).await?;
    file.write_all(&body).await?;
    file.write_all(&footer.encode()).await?;
    file.sync_data().await?;
    Ok(path)
}

pub async fn read_latest_authz_tuple_segment(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Option<DecodedAuthzSegment>> {
    let Some(path) = latest_authz_tuple_segment_path(storage, tenant_id).await? else {
        return Ok(None);
    };
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("read authz tuple segment {}", path.display()))?;
    Ok(Some(decode_authz_tuple_segment(&bytes)?))
}

pub fn decode_authz_tuple_segment(bytes: &[u8]) -> Result<DecodedAuthzSegment> {
    let envelope = BinaryEnvelopeHeader::decode(bytes)?;
    if envelope.family != FileFamily::AuthzTupleSegment {
        return Err(anyhow!("authz tuple segment file family mismatch"));
    }
    if bytes.len() < COMMON_FOOTER_LEN {
        return Err(anyhow!("authz tuple segment is shorter than footer"));
    }
    let header_end = COMMON_HEADER_LEN
        .checked_add(envelope.header_json.len())
        .ok_or_else(|| anyhow!("authz tuple segment header length overflow"))?;
    let footer_start = bytes
        .len()
        .checked_sub(COMMON_FOOTER_LEN)
        .ok_or_else(|| anyhow!("authz tuple segment footer length overflow"))?;
    if footer_start < header_end {
        return Err(anyhow!("authz tuple segment body overlaps header"));
    }
    let encoded_header = &bytes[..header_end];
    let body_bytes = &bytes[header_end..footer_start];
    let footer = BinaryFileFooter::decode(&bytes[footer_start..])?;
    footer.verify(encoded_header, body_bytes)?;
    let header: AuthzSegmentHeader = serde_json::from_slice(&envelope.header_json)?;
    let body = SegmentBody::decode(body_bytes)?;
    let mut records = Vec::new();
    for block in &body.data_blocks {
        for record in block.decode_uncompressed_records()? {
            records.push(authz_record_from_segment_record(record)?);
        }
    }
    Ok(DecodedAuthzSegment { header, records })
}

async fn latest_authz_tuple_segment_path(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Option<PathBuf>> {
    let dir = storage.authz_tuple_segment_dir(tenant_id);
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut latest: Option<(u64, PathBuf)> = None;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(generation) = segment_generation_from_name(name) else {
            continue;
        };
        match latest {
            Some((current, _)) if generation <= current => {}
            _ => latest = Some((generation, path)),
        }
    }
    Ok(latest.map(|(_, path)| path))
}

fn segment_generation_from_name(name: &str) -> Option<u64> {
    name.strip_prefix("generation-")?
        .strip_suffix(".anauthz")?
        .parse()
        .ok()
}

fn segment_records_from_authz_records(records: &[AuthzTupleRecord]) -> Result<Vec<SegmentRecord>> {
    let mut output = Vec::with_capacity(records.len());
    for record in records {
        output.push(SegmentRecord::new(
            segment_key(record)?,
            tuple_value(record)?.encode(),
        ));
    }
    output.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(output)
}

fn authz_record_from_segment_record(record: SegmentRecord) -> Result<AuthzTupleRecord> {
    let (key, key_used) = TupleKey::decode(&record.key)?;
    let revision_start = key_used;
    let revision_end = revision_start
        .checked_add(8)
        .ok_or_else(|| anyhow!("authz tuple segment key revision overflow"))?;
    if record.key.len() != revision_end {
        return Err(anyhow!("authz tuple segment key has trailing bytes"));
    }
    let key_revision = u64::from_le_bytes(record.key[revision_start..revision_end].try_into()?);
    let (value, value_used) = TupleValue::decode(&record.value)?;
    if value_used != record.value.len() {
        return Err(anyhow!("authz tuple segment value has trailing bytes"));
    }
    if key_revision != value.revision {
        return Err(anyhow!(
            "authz tuple key revision differs from value revision"
        ));
    }
    Ok(AuthzTupleRecord {
        revision: i64::try_from(value.revision).context("authz revision exceeds i64")?,
        tenant_id: 0,
        namespace: String::from_utf8(key.namespace)?,
        object_id: String::from_utf8(key.object_id)?,
        relation: String::from_utf8(key.relation)?,
        subject_kind: subject_kind_from_code(key.subject_kind)?.to_string(),
        subject_id: String::from_utf8(key.subject_id)?,
        caveat_hash: caveat_hash_to_string(key.caveat_hash),
        operation: operation_to_string(value.operation).to_string(),
        written_by: String::from_utf8(value.written_by)?,
        reason: String::from_utf8(value.reason)?,
        record_hash: hex::encode(value.record_hash),
        written_at: chrono::DateTime::from_timestamp_nanos(value.written_at_nanos),
    })
}

fn segment_key(record: &AuthzTupleRecord) -> Result<Vec<u8>> {
    let key = TupleKey {
        namespace: record.namespace.as_bytes().to_vec(),
        object_id: record.object_id.as_bytes().to_vec(),
        relation: record.relation.as_bytes().to_vec(),
        subject_kind: subject_kind_code(&record.subject_kind)?,
        subject_id: record.subject_id.as_bytes().to_vec(),
        caveat_hash: caveat_hash_from_string(&record.caveat_hash)?,
    };
    let mut encoded = key.encode();
    encoded.extend_from_slice(&u64::try_from(record.revision)?.to_le_bytes());
    Ok(encoded)
}

fn tuple_value(record: &AuthzTupleRecord) -> Result<TupleValue> {
    Ok(TupleValue::with_record_hash(
        operation_from_string(&record.operation)?,
        u64::try_from(record.revision)?,
        record
            .written_at
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow!("authz tuple timestamp cannot be represented in nanoseconds"))?,
        record.written_by.as_bytes().to_vec(),
        record.reason.as_bytes().to_vec(),
        hash32_from_hex(&record.record_hash)?,
    ))
}

fn operation_from_string(operation: &str) -> Result<TupleOperation> {
    match operation {
        "add" => Ok(TupleOperation::Add),
        "remove" => Ok(TupleOperation::Remove),
        other => Err(anyhow!("unsupported authz tuple operation {other}")),
    }
}

fn operation_to_string(operation: TupleOperation) -> &'static str {
    match operation {
        TupleOperation::Add => "add",
        TupleOperation::Remove => "remove",
    }
}

fn subject_kind_code(subject_kind: &str) -> Result<u8> {
    match subject_kind {
        "user" => Ok(1),
        "group" => Ok(2),
        "service" => Ok(3),
        "public" => Ok(4),
        "anonymous" => Ok(5),
        "app" => Ok(6),
        other => Err(anyhow!("unsupported authz subject kind {other}")),
    }
}

fn subject_kind_from_code(code: u8) -> Result<&'static str> {
    match code {
        1 => Ok("user"),
        2 => Ok("group"),
        3 => Ok("service"),
        4 => Ok("public"),
        5 => Ok("anonymous"),
        6 => Ok("app"),
        other => Err(anyhow!("unsupported authz subject kind code {other}")),
    }
}

fn caveat_hash_from_string(value: &str) -> Result<Hash32> {
    if value.is_empty() {
        return Ok([0; 32]);
    }
    hash32_from_hex(value)
}

fn caveat_hash_to_string(value: Hash32) -> String {
    if value == [0; 32] {
        String::new()
    } else {
        hex::encode(value)
    }
}

fn hash32_from_hex(value: &str) -> Result<Hash32> {
    let bytes = hex::decode(value).with_context(|| "decode hash32 hex")?;
    if bytes.len() != 32 {
        return Err(anyhow!("hash32 hex must decode to 32 bytes"));
    }
    Ok(bytes.try_into().expect("checked hash length"))
}

fn segment_record_hash_bounds(records: &[SegmentRecord]) -> (Hash32, Hash32) {
    let first = records
        .first()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    let last = records
        .last()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

fn partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/authz_tuple").as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::tempdir;

    fn record(revision: i64, operation: &str) -> AuthzTupleRecord {
        AuthzTupleRecord {
            revision,
            tenant_id: 7,
            namespace: "document".to_string(),
            object_id: "alpha".to_string(),
            relation: "viewer".to_string(),
            subject_kind: "user".to_string(),
            subject_id: "alice".to_string(),
            caveat_hash: String::new(),
            operation: operation.to_string(),
            written_by: "node".to_string(),
            reason: "test".to_string(),
            record_hash: hex::encode(hash32(format!("record-{revision}").as_bytes())),
            written_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn authz_tuple_segment_uses_exact_binary_records() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let records = vec![record(2, "remove"), record(1, "add")];
        let path = write_authz_tuple_segment(&storage, 7, &records)
            .await
            .unwrap();
        assert_eq!(path, storage.authz_tuple_segment_path(7, 2));

        let decoded = decode_authz_tuple_segment(&tokio::fs::read(path).await.unwrap()).unwrap();
        assert_eq!(decoded.header.partition_family, "authz_tuple");
        assert_eq!(decoded.records.len(), 2);
        assert_eq!(decoded.records[0].revision, 1);
        assert_eq!(decoded.records[1].operation, "remove");

        let latest = read_latest_authz_tuple_segment(&storage, 7)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.records.len(), 2);
    }
}
