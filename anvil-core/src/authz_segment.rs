use crate::{
    core_store::{CompareAndSwapRef, CoreObjectRef, CoreStore, GetBlob, PutBlob},
    formats::{
        BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
        Hash32,
        authz::{TupleKey, TupleOperation, TupleValue},
        hash32,
        segment::{SegmentBody, SegmentRecord},
    },
    persistence::AuthzTupleRecord,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

const AUTHZ_TUPLE_SEGMENT_REF_PREFIX: &str = "authz_tuple_segment:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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

#[cfg(test)]
async fn write_authz_tuple_segment(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
) -> Result<String> {
    write_authz_tuple_segment_inner(storage, tenant_id, records, 0).await
}

pub(crate) async fn write_authz_tuple_segment_with_fence(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    source_fence_token: u64,
) -> Result<String> {
    write_authz_tuple_segment_inner(storage, tenant_id, records, source_fence_token).await
}

async fn write_authz_tuple_segment_inner(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    source_fence_token: u64,
) -> Result<String> {
    let generation = records
        .iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or(0);
    let generation = u64::try_from(generation).context("authz segment generation is negative")?;
    let ref_name = authz_tuple_segment_ref_name(tenant_id, generation)?;

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

    let mut bytes = Vec::with_capacity(encoded_header.len() + body.len() + COMMON_FOOTER_LEN);
    bytes.extend_from_slice(&encoded_header);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&footer.encode());

    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.clone(),
            bytes,
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: format!("authz-tuple-segment:{tenant_id}:{generation}"),
        })
        .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.clone(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(ref_name)
}

pub async fn read_latest_authz_tuple_segment(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Option<DecodedAuthzSegment>> {
    let Some(segment_ref) = latest_authz_tuple_segment_ref(storage, tenant_id).await? else {
        return Ok(None);
    };
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(&segment_ref)
        .await?
        .ok_or_else(|| anyhow!("authz tuple segment ref is missing"))?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
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

async fn latest_authz_tuple_segment_ref(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Option<String>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut refs = store
        .list_ref_names(&authz_tuple_segment_ref_prefix(tenant_id)?)
        .await?;
    refs.sort_by_key(|ref_name| segment_generation_from_ref(ref_name).unwrap_or(0));
    Ok(refs.pop())
}

fn authz_tuple_segment_ref_prefix(tenant_id: i64) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!("authz tuple segment tenant id must be nonnegative"));
    }
    Ok(format!(
        "{AUTHZ_TUPLE_SEGMENT_REF_PREFIX}tenant:{tenant_id}:"
    ))
}

fn authz_tuple_segment_ref_name(tenant_id: i64, generation: u64) -> Result<String> {
    Ok(format!(
        "{}generation:{generation:020}",
        authz_tuple_segment_ref_prefix(tenant_id)?
    ))
}

fn segment_generation_from_ref(ref_name: &str) -> Option<u64> {
    ref_name.rsplit_once(":generation:")?.1.parse().ok()
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?)
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
    if record.key.len() != revision_end && record.key.len() != revision_end + 4 {
        return Err(anyhow!("authz tuple segment key has trailing bytes"));
    }
    let key_revision = u64::from_le_bytes(record.key[revision_start..revision_end].try_into()?);
    let revision_ordinal = if record.key.len() == revision_end + 4 {
        u32::from_le_bytes(record.key[revision_end..revision_end + 4].try_into()?)
    } else {
        0
    };
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
        revision_ordinal,
        tenant_id: 0,
        namespace: String::from_utf8(key.namespace)?,
        object_id: String::from_utf8(key.object_id)?,
        relation: String::from_utf8(key.relation)?,
        subject_kind: String::from_utf8(key.subject_kind)?,
        subject_id: String::from_utf8(key.subject_id)?,
        caveat_hash: caveat_hash_to_string(key.caveat_hash),
        operation: operation_to_string(value.operation).to_string(),
        written_by: String::from_utf8(value.written_by)?,
        reason: String::from_utf8(value.reason)?,
        mutation_id: uuid::Uuid::nil(),
        record_hash: hex::encode(value.record_hash),
        written_at: chrono::DateTime::from_timestamp_nanos(value.written_at_nanos),
    })
}

fn segment_key(record: &AuthzTupleRecord) -> Result<Vec<u8>> {
    let key = TupleKey {
        namespace: record.namespace.as_bytes().to_vec(),
        object_id: record.object_id.as_bytes().to_vec(),
        relation: record.relation.as_bytes().to_vec(),
        subject_kind: record.subject_kind.as_bytes().to_vec(),
        subject_id: record.subject_id.as_bytes().to_vec(),
        caveat_hash: caveat_hash_from_string(&record.caveat_hash)?,
    };
    let mut encoded = key.encode();
    encoded.extend_from_slice(&u64::try_from(record.revision)?.to_le_bytes());
    encoded.extend_from_slice(&record.revision_ordinal.to_le_bytes());
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
            revision_ordinal: 0,
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
            mutation_id: uuid::Uuid::new_v4(),
            record_hash: hex::encode(hash32(format!("record-{revision}").as_bytes())),
            written_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn authz_tuple_segment_uses_exact_binary_records() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let records = vec![record(2, "remove"), record(1, "add")];
        let segment_ref = write_authz_tuple_segment(&storage, 7, &records)
            .await
            .unwrap();
        assert_eq!(
            segment_ref,
            "authz_tuple_segment:tenant:7:generation:00000000000000000002"
        );

        let decoded = read_latest_authz_tuple_segment(&storage, 7)
            .await
            .unwrap()
            .unwrap();
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
