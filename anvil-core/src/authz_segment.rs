use crate::{
    core_store::{
        CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob,
        core_object_ref_from_logical_file_write,
    },
    formats::{
        FileFamily, Hash32,
        authz::{TupleKey, TupleOperation, TupleValue},
        decode_writer_segment, encode_writer_segment_header, hash32, header_field_string,
        header_field_u64, required_header_string, required_header_u64,
        segment::SegmentRecord,
        single_body_range_index,
        table::{TableRow, WriterBodyTable, decode_writer_body_tables, encode_writer_body_tables},
        unix_nanos_from_rfc3339,
        writer::{
            WriterBuildOutput, WriterFamily, WriterSegmentBuildInput,
            build_writer_segment_logical_file, canonical_logical_file_id,
        },
    },
    persistence::AuthzTupleRecord,
    storage::Storage,
    writer_segment_catalog::{
        WriterSegmentCatalogRecord, latest_writer_segment_catalog_record,
        read_writer_segment_catalog_record, write_writer_segment_catalog_record,
    },
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};

const AUTHZ_TUPLE_SEGMENT_REF_PREFIX: &str = "authz_tuple_segment:";
const AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY: &str = "authz_tuple";
const TABLE_AUTHZ_SCHEMA_DESCRIPTOR: u16 = 0x0501;
const TABLE_AUTHZ_TUPLE: u16 = 0x0502;
const TABLE_AUTHZ_RELATION_RULE: u16 = 0x0503;
const TABLE_AUTHZ_USERSET_EDGE: u16 = 0x0504;
const TABLE_AUTHZ_REVISION_LOG: u16 = 0x0506;
const TABLE_AUTHZ_LIST_OBJECTS: u16 = 0x0507;
const TABLE_AUTHZ_LIST_SUBJECTS: u16 = 0x0508;

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
        codec: "writer-body-table-v1".to_string(),
    };
    let segment_records = segment_records_from_authz_records(records)?;
    let body = encode_writer_body_tables(&authz_writer_tables(&segment_records))?;
    let segment_hash = hash32(&body);
    let logical_file_id =
        canonical_logical_file_id(WriterFamily::Authz, generation, &ref_name, &segment_hash);
    let (first_hash, last_hash) = segment_record_hash_bounds(&segment_records);
    let header_proto = encode_authz_header_proto(&logical_file_id, &header);
    let range_index = single_body_range_index(
        body.len(),
        segment_records.len() as u64,
        first_hash,
        last_hash,
    )?;
    let built_segment = build_writer_segment_logical_file(WriterSegmentBuildInput {
        file_family: FileFamily::AuthzTupleSegment,
        writer_family: WriterFamily::Authz,
        writer_generation: generation,
        logical_file_id,
        header_proto,
        body,
        range_index,
        record_count: segment_records.len() as u64,
        first_record_hash: first_hash,
        last_record_hash: last_hash,
        boundary_values: Vec::new(),
        mutation_id: format!("authz-tuple-segment:{tenant_id}:{generation}"),
        region_id: "local".to_string(),
        pipeline_policy: CorePipelinePolicy::default(),
        trace_context: CoreTraceContext::default(),
    })?;
    let store = CoreStore::new(storage.clone()).await?;
    let receipt = store
        .write_format_build_output(WriterBuildOutput {
            logical_files: vec![built_segment.logical_file],
            core_meta_mutations: Vec::new(),
        })
        .await?;
    let written = receipt
        .written_logical_files
        .first()
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no authz logical file"))?;
    let object_ref = core_object_ref_from_logical_file_write(written);
    write_writer_segment_catalog_record(
        storage,
        &WriterSegmentCatalogRecord {
            family: AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY.to_string(),
            scope: authz_tuple_segment_scope(tenant_id)?,
            segment_ref: ref_name.clone(),
            core_object_ref_target: encode_core_object_ref_target(&object_ref)?,
            segment_hash: hex::encode(segment_hash),
            segment_length: written.manifest.logical_size,
            generation,
            source_cursor: generation,
            created_at_unix_nanos: unix_nanos_from_rfc3339(&header.created_at),
        },
    )
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
    let record = read_authz_tuple_segment_catalog_record(storage, tenant_id, &segment_ref)?
        .ok_or_else(|| anyhow!("authz tuple segment catalog row is missing"))?;
    let store = CoreStore::new(storage.clone()).await?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&record.core_object_ref_target)?,
        })
        .await?;
    Ok(Some(decode_authz_tuple_segment(&bytes)?))
}

pub fn decode_authz_tuple_segment(bytes: &[u8]) -> Result<DecodedAuthzSegment> {
    let segment = decode_writer_segment(bytes, FileFamily::AuthzTupleSegment)?;
    let header = decode_authz_header_proto(&segment.header)?;
    let mut records = Vec::new();
    for table in decode_writer_body_tables(segment.body)? {
        for row in table.rows {
            records.push(authz_record_from_segment_record(SegmentRecord::new(
                row.key, row.value,
            ))?);
        }
    }
    Ok(DecodedAuthzSegment { header, records })
}

fn encode_authz_header_proto(logical_file_id: &str, header: &AuthzSegmentHeader) -> Vec<u8> {
    encode_writer_segment_header(
        "anvil.authz.tuple_segment_header.v1",
        logical_file_id,
        FileFamily::AuthzTupleSegment,
        header.generation,
        None,
        None,
        unix_nanos_from_rfc3339(&header.created_at),
        vec![
            header_field_string("tenant_id", header.tenant_id.clone()),
            header_field_string("partition_family", header.partition_family.clone()),
            header_field_string("partition_id", header.partition_id.clone()),
            header_field_u64("source_fence_token", header.source_fence_token),
            header_field_string("key_order", header.key_order.clone()),
            header_field_string("created_at", header.created_at.clone()),
            header_field_string("codec", header.codec.clone()),
        ],
    )
}

fn decode_authz_header_proto(
    header: &crate::formats::WriterSegmentHeaderProto,
) -> Result<AuthzSegmentHeader> {
    Ok(AuthzSegmentHeader {
        tenant_id: required_header_string(header, "tenant_id")?,
        partition_family: required_header_string(header, "partition_family")?,
        partition_id: required_header_string(header, "partition_id")?,
        generation: header.writer_generation,
        source_fence_token: required_header_u64(header, "source_fence_token")?,
        key_order: required_header_string(header, "key_order")?,
        created_at: required_header_string(header, "created_at")?,
        codec: required_header_string(header, "codec")?,
    })
}

async fn latest_authz_tuple_segment_ref(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Option<String>> {
    Ok(latest_writer_segment_catalog_record(
        storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &authz_tuple_segment_scope(tenant_id)?,
    )?
    .map(|record| record.segment_ref))
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

fn authz_tuple_segment_scope(tenant_id: i64) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!("authz tuple segment tenant id must be nonnegative"));
    }
    Ok(format!("tenant/{tenant_id}"))
}

fn read_authz_tuple_segment_catalog_record(
    storage: &Storage,
    tenant_id: i64,
    segment_ref: &str,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    read_writer_segment_catalog_record(
        storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &authz_tuple_segment_scope(tenant_id)?,
        segment_ref,
    )
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    crate::core_store::encode_core_object_ref_target(object_ref)
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    crate::core_store::decode_core_object_ref_target(target)
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

fn authz_writer_tables(records: &[SegmentRecord]) -> Vec<WriterBodyTable> {
    let tuple_rows = records
        .iter()
        .map(|record| TableRow {
            key: record.key.clone(),
            value: record.value.clone(),
        })
        .collect::<Vec<_>>();
    [
        (TABLE_AUTHZ_SCHEMA_DESCRIPTOR, Vec::new()),
        (TABLE_AUTHZ_TUPLE, tuple_rows),
        (TABLE_AUTHZ_RELATION_RULE, Vec::new()),
        (TABLE_AUTHZ_USERSET_EDGE, Vec::new()),
        (TABLE_AUTHZ_REVISION_LOG, Vec::new()),
        (TABLE_AUTHZ_LIST_OBJECTS, Vec::new()),
        (TABLE_AUTHZ_LIST_SUBJECTS, Vec::new()),
    ]
    .into_iter()
    .map(|(table_id, rows)| WriterBodyTable {
        table_id,
        row_type_id: table_id,
        rows,
    })
    .collect()
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
