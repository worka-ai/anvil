use crate::{
    core_store::{
        CF_MATERIALISATION, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaRowCommonProto,
        CoreMetaStore, CoreMetaTuplePart, CoreMetaVisibilityState, CoreMutationOperation,
        CoreStore, TABLE_WRITER_SEGMENT_ROW, core_meta_root_key_hash, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    storage::Storage,
};
use anyhow::{Result, bail};
use prost::Message;

const WRITER_SEGMENT_ROW_SCHEMA: &str = "anvil.coremeta.writer_segment_locator.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterSegmentCatalogRecord {
    pub family: String,
    pub scope: String,
    pub segment_ref: String,
    pub core_object_ref_target: String,
    pub segment_hash: String,
    pub segment_length: u64,
    pub generation: u64,
    pub source_cursor: u64,
    pub created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct WriterSegmentCatalogRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    family: String,
    #[prost(string, tag = "4")]
    scope: String,
    #[prost(string, tag = "5")]
    segment_ref: String,
    #[prost(string, tag = "6")]
    core_object_ref_target: String,
    #[prost(string, tag = "7")]
    segment_hash: String,
    #[prost(uint64, tag = "8")]
    segment_length: u64,
    #[prost(uint64, tag = "9")]
    generation: u64,
    #[prost(uint64, tag = "10")]
    source_cursor: u64,
    #[prost(uint64, tag = "11")]
    created_at_unix_nanos: u64,
}

pub async fn write_writer_segment_catalog_record(
    storage: &Storage,
    record: &WriterSegmentCatalogRecord,
) -> Result<()> {
    let payload = encode_record(record)?;
    let tuple_key = tuple_key(record)?;
    let store = CoreStore::new(storage.clone()).await?;
    let op = CoreMetaBatchOp {
        cf: CF_MATERIALISATION,
        table_id: TABLE_WRITER_SEGMENT_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    store
        .commit_coremeta_batch_by_embedded_roots(
            &format!(
                "writer-segment:{}:{}:{}",
                record.family, record.scope, record.generation
            ),
            &[op],
        )
        .await?;
    Ok(())
}

pub fn writer_segment_catalog_put_operation(
    partition_id: String,
    record: &WriterSegmentCatalogRecord,
) -> Result<CoreMutationOperation> {
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id,
        cf: CF_MATERIALISATION.to_string(),
        table_id: TABLE_WRITER_SEGMENT_ROW,
        tuple_key: tuple_key(record)?,
        payload: encode_record(record)?,
    })
}

pub fn read_writer_segment_catalog_record(
    storage: &Storage,
    family: &str,
    scope: &str,
    segment_ref: &str,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let Some(payload) = meta.get(
        CF_MATERIALISATION,
        TABLE_WRITER_SEGMENT_ROW,
        &tuple_key_parts(family, scope, segment_ref)?,
    )?
    else {
        return Ok(None);
    };
    let record = decode_record(&payload)?;
    validate_scope(&record, family, scope, segment_ref)?;
    Ok(Some(record))
}

pub fn latest_writer_segment_catalog_record(
    storage: &Storage,
    family: &str,
    scope: &str,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    Ok(list_writer_segment_catalog_records(storage, family, scope)?
        .into_iter()
        .max_by_key(|record| (record.generation, record.created_at_unix_nanos)))
}

pub fn list_writer_segment_catalog_records(
    storage: &Storage,
    family: &str,
    scope: &str,
) -> Result<Vec<WriterSegmentCatalogRecord>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let mut records = Vec::new();
    for row in meta.scan_prefix(
        CF_MATERIALISATION,
        TABLE_WRITER_SEGMENT_ROW,
        &tuple_prefix(family, scope)?,
    )? {
        let record = decode_record(&row.payload)?;
        if record.family != family || record.scope != scope {
            bail!("writer segment catalog row scope mismatch");
        }
        records.push(record);
    }
    records.sort_by_key(|record| (record.generation, record.created_at_unix_nanos));
    Ok(records)
}

fn encode_record(record: &WriterSegmentCatalogRecord) -> Result<Vec<u8>> {
    validate_record(record)?;
    Ok(encode_deterministic_proto(
        &WriterSegmentCatalogRecordProto {
            common: Some(CoreMetaRowCommonProto {
                realm_id: record.scope.clone(),
                root_key_hash: core_meta_root_key_hash(&format!(
                    "writer-segment/{}/{}",
                    record.family, record.scope
                )),
                root_generation: record.generation,
                transaction_id: format!(
                    "writer-segment:{}:{}:{}",
                    record.family, record.scope, record.generation
                ),
                visibility_state: CoreMetaVisibilityState::Committed as i32,
                created_at_unix_nanos: record.created_at_unix_nanos,
                payload_schema_version: 1,
            }),
            schema: WRITER_SEGMENT_ROW_SCHEMA.to_string(),
            family: record.family.clone(),
            scope: record.scope.clone(),
            segment_ref: record.segment_ref.clone(),
            core_object_ref_target: record.core_object_ref_target.clone(),
            segment_hash: record.segment_hash.clone(),
            segment_length: record.segment_length,
            generation: record.generation,
            source_cursor: record.source_cursor,
            created_at_unix_nanos: record.created_at_unix_nanos,
        },
    ))
}

fn decode_record(bytes: &[u8]) -> Result<WriterSegmentCatalogRecord> {
    let proto = decode_deterministic_proto::<WriterSegmentCatalogRecordProto>(
        bytes,
        "writer segment catalog row",
    )?;
    if proto.schema != WRITER_SEGMENT_ROW_SCHEMA {
        bail!("writer segment catalog row has invalid schema");
    }
    let record = WriterSegmentCatalogRecord {
        family: proto.family,
        scope: proto.scope,
        segment_ref: proto.segment_ref,
        core_object_ref_target: proto.core_object_ref_target,
        segment_hash: proto.segment_hash,
        segment_length: proto.segment_length,
        generation: proto.generation,
        source_cursor: proto.source_cursor,
        created_at_unix_nanos: proto.created_at_unix_nanos,
    };
    validate_record(&record)?;
    Ok(record)
}

fn validate_record(record: &WriterSegmentCatalogRecord) -> Result<()> {
    require_nonempty(&record.family, "family")?;
    require_nonempty(&record.scope, "scope")?;
    require_nonempty(&record.segment_ref, "segment_ref")?;
    require_nonempty(&record.core_object_ref_target, "core_object_ref_target")?;
    if !record
        .core_object_ref_target
        .starts_with("core-object-ref:")
    {
        bail!("writer segment catalog row must carry a CoreStore object ref target");
    }
    if record.segment_length == 0 {
        bail!("writer segment catalog row segment length must be nonzero");
    }
    if record.generation == 0 {
        bail!("writer segment catalog row generation must be nonzero");
    }
    Ok(())
}

fn validate_scope(
    record: &WriterSegmentCatalogRecord,
    family: &str,
    scope: &str,
    segment_ref: &str,
) -> Result<()> {
    if record.family != family || record.scope != scope || record.segment_ref != segment_ref {
        bail!("writer segment catalog row scope mismatch");
    }
    Ok(())
}

fn tuple_key(record: &WriterSegmentCatalogRecord) -> Result<Vec<u8>> {
    tuple_key_parts(&record.family, &record.scope, &record.segment_ref)
}

fn tuple_key_parts(family: &str, scope: &str, segment_ref: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("writer-segment"),
        CoreMetaTuplePart::Utf8(family),
        CoreMetaTuplePart::Utf8(scope),
        CoreMetaTuplePart::Utf8(segment_ref),
    ])
}

fn tuple_prefix(family: &str, scope: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("writer-segment"),
        CoreMetaTuplePart::Utf8(family),
        CoreMetaTuplePart::Utf8(scope),
    ])
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        bail!("writer segment catalog {field} must not be empty");
    }
    Ok(())
}
