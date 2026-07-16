use crate::{
    core_store::{
        CF_INDEX_DEFS, CF_INDEX_ROWS, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaRowCommonProto,
        CoreMetaStore, CoreMetaTuplePart, CoreMetaVisibilityState, CoreStore,
        TABLE_INDEX_DEFINITION_ROW, TABLE_INDEX_ROW, core_meta_root_key_hash, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow, bail};
use prost::Message;

const INDEX_SEGMENT_ROW_SCHEMA: &str = "anvil.coremeta.index_segment_row.v1";
const INDEX_SEGMENT_AUTHZ_SCOPE_SCHEMA: &str = "anvil.index.segment_authz_scope.v1";
const INDEX_DEFINITION_CURRENT_ROW_SCHEMA: &str = "anvil.coremeta.index_definition_current.v1";
const INDEX_DEFINITION_STATE_ROW_SCHEMA: &str = "anvil.coremeta.index_definition_state.v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSegmentCoreMetaRecord {
    pub index_id: String,
    pub index_kind: String,
    pub writer_family: String,
    pub segment_ref: String,
    pub core_object_ref_target: String,
    pub segment_hash: String,
    pub segment_length: u64,
    pub generation: u64,
    pub source_kind: String,
    pub source_cursor: u64,
    pub authz_realm_id: String,
    pub authz_scope_hash: String,
    pub authz_revision: u64,
    pub row_count: u64,
    pub field_names: Vec<String>,
    pub created_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinitionCurrentCoreMetaRecord {
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub index_name: String,
    pub deleted: bool,
    pub cursor: i64,
    pub index_version: i64,
    pub event_payload: Vec<u8>,
    pub updated_at_unix_nanos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDefinitionStateCoreMetaRecord {
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub latest_cursor: i64,
    pub max_index_id: i64,
    pub updated_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct IndexSegmentCoreMetaRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    index_id: String,
    #[prost(string, tag = "4")]
    index_kind: String,
    #[prost(string, tag = "5")]
    writer_family: String,
    #[prost(string, tag = "6")]
    segment_ref: String,
    #[prost(string, tag = "7")]
    core_object_ref_target: String,
    #[prost(string, tag = "8")]
    segment_hash: String,
    #[prost(uint64, tag = "9")]
    segment_length: u64,
    #[prost(uint64, tag = "10")]
    generation: u64,
    #[prost(string, tag = "11")]
    source_kind: String,
    #[prost(uint64, tag = "12")]
    source_cursor: u64,
    #[prost(string, tag = "13")]
    authz_realm_id: String,
    #[prost(string, tag = "14")]
    authz_scope_hash: String,
    #[prost(uint64, tag = "15")]
    authz_revision: u64,
    #[prost(uint64, tag = "16")]
    row_count: u64,
    #[prost(string, repeated, tag = "17")]
    field_names: Vec<String>,
    #[prost(uint64, tag = "18")]
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDefinitionCurrentCoreMetaRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    index_name: String,
    #[prost(bool, tag = "6")]
    deleted: bool,
    #[prost(int64, tag = "7")]
    cursor: i64,
    #[prost(int64, tag = "8")]
    index_version: i64,
    #[prost(bytes, tag = "9")]
    event_payload: Vec<u8>,
    #[prost(uint64, tag = "10")]
    updated_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDefinitionStateCoreMetaRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(int64, tag = "5")]
    latest_cursor: i64,
    #[prost(int64, tag = "6")]
    max_index_id: i64,
    #[prost(uint64, tag = "7")]
    updated_at_unix_nanos: u64,
}

pub fn segment_authz_scope_hash(index_kind: &str, authorization_mode: &str) -> String {
    let payload = format!(
        "{INDEX_SEGMENT_AUTHZ_SCOPE_SCHEMA}\0index_kind={index_kind}\0authorization_mode={authorization_mode}\0namespace=object\0relation=reader"
    );
    format!("blake3:{}", blake3::hash(payload.as_bytes()).to_hex())
}

pub fn typed_segment_index_kind(source_kind: &str) -> &'static str {
    if source_kind == "object_metadata" {
        "metadata"
    } else {
        "typed_json"
    }
}

pub async fn write_index_segment_coremeta_record(
    storage: &Storage,
    record: &IndexSegmentCoreMetaRecord,
) -> Result<()> {
    validate_index_segment_record(record)?;
    let payload = encode_index_segment_record(record);
    let tuple_key = index_segment_tuple_key(record)?;
    let store = CoreStore::new(storage.clone()).await?;
    let op = CoreMetaBatchOp {
        cf: CF_INDEX_ROWS,
        table_id: TABLE_INDEX_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    store
        .commit_coremeta_batch_by_embedded_roots(
            &format!("index-segment:{}:{}", record.index_id, record.generation),
            &[op],
        )
        .await?;
    Ok(())
}

pub fn latest_index_segment_coremeta_record(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    latest_index_segment_coremeta_record_matching(storage, index_id, None)
}

pub fn latest_index_segment_coremeta_record_for_family(
    storage: &Storage,
    index_id: &str,
    writer_family: &str,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    latest_index_segment_coremeta_record_matching(storage, index_id, Some(writer_family))
}

pub fn index_segment_coremeta_record_for_family_generation(
    storage: &Storage,
    index_id: &str,
    writer_family: &str,
    generation: u64,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    let mut selected = None;
    for record in list_index_segment_coremeta_records(storage, index_id)? {
        if record.writer_family != writer_family || record.generation != generation {
            continue;
        }
        if selected
            .as_ref()
            .is_none_or(|current: &IndexSegmentCoreMetaRecord| {
                record.created_at_unix_nanos > current.created_at_unix_nanos
            })
        {
            selected = Some(record);
        }
    }
    Ok(selected)
}

fn latest_index_segment_coremeta_record_matching(
    storage: &Storage,
    index_id: &str,
    writer_family: Option<&str>,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let prefix = index_segment_tuple_prefix(index_id)?;
    let mut latest = None;
    for row in meta.scan_prefix(CF_INDEX_ROWS, TABLE_INDEX_ROW, &prefix)? {
        let record = decode_index_segment_record(&row.payload)?;
        if record.index_id != index_id {
            bail!("CoreMeta index segment row scope mismatch");
        }
        if writer_family.is_some_and(|family| record.writer_family != family) {
            continue;
        }
        if latest
            .as_ref()
            .is_none_or(|current: &IndexSegmentCoreMetaRecord| {
                (record.generation, record.created_at_unix_nanos)
                    > (current.generation, current.created_at_unix_nanos)
            })
        {
            latest = Some(record);
        }
    }
    Ok(latest)
}

pub fn read_index_segment_coremeta_record_by_ref(
    storage: &Storage,
    index_id: &str,
    segment_ref: &str,
) -> Result<Option<IndexSegmentCoreMetaRecord>> {
    for record in list_index_segment_coremeta_records(storage, index_id)? {
        if record.segment_ref == segment_ref {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

pub fn list_index_segment_coremeta_records(
    storage: &Storage,
    index_id: &str,
) -> Result<Vec<IndexSegmentCoreMetaRecord>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let prefix = index_segment_tuple_prefix(index_id)?;
    let mut records = meta
        .scan_prefix(CF_INDEX_ROWS, TABLE_INDEX_ROW, &prefix)?
        .into_iter()
        .map(|row| decode_index_segment_record(&row.payload))
        .collect::<Result<Vec<_>>>()?;
    records.sort_by_key(|record| (record.generation, record.created_at_unix_nanos));
    Ok(records)
}

pub async fn write_index_definition_current_coremeta_record(
    storage: &Storage,
    record: &IndexDefinitionCurrentCoreMetaRecord,
) -> Result<()> {
    validate_index_definition_current_record(record)?;
    let tuple_key =
        index_definition_current_tuple_key(record.tenant_id, record.bucket_id, &record.index_name)?;
    let payload = encode_index_definition_current_record(record);
    let store = CoreStore::new(storage.clone()).await?;
    let op = CoreMetaBatchOp {
        cf: CF_INDEX_DEFS,
        table_id: TABLE_INDEX_DEFINITION_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    store
        .commit_coremeta_batch_by_embedded_roots(
            &format!(
                "index-definition-current:{}:{}:{}",
                record.tenant_id, record.bucket_id, record.cursor
            ),
            &[op],
        )
        .await?;
    Ok(())
}

pub fn read_index_definition_current_coremeta_record(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<Option<IndexDefinitionCurrentCoreMetaRecord>> {
    let Some(payload) = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_INDEX_DEFS,
        TABLE_INDEX_DEFINITION_ROW,
        &index_definition_current_tuple_key(tenant_id, bucket_id, index_name)?,
    )?
    else {
        return Ok(None);
    };
    let record = decode_index_definition_current_record(&payload)?;
    validate_index_definition_current_scope(&record, tenant_id, bucket_id, index_name)?;
    Ok(Some(record))
}

pub fn list_index_definition_current_coremeta_records(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<IndexDefinitionCurrentCoreMetaRecord>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let mut records = Vec::new();
    for row in meta.scan_prefix(
        CF_INDEX_DEFS,
        TABLE_INDEX_DEFINITION_ROW,
        &index_definition_current_tuple_prefix(tenant_id, bucket_id)?,
    )? {
        let record = decode_index_definition_current_record(&row.payload)?;
        validate_index_definition_current_bucket_scope(&record, tenant_id, bucket_id)?;
        records.push(record);
    }
    records.sort_by(|left, right| left.index_name.cmp(&right.index_name));
    Ok(records)
}

pub async fn write_index_definition_state_coremeta_record(
    storage: &Storage,
    record: &IndexDefinitionStateCoreMetaRecord,
) -> Result<()> {
    validate_index_definition_state_record(record)?;
    let tuple_key = index_definition_state_tuple_key(record.tenant_id, record.bucket_id)?;
    let payload = encode_index_definition_state_record(record);
    let store = CoreStore::new(storage.clone()).await?;
    let op = CoreMetaBatchOp {
        cf: CF_INDEX_DEFS,
        table_id: TABLE_INDEX_DEFINITION_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    store
        .commit_coremeta_batch_by_embedded_roots(
            &format!(
                "index-definition-state:{}:{}:{}",
                record.tenant_id, record.bucket_id, record.latest_cursor
            ),
            &[op],
        )
        .await?;
    Ok(())
}

pub fn read_index_definition_state_coremeta_record(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Option<IndexDefinitionStateCoreMetaRecord>> {
    let Some(payload) = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_INDEX_DEFS,
        TABLE_INDEX_DEFINITION_ROW,
        &index_definition_state_tuple_key(tenant_id, bucket_id)?,
    )?
    else {
        return Ok(None);
    };
    let record = decode_index_definition_state_record(&payload)?;
    if record.tenant_id != tenant_id || record.bucket_id != bucket_id {
        bail!("CoreMeta index definition state row scope mismatch");
    }
    Ok(Some(record))
}

fn encode_index_segment_record(record: &IndexSegmentCoreMetaRecord) -> Vec<u8> {
    encode_deterministic_proto(&IndexSegmentCoreMetaRecordProto {
        schema: INDEX_SEGMENT_ROW_SCHEMA.to_string(),
        common: Some(CoreMetaRowCommonProto {
            realm_id: record.authz_realm_id.clone(),
            root_key_hash: index_segment_root_key_hash(&record.index_id),
            root_generation: record.generation,
            transaction_id: format!("index-segment:{}:{}", record.index_id, record.generation),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: record.created_at_unix_nanos,
            payload_schema_version: 1,
        }),
        index_id: record.index_id.clone(),
        index_kind: record.index_kind.clone(),
        writer_family: record.writer_family.clone(),
        segment_ref: record.segment_ref.clone(),
        core_object_ref_target: record.core_object_ref_target.clone(),
        segment_hash: record.segment_hash.clone(),
        segment_length: record.segment_length,
        generation: record.generation,
        source_kind: record.source_kind.clone(),
        source_cursor: record.source_cursor,
        authz_realm_id: record.authz_realm_id.clone(),
        authz_scope_hash: record.authz_scope_hash.clone(),
        authz_revision: record.authz_revision,
        row_count: record.row_count,
        field_names: record.field_names.clone(),
        created_at_unix_nanos: record.created_at_unix_nanos,
    })
}

fn encode_index_definition_current_record(
    record: &IndexDefinitionCurrentCoreMetaRecord,
) -> Vec<u8> {
    encode_deterministic_proto(&IndexDefinitionCurrentCoreMetaRecordProto {
        schema: INDEX_DEFINITION_CURRENT_ROW_SCHEMA.to_string(),
        common: Some(CoreMetaRowCommonProto {
            realm_id: format!("tenant:{}", record.tenant_id),
            root_key_hash: index_definition_root_key_hash(record.tenant_id, record.bucket_id),
            root_generation: record.cursor as u64,
            transaction_id: format!(
                "index-definition-current:{}:{}:{}",
                record.tenant_id, record.bucket_id, record.cursor
            ),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: record.updated_at_unix_nanos,
            payload_schema_version: 1,
        }),
        tenant_id: record.tenant_id,
        bucket_id: record.bucket_id,
        index_name: record.index_name.clone(),
        deleted: record.deleted,
        cursor: record.cursor,
        index_version: record.index_version,
        event_payload: record.event_payload.clone(),
        updated_at_unix_nanos: record.updated_at_unix_nanos,
    })
}

fn decode_index_definition_current_record(
    bytes: &[u8],
) -> Result<IndexDefinitionCurrentCoreMetaRecord> {
    let proto = decode_deterministic_proto::<IndexDefinitionCurrentCoreMetaRecordProto>(
        bytes,
        "index definition current row",
    )?;
    if proto.schema != INDEX_DEFINITION_CURRENT_ROW_SCHEMA {
        bail!("CoreMeta index definition current row has invalid schema");
    }
    let record = IndexDefinitionCurrentCoreMetaRecord {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        index_name: proto.index_name,
        deleted: proto.deleted,
        cursor: proto.cursor,
        index_version: proto.index_version,
        event_payload: proto.event_payload,
        updated_at_unix_nanos: proto.updated_at_unix_nanos,
    };
    validate_index_definition_current_record(&record)?;
    Ok(record)
}

fn encode_index_definition_state_record(record: &IndexDefinitionStateCoreMetaRecord) -> Vec<u8> {
    encode_deterministic_proto(&IndexDefinitionStateCoreMetaRecordProto {
        schema: INDEX_DEFINITION_STATE_ROW_SCHEMA.to_string(),
        common: Some(CoreMetaRowCommonProto {
            realm_id: format!("tenant:{}", record.tenant_id),
            root_key_hash: index_definition_root_key_hash(record.tenant_id, record.bucket_id),
            root_generation: record.latest_cursor as u64,
            transaction_id: format!(
                "index-definition-state:{}:{}:{}",
                record.tenant_id, record.bucket_id, record.latest_cursor
            ),
            visibility_state: CoreMetaVisibilityState::Committed as i32,
            created_at_unix_nanos: record.updated_at_unix_nanos,
            payload_schema_version: 1,
        }),
        tenant_id: record.tenant_id,
        bucket_id: record.bucket_id,
        latest_cursor: record.latest_cursor,
        max_index_id: record.max_index_id,
        updated_at_unix_nanos: record.updated_at_unix_nanos,
    })
}

fn decode_index_definition_state_record(
    bytes: &[u8],
) -> Result<IndexDefinitionStateCoreMetaRecord> {
    let proto = decode_deterministic_proto::<IndexDefinitionStateCoreMetaRecordProto>(
        bytes,
        "index definition state row",
    )?;
    if proto.schema != INDEX_DEFINITION_STATE_ROW_SCHEMA {
        bail!("CoreMeta index definition state row has invalid schema");
    }
    let record = IndexDefinitionStateCoreMetaRecord {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        latest_cursor: proto.latest_cursor,
        max_index_id: proto.max_index_id,
        updated_at_unix_nanos: proto.updated_at_unix_nanos,
    };
    validate_index_definition_state_record(&record)?;
    Ok(record)
}

fn decode_index_segment_record(bytes: &[u8]) -> Result<IndexSegmentCoreMetaRecord> {
    let proto =
        decode_deterministic_proto::<IndexSegmentCoreMetaRecordProto>(bytes, "index segment row")?;
    if proto.schema != INDEX_SEGMENT_ROW_SCHEMA {
        bail!("CoreMeta index segment row has invalid schema");
    }
    let record = IndexSegmentCoreMetaRecord {
        index_id: proto.index_id,
        index_kind: proto.index_kind,
        writer_family: proto.writer_family,
        segment_ref: proto.segment_ref,
        core_object_ref_target: proto.core_object_ref_target,
        segment_hash: proto.segment_hash,
        segment_length: proto.segment_length,
        generation: proto.generation,
        source_kind: proto.source_kind,
        source_cursor: proto.source_cursor,
        authz_realm_id: proto.authz_realm_id,
        authz_scope_hash: proto.authz_scope_hash,
        authz_revision: proto.authz_revision,
        row_count: proto.row_count,
        field_names: proto.field_names,
        created_at_unix_nanos: proto.created_at_unix_nanos,
    };
    validate_index_segment_record(&record)?;
    Ok(record)
}

fn validate_index_segment_record(record: &IndexSegmentCoreMetaRecord) -> Result<()> {
    require_nonempty(&record.index_id, "index_id")?;
    require_nonempty(&record.index_kind, "index_kind")?;
    require_nonempty(&record.writer_family, "writer_family")?;
    require_nonempty(&record.segment_ref, "segment_ref")?;
    if !record
        .core_object_ref_target
        .starts_with("core-object-ref:")
    {
        bail!("index segment CoreMeta row must carry a CoreStore object ref target");
    }
    validate_hex32(&record.segment_hash, "segment_hash")?;
    if record.segment_length == 0 {
        bail!("index segment length must be nonzero");
    }
    if record.generation == 0 {
        bail!("index segment generation must be nonzero");
    }
    require_nonempty(&record.source_kind, "source_kind")?;
    require_nonempty(&record.authz_realm_id, "authz_realm_id")?;
    validate_hash(&record.authz_scope_hash, "authz_scope_hash")?;
    Ok(())
}

fn validate_index_definition_current_record(
    record: &IndexDefinitionCurrentCoreMetaRecord,
) -> Result<()> {
    validate_index_definition_scope(record.tenant_id, record.bucket_id)?;
    require_nonempty(&record.index_name, "index_name")?;
    if record.index_name.chars().any(char::is_control) {
        bail!("index_name must not contain control characters");
    }
    if record.cursor <= 0 {
        bail!("index definition current cursor must be positive");
    }
    if record.index_version < 0 {
        bail!("index definition version must not be negative");
    }
    if record.event_payload.is_empty() {
        bail!("index definition current row must carry an event payload");
    }
    Ok(())
}

fn validate_index_definition_current_bucket_scope(
    record: &IndexDefinitionCurrentCoreMetaRecord,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<()> {
    if record.tenant_id != tenant_id || record.bucket_id != bucket_id {
        bail!("CoreMeta index definition current row bucket scope mismatch");
    }
    Ok(())
}

fn validate_index_definition_current_scope(
    record: &IndexDefinitionCurrentCoreMetaRecord,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<()> {
    validate_index_definition_current_bucket_scope(record, tenant_id, bucket_id)?;
    if record.index_name != index_name {
        bail!("CoreMeta index definition current row name scope mismatch");
    }
    Ok(())
}

fn validate_index_definition_state_record(
    record: &IndexDefinitionStateCoreMetaRecord,
) -> Result<()> {
    validate_index_definition_scope(record.tenant_id, record.bucket_id)?;
    if record.latest_cursor < 0 || record.max_index_id < 0 {
        bail!("index definition state counters must not be negative");
    }
    Ok(())
}

fn validate_index_definition_scope(tenant_id: i64, bucket_id: i64) -> Result<()> {
    if tenant_id < 0 || bucket_id < 0 {
        bail!("index definition CoreMeta scope ids must be nonnegative");
    }
    Ok(())
}

fn index_definition_root_anchor_key(tenant_id: i64, bucket_id: i64) -> String {
    format!("tenant/{tenant_id}/bucket/{bucket_id}/index_definition")
}

fn index_definition_root_key_hash(tenant_id: i64, bucket_id: i64) -> String {
    core_meta_root_key_hash(&index_definition_root_anchor_key(tenant_id, bucket_id))
}

fn index_segment_root_key_hash(index_id: &str) -> String {
    core_meta_root_key_hash(&format!("index/{index_id}/segments"))
}

fn index_definition_current_tuple_prefix(tenant_id: i64, bucket_id: i64) -> Result<Vec<u8>> {
    validate_index_definition_scope(tenant_id, bucket_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_definition_current"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

fn index_definition_current_tuple_key(
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
) -> Result<Vec<u8>> {
    require_nonempty(index_name, "index_name")?;
    if index_name.chars().any(char::is_control) {
        bail!("index_name must not contain control characters");
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_definition_current"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Utf8(index_name),
    ])
}

fn index_definition_state_tuple_key(tenant_id: i64, bucket_id: i64) -> Result<Vec<u8>> {
    validate_index_definition_scope(tenant_id, bucket_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("index_definition_state"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

fn index_segment_tuple_prefix(index_id: &str) -> Result<Vec<u8>> {
    tuple_key(&[TuplePart::Str("index_segment"), TuplePart::Str(index_id)])
}

fn index_segment_tuple_key(record: &IndexSegmentCoreMetaRecord) -> Result<Vec<u8>> {
    tuple_key(&[
        TuplePart::Str("index_segment"),
        TuplePart::Str(&record.index_id),
        TuplePart::Str(&record.index_kind),
        TuplePart::U64(record.generation),
        TuplePart::Str(&record.segment_hash),
    ])
}

enum TuplePart<'a> {
    Str(&'a str),
    U64(u64),
}

fn tuple_key(parts: &[TuplePart<'_>]) -> Result<Vec<u8>> {
    if parts.len() > u16::MAX as usize {
        bail!("CoreMeta tuple key has too many parts");
    }
    let mut out = Vec::new();
    out.extend_from_slice(&(parts.len() as u16).to_le_bytes());
    for part in parts {
        match part {
            TuplePart::Str(value) => {
                if value.as_bytes().contains(&0) {
                    bail!("CoreMeta tuple string part contains NUL");
                }
                push_tuple_part(&mut out, 0x01, value.as_bytes())?;
            }
            TuplePart::U64(value) => push_tuple_part(&mut out, 0x03, &value.to_le_bytes())?,
        }
    }
    Ok(out)
}

fn push_tuple_part(out: &mut Vec<u8>, kind: u8, value: &[u8]) -> Result<()> {
    if value.len() > u16::MAX as usize {
        return Err(anyhow!("CoreMeta tuple part exceeds u16 length"));
    }
    out.push(kind);
    out.push(0);
    out.extend_from_slice(&(value.len() as u16).to_le_bytes());
    out.extend_from_slice(value);
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("{field} must be hex32");
    }
    Ok(())
}

fn validate_hash(value: &str, field: &'static str) -> Result<()> {
    let Some((algorithm, hex)) = value.split_once(':') else {
        bail!("{field} must be algorithm:hex");
    };
    if algorithm.is_empty()
        || hex.is_empty()
        || !algorithm
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || !hex.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("{field} must be algorithm:hex");
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}
