use crate::{
    authz_journal::{
        self,
        resolver::{SchemaRuleIndex, UsersetRef, collect_subjects_for_userset},
    },
    authz_realm_schema,
    authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace, split_realm_namespace},
    authz_userset_index::AuthzDerivedUsersetEntry,
    core_store::{CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob},
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
    query_planner::{
        AuthzCandidateReader, AuthzCandidateRequest, AuthzDecision, CandidateSet,
        CandidateSetScope, ObjectAuthzKey, stable_doc_ordinal,
    },
    storage::Storage,
    writer_segment_catalog::{
        WriterSegmentCatalogRecord, latest_writer_segment_catalog_record,
        list_writer_segment_catalog_records, read_writer_segment_catalog_record,
        write_writer_segment_catalog_record,
    },
};
use anyhow::{Context, Result, anyhow, bail};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, LazyLock},
};

mod delta;

use delta::read_authz_tuple_segment_at_revision;
pub(crate) use delta::write_authz_tuple_delta_segment;

const AUTHZ_TUPLE_SEGMENT_REF_PREFIX: &str = "authz_tuple_segment:";
const AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY: &str = "authz_tuple";
const TABLE_AUTHZ_SCHEMA_DESCRIPTOR: u16 = 0x0501;
const TABLE_AUTHZ_TUPLE: u16 = 0x0502;
const TABLE_AUTHZ_RELATION_RULE: u16 = 0x0503;
const TABLE_AUTHZ_USERSET_EDGE: u16 = 0x0504;
const TABLE_AUTHZ_CAVEAT_DESCRIPTOR: u16 = 0x0505;
const TABLE_AUTHZ_REVISION_LOG: u16 = 0x0506;
const TABLE_AUTHZ_LIST_OBJECTS: u16 = 0x0507;
const TABLE_AUTHZ_LIST_SUBJECTS: u16 = 0x0508;
const AUTHZ_DELTA_CHECKPOINT_INTERVAL: u64 = 256;

static AUTHZ_SEGMENT_CATCHUP_LOCKS: LazyLock<
    std::sync::Mutex<BTreeMap<i64, Arc<tokio::sync::Mutex<()>>>>,
> = LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

fn authz_segment_catchup_lock(tenant_id: i64) -> Result<Arc<tokio::sync::Mutex<()>>> {
    let mut locks = AUTHZ_SEGMENT_CATCHUP_LOCKS
        .lock()
        .map_err(|_| anyhow!("authz segment catch-up lock is poisoned"))?;
    Ok(locks
        .entry(tenant_id)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuthzSegmentHeader {
    pub tenant_id: String,
    pub partition_family: String,
    pub partition_id: String,
    pub generation: u64,
    pub base_revision: u64,
    pub segment_kind: String,
    pub schema_replacement: bool,
    pub relation_rule_replacement: bool,
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
    pub schema_descriptors: Vec<AuthzSchemaDescriptorRow>,
    pub relation_rules: Vec<AuthzRelationRuleRow>,
    pub userset_edges: Vec<AuthzUsersetEdgeRow>,
    pub revision_checkpoints: Vec<AuthzRevisionCheckpointRow>,
    pub list_objects: Vec<AuthzListObjectsRow>,
    pub list_subjects: Vec<AuthzListSubjectsRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzSchemaDescriptorRow {
    pub tenant_id: i64,
    pub realm_id: String,
    pub namespace: String,
    pub schema_id: String,
    pub schema_revision: u64,
    pub schema_digest: String,
    pub binding_generation: u64,
    pub authz_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzRelationRuleRow {
    pub realm_id: String,
    pub namespace: String,
    pub relation: String,
    pub rule_kind: String,
    pub inherited_relation: String,
    pub tuple_relation: String,
    pub target_relation: String,
    pub schema_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzUsersetEdgeRow {
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
    pub source: String,
    pub revision: u64,
    pub operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzRevisionCheckpointRow {
    pub tenant_id: i64,
    pub revision: u64,
    pub source_fence_token: u64,
    pub tuple_record_count: u64,
    pub active_tuple_count: u64,
    pub derived_userset_count: u64,
    pub list_objects_count: u64,
    pub list_subjects_count: u64,
    pub tuple_records_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzListObjectsRow {
    pub namespace: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
    pub object_id: String,
    pub doc_ordinal: u64,
    pub revision: u64,
    pub operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzListSubjectsRow {
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
    pub doc_ordinal: u64,
    pub revision: u64,
    pub operation: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzSchemaDescriptorRowProto {
    #[prost(int64, tag = "1")]
    tenant_id: i64,
    #[prost(string, tag = "2")]
    realm_id: String,
    #[prost(string, tag = "3")]
    namespace: String,
    #[prost(string, tag = "4")]
    schema_id: String,
    #[prost(uint64, tag = "5")]
    schema_revision: u64,
    #[prost(string, tag = "6")]
    schema_digest: String,
    #[prost(uint64, tag = "7")]
    binding_generation: u64,
    #[prost(uint64, tag = "8")]
    authz_revision: u64,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzRelationRuleRowProto {
    #[prost(string, tag = "1")]
    realm_id: String,
    #[prost(string, tag = "2")]
    namespace: String,
    #[prost(string, tag = "3")]
    relation: String,
    #[prost(string, tag = "4")]
    rule_kind: String,
    #[prost(string, tag = "5")]
    inherited_relation: String,
    #[prost(string, tag = "6")]
    tuple_relation: String,
    #[prost(string, tag = "7")]
    target_relation: String,
    #[prost(uint64, tag = "8")]
    schema_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzUsersetEdgeRowProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    object_id: String,
    #[prost(string, tag = "3")]
    relation: String,
    #[prost(string, tag = "4")]
    subject_kind: String,
    #[prost(string, tag = "5")]
    subject_id: String,
    #[prost(string, tag = "6")]
    caveat_hash: String,
    #[prost(string, tag = "7")]
    source: String,
    #[prost(uint64, tag = "8")]
    revision: u64,
    #[prost(string, tag = "9")]
    operation: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzRevisionCheckpointRowProto {
    #[prost(int64, tag = "1")]
    tenant_id: i64,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(uint64, tag = "3")]
    source_fence_token: u64,
    #[prost(uint64, tag = "4")]
    tuple_record_count: u64,
    #[prost(uint64, tag = "5")]
    active_tuple_count: u64,
    #[prost(uint64, tag = "6")]
    derived_userset_count: u64,
    #[prost(uint64, tag = "7")]
    list_objects_count: u64,
    #[prost(uint64, tag = "8")]
    list_subjects_count: u64,
    #[prost(string, tag = "9")]
    tuple_records_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzListObjectsRowProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    relation: String,
    #[prost(string, tag = "3")]
    subject_kind: String,
    #[prost(string, tag = "4")]
    subject_id: String,
    #[prost(string, tag = "5")]
    caveat_hash: String,
    #[prost(string, tag = "6")]
    object_id: String,
    #[prost(uint64, tag = "7")]
    doc_ordinal: u64,
    #[prost(uint64, tag = "8")]
    revision: u64,
    #[prost(string, tag = "9")]
    operation: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzListSubjectsRowProto {
    #[prost(string, tag = "1")]
    namespace: String,
    #[prost(string, tag = "2")]
    object_id: String,
    #[prost(string, tag = "3")]
    relation: String,
    #[prost(string, tag = "4")]
    subject_kind: String,
    #[prost(string, tag = "5")]
    subject_id: String,
    #[prost(string, tag = "6")]
    caveat_hash: String,
    #[prost(uint64, tag = "7")]
    doc_ordinal: u64,
    #[prost(uint64, tag = "8")]
    revision: u64,
    #[prost(string, tag = "9")]
    operation: String,
}

#[cfg(test)]
async fn write_authz_tuple_segment(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
) -> Result<String> {
    write_authz_tuple_checkpoint_segment(storage, tenant_id, records, &[], 0).await
}

pub(crate) async fn write_authz_tuple_checkpoint_segment(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    source_fence_token: u64,
) -> Result<String> {
    let segment_records = segment_records_from_authz_records(records)?;
    let active_records = active_tuple_records(records);
    let schema_rows = schema_descriptor_rows(storage, tenant_id, &active_records).await?;
    let bound_relation_rule_rows =
        bound_relation_rule_rows(storage, tenant_id, &active_records).await?;
    let relation_rule_rows =
        all_relation_rule_rows(storage, tenant_id, &bound_relation_rule_rows).await?;
    let generation = authz_tuple_segment_generation(records, &schema_rows, &relation_rule_rows)?;
    let segment_tables = authz_writer_tables_with_rows(
        storage,
        tenant_id,
        records,
        derived_usersets,
        &segment_records,
        generation,
        source_fence_token,
        &schema_rows,
        &relation_rule_rows,
        &bound_relation_rule_rows,
    )
    .await?;
    write_authz_tuple_segment_tables(
        storage,
        tenant_id,
        0,
        generation,
        "checkpoint",
        true,
        true,
        source_fence_token,
        &segment_records,
        segment_tables,
    )
    .await
}

async fn write_authz_tuple_segment_tables(
    storage: &Storage,
    tenant_id: i64,
    base_revision: u64,
    generation: u64,
    segment_kind: &str,
    schema_replacement: bool,
    relation_rule_replacement: bool,
    source_fence_token: u64,
    segment_records: &[SegmentRecord],
    segment_tables: Vec<WriterBodyTable>,
) -> Result<String> {
    let ref_name = authz_tuple_segment_ref_name(tenant_id, generation)?;
    let header = AuthzSegmentHeader {
        tenant_id: tenant_id.to_string(),
        partition_family: "authz_tuple".to_string(),
        partition_id: hex::encode(partition_id(tenant_id)),
        generation,
        base_revision,
        segment_kind: segment_kind.to_string(),
        schema_replacement,
        relation_rule_replacement,
        source_fence_token,
        key_order: "tuple_key_revision".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        codec: "writer-body-table-v1".to_string(),
    };
    let body = encode_writer_body_tables(&segment_tables)?;
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
    let object_ref = receipt
        .written_object_refs
        .first()
        .cloned()
        .ok_or_else(|| anyhow!("CoreFormatWriter returned no authz object"))?;
    write_writer_segment_catalog_record(
        storage,
        &WriterSegmentCatalogRecord {
            family: AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY.to_string(),
            scope: authz_tuple_segment_scope(tenant_id)?,
            segment_ref: ref_name.clone(),
            core_object_ref_target: encode_core_object_ref_target(&object_ref)?,
            segment_hash: hex::encode(segment_hash),
            segment_length: object_ref.logical_size,
            generation,
            source_cursor: generation,
            created_at_unix_nanos: unix_nanos_from_rfc3339(&header.created_at),
        },
    )
    .await?;
    Ok(ref_name)
}

fn authz_tuple_segment_generation(
    records: &[AuthzTupleRecord],
    schema_rows: &[AuthzSchemaDescriptorRow],
    relation_rule_rows: &[AuthzRelationRuleRow],
) -> Result<u64> {
    let record_generation = records
        .iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or(0);
    let record_generation =
        u64::try_from(record_generation).context("authz segment generation is negative")?;
    let schema_generation = schema_rows
        .iter()
        .map(|row| {
            row.authz_revision
                .max(row.schema_revision)
                .max(row.binding_generation)
        })
        .max()
        .unwrap_or(0);
    let relation_generation = relation_rule_rows
        .iter()
        .map(|row| row.schema_generation)
        .max()
        .unwrap_or(0);
    Ok(record_generation
        .max(schema_generation)
        .max(relation_generation))
}

pub async fn read_latest_authz_tuple_segment(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Option<DecodedAuthzSegment>> {
    let Some(record) = latest_writer_segment_catalog_record(
        storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &authz_tuple_segment_scope(tenant_id)?,
    )?
    else {
        return Ok(None);
    };
    read_authz_tuple_segment_at_revision(storage, tenant_id, record.generation).await
}

async fn read_authz_tuple_segment_ref(
    storage: &Storage,
    tenant_id: i64,
    generation: u64,
    segment_ref: &str,
) -> Result<Option<DecodedAuthzSegment>> {
    let Some(record) =
        read_authz_tuple_segment_catalog_record(storage, tenant_id, generation, segment_ref)?
    else {
        return Ok(None);
    };
    let store = CoreStore::new(storage.clone()).await?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&record.core_object_ref_target)?,
        })
        .await?;
    Ok(Some(decode_authz_tuple_segment(&bytes)?))
}

pub async fn ensure_authz_tuple_segment_at_revision(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
) -> Result<Option<DecodedAuthzSegment>> {
    if target_revision == 0 {
        return Ok(None);
    }
    if let Some(segment) =
        read_authz_tuple_segment_at_revision(storage, tenant_id, target_revision).await?
    {
        if authz_segment_covers_revision(&segment, target_revision) {
            return Ok(Some(segment));
        }
    }

    let catchup_lock = authz_segment_catchup_lock(tenant_id)?;
    let _guard = catchup_lock.lock().await;

    if let Some(segment) =
        read_authz_tuple_segment_at_revision(storage, tenant_id, target_revision).await?
    {
        if authz_segment_covers_revision(&segment, target_revision) {
            return Ok(Some(segment));
        }
    }

    let latest_revision = authz_journal::latest_authz_revision(storage, tenant_id).await?;
    let latest_revision = u64::try_from(latest_revision.max(0))
        .context("latest authz revision exceeds supported range")?;
    if latest_revision < target_revision {
        bail!("AuthzCandidateSetStale");
    }

    let source_fence_token =
        authz_journal::latest_authz_journal_fence_token(storage, tenant_id).await?;
    let segment_ref = authz_tuple_segment_ref_name(tenant_id, target_revision)?;
    if read_authz_tuple_segment_catalog_record(storage, tenant_id, target_revision, &segment_ref)?
        .is_none()
    {
        // Historical stores may predate synchronous segment materialisation. Build the
        // requested revision as a checkpoint instead of replaying every missing revision.
        authz_journal::materialize_authz_tuple_segment_at_revision(
            storage,
            tenant_id,
            target_revision,
            source_fence_token,
        )
        .await?;
    }

    let Some(segment) =
        read_authz_tuple_segment_at_revision(storage, tenant_id, target_revision).await?
    else {
        bail!("AuthzCandidateSetStale");
    };
    if !authz_segment_covers_revision(&segment, target_revision) {
        bail!("AuthzCandidateSetStale");
    }
    Ok(Some(segment))
}

fn authz_segment_covers_revision(segment: &DecodedAuthzSegment, target_revision: u64) -> bool {
    segment.header.generation == target_revision
        && segment
            .records
            .iter()
            .all(|record| u64::try_from(record.revision.max(0)).unwrap_or(0) <= target_revision)
}

pub fn decode_authz_tuple_segment(bytes: &[u8]) -> Result<DecodedAuthzSegment> {
    let segment = decode_writer_segment(bytes, FileFamily::AuthzTupleSegment)?;
    let header = decode_authz_header_proto(&segment.header)?;
    let mut schema_descriptors = Vec::new();
    let mut records = Vec::new();
    let mut relation_rules = Vec::new();
    let mut userset_edges = Vec::new();
    let mut revision_checkpoints = Vec::new();
    let mut list_objects = Vec::new();
    let mut list_subjects = Vec::new();
    for table in decode_writer_body_tables(segment.body)? {
        match table.table_id {
            TABLE_AUTHZ_SCHEMA_DESCRIPTOR => {
                for row in table.rows {
                    schema_descriptors.push(decode_schema_descriptor_row(&row.value)?);
                }
            }
            TABLE_AUTHZ_TUPLE => {
                for row in table.rows {
                    let mut record =
                        authz_record_from_segment_record(SegmentRecord::new(row.key, row.value))?;
                    record.tenant_id = header.tenant_id.parse::<i64>().unwrap_or_default();
                    records.push(record);
                }
            }
            TABLE_AUTHZ_RELATION_RULE => {
                for row in table.rows {
                    relation_rules.push(decode_relation_rule_row(&row.value)?);
                }
            }
            TABLE_AUTHZ_USERSET_EDGE => {
                for row in table.rows {
                    userset_edges.push(decode_userset_edge_row(&row.value)?);
                }
            }
            TABLE_AUTHZ_REVISION_LOG => {
                for row in table.rows {
                    revision_checkpoints.push(decode_revision_checkpoint_row(&row.value)?);
                }
            }
            TABLE_AUTHZ_LIST_OBJECTS => {
                for row in table.rows {
                    list_objects.push(decode_list_objects_row(&row.value)?);
                }
            }
            TABLE_AUTHZ_LIST_SUBJECTS => {
                for row in table.rows {
                    list_subjects.push(decode_list_subjects_row(&row.value)?);
                }
            }
            TABLE_AUTHZ_CAVEAT_DESCRIPTOR => {}
            _ => return Err(anyhow!("unknown authz segment table {}", table.table_id)),
        }
    }
    Ok(DecodedAuthzSegment {
        header,
        records,
        schema_descriptors,
        relation_rules,
        userset_edges,
        revision_checkpoints,
        list_objects,
        list_subjects,
    })
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
            header_field_u64("base_revision", header.base_revision),
            header_field_string("segment_kind", header.segment_kind.clone()),
            header_field_u64("schema_replacement", u64::from(header.schema_replacement)),
            header_field_u64(
                "relation_rule_replacement",
                u64::from(header.relation_rule_replacement),
            ),
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
        base_revision: required_header_u64(header, "base_revision")?,
        segment_kind: required_header_string(header, "segment_kind")?,
        schema_replacement: required_header_u64(header, "schema_replacement")? != 0,
        relation_rule_replacement: required_header_u64(header, "relation_rule_replacement")? != 0,
        source_fence_token: required_header_u64(header, "source_fence_token")?,
        key_order: required_header_string(header, "key_order")?,
        created_at: required_header_string(header, "created_at")?,
        codec: required_header_string(header, "codec")?,
    })
}

pub(crate) fn authz_tuple_segment_requires_checkpoint(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
) -> Result<bool> {
    let previous = list_writer_segment_catalog_records(
        storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &authz_tuple_segment_scope(tenant_id)?,
    )?
    .into_iter()
    .filter(|record| record.generation < target_revision)
    .max_by_key(|record| (record.generation, record.created_at_unix_nanos));
    Ok(previous.is_none_or(|record| {
        record.generation.saturating_add(1) != target_revision
            || target_revision % AUTHZ_DELTA_CHECKPOINT_INTERVAL == 0
    }))
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
    generation: u64,
    segment_ref: &str,
) -> Result<Option<WriterSegmentCatalogRecord>> {
    read_writer_segment_catalog_record(
        storage,
        AUTHZ_TUPLE_SEGMENT_CATALOG_FAMILY,
        &authz_tuple_segment_scope(tenant_id)?,
        generation,
        segment_ref,
    )
}

pub(crate) fn existing_authz_tuple_segment_ref(
    storage: &Storage,
    tenant_id: i64,
    revision: u64,
) -> Result<Option<String>> {
    let segment_ref = authz_tuple_segment_ref_name(tenant_id, revision)?;
    Ok(
        read_authz_tuple_segment_catalog_record(storage, tenant_id, revision, &segment_ref)?
            .map(|record| record.segment_ref),
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

async fn authz_writer_tables_with_rows(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    segment_records: &[SegmentRecord],
    generation: u64,
    source_fence_token: u64,
    schema_rows: &[AuthzSchemaDescriptorRow],
    relation_rule_rows: &[AuthzRelationRuleRow],
    bound_relation_rule_rows: &[AuthzRelationRuleRow],
) -> Result<Vec<WriterBodyTable>> {
    let tuple_rows = segment_records
        .iter()
        .map(|record| TableRow {
            key: record.key.clone(),
            value: record.value.clone(),
        })
        .collect::<Vec<_>>();
    let active_records = active_tuple_records(records);
    let current = tuple_view_from_active_records(&active_records);
    let userset_edge_rows = userset_edge_rows(&active_records, derived_usersets, generation)?;
    let list_object_rows = list_object_rows(
        storage,
        tenant_id,
        &active_records,
        derived_usersets,
        &bound_relation_rule_rows,
        &current,
        generation,
    )
    .await?;
    let list_subject_rows = list_subject_rows(
        storage,
        tenant_id,
        &active_records,
        derived_usersets,
        &bound_relation_rule_rows,
        &current,
        generation,
    )
    .await?;
    let checkpoint_rows = vec![AuthzRevisionCheckpointRow {
        tenant_id,
        revision: generation,
        source_fence_token,
        tuple_record_count: records.len() as u64,
        active_tuple_count: active_records.len() as u64,
        derived_userset_count: derived_usersets.len() as u64,
        list_objects_count: list_object_rows.len() as u64,
        list_subjects_count: list_subject_rows.len() as u64,
        tuple_records_hash: hex::encode(tuple_records_hash(records)?),
    }];
    Ok([
        (
            TABLE_AUTHZ_SCHEMA_DESCRIPTOR,
            table_rows_from(
                schema_rows.to_vec(),
                schema_descriptor_key,
                encode_schema_descriptor_row,
            )?,
        ),
        (TABLE_AUTHZ_TUPLE, tuple_rows),
        (
            TABLE_AUTHZ_RELATION_RULE,
            table_rows_from(
                relation_rule_rows.to_vec(),
                relation_rule_key,
                encode_relation_rule_row,
            )?,
        ),
        (
            TABLE_AUTHZ_USERSET_EDGE,
            table_rows_from(userset_edge_rows, userset_edge_key, encode_userset_edge_row)?,
        ),
        (TABLE_AUTHZ_CAVEAT_DESCRIPTOR, Vec::new()),
        (
            TABLE_AUTHZ_REVISION_LOG,
            table_rows_from(
                checkpoint_rows,
                revision_checkpoint_key,
                encode_revision_checkpoint_row,
            )?,
        ),
        (
            TABLE_AUTHZ_LIST_OBJECTS,
            table_rows_from(list_object_rows, list_object_key, encode_list_objects_row)?,
        ),
        (
            TABLE_AUTHZ_LIST_SUBJECTS,
            table_rows_from(
                list_subject_rows,
                list_subject_key,
                encode_list_subjects_row,
            )?,
        ),
    ]
    .into_iter()
    .map(|(table_id, rows)| WriterBodyTable {
        table_id,
        row_type_id: table_id,
        rows,
    })
    .collect::<Vec<_>>())
}

fn table_rows_from<T>(
    mut rows: Vec<T>,
    key_fn: fn(&T) -> Result<Vec<u8>>,
    encode_fn: fn(&T) -> Result<Vec<u8>>,
) -> Result<Vec<TableRow>>
where
    T: Ord,
{
    rows.sort();
    let mut by_key = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for row in rows {
        by_key.insert(key_fn(&row)?, encode_fn(&row)?);
    }
    Ok(by_key
        .into_iter()
        .map(|(key, value)| TableRow { key, value })
        .collect())
}

fn active_tuple_records(records: &[AuthzTupleRecord]) -> Vec<AuthzTupleRecord> {
    let mut current = BTreeMap::<TupleIdentity, AuthzTupleRecord>::new();
    let mut ordered = records.to_vec();
    ordered.sort_by_key(|record| (record.revision, record.revision_ordinal));
    for record in ordered {
        current.insert(TupleIdentity::from(&record), record);
    }
    current
        .into_values()
        .filter(|record| record.operation == "add")
        .collect()
}

fn tuple_view_from_active_records(
    active_records: &[AuthzTupleRecord],
) -> BTreeMap<authz_journal::TupleViewKey, AuthzTupleRecord> {
    active_records
        .iter()
        .filter(|record| record.operation == "add")
        .map(|record| (authz_journal::TupleViewKey::from(record), record.clone()))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TupleIdentity {
    namespace: String,
    object_id: String,
    relation: String,
    subject_kind: String,
    subject_id: String,
    caveat_hash: String,
}

impl From<&AuthzTupleRecord> for TupleIdentity {
    fn from(record: &AuthzTupleRecord) -> Self {
        Self {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
        }
    }
}

async fn schema_descriptor_rows(
    storage: &Storage,
    tenant_id: i64,
    active_records: &[AuthzTupleRecord],
) -> Result<Vec<AuthzSchemaDescriptorRow>> {
    let namespace_parts = active_records
        .iter()
        .map(|record| namespace_realm_parts(&record.namespace))
        .collect::<BTreeSet<_>>();
    let mut rows = BTreeSet::new();
    for revision in authz_realm_schema::list_schema_revisions(storage, tenant_id).await? {
        for namespace in &revision.namespaces {
            rows.insert(AuthzSchemaDescriptorRow {
                tenant_id,
                realm_id: String::new(),
                namespace: namespace.namespace.clone(),
                schema_id: revision.schema_ref.schema_id.clone(),
                schema_revision: revision.schema_ref.schema_revision,
                schema_digest: revision.schema_ref.schema_digest.clone(),
                binding_generation: 0,
                authz_revision: revision.authz_revision,
            });
        }
    }
    for binding in authz_realm_schema::list_schema_bindings(storage, tenant_id).await? {
        let Some(revision) = authz_realm_schema::read_schema_revision(
            storage,
            tenant_id,
            &binding.schema_ref.schema_id,
            Some(binding.schema_ref.schema_revision),
        )
        .await?
        else {
            continue;
        };
        for namespace in &revision.namespaces {
            rows.insert(AuthzSchemaDescriptorRow {
                tenant_id,
                realm_id: binding.realm_id.clone(),
                namespace: canonical_bound_namespace(&binding.realm_id, &namespace.namespace),
                schema_id: binding.schema_ref.schema_id.clone(),
                schema_revision: binding.schema_ref.schema_revision,
                schema_digest: binding.schema_ref.schema_digest.clone(),
                binding_generation: binding.binding_generation,
                authz_revision: binding.authz_revision,
            });
        }
    }
    for (realm_id, namespace) in namespace_parts {
        if let Some(binding) =
            authz_realm_schema::read_schema_binding(storage, tenant_id, &realm_id).await?
        {
            rows.insert(AuthzSchemaDescriptorRow {
                tenant_id,
                realm_id,
                namespace,
                schema_id: binding.schema_ref.schema_id,
                schema_revision: binding.schema_ref.schema_revision,
                schema_digest: binding.schema_ref.schema_digest,
                binding_generation: binding.binding_generation,
                authz_revision: binding.authz_revision,
            });
        } else {
            rows.insert(AuthzSchemaDescriptorRow {
                tenant_id,
                realm_id,
                namespace,
                schema_id: String::new(),
                schema_revision: 0,
                schema_digest: String::new(),
                binding_generation: 0,
                authz_revision: 0,
            });
        }
    }
    if rows.is_empty() {
        rows.insert(AuthzSchemaDescriptorRow {
            tenant_id,
            realm_id: DEFAULT_AUTHZ_REALM_ID.to_string(),
            namespace: "_empty".to_string(),
            schema_id: String::new(),
            schema_revision: 0,
            schema_digest: String::new(),
            binding_generation: 0,
            authz_revision: 0,
        });
    }
    Ok(rows.into_iter().collect())
}

async fn bound_relation_rule_rows(
    storage: &Storage,
    tenant_id: i64,
    active_records: &[AuthzTupleRecord],
) -> Result<Vec<AuthzRelationRuleRow>> {
    let namespace_parts = active_records
        .iter()
        .map(|record| {
            let (realm_id, local_namespace) = namespace_realm_parts(&record.namespace);
            (realm_id, local_namespace, record.namespace.clone())
        })
        .collect::<BTreeSet<_>>();
    let mut rows = BTreeSet::new();
    for (realm_id, namespace, canonical_namespace) in namespace_parts {
        let Some(binding) =
            authz_realm_schema::read_schema_binding(storage, tenant_id, &realm_id).await?
        else {
            continue;
        };
        let Some(schema) = authz_realm_schema::read_bound_namespace_schema(
            storage, tenant_id, &realm_id, &namespace,
        )
        .await?
        else {
            continue;
        };
        for relation in schema.relations {
            let relation_name = relation.relation;
            if relation.rules.is_empty() {
                rows.insert(AuthzRelationRuleRow {
                    realm_id: realm_id.clone(),
                    namespace: canonical_namespace.clone(),
                    relation: relation_name.clone(),
                    rule_kind: "direct".to_string(),
                    inherited_relation: String::new(),
                    tuple_relation: String::new(),
                    target_relation: String::new(),
                    schema_generation: binding.schema_ref.schema_revision,
                });
            }
            for rule in relation.rules {
                rows.insert(AuthzRelationRuleRow {
                    realm_id: realm_id.clone(),
                    namespace: canonical_namespace.clone(),
                    relation: relation_name.clone(),
                    rule_kind: rule.kind,
                    inherited_relation: rule.relation,
                    tuple_relation: rule.tuple_relation,
                    target_relation: rule.target_relation,
                    schema_generation: binding.schema_ref.schema_revision,
                });
            }
        }
    }
    Ok(rows.into_iter().collect())
}

async fn all_relation_rule_rows(
    storage: &Storage,
    tenant_id: i64,
    bound_rows: &[AuthzRelationRuleRow],
) -> Result<Vec<AuthzRelationRuleRow>> {
    let mut rows = bound_rows.iter().cloned().collect::<BTreeSet<_>>();
    for revision in authz_realm_schema::list_schema_revisions(storage, tenant_id).await? {
        for namespace in &revision.namespaces {
            insert_relation_rule_rows(
                &mut rows,
                "",
                &namespace.namespace,
                revision.schema_ref.schema_revision,
                &namespace.relations,
            );
        }
    }
    for binding in authz_realm_schema::list_schema_bindings(storage, tenant_id).await? {
        let Some(revision) = authz_realm_schema::read_schema_revision(
            storage,
            tenant_id,
            &binding.schema_ref.schema_id,
            Some(binding.schema_ref.schema_revision),
        )
        .await?
        else {
            continue;
        };
        for namespace in &revision.namespaces {
            insert_relation_rule_rows(
                &mut rows,
                &binding.realm_id,
                &canonical_bound_namespace(&binding.realm_id, &namespace.namespace),
                binding.schema_ref.schema_revision,
                &namespace.relations,
            );
        }
    }
    Ok(rows.into_iter().collect())
}

fn insert_relation_rule_rows(
    rows: &mut BTreeSet<AuthzRelationRuleRow>,
    realm_id: &str,
    namespace: &str,
    schema_generation: u64,
    relations: &[crate::anvil_api::AuthzRelationSchema],
) {
    for relation in relations {
        if relation.rules.is_empty() {
            rows.insert(AuthzRelationRuleRow {
                realm_id: realm_id.to_string(),
                namespace: namespace.to_string(),
                relation: relation.relation.clone(),
                rule_kind: "direct".to_string(),
                inherited_relation: String::new(),
                tuple_relation: String::new(),
                target_relation: String::new(),
                schema_generation,
            });
        }
        for rule in &relation.rules {
            rows.insert(AuthzRelationRuleRow {
                realm_id: realm_id.to_string(),
                namespace: namespace.to_string(),
                relation: relation.relation.clone(),
                rule_kind: rule.kind.clone(),
                inherited_relation: rule.relation.clone(),
                tuple_relation: rule.tuple_relation.clone(),
                target_relation: rule.target_relation.clone(),
                schema_generation,
            });
        }
    }
}

fn userset_edge_rows(
    active_records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    generation: u64,
) -> Result<Vec<AuthzUsersetEdgeRow>> {
    let mut rows = BTreeSet::new();
    for record in active_records {
        if record.subject_kind == "userset" {
            rows.insert(AuthzUsersetEdgeRow {
                namespace: record.namespace.clone(),
                object_id: record.object_id.clone(),
                relation: record.relation.clone(),
                subject_kind: record.subject_kind.clone(),
                subject_id: record.subject_id.clone(),
                caveat_hash: record.caveat_hash.clone(),
                source: "tuple".to_string(),
                revision: u64::try_from(record.revision)
                    .context("authz tuple revision must be nonnegative")?,
                operation: "add".to_string(),
            });
        }
    }
    for entry in derived_usersets {
        rows.insert(AuthzUsersetEdgeRow {
            namespace: entry.namespace.clone(),
            object_id: entry.object_id.clone(),
            relation: entry.relation.clone(),
            subject_kind: entry.subject_kind.clone(),
            subject_id: entry.subject_id.clone(),
            caveat_hash: entry.caveat_hash.clone(),
            source: "derived_userset".to_string(),
            revision: generation,
            operation: "add".to_string(),
        });
    }
    Ok(rows.into_iter().collect())
}

async fn list_object_rows(
    storage: &Storage,
    tenant_id: i64,
    active_records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    relation_rule_rows: &[AuthzRelationRuleRow],
    current: &BTreeMap<authz_journal::TupleViewKey, AuthzTupleRecord>,
    generation: u64,
) -> Result<Vec<AuthzListObjectsRow>> {
    let mut rows = BTreeSet::new();
    for record in active_records {
        rows.insert(AuthzListObjectsRow {
            namespace: record.namespace.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
            object_id: record.object_id.clone(),
            doc_ordinal: authz_doc_ordinal(&record.namespace, &record.object_id),
            revision: u64::try_from(record.revision)
                .context("authz tuple revision must be nonnegative")?,
            operation: "add".to_string(),
        });
    }
    for entry in derived_usersets {
        rows.insert(AuthzListObjectsRow {
            namespace: entry.namespace.clone(),
            relation: entry.relation.clone(),
            subject_kind: entry.subject_kind.clone(),
            subject_id: entry.subject_id.clone(),
            caveat_hash: entry.caveat_hash.clone(),
            object_id: entry.object_id.clone(),
            doc_ordinal: authz_doc_ordinal(&entry.namespace, &entry.object_id),
            revision: generation,
            operation: "add".to_string(),
        });
    }
    let schema_index = SchemaRuleIndex::load(
        storage,
        tenant_id,
        current,
        active_records
            .iter()
            .map(|record| record.namespace.as_str()),
    )
    .await?;
    for userset in materialized_userset_targets(active_records, relation_rule_rows) {
        for subject in collect_subjects_for_userset(current, &schema_index, &userset)? {
            rows.insert(AuthzListObjectsRow {
                namespace: userset.namespace.clone(),
                relation: userset.relation.clone(),
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
                object_id: userset.object_id.clone(),
                doc_ordinal: authz_doc_ordinal(&userset.namespace, &userset.object_id),
                revision: generation,
                operation: "add".to_string(),
            });
        }
    }
    Ok(rows.into_iter().collect())
}

async fn list_subject_rows(
    storage: &Storage,
    tenant_id: i64,
    active_records: &[AuthzTupleRecord],
    derived_usersets: &[AuthzDerivedUsersetEntry],
    relation_rule_rows: &[AuthzRelationRuleRow],
    current: &BTreeMap<authz_journal::TupleViewKey, AuthzTupleRecord>,
    generation: u64,
) -> Result<Vec<AuthzListSubjectsRow>> {
    let mut rows = BTreeSet::new();
    for record in active_records {
        rows.insert(AuthzListSubjectsRow {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
            doc_ordinal: authz_doc_ordinal(&record.namespace, &record.object_id),
            revision: u64::try_from(record.revision)
                .context("authz tuple revision must be nonnegative")?,
            operation: "add".to_string(),
        });
    }
    for entry in derived_usersets {
        rows.insert(AuthzListSubjectsRow {
            namespace: entry.namespace.clone(),
            object_id: entry.object_id.clone(),
            relation: entry.relation.clone(),
            subject_kind: entry.subject_kind.clone(),
            subject_id: entry.subject_id.clone(),
            caveat_hash: entry.caveat_hash.clone(),
            doc_ordinal: authz_doc_ordinal(&entry.namespace, &entry.object_id),
            revision: generation,
            operation: "add".to_string(),
        });
    }
    let schema_index = SchemaRuleIndex::load(
        storage,
        tenant_id,
        current,
        active_records
            .iter()
            .map(|record| record.namespace.as_str()),
    )
    .await?;
    for userset in materialized_userset_targets(active_records, relation_rule_rows) {
        for subject in collect_subjects_for_userset(current, &schema_index, &userset)? {
            rows.insert(AuthzListSubjectsRow {
                namespace: userset.namespace.clone(),
                object_id: userset.object_id.clone(),
                relation: userset.relation.clone(),
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
                doc_ordinal: authz_doc_ordinal(&userset.namespace, &userset.object_id),
                revision: generation,
                operation: "add".to_string(),
            });
        }
    }
    Ok(rows.into_iter().collect())
}

fn materialized_userset_targets(
    active_records: &[AuthzTupleRecord],
    relation_rule_rows: &[AuthzRelationRuleRow],
) -> BTreeSet<UsersetRef> {
    let object_namespaces = active_records
        .iter()
        .map(|record| (record.namespace.clone(), record.object_id.clone()))
        .collect::<BTreeSet<_>>();
    let direct_relations = active_records
        .iter()
        .map(|record| {
            (
                record.namespace.clone(),
                record.object_id.clone(),
                record.relation.clone(),
            )
        })
        .collect::<BTreeSet<_>>();
    let schema_relations = relation_rule_rows
        .iter()
        .flat_map(|rule| {
            object_namespaces
                .iter()
                .filter(move |(namespace, _)| namespace == &rule.namespace)
                .map(move |(namespace, object_id)| {
                    (namespace.clone(), object_id.clone(), rule.relation.clone())
                })
        })
        .collect::<BTreeSet<_>>();
    direct_relations
        .into_iter()
        .chain(schema_relations)
        .map(|(namespace, object_id, relation)| UsersetRef {
            namespace,
            object_id,
            relation,
        })
        .collect()
}

fn namespace_realm_parts(namespace: &str) -> (String, String) {
    split_realm_namespace(namespace)
        .map(|(realm_id, local_namespace)| (realm_id, local_namespace.to_string()))
        .unwrap_or_else(|| (DEFAULT_AUTHZ_REALM_ID.to_string(), namespace.to_string()))
}

fn canonical_bound_namespace(realm_id: &str, namespace: &str) -> String {
    if realm_id == DEFAULT_AUTHZ_REALM_ID {
        namespace.to_string()
    } else {
        encode_realm_namespace(realm_id, namespace)
    }
}

fn authz_doc_ordinal(namespace: &str, object_id: &str) -> u64 {
    stable_doc_ordinal(&[namespace, object_id])
}

fn tuple_records_hash(records: &[AuthzTupleRecord]) -> Result<Hash32> {
    let mut bytes = Vec::new();
    for record in records {
        bytes.extend(segment_key(record)?);
        bytes.extend(tuple_value(record)?.encode());
    }
    Ok(hash32(&bytes))
}

fn schema_descriptor_key(row: &AuthzSchemaDescriptorRow) -> Result<Vec<u8>> {
    Ok(key_parts(&[
        &row.realm_id,
        &row.namespace,
        &row.schema_id,
        &row.schema_revision.to_string(),
    ]))
}

fn relation_rule_key(row: &AuthzRelationRuleRow) -> Result<Vec<u8>> {
    Ok(key_parts(&[
        &row.realm_id,
        &row.namespace,
        &row.relation,
        &row.rule_kind,
        &row.inherited_relation,
        &row.tuple_relation,
        &row.target_relation,
        &row.schema_generation.to_string(),
    ]))
}

fn userset_edge_key(row: &AuthzUsersetEdgeRow) -> Result<Vec<u8>> {
    Ok(key_parts(&[
        &row.namespace,
        &row.object_id,
        &row.relation,
        &row.subject_kind,
        &row.subject_id,
        &row.caveat_hash,
        &row.source,
    ]))
}

fn revision_checkpoint_key(row: &AuthzRevisionCheckpointRow) -> Result<Vec<u8>> {
    Ok(row.revision.to_le_bytes().to_vec())
}

fn list_object_key(row: &AuthzListObjectsRow) -> Result<Vec<u8>> {
    Ok(key_parts(&[
        &row.namespace,
        &row.relation,
        &row.subject_kind,
        &row.subject_id,
        &row.caveat_hash,
        &row.object_id,
    ]))
}

fn list_subject_key(row: &AuthzListSubjectsRow) -> Result<Vec<u8>> {
    Ok(key_parts(&[
        &row.namespace,
        &row.object_id,
        &row.relation,
        &row.subject_kind,
        &row.subject_id,
        &row.caveat_hash,
    ]))
}

fn key_parts(parts: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in parts {
        out.extend_from_slice(&(part.len() as u32).to_le_bytes());
        out.extend_from_slice(part.as_bytes());
    }
    out
}

fn encode_schema_descriptor_row(row: &AuthzSchemaDescriptorRow) -> Result<Vec<u8>> {
    encode_proto(&AuthzSchemaDescriptorRowProto {
        tenant_id: row.tenant_id,
        realm_id: row.realm_id.clone(),
        namespace: row.namespace.clone(),
        schema_id: row.schema_id.clone(),
        schema_revision: row.schema_revision,
        schema_digest: row.schema_digest.clone(),
        binding_generation: row.binding_generation,
        authz_revision: row.authz_revision,
    })
}

fn decode_schema_descriptor_row(bytes: &[u8]) -> Result<AuthzSchemaDescriptorRow> {
    let proto =
        decode_proto::<AuthzSchemaDescriptorRowProto>(bytes, "authz schema descriptor row")?;
    Ok(AuthzSchemaDescriptorRow {
        tenant_id: proto.tenant_id,
        realm_id: proto.realm_id,
        namespace: proto.namespace,
        schema_id: proto.schema_id,
        schema_revision: proto.schema_revision,
        schema_digest: proto.schema_digest,
        binding_generation: proto.binding_generation,
        authz_revision: proto.authz_revision,
    })
}

fn encode_relation_rule_row(row: &AuthzRelationRuleRow) -> Result<Vec<u8>> {
    encode_proto(&AuthzRelationRuleRowProto {
        realm_id: row.realm_id.clone(),
        namespace: row.namespace.clone(),
        relation: row.relation.clone(),
        rule_kind: row.rule_kind.clone(),
        inherited_relation: row.inherited_relation.clone(),
        tuple_relation: row.tuple_relation.clone(),
        target_relation: row.target_relation.clone(),
        schema_generation: row.schema_generation,
    })
}

fn decode_relation_rule_row(bytes: &[u8]) -> Result<AuthzRelationRuleRow> {
    let proto = decode_proto::<AuthzRelationRuleRowProto>(bytes, "authz relation rule row")?;
    Ok(AuthzRelationRuleRow {
        realm_id: proto.realm_id,
        namespace: proto.namespace,
        relation: proto.relation,
        rule_kind: proto.rule_kind,
        inherited_relation: proto.inherited_relation,
        tuple_relation: proto.tuple_relation,
        target_relation: proto.target_relation,
        schema_generation: proto.schema_generation,
    })
}

fn encode_userset_edge_row(row: &AuthzUsersetEdgeRow) -> Result<Vec<u8>> {
    encode_proto(&AuthzUsersetEdgeRowProto {
        namespace: row.namespace.clone(),
        object_id: row.object_id.clone(),
        relation: row.relation.clone(),
        subject_kind: row.subject_kind.clone(),
        subject_id: row.subject_id.clone(),
        caveat_hash: row.caveat_hash.clone(),
        source: row.source.clone(),
        revision: row.revision,
        operation: row.operation.clone(),
    })
}

fn decode_userset_edge_row(bytes: &[u8]) -> Result<AuthzUsersetEdgeRow> {
    let proto = decode_proto::<AuthzUsersetEdgeRowProto>(bytes, "authz userset edge row")?;
    Ok(AuthzUsersetEdgeRow {
        namespace: proto.namespace,
        object_id: proto.object_id,
        relation: proto.relation,
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
        caveat_hash: proto.caveat_hash,
        source: proto.source,
        revision: proto.revision,
        operation: validate_delta_operation(proto.operation)?,
    })
}

fn encode_revision_checkpoint_row(row: &AuthzRevisionCheckpointRow) -> Result<Vec<u8>> {
    encode_proto(&AuthzRevisionCheckpointRowProto {
        tenant_id: row.tenant_id,
        revision: row.revision,
        source_fence_token: row.source_fence_token,
        tuple_record_count: row.tuple_record_count,
        active_tuple_count: row.active_tuple_count,
        derived_userset_count: row.derived_userset_count,
        list_objects_count: row.list_objects_count,
        list_subjects_count: row.list_subjects_count,
        tuple_records_hash: row.tuple_records_hash.clone(),
    })
}

fn decode_revision_checkpoint_row(bytes: &[u8]) -> Result<AuthzRevisionCheckpointRow> {
    let proto =
        decode_proto::<AuthzRevisionCheckpointRowProto>(bytes, "authz revision checkpoint row")?;
    Ok(AuthzRevisionCheckpointRow {
        tenant_id: proto.tenant_id,
        revision: proto.revision,
        source_fence_token: proto.source_fence_token,
        tuple_record_count: proto.tuple_record_count,
        active_tuple_count: proto.active_tuple_count,
        derived_userset_count: proto.derived_userset_count,
        list_objects_count: proto.list_objects_count,
        list_subjects_count: proto.list_subjects_count,
        tuple_records_hash: proto.tuple_records_hash,
    })
}

fn encode_list_objects_row(row: &AuthzListObjectsRow) -> Result<Vec<u8>> {
    encode_proto(&AuthzListObjectsRowProto {
        namespace: row.namespace.clone(),
        relation: row.relation.clone(),
        subject_kind: row.subject_kind.clone(),
        subject_id: row.subject_id.clone(),
        caveat_hash: row.caveat_hash.clone(),
        object_id: row.object_id.clone(),
        doc_ordinal: row.doc_ordinal,
        revision: row.revision,
        operation: row.operation.clone(),
    })
}

fn decode_list_objects_row(bytes: &[u8]) -> Result<AuthzListObjectsRow> {
    let proto = decode_proto::<AuthzListObjectsRowProto>(bytes, "authz list objects row")?;
    Ok(AuthzListObjectsRow {
        namespace: proto.namespace,
        relation: proto.relation,
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
        caveat_hash: proto.caveat_hash,
        object_id: proto.object_id,
        doc_ordinal: proto.doc_ordinal,
        revision: proto.revision,
        operation: validate_delta_operation(proto.operation)?,
    })
}

fn encode_list_subjects_row(row: &AuthzListSubjectsRow) -> Result<Vec<u8>> {
    encode_proto(&AuthzListSubjectsRowProto {
        namespace: row.namespace.clone(),
        object_id: row.object_id.clone(),
        relation: row.relation.clone(),
        subject_kind: row.subject_kind.clone(),
        subject_id: row.subject_id.clone(),
        caveat_hash: row.caveat_hash.clone(),
        doc_ordinal: row.doc_ordinal,
        revision: row.revision,
        operation: row.operation.clone(),
    })
}

fn decode_list_subjects_row(bytes: &[u8]) -> Result<AuthzListSubjectsRow> {
    let proto = decode_proto::<AuthzListSubjectsRowProto>(bytes, "authz list subjects row")?;
    Ok(AuthzListSubjectsRow {
        namespace: proto.namespace,
        object_id: proto.object_id,
        relation: proto.relation,
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
        caveat_hash: proto.caveat_hash,
        doc_ordinal: proto.doc_ordinal,
        revision: proto.revision,
        operation: validate_delta_operation(proto.operation)?,
    })
}

fn validate_delta_operation(operation: String) -> Result<String> {
    if matches!(operation.as_str(), "add" | "remove") {
        Ok(operation)
    } else {
        bail!("authz delta row operation must be add or remove")
    }
}

fn encode_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn decode_proto<T: Message + Default>(bytes: &[u8], label: &str) -> Result<T> {
    let proto = T::decode(bytes).with_context(|| format!("decode {label}"))?;
    let mut encoded = Vec::with_capacity(proto.encoded_len());
    proto.encode(&mut encoded)?;
    if encoded != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(proto)
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

#[derive(Debug, Clone)]
pub struct AuthzSegmentCandidateReader {
    storage: Storage,
    tenant_id: i64,
}

impl AuthzSegmentCandidateReader {
    pub fn new(storage: Storage, tenant_id: i64) -> Self {
        Self { storage, tenant_id }
    }
}

impl AuthzCandidateReader for AuthzSegmentCandidateReader {
    async fn candidate_set(&self, request: AuthzCandidateRequest) -> Result<CandidateSet> {
        let scope = request.candidate_scope.clone();
        let Some(segment) =
            ensure_authz_tuple_segment_at_revision(&self.storage, self.tenant_id, request.revision)
                .await?
        else {
            return Ok(CandidateSet::empty(scope));
        };
        let requested_revision = request.revision;
        let subject = parse_authz_candidate_subject(&request.subject);
        let partition_id = request.partition_id;
        let mut ordinals = Vec::new();
        let rows = segment.list_objects;
        for row in &rows {
            if row.revision > requested_revision
                || row.namespace != request.object_namespace
                || row.relation != request.relation
            {
                continue;
            }
            if row.subject_kind == subject.subject_kind
                && row.subject_id == subject.subject_id
                && row.caveat_hash == subject.caveat_hash
            {
                ordinals.push(row.doc_ordinal);
            }
        }
        ordinals.sort_unstable();
        ordinals.dedup();
        Ok(bitmap_candidate_set(scope, partition_id, ordinals))
    }

    async fn verify_page(
        &self,
        request: AuthzCandidateRequest,
        object_keys: Vec<ObjectAuthzKey>,
    ) -> Result<Vec<AuthzDecision>> {
        let Some(segment) =
            ensure_authz_tuple_segment_at_revision(&self.storage, self.tenant_id, request.revision)
                .await?
        else {
            return Ok(object_keys
                .into_iter()
                .map(|object_key| AuthzDecision {
                    object_key,
                    allowed: false,
                    revision: request.revision,
                })
                .collect());
        };
        let requested_revision = request.revision;
        let subject = parse_authz_candidate_subject(&request.subject);
        let allowed = segment
            .list_objects
            .into_iter()
            .filter(|row| {
                row.revision <= requested_revision
                    && row.namespace == request.object_namespace
                    && row.relation == request.relation
                    && row.subject_kind == subject.subject_kind
                    && row.subject_id == subject.subject_id
                    && row.caveat_hash == subject.caveat_hash
            })
            .map(|row| row.object_id)
            .collect::<BTreeSet<_>>();
        Ok(object_keys
            .into_iter()
            .map(|object_key| {
                let object_id = object_key.canonical_object_id.clone();
                AuthzDecision {
                    object_key,
                    allowed: allowed.contains(&object_id),
                    revision: requested_revision,
                }
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
struct CandidateSubject {
    subject_kind: String,
    subject_id: String,
    caveat_hash: String,
}

fn parse_authz_candidate_subject(subject: &str) -> CandidateSubject {
    let (subject_kind, rest) = subject
        .split_once(':')
        .map(|(kind, id)| (kind.to_string(), id.to_string()))
        .unwrap_or_else(|| ("user".to_string(), subject.to_string()));
    let (subject_id, caveat_hash) = rest
        .split_once('@')
        .map(|(id, caveat)| (id.to_string(), caveat.to_string()))
        .unwrap_or((rest, String::new()));
    CandidateSubject {
        subject_kind,
        subject_id,
        caveat_hash,
    }
}

fn bitmap_candidate_set(
    scope: CandidateSetScope,
    partition_id: u64,
    ordinals: Vec<u64>,
) -> CandidateSet {
    if ordinals.is_empty() {
        return CandidateSet::empty(scope);
    }
    CandidateSet::bitmap_from_ordinals(scope, partition_id, ordinals)
}

#[cfg(test)]
mod tests;
