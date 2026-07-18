use crate::authz_coremeta_payload::{decode_authz_payload_row, encode_authz_payload_row};
use crate::authz_realm_schema;
use crate::authz_schema_contract::{
    AuthzSchemaContractError, AuthzTupleShape, validate_tuple_batch,
};
use crate::authz_scope::{DEFAULT_AUTHZ_REALM_ID, split_realm_namespace};
use crate::authz_segment;
use crate::authz_userset_index::{
    AuthzDerivedUsersetEntry, DEFAULT_DERIVED_USERSET_INDEX_ID,
    list_derived_userset_objects_at_revision, lookup_derived_userset_index_at_revision,
};
use crate::core_store::{
    CF_AUTHZ, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, CoreMetaTuplePart,
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
    TABLE_AUTHZ_TUPLE_PAGE_ROW, core_meta_committed_row_common, core_meta_root_key_hash,
    core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, LazyLock},
};

mod idempotency;
pub(crate) mod resolver;

const AUTHZ_TUPLE_JOURNAL_BODY_SCHEMA: &str = "anvil.authz_tuple.journal_body.v1";
const AUTHZ_TUPLE_BATCH_JOURNAL_BODY_SCHEMA: &str = "anvil.authz_tuple.batch_journal_body.v1";
const AUTHZ_TUPLE_CURRENT_ROW_SCHEMA: &str = "anvil.authz_tuple.current_row.v1";
const AUTHZ_TUPLE_PAGE_PAYLOAD_KIND: &str = "authz_tuple_page";
const AUTHZ_TUPLE_RECORD_KIND: &str = "authz_tuple";
const AUTHZ_TUPLE_BATCH_RECORD_KIND: &str = "authz_tuple_batch";

static AUTHZ_TUPLE_WRITE_LOCKS: LazyLock<
    std::sync::Mutex<BTreeMap<i64, Arc<tokio::sync::Mutex<()>>>>,
> = LazyLock::new(|| std::sync::Mutex::new(BTreeMap::new()));

fn authz_tuple_write_lock(tenant_id: i64) -> Result<Arc<tokio::sync::Mutex<()>>> {
    let mut locks = AUTHZ_TUPLE_WRITE_LOCKS
        .lock()
        .map_err(|_| anyhow!("authz tuple write lock is poisoned"))?;
    Ok(locks
        .entry(tenant_id)
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum AuthzTupleOperationProto {
    Unspecified = 0,
    Add = 1,
    Remove = 2,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleRecordProto {
    #[prost(int64, tag = "1")]
    revision: i64,
    #[prost(uint32, tag = "2")]
    revision_ordinal: u32,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(string, tag = "4")]
    namespace: String,
    #[prost(string, tag = "5")]
    object_id: String,
    #[prost(string, tag = "6")]
    relation: String,
    #[prost(string, tag = "7")]
    subject_kind: String,
    #[prost(string, tag = "8")]
    subject_id: String,
    #[prost(string, tag = "9")]
    caveat_hash: String,
    #[prost(enumeration = "AuthzTupleOperationProto", tag = "10")]
    operation: i32,
    #[prost(string, tag = "11")]
    written_by: String,
    #[prost(string, tag = "12")]
    reason: String,
    #[prost(string, tag = "13")]
    mutation_id: String,
    #[prost(string, tag = "14")]
    record_hash: String,
    #[prost(int64, tag = "15")]
    written_at_unix_nanos: i64,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(message, optional, tag = "2")]
    record: Option<AuthzTupleRecordProto>,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleBatchJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    revision: i64,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(message, repeated, tag = "4")]
    records: Vec<AuthzTupleRecordProto>,
    #[prost(uint64, tag = "5")]
    fence_token: u64,
    #[prost(string, tag = "6")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct AuthzTupleCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(message, optional, tag = "3")]
    record: Option<AuthzTupleRecordProto>,
}

#[derive(Clone, Copy)]
pub struct AuthzTupleWrite<'a> {
    pub tenant_id: i64,
    pub namespace: &'a str,
    pub object_id: &'a str,
    pub relation: &'a str,
    pub subject_kind: &'a str,
    pub subject_id: &'a str,
    pub caveat_hash: &'a str,
    pub operation: &'a str,
    pub written_by: &'a str,
    pub reason: &'a str,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthzTupleFilter {
    pub namespace: Option<String>,
    pub object_id: Option<String>,
    pub relation: Option<String>,
    pub subject_kind: Option<String>,
    pub subject_id: Option<String>,
    pub caveat_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzSubjectRef {
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
}

pub(crate) async fn write_authz_tuple_with_permit(
    storage: &Storage,
    input: AuthzTupleWrite<'_>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AuthzTupleRecord> {
    require_authz_permit(input.tenant_id, permit)?;
    validate_optional_caveat_hash(input.caveat_hash)?;
    let write_lock = authz_tuple_write_lock(input.tenant_id)?;
    let _guard = write_lock.lock().await;
    let schema_binding_precondition =
        validate_writes_against_bound_schema(storage, std::slice::from_ref(&input), None).await?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_authz_tuple_inner(
        storage,
        input,
        permit.fence_token,
        Some(partition_precondition),
        schema_binding_precondition,
    )
    .await
}

pub(crate) async fn write_authz_tuple_batch_with_permit(
    storage: &Storage,
    inputs: Vec<AuthzTupleWrite<'_>>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<Vec<AuthzTupleRecord>> {
    let Some(first) = inputs.first() else {
        return Err(anyhow!("authz tuple batch must not be empty"));
    };
    let tenant_id = first.tenant_id;
    require_authz_permit(tenant_id, permit)?;
    for input in &inputs {
        if input.tenant_id != tenant_id {
            return Err(anyhow!("authz tuple batch must target one tenant"));
        }
        validate_optional_caveat_hash(input.caveat_hash)?;
    }
    let write_lock = authz_tuple_write_lock(tenant_id)?;
    let _guard = write_lock.lock().await;
    let schema_binding_precondition =
        validate_writes_against_bound_schema(storage, &inputs, None).await?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_authz_tuple_batch_inner(
        storage,
        inputs,
        permit.fence_token,
        Some(partition_precondition),
        schema_binding_precondition,
    )
    .await
}

pub(crate) async fn replay_authz_tuple_batch(
    storage: &Storage,
    inputs: &[AuthzTupleWrite<'_>],
    options: &crate::persistence::AuthzTupleBatchWriteOptions,
) -> Result<Option<crate::persistence::AuthzTupleBatchWriteOutcome>> {
    idempotency::replay(storage, inputs, options).await
}

pub(crate) async fn write_authz_tuple_batch_conditionally_with_permit(
    storage: &Storage,
    inputs: Vec<AuthzTupleWrite<'_>>,
    options: &crate::persistence::AuthzTupleBatchWriteOptions,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<crate::persistence::AuthzTupleBatchWriteOutcome> {
    let Some(first) = inputs.first() else {
        return Err(anyhow!("authz tuple batch must not be empty"));
    };
    let tenant_id = first.tenant_id;
    require_authz_permit(tenant_id, permit)?;
    for input in &inputs {
        if input.tenant_id != tenant_id {
            return Err(anyhow!("authz tuple batch must target one tenant"));
        }
        validate_optional_caveat_hash(input.caveat_hash)?;
    }
    if let Some(operation_id) = options.operation_id.as_deref() {
        idempotency::validate_operation_id(operation_id)?;
    }
    let write_lock = authz_tuple_write_lock(tenant_id)?;
    let _guard = write_lock.lock().await;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_authz_tuple_batch_conditionally_inner(
        storage,
        inputs,
        options,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

pub(crate) fn validate_authz_batch_operation_id(operation_id: &str) -> Result<()> {
    idempotency::validate_operation_id(operation_id)
}

async fn validate_writes_against_bound_schema(
    storage: &Storage,
    inputs: &[AuthzTupleWrite<'_>],
    expected_realm_id: Option<&str>,
) -> Result<crate::persistence::AuthzSchemaBindingPrecondition> {
    let first = inputs
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch must not be empty"))?;
    let realm_id = split_realm_namespace(first.namespace)
        .map(|(realm_id, _)| realm_id)
        .unwrap_or_else(|| DEFAULT_AUTHZ_REALM_ID.to_string());
    if expected_realm_id.is_some_and(|expected| expected != realm_id) {
        return Err(AuthzSchemaContractError::new(
            "authorization tuple scope does not match the conditional batch realm",
        )
        .into());
    }
    let snapshot =
        authz_realm_schema::read_bound_schema_snapshot(storage, first.tenant_id, &realm_id).await?;
    let schema = snapshot.schema.as_ref().ok_or_else(|| {
        AuthzSchemaContractError::new(format!(
            "authorization realm {realm_id} has no bound schema revision"
        ))
    })?;
    let tuples = inputs
        .iter()
        .map(|input| AuthzTupleShape {
            namespace: input.namespace,
            object_id: input.object_id,
            relation: input.relation,
            subject_kind: input.subject_kind,
            subject_id: input.subject_id,
            operation: input.operation,
        })
        .collect::<Vec<_>>();
    validate_tuple_batch(&schema.namespaces, &realm_id, &tuples)?;
    Ok(snapshot.binding_precondition)
}

fn handle_schema_fenced_write_result(
    storage: &Storage,
    schema_binding_precondition: &crate::persistence::AuthzSchemaBindingPrecondition,
    result: Result<()>,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(_)
            if !idempotency::schema_binding_is_current(storage, schema_binding_precondition)? =>
        {
            Err(anyhow!(
                crate::persistence::AuthzTupleBatchWriteError::SchemaBindingChanged
            ))
        }
        Err(error) => Err(error),
    }
}

async fn write_authz_tuple_inner(
    storage: &Storage,
    input: AuthzTupleWrite<'_>,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    schema_binding_precondition: crate::persistence::AuthzSchemaBindingPrecondition,
) -> Result<AuthzTupleRecord> {
    validate_optional_caveat_hash(input.caveat_hash)?;
    let revision = latest_authz_revision(storage, input.tenant_id)
        .await?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("authz revision overflow"))?;
    let record = build_authz_tuple_record(input, revision, 0)?;
    let write_result = append_authz_tuple_record_inner(
        storage,
        &record,
        fence_token,
        partition_precondition,
        Some(&schema_binding_precondition),
    )
    .await;
    handle_schema_fenced_write_result(storage, &schema_binding_precondition, write_result)?;
    Ok(record)
}

async fn write_authz_tuple_batch_inner(
    storage: &Storage,
    inputs: Vec<AuthzTupleWrite<'_>>,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    schema_binding_precondition: crate::persistence::AuthzSchemaBindingPrecondition,
) -> Result<Vec<AuthzTupleRecord>> {
    let tenant_id = inputs
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch must not be empty"))?
        .tenant_id;
    let revision = latest_authz_revision(storage, tenant_id)
        .await?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("authz revision overflow"))?;
    let mut records = Vec::with_capacity(inputs.len());
    for (idx, input) in inputs.into_iter().enumerate() {
        records.push(build_authz_tuple_record(
            input,
            revision,
            u32::try_from(idx).context("authz tuple batch ordinal overflow")?,
        )?);
    }
    let write_result = append_authz_tuple_batch_inner(
        storage,
        tenant_id,
        &records,
        fence_token,
        partition_precondition,
        None,
        Some(&schema_binding_precondition),
    )
    .await;
    handle_schema_fenced_write_result(storage, &schema_binding_precondition, write_result)?;
    Ok(records)
}

async fn write_authz_tuple_batch_conditionally_inner(
    storage: &Storage,
    inputs: Vec<AuthzTupleWrite<'_>>,
    options: &crate::persistence::AuthzTupleBatchWriteOptions,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<crate::persistence::AuthzTupleBatchWriteOutcome> {
    if let Some(replay) = idempotency::replay(storage, &inputs, options).await? {
        return Ok(replay);
    }
    let schema_binding_precondition = validate_writes_against_bound_schema(
        storage,
        &inputs,
        Some(options.authz_realm_id.as_str()),
    )
    .await?;
    let tenant_id = inputs
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch must not be empty"))?
        .tenant_id;
    let current_revision = latest_authz_revision(storage, tenant_id).await?;
    if let Some(expected) = options.expected_revision
        && expected != current_revision
    {
        return Err(anyhow!(
            crate::persistence::AuthzTupleBatchWriteError::RevisionConflict {
                expected,
                actual: current_revision,
            }
        ));
    }
    let revision = current_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("authz revision overflow"))?;
    let mut records = Vec::with_capacity(inputs.len());
    for (idx, input) in inputs.iter().copied().enumerate() {
        records.push(build_authz_tuple_record(
            input,
            revision,
            u32::try_from(idx).context("authz tuple batch ordinal overflow")?,
        )?);
    }
    let receipt = idempotency::prepare_receipt(&inputs, options, &records)?;
    let write_result = append_authz_tuple_batch_inner(
        storage,
        tenant_id,
        &records,
        fence_token,
        partition_precondition,
        receipt.as_ref(),
        Some(&schema_binding_precondition),
    )
    .await;
    if let Err(error) = write_result {
        if let Some(replay) = idempotency::replay(storage, &inputs, options).await? {
            return Ok(replay);
        }
        if !idempotency::schema_binding_is_current(storage, &schema_binding_precondition)? {
            return Err(anyhow!(
                crate::persistence::AuthzTupleBatchWriteError::SchemaBindingChanged
            ));
        }
        return Err(error);
    }
    Ok(crate::persistence::AuthzTupleBatchWriteOutcome {
        records,
        replayed: false,
    })
}

fn build_authz_tuple_record(
    input: AuthzTupleWrite<'_>,
    revision: i64,
    revision_ordinal: u32,
) -> Result<AuthzTupleRecord> {
    let written_at = chrono::Utc::now();
    let mutation_id = uuid::Uuid::new_v4();
    let record_hash = authz_record_hash(AuthzRecordHashInput {
        revision,
        revision_ordinal,
        tenant_id: input.tenant_id,
        namespace: input.namespace,
        object_id: input.object_id,
        relation: input.relation,
        subject_kind: input.subject_kind,
        subject_id: input.subject_id,
        caveat_hash: input.caveat_hash,
        operation: input.operation,
        written_by: input.written_by,
        reason: input.reason,
    });
    Ok(AuthzTupleRecord {
        revision,
        revision_ordinal,
        tenant_id: input.tenant_id,
        namespace: input.namespace.to_string(),
        object_id: input.object_id.to_string(),
        relation: input.relation.to_string(),
        subject_kind: input.subject_kind.to_string(),
        subject_id: input.subject_id.to_string(),
        caveat_hash: input.caveat_hash.to_string(),
        operation: input.operation.to_string(),
        written_by: input.written_by.to_string(),
        reason: input.reason.to_string(),
        mutation_id,
        record_hash,
        written_at,
    })
}

#[cfg(test)]
pub(crate) async fn test_append_authz_tuple_record_unfenced(
    storage: &Storage,
    record: &AuthzTupleRecord,
) -> Result<()> {
    append_authz_tuple_record_inner(storage, record, 0, None, None).await
}

#[cfg(test)]
pub(crate) async fn append_authz_tuple_record_with_permit(
    storage: &Storage,
    record: &AuthzTupleRecord,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_authz_permit(record.tenant_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    append_authz_tuple_record_inner(
        storage,
        record,
        permit.fence_token,
        Some(partition_precondition),
        None,
    )
    .await
}

async fn append_authz_tuple_record_inner(
    storage: &Storage,
    record: &AuthzTupleRecord,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    schema_binding_precondition: Option<&crate::persistence::AuthzSchemaBindingPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = authz_tuple_stream_id(record.tenant_id);
    let payload = encode_authz_tuple_journal_body(record, fence_token)?;

    let partition_id = hex::encode(authz_partition_id(record.tenant_id));
    let step_started_at = std::time::Instant::now();
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("authz-tuple:{}", record.mutation_id),
            scope_partition: partition_id.clone(),
            committed_by_principal: authz_partition_principal(record.tenant_id),
            preconditions: partition_precondition
                .into_iter()
                .chain(schema_binding_precondition.map(idempotency::schema_binding_precondition))
                .collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind: AUTHZ_TUPLE_RECORD_KIND.to_string(),
                payload,
                idempotency_key: Some(format!("authz-tuple:{}", record.mutation_id)),
            }],
        })
        .await?;
    crate::emit_test_timing(
        "authz_journal.append_record commit_mutation_batch",
        step_started_at.elapsed(),
    );
    let step_started_at = std::time::Instant::now();
    write_authz_tuple_records_to_current_rows(storage, std::slice::from_ref(record)).await?;
    crate::emit_test_timing(
        "authz_journal.append_record write_current_rows",
        step_started_at.elapsed(),
    );
    record_authz_materialization_deferred(
        record.tenant_id,
        record.revision,
        std::slice::from_ref(record).len(),
    );
    Ok(())
}

async fn append_authz_tuple_batch_inner(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    idempotency_receipt: Option<&idempotency::PreparedAuthzIdempotencyReceipt>,
    schema_binding_precondition: Option<&crate::persistence::AuthzSchemaBindingPrecondition>,
) -> Result<()> {
    if records.is_empty() {
        return Err(anyhow!("authz tuple batch must not be empty"));
    }
    let revision = records[0].revision;
    if records
        .iter()
        .any(|record| record.tenant_id != tenant_id || record.revision != revision)
    {
        return Err(anyhow!(
            "authz tuple batch records must target one tenant and revision"
        ));
    }
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = authz_tuple_stream_id(tenant_id);
    let payload = encode_authz_tuple_batch_journal_body(tenant_id, revision, records, fence_token)?;

    let partition_id = hex::encode(authz_partition_id(tenant_id));
    let step_started_at = std::time::Instant::now();
    let transaction_id = idempotency_receipt
        .map(|receipt| receipt.transaction_id.clone())
        .unwrap_or_else(|| format!("authz-tuple-batch:{tenant_id}:{revision}"));
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    if let Some(schema_binding_precondition) = schema_binding_precondition {
        preconditions.push(idempotency::schema_binding_precondition(
            schema_binding_precondition,
        ));
    }
    if let Some(receipt) = idempotency_receipt {
        preconditions.push(idempotency::receipt_precondition(receipt));
    }
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id,
        record_kind: AUTHZ_TUPLE_BATCH_RECORD_KIND.to_string(),
        payload,
        idempotency_key: Some(transaction_id.clone()),
    }];
    if let Some(receipt) = idempotency_receipt {
        operations.push(idempotency::receipt_operation(&partition_id, receipt));
    }
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: partition_id,
            committed_by_principal: authz_partition_principal(tenant_id),
            preconditions,
            operations,
        })
        .await?;
    crate::emit_test_timing(
        "authz_journal.append_batch commit_mutation_batch",
        step_started_at.elapsed(),
    );
    let step_started_at = std::time::Instant::now();
    write_authz_tuple_records_to_current_rows(storage, records).await?;
    crate::emit_test_timing(
        "authz_journal.append_batch write_current_rows",
        step_started_at.elapsed(),
    );
    record_authz_materialization_deferred(tenant_id, revision, records.len());
    Ok(())
}

fn record_authz_materialization_deferred(tenant_id: i64, revision: i64, record_count: usize) {
    let tenant_id = tenant_id.to_string();
    let revision = revision.to_string();
    crate::perf::record_counter(
        "authz_materialization_deferred",
        &[("tenant_id", &tenant_id), ("revision", &revision)],
        record_count as u64,
    );
    crate::emit_test_timing(
        "authz_journal.materialize_segment deferred",
        std::time::Duration::ZERO,
    );
}

pub(crate) async fn materialize_authz_tuple_segment(
    storage: &Storage,
    tenant_id: i64,
    source_fence_token: u64,
) -> Result<String> {
    let target_revision = latest_authz_revision(storage, tenant_id).await?.max(0) as u64;
    materialize_authz_tuple_segment_at_revision(
        storage,
        tenant_id,
        target_revision,
        source_fence_token,
    )
    .await
}

pub(crate) async fn materialize_authz_tuple_segment_at_revision(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<String> {
    materialize_authz_tuple_segment_at_revision_with_derived(
        storage,
        tenant_id,
        target_revision,
        source_fence_token,
        None,
    )
    .await
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthzMaterializationOutcome {
    pub processed_revision: u64,
    pub source_record_count: u64,
    pub source_records_hash: String,
    pub generation: u64,
    pub segment_ref: String,
}

pub(crate) async fn materialize_authz_derived_state_at_revision(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
) -> Result<AuthzMaterializationOutcome> {
    let derived = crate::authz_userset_index::build_expected_derived_userset_index_at_revision(
        storage,
        tenant_id,
        DEFAULT_DERIVED_USERSET_INDEX_ID,
        target_revision,
    )
    .await?;
    crate::authz_userset_index::write_derived_userset_index(storage, &derived).await?;
    let segment_ref = materialize_authz_tuple_segment_at_revision_with_derived(
        storage,
        tenant_id,
        target_revision,
        source_fence_token,
        Some(derived.entries.clone()),
    )
    .await?;
    Ok(AuthzMaterializationOutcome {
        processed_revision: derived.processed_revision,
        source_record_count: derived.source_record_count,
        source_records_hash: derived.source_records_hash,
        generation: derived.generation,
        segment_ref,
    })
}

async fn materialize_authz_tuple_segment_at_revision_with_derived(
    storage: &Storage,
    tenant_id: i64,
    target_revision: u64,
    source_fence_token: u64,
    derived_entries: Option<Vec<AuthzDerivedUsersetEntry>>,
) -> Result<String> {
    if let Some(segment_ref) =
        authz_segment::existing_authz_tuple_segment_ref(storage, tenant_id, target_revision)?
    {
        return Ok(segment_ref);
    }
    let write_checkpoint = authz_segment::authz_tuple_segment_requires_checkpoint(
        storage,
        tenant_id,
        target_revision,
    )?;
    let step_started_at = std::time::Instant::now();
    let derived_entries = if let Some(derived_entries) = derived_entries {
        derived_entries
    } else {
        crate::authz_userset_index::build_expected_derived_userset_index_at_revision(
            storage,
            tenant_id,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            target_revision,
        )
        .await?
        .entries
    };
    let previous_derived_entries = if !write_checkpoint && target_revision > 1 {
        crate::authz_userset_index::build_expected_derived_userset_index_at_revision(
            storage,
            tenant_id,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            target_revision - 1,
        )
        .await?
        .entries
    } else {
        Vec::new()
    };
    crate::emit_test_timing(
        "authz_journal.materialize_segment build_derived_usersets",
        step_started_at.elapsed(),
    );
    let step_started_at = std::time::Instant::now();
    let target_revision = i64::try_from(target_revision)
        .context("authorization segment revision exceeds supported range")?;
    let records = read_authz_tuple_records_for_segment_materialization(storage, tenant_id)
        .await?
        .into_iter()
        .filter(|record| record.revision <= target_revision)
        .collect::<Vec<_>>();
    crate::emit_test_timing(
        "authz_journal.materialize_segment read_segment_source_records",
        step_started_at.elapsed(),
    );
    let step_started_at = std::time::Instant::now();
    let target_revision = u64::try_from(target_revision)?;
    let segment_ref = write_authz_tuple_segment_with_derived(
        storage,
        tenant_id,
        &records,
        &derived_entries,
        &previous_derived_entries,
        target_revision,
        source_fence_token,
        write_checkpoint,
    )
    .await?;
    crate::emit_test_timing(
        if write_checkpoint {
            "authz_journal.materialize_segment write_checkpoint_segment"
        } else {
            "authz_journal.materialize_segment write_delta_segment"
        },
        step_started_at.elapsed(),
    );
    Ok(segment_ref)
}

async fn write_authz_tuple_segment_with_derived(
    storage: &Storage,
    tenant_id: i64,
    records: &[AuthzTupleRecord],
    derived_entries: &[AuthzDerivedUsersetEntry],
    previous_derived_entries: &[AuthzDerivedUsersetEntry],
    target_revision: u64,
    source_fence_token: u64,
    write_checkpoint: bool,
) -> Result<String> {
    if write_checkpoint {
        authz_segment::write_authz_tuple_checkpoint_segment(
            storage,
            tenant_id,
            records,
            derived_entries,
            source_fence_token,
        )
        .await
    } else {
        authz_segment::write_authz_tuple_delta_segment(
            storage,
            tenant_id,
            records,
            derived_entries,
            previous_derived_entries,
            target_revision,
            source_fence_token,
        )
        .await
    }
}

#[allow(dead_code)]
async fn advance_authz_materialization(
    storage: &Storage,
    tenant_id: i64,
    batch_records: &[AuthzTupleRecord],
    source_fence_token: u64,
) -> Result<()> {
    debug_assert!(!batch_records.is_empty());
    let revision = batch_records
        .iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or_default();
    let revision = u64::try_from(revision).context("authorization revision must be nonnegative")?;
    let derived_entries = crate::authz_userset_index::advance_derived_userset_index_from_batch(
        storage,
        tenant_id,
        DEFAULT_DERIVED_USERSET_INDEX_ID,
        batch_records,
    )
    .await?
    .entries;
    materialize_authz_tuple_segment_at_revision_with_derived(
        storage,
        tenant_id,
        revision,
        source_fence_token,
        Some(derived_entries),
    )
    .await?;
    Ok(())
}

pub async fn latest_authz_revision(storage: &Storage, tenant_id: i64) -> Result<i64> {
    let tuple_revision = latest_authz_tuple_revision(storage, tenant_id).await?;
    let schema_revision = crate::authz_realm_schema::list_schema_revisions(storage, tenant_id)
        .await?
        .into_iter()
        .map(|record| record.authz_revision)
        .chain(
            crate::authz_realm_schema::list_schema_bindings(storage, tenant_id)
                .await?
                .into_iter()
                .map(|record| record.authz_revision),
        )
        .max()
        .unwrap_or(0);
    Ok(tuple_revision.max(i64::try_from(schema_revision)?))
}

pub(crate) async fn latest_authz_tuple_revision(storage: &Storage, tenant_id: i64) -> Result<i64> {
    let store = CoreStore::new(storage.clone()).await?;
    let stream_id = authz_tuple_stream_id(tenant_id);
    let (sequence, _) = store.raw_stream_head(&stream_id).await?;
    if sequence == 0 {
        return Ok(0);
    }
    let record = store
        .read_stream(ReadStream {
            stream_id,
            after_sequence: sequence.saturating_sub(1),
            limit: 1,
        })
        .await?
        .pop()
        .ok_or_else(|| anyhow!("authorization journal head record is missing"))?;
    match record.record_kind.as_str() {
        AUTHZ_TUPLE_RECORD_KIND => Ok(decode_authz_tuple_journal_body(&record.payload)?.revision),
        AUTHZ_TUPLE_BATCH_RECORD_KIND => decode_authz_tuple_batch_journal_body(&record.payload)?
            .into_iter()
            .map(|record| record.revision)
            .max()
            .ok_or_else(|| anyhow!("authorization journal head batch is empty")),
        other => Err(anyhow!(
            "authorization journal head has unsupported record kind {other}"
        )),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn check_authz_tuple(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    check_authz_tuple_at_revision(
        storage,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
        i64::MAX,
    )
    .await
}

pub fn validate_optional_caveat_hash(value: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    if value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        Ok(())
    } else {
        Err(anyhow!("caveat_hash must be empty or hex32"))
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn check_authz_tuple_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<Option<AuthzTupleRecord>> {
    let records = if revision == i64::MAX {
        read_all_authz_tuple_records(storage, tenant_id).await?
    } else {
        read_all_authz_tuple_records_from_journal(storage, tenant_id).await?
    };
    Ok(records
        .into_iter()
        .filter(|record| {
            record.revision <= revision
                && record.namespace == namespace
                && record.object_id == object_id
                && record.relation == relation
                && record.subject_kind == subject_kind
                && record.subject_id == subject_id
                && record.caveat_hash == caveat_hash
        })
        .max_by_key(|record| (record.revision, record.revision_ordinal)))
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_permission_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<bool> {
    if revision >= 0 {
        match lookup_derived_userset_index_at_revision(
            storage,
            tenant_id,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
            revision as u64,
        )
        .await
        {
            Ok(Some(true)) => return Ok(true),
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    tenant_id,
                    revision,
                    error = %error,
                    "derived userset index lookup failed; falling back to revision resolver"
                );
            }
        }

        // The current materialized rows are substantially cheaper than replaying the
        // append journal. Double-check the revision around the read so an update racing
        // this lookup falls back to the historical resolver instead of mixing revisions.
        if latest_authz_revision(storage, tenant_id).await? == revision {
            let allowed = resolve_current_permission(
                storage,
                tenant_id,
                namespace,
                object_id,
                relation,
                subject_kind,
                subject_id,
                caveat_hash,
            )
            .await?;
            if latest_authz_revision(storage, tenant_id).await? == revision {
                return Ok(allowed);
            }
        }
    }

    resolve_permission_from_current_view_at_revision(
        storage,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
        revision,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_current_permission(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<bool> {
    resolve_permission_from_current_view_at_revision(
        storage,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
        i64::MAX,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_permission_from_current_view_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<bool> {
    let current = current_authz_view_at_revision(storage, tenant_id, revision).await?;
    let subject = resolver::SubjectRef {
        kind: subject_kind.to_string(),
        id: subject_id.to_string(),
        caveat_hash: caveat_hash.to_string(),
    };
    let userset = resolver::UsersetRef {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
    };
    let schema_index =
        resolver::SchemaRuleIndex::load(storage, tenant_id, &current, [namespace]).await?;
    resolver::resolve_userset(&current, &schema_index, &userset, &subject)
}

pub async fn list_authz_tuple_log(
    storage: &Storage,
    tenant_id: i64,
    after_revision: i64,
    namespace: &str,
    limit: usize,
) -> Result<Vec<AuthzTupleRecord>> {
    let mut records = read_all_authz_tuple_records_from_journal(storage, tenant_id).await?;
    records.retain(|record| {
        record.revision > after_revision && (namespace.is_empty() || record.namespace == namespace)
    });
    records.sort_by_key(|record| (record.revision, record.revision_ordinal));
    if limit > 0 && records.len() > limit {
        records.truncate(limit);
    }
    Ok(records)
}

pub async fn read_current_authz_tuples_at_revision(
    storage: &Storage,
    tenant_id: i64,
    filter: AuthzTupleFilter,
    revision: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    let mut records: Vec<_> = current_authz_view_at_revision(storage, tenant_id, revision)
        .await?
        .into_values()
        .filter(|record| record.operation == "add")
        .filter(|record| matches_authz_tuple_filter(record, &filter))
        .collect();
    records.sort_by(|left, right| {
        (
            &left.namespace,
            &left.object_id,
            &left.relation,
            &left.subject_kind,
            &left.subject_id,
            &left.caveat_hash,
        )
            .cmp(&(
                &right.namespace,
                &right.object_id,
                &right.relation,
                &right.subject_kind,
                &right.subject_id,
                &right.caveat_hash,
            ))
    });
    Ok(records)
}

pub async fn list_current_authz_objects_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<Vec<String>> {
    let mut objects = BTreeSet::new();
    if revision >= 0 {
        match list_derived_userset_objects_at_revision(
            storage,
            tenant_id,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            namespace,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
            revision as u64,
        )
        .await
        {
            Ok(Some(index_objects)) => objects.extend(index_objects),
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    tenant_id,
                    revision,
                    error = %error,
                    "derived userset object listing failed; falling back to revision resolver"
                );
            }
        }
    }

    let filter = AuthzTupleFilter {
        namespace: Some(namespace.to_string()),
        relation: Some(relation.to_string()),
        subject_kind: Some(subject_kind.to_string()),
        subject_id: Some(subject_id.to_string()),
        caveat_hash: Some(caveat_hash.to_string()),
        ..AuthzTupleFilter::default()
    };
    let records =
        read_current_authz_tuples_at_revision(storage, tenant_id, filter, revision).await?;
    objects.extend(records.into_iter().map(|record| record.object_id));

    let current = current_authz_view_at_revision(storage, tenant_id, revision).await?;
    let subject = resolver::SubjectRef {
        kind: subject_kind.to_string(),
        id: subject_id.to_string(),
        caveat_hash: caveat_hash.to_string(),
    };
    let candidates = current
        .values()
        .filter(|record| record.namespace == namespace && record.operation == "add")
        .map(|record| resolver::UsersetRef {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: relation.to_string(),
        })
        .collect::<BTreeSet<_>>();
    let schema_index =
        resolver::SchemaRuleIndex::load(storage, tenant_id, &current, [namespace]).await?;
    for userset in candidates {
        if resolver::resolve_userset(&current, &schema_index, &userset, &subject)? {
            objects.insert(userset.object_id);
        }
    }

    Ok(objects.into_iter().collect())
}

pub async fn list_current_authz_subjects_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: Option<&str>,
    revision: i64,
) -> Result<Vec<AuthzSubjectRef>> {
    let current = current_authz_view_at_revision(storage, tenant_id, revision).await?;
    let userset = resolver::UsersetRef {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
    };
    let schema_index =
        resolver::SchemaRuleIndex::load(storage, tenant_id, &current, [namespace]).await?;
    Ok(
        resolver::collect_subjects_for_userset(&current, &schema_index, &userset)?
            .into_iter()
            .filter(|subject| subject_kind.is_none_or(|kind| subject.kind == kind))
            .map(|subject| AuthzSubjectRef {
                subject_kind: subject.kind,
                subject_id: subject.id,
                caveat_hash: subject.caveat_hash,
            })
            .collect(),
    )
}

fn matches_authz_tuple_filter(record: &AuthzTupleRecord, filter: &AuthzTupleFilter) -> bool {
    filter
        .namespace
        .as_ref()
        .is_none_or(|value| record.namespace == *value)
        && filter
            .object_id
            .as_ref()
            .is_none_or(|value| record.object_id == *value)
        && filter
            .relation
            .as_ref()
            .is_none_or(|value| record.relation == *value)
        && filter
            .subject_kind
            .as_ref()
            .is_none_or(|value| record.subject_kind == *value)
        && filter
            .subject_id
            .as_ref()
            .is_none_or(|value| record.subject_id == *value)
        && filter
            .caveat_hash
            .as_ref()
            .is_none_or(|value| record.caveat_hash == *value)
}

async fn current_authz_view_at_revision(
    storage: &Storage,
    tenant_id: i64,
    revision: i64,
) -> Result<BTreeMap<TupleViewKey, AuthzTupleRecord>> {
    let tuple_revision = latest_authz_tuple_revision(storage, tenant_id).await?;
    let mut records = if revision >= tuple_revision {
        read_all_authz_tuple_records(storage, tenant_id).await?
    } else {
        read_all_authz_tuple_records_from_journal(storage, tenant_id).await?
    };
    records.retain(|record| record.revision <= revision);
    records.sort_by_key(|record| (record.revision, record.revision_ordinal));
    let mut current = BTreeMap::new();
    for record in records {
        current.insert(TupleViewKey::from(&record), record);
    }
    Ok(current)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TupleViewKey {
    pub(crate) namespace: String,
    pub(crate) object_id: String,
    pub(crate) relation: String,
    pub(crate) subject_kind: String,
    pub(crate) subject_id: String,
    pub(crate) caveat_hash: String,
}

impl From<&AuthzTupleRecord> for TupleViewKey {
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

fn encode_authz_tuple_journal_body(record: &AuthzTupleRecord, fence_token: u64) -> Result<Vec<u8>> {
    encode_deterministic_proto(&AuthzTupleJournalBodyProto {
        schema: AUTHZ_TUPLE_JOURNAL_BODY_SCHEMA.to_string(),
        record: Some(authz_record_to_proto(record)?),
        fence_token,
        mutation_id: record.mutation_id.to_string(),
    })
}

fn encode_authz_tuple_batch_journal_body(
    tenant_id: i64,
    revision: i64,
    records: &[AuthzTupleRecord],
    fence_token: u64,
) -> Result<Vec<u8>> {
    let mutation_id = records
        .first()
        .ok_or_else(|| anyhow!("authz tuple batch body must not be empty"))?
        .mutation_id
        .to_string();
    encode_deterministic_proto(&AuthzTupleBatchJournalBodyProto {
        schema: AUTHZ_TUPLE_BATCH_JOURNAL_BODY_SCHEMA.to_string(),
        revision,
        tenant_id,
        records: records
            .iter()
            .map(authz_record_to_proto)
            .collect::<Result<Vec<_>>>()?,
        fence_token,
        mutation_id,
    })
}

fn decode_authz_tuple_journal_body(bytes: &[u8]) -> Result<AuthzTupleRecord> {
    let body = AuthzTupleJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&body, bytes, "authz tuple journal body")?;
    if body.schema != AUTHZ_TUPLE_JOURNAL_BODY_SCHEMA {
        return Err(anyhow!("authz tuple journal body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&body.mutation_id)
        .context("authz tuple journal body mutation_id is not a UUID")?;
    let record = authz_record_from_proto(
        body.record
            .ok_or_else(|| anyhow!("authz tuple journal body is missing record"))?,
    )?;
    if record.mutation_id.to_string() != body.mutation_id {
        return Err(anyhow!(
            "authz tuple journal body mutation_id does not match record"
        ));
    }
    Ok(record)
}

fn decode_authz_tuple_batch_journal_body(bytes: &[u8]) -> Result<Vec<AuthzTupleRecord>> {
    let body = AuthzTupleBatchJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&body, bytes, "authz tuple batch journal body")?;
    if body.schema != AUTHZ_TUPLE_BATCH_JOURNAL_BODY_SCHEMA {
        return Err(anyhow!("authz tuple batch journal body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&body.mutation_id)
        .context("authz tuple batch journal body mutation_id is not a UUID")?;
    if body.records.is_empty() {
        return Err(anyhow!("authz tuple batch journal body is empty"));
    }
    body.records
        .into_iter()
        .map(|record| {
            if record.tenant_id != body.tenant_id || record.revision != body.revision {
                return Err(anyhow!("authz tuple batch body contains mismatched record"));
            }
            authz_record_from_proto(record)
        })
        .collect()
}

fn decode_authz_tuple_journal_body_fence(bytes: &[u8]) -> Result<u64> {
    let body = AuthzTupleJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&body, bytes, "authz tuple journal body")?;
    if body.schema != AUTHZ_TUPLE_JOURNAL_BODY_SCHEMA {
        return Err(anyhow!("authz tuple journal body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&body.mutation_id)
        .context("authz tuple journal body mutation_id is not a UUID")?;
    Ok(body.fence_token)
}

fn decode_authz_tuple_batch_journal_body_fence(bytes: &[u8]) -> Result<u64> {
    let body = AuthzTupleBatchJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&body, bytes, "authz tuple batch journal body")?;
    if body.schema != AUTHZ_TUPLE_BATCH_JOURNAL_BODY_SCHEMA {
        return Err(anyhow!("authz tuple batch journal body schema mismatch"));
    }
    let _mutation_id = uuid::Uuid::parse_str(&body.mutation_id)
        .context("authz tuple batch journal body mutation_id is not a UUID")?;
    Ok(body.fence_token)
}

async fn write_authz_tuple_records_to_current_rows(
    storage: &Storage,
    records: &[AuthzTupleRecord],
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut keys = Vec::with_capacity(records.len());
    let mut payloads = Vec::with_capacity(records.len());
    for record in records {
        keys.push(authz_tuple_current_row_key(record)?);
        let record_payload = encode_authz_tuple_current_row(record)?;
        payloads.push(
            encode_authz_payload_row(
                storage,
                authz_tuple_current_common(record),
                AUTHZ_TUPLE_PAGE_PAYLOAD_KIND,
                &format!(
                    "tenant/{}/tuple/{}/{}/{}/{}",
                    record.tenant_id,
                    record.namespace,
                    record.object_id,
                    record.relation,
                    record.revision
                ),
                record.revision.max(0) as u64,
                &record.mutation_id.to_string(),
                record_payload,
            )
            .await?,
        );
    }
    let ops = keys
        .iter()
        .zip(payloads.iter())
        .map(|(key, payload)| CoreMetaBatchOp {
            cf: CF_AUTHZ,
            table_id: TABLE_AUTHZ_TUPLE_PAGE_ROW,
            tuple_key: key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(payload),
        })
        .collect::<Vec<_>>();
    let tenant_id = records[0].tenant_id;
    let revision = records
        .iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or(0);
    core_store
        .commit_coremeta_batch_by_embedded_roots(
            &format!("authz-current:{tenant_id}:{revision}"),
            &ops,
        )
        .await?;
    Ok(())
}

async fn read_authz_tuple_records_from_current_rows(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let mut records = Vec::new();
    for row in meta.scan_prefix(
        CF_AUTHZ,
        TABLE_AUTHZ_TUPLE_PAGE_ROW,
        &authz_tuple_current_row_prefix(tenant_id)?,
    )? {
        let record_payload = decode_authz_payload_row(
            storage,
            tenant_id,
            &row.payload,
            AUTHZ_TUPLE_PAGE_PAYLOAD_KIND,
        )
        .await?;
        let record = decode_authz_tuple_current_row(&record_payload)?;
        if record.tenant_id != tenant_id {
            return Err(anyhow!("authz tuple current row tenant mismatch"));
        }
        records.push(record);
    }
    records.sort_by_key(|record| (record.revision, record.revision_ordinal));
    Ok(records)
}

fn encode_authz_tuple_current_row(record: &AuthzTupleRecord) -> Result<Vec<u8>> {
    encode_deterministic_proto(&AuthzTupleCurrentRowProto {
        common: Some(authz_tuple_current_common(record)),
        schema: AUTHZ_TUPLE_CURRENT_ROW_SCHEMA.to_string(),
        record: Some(authz_record_to_proto(record)?),
    })
}

fn authz_tuple_current_common(
    record: &AuthzTupleRecord,
) -> crate::core_store::CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        format!("tenant/{}", record.tenant_id),
        core_meta_root_key_hash(&format!("authz/{}", record.tenant_id)),
        record.revision.max(0) as u64,
        record.mutation_id.to_string(),
        record
            .written_at
            .timestamp_nanos_opt()
            .unwrap_or_default()
            .max(0) as u64,
    )
}

fn decode_authz_tuple_current_row(bytes: &[u8]) -> Result<AuthzTupleRecord> {
    let row = AuthzTupleCurrentRowProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "authz tuple current row")?;
    if row.schema != AUTHZ_TUPLE_CURRENT_ROW_SCHEMA {
        return Err(anyhow!("authz tuple current row schema mismatch"));
    }
    row.common
        .as_ref()
        .ok_or_else(|| anyhow!("authz tuple current row missing CoreMeta common"))?;
    authz_record_from_proto(
        row.record
            .ok_or_else(|| anyhow!("authz tuple current row is missing record"))?,
    )
}

fn authz_tuple_current_row_key(record: &AuthzTupleRecord) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("authz-current"),
        CoreMetaTuplePart::I64(record.tenant_id),
        CoreMetaTuplePart::Utf8(&record.namespace),
        CoreMetaTuplePart::Utf8(&record.object_id),
        CoreMetaTuplePart::Utf8(&record.relation),
        CoreMetaTuplePart::Utf8(&record.subject_kind),
        CoreMetaTuplePart::Utf8(&record.subject_id),
        CoreMetaTuplePart::Utf8(&record.caveat_hash),
    ])
}

fn authz_tuple_current_row_prefix(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("authz-current"),
        CoreMetaTuplePart::I64(tenant_id),
    ])
}

fn authz_record_to_proto(record: &AuthzTupleRecord) -> Result<AuthzTupleRecordProto> {
    let written_at_unix_nanos = record
        .written_at
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("authz tuple timestamp cannot be represented in nanoseconds"))?;
    Ok(AuthzTupleRecordProto {
        revision: record.revision,
        revision_ordinal: record.revision_ordinal,
        tenant_id: record.tenant_id,
        namespace: record.namespace.clone(),
        object_id: record.object_id.clone(),
        relation: record.relation.clone(),
        subject_kind: record.subject_kind.clone(),
        subject_id: record.subject_id.clone(),
        caveat_hash: record.caveat_hash.clone(),
        operation: authz_operation_to_proto(&record.operation)? as i32,
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        mutation_id: record.mutation_id.to_string(),
        record_hash: record.record_hash.clone(),
        written_at_unix_nanos,
    })
}

fn authz_record_from_proto(proto: AuthzTupleRecordProto) -> Result<AuthzTupleRecord> {
    Ok(AuthzTupleRecord {
        revision: proto.revision,
        revision_ordinal: proto.revision_ordinal,
        tenant_id: proto.tenant_id,
        namespace: proto.namespace,
        object_id: proto.object_id,
        relation: proto.relation,
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
        caveat_hash: proto.caveat_hash,
        operation: authz_operation_from_proto(proto.operation)?.to_string(),
        written_by: proto.written_by,
        reason: proto.reason,
        mutation_id: uuid::Uuid::parse_str(&proto.mutation_id)
            .context("authz tuple mutation_id is not a UUID")?,
        record_hash: proto.record_hash,
        written_at: chrono::DateTime::from_timestamp_nanos(proto.written_at_unix_nanos),
    })
}

fn authz_operation_to_proto(operation: &str) -> Result<AuthzTupleOperationProto> {
    match operation {
        "add" => Ok(AuthzTupleOperationProto::Add),
        "remove" => Ok(AuthzTupleOperationProto::Remove),
        other => Err(anyhow!("unsupported authz tuple operation {other}")),
    }
}

fn authz_operation_from_proto(operation: i32) -> Result<&'static str> {
    match AuthzTupleOperationProto::try_from(operation)
        .map_err(|_| anyhow!("unknown authz tuple operation {operation}"))?
    {
        AuthzTupleOperationProto::Add => Ok("add"),
        AuthzTupleOperationProto::Remove => Ok("remove"),
        AuthzTupleOperationProto::Unspecified => {
            Err(anyhow!("authz tuple operation must be specified"))
        }
    }
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    let encoded = encode_deterministic_proto(message)?;
    if encoded != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

async fn read_all_authz_tuple_records(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    read_authz_tuple_records_from_current_rows(storage, tenant_id).await
}

async fn read_all_authz_tuple_records_from_journal(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_records = core_store
        .read_stream(ReadStream {
            stream_id: authz_tuple_stream_id(tenant_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut records = Vec::new();
    for stream_record in stream_records {
        match stream_record.record_kind.as_str() {
            AUTHZ_TUPLE_RECORD_KIND => {
                records.push(decode_authz_tuple_journal_body(&stream_record.payload)?);
            }
            AUTHZ_TUPLE_BATCH_RECORD_KIND => {
                records.extend(decode_authz_tuple_batch_journal_body(
                    &stream_record.payload,
                )?);
            }
            _ => {}
        }
    }
    Ok(records)
}

async fn read_authz_tuple_records_at_revision_from_journal(
    storage: &Storage,
    tenant_id: i64,
    revision: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    let mut records = read_all_authz_tuple_records_from_journal(storage, tenant_id)
        .await?
        .into_iter()
        .filter(|record| record.revision == revision)
        .collect::<Vec<_>>();
    records.sort_by_key(|record| record.revision_ordinal);
    Ok(records)
}

async fn read_authz_tuple_records_for_segment_materialization(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    let mut by_mutation = BTreeMap::<String, AuthzTupleRecord>::new();
    for record in read_all_authz_tuple_records_from_journal(storage, tenant_id).await? {
        by_mutation.insert(record.mutation_id.to_string(), record);
    }
    for record in read_authz_tuple_records_from_current_rows(storage, tenant_id).await? {
        by_mutation.insert(record.mutation_id.to_string(), record);
    }
    let mut records = by_mutation.into_values().collect::<Vec<_>>();
    records.sort_by_key(|record| (record.revision, record.revision_ordinal));
    Ok(records)
}

pub fn authz_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/authz_tuple").as_bytes())
}

fn authz_tuple_stream_id(tenant_id: i64) -> String {
    format!("authz_tuple:tenant:{tenant_id}")
}

fn authz_partition_principal(tenant_id: i64) -> String {
    format!("partition-owner:authz_tuple:{tenant_id}")
}

pub(crate) async fn latest_authz_journal_fence_token(
    storage: &Storage,
    tenant_id: i64,
) -> Result<u64> {
    Ok(read_authz_journal_payload_fences(storage, tenant_id)
        .await?
        .into_iter()
        .max()
        .unwrap_or(0))
}

#[cfg(test)]
pub(crate) async fn read_authz_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<u64>> {
    read_authz_journal_payload_fences(storage, tenant_id).await
}

async fn read_authz_journal_payload_fences(storage: &Storage, tenant_id: i64) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(ReadStream {
            stream_id: authz_tuple_stream_id(tenant_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?
        .into_iter()
        .filter_map(|record| match record.record_kind.as_str() {
            AUTHZ_TUPLE_RECORD_KIND => Some(decode_authz_tuple_journal_body_fence(&record.payload)),
            AUTHZ_TUPLE_BATCH_RECORD_KIND => {
                Some(decode_authz_tuple_batch_journal_body_fence(&record.payload))
            }
            _ => None,
        })
        .collect::<Result<Vec<_>>>()?)
}

fn require_authz_permit(tenant_id: i64, permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "authz_tuple"
        || permit.partition_id != hex::encode(authz_partition_id(tenant_id))
    {
        return Err(anyhow!(
            "partition write permit does not target this authorization tuple partition"
        ));
    }
    Ok(())
}

struct AuthzRecordHashInput<'a> {
    revision: i64,
    revision_ordinal: u32,
    tenant_id: i64,
    namespace: &'a str,
    object_id: &'a str,
    relation: &'a str,
    subject_kind: &'a str,
    subject_id: &'a str,
    caveat_hash: &'a str,
    operation: &'a str,
    written_by: &'a str,
    reason: &'a str,
}

fn authz_record_hash(input: AuthzRecordHashInput<'_>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&input.revision.to_le_bytes());
    hasher.update(&input.revision_ordinal.to_le_bytes());
    hasher.update(&input.tenant_id.to_le_bytes());
    for part in [
        input.namespace,
        input.object_id,
        input.relation,
        input.subject_kind,
        input.subject_id,
        input.caveat_hash,
        input.operation,
        input.written_by,
        input.reason,
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests;
