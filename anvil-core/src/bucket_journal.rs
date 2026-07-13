use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreStore, CoreTransaction, CoreTransactionState, CoreTransactionUpdate, ReadStream,
    TABLE_BUCKET_CURRENT_BY_ID_ROW, TABLE_BUCKET_CURRENT_BY_NAME_ROW,
    core_meta_committed_row_common, core_meta_payload_digest, core_meta_root_key_hash,
    core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{Bucket, BucketMetadataEvent};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use prost::Message;
use serde_json::{Value as JsonValue, json};
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

const BUCKET_CURRENT_ROW_SCHEMA: &str = "anvil.core.bucket_current.v1";
const BUCKET_METADATA_BODY_SCHEMA: &str = "anvil.core.bucket_metadata.v1";
const BUCKET_METADATA_RECORD_KIND: &str = "bucket_metadata";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketJournalMutation {
    Create,
    Update,
    Delete,
}

impl BucketJournalMutation {
    fn event_name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketJournalBody {
    event: String,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    region: String,
    is_public_read: bool,
    mutation_id: String,
    fence_token: u64,
    created_at: String,
    emitted_at: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct BucketJournalBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    event: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    bucket_name: String,
    #[prost(string, tag = "6")]
    region: String,
    #[prost(bool, tag = "7")]
    is_public_read: bool,
    #[prost(string, tag = "8")]
    mutation_id: String,
    #[prost(string, tag = "9")]
    created_at: String,
    #[prost(string, optional, tag = "10")]
    emitted_at: Option<String>,
    #[prost(uint64, tag = "11")]
    fence_token: u64,
}

#[derive(Clone, PartialEq, Message)]
struct BucketCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bool, tag = "3")]
    deleted: bool,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(int64, tag = "5")]
    tenant_id: i64,
    #[prost(string, tag = "6")]
    bucket_name: String,
    #[prost(string, tag = "7")]
    region: String,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(bool, tag = "9")]
    is_public_read: bool,
}

#[cfg(test)]
async fn append_bucket_mutation(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
) -> Result<()> {
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        BucketJournalScope::Tenant(bucket.tenant_id),
        0,
        None,
    )
    .await?;
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        BucketJournalScope::Global,
        0,
        None,
    )
    .await
}

pub(crate) async fn append_bucket_mutation_with_permits(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    tenant_permit: &PartitionWritePermit,
    global_permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let total_start = std::time::Instant::now();
    let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
    let global_scope = BucketJournalScope::Global;
    require_bucket_scope_permit(tenant_scope, tenant_permit)?;
    require_bucket_scope_permit(global_scope, global_permit)?;
    let tenant_precondition = async {
        let step_start = std::time::Instant::now();
        let precondition =
            partition_write_precondition(storage, tenant_permit, partition_owner_signing_key).await;
        crate::emit_test_timing(
            "bucket_journal.append_bucket_mutation tenant_precondition",
            step_start.elapsed(),
        );
        precondition
    };
    let global_precondition = async {
        let step_start = std::time::Instant::now();
        let precondition =
            partition_write_precondition(storage, global_permit, partition_owner_signing_key).await;
        crate::emit_test_timing(
            "bucket_journal.append_bucket_mutation global_precondition",
            step_start.elapsed(),
        );
        precondition
    };
    let (tenant_precondition, global_precondition) =
        tokio::join!(tenant_precondition, global_precondition);
    let tenant_precondition = tenant_precondition?;
    let global_precondition = global_precondition?;

    let step_start = std::time::Instant::now();
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        tenant_scope,
        tenant_permit.fence_token,
        Some(tenant_precondition),
    )
    .await?;
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation tenant_append",
        step_start.elapsed(),
    );
    let step_start = std::time::Instant::now();
    append_bucket_mutation_to_stream(
        storage,
        bucket,
        mutation,
        global_scope,
        global_permit.fence_token,
        Some(global_precondition),
    )
    .await?;
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation global_append",
        step_start.elapsed(),
    );
    crate::emit_test_timing(
        "bucket_journal.append_bucket_mutation total",
        total_start.elapsed(),
    );
    Ok(())
}

pub(crate) async fn stage_bucket_mutation_in_transaction(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let transaction = core_store
        .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
        .await?;
    let mutation_id = uuid::Uuid::new_v4().to_string();
    let row_generation = current_unix_nanos();
    let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
    let common_realm_id = tenant_scope.realm_id();
    let common_root_key_hash = tenant_scope.root_key_hash();
    let mut preconditions = Vec::new();
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: transaction.scope_partition.clone(),
        stream_id: tenant_scope.stream_id(),
        record_kind: BUCKET_METADATA_RECORD_KIND.to_string(),
        payload: encode_bucket_journal_body(&BucketJournalBody {
            event: mutation.event_name().to_string(),
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            bucket_name: bucket.name.clone(),
            region: bucket.region.clone(),
            is_public_read: bucket.is_public_read,
            mutation_id: mutation_id.clone(),
            fence_token: 0,
            created_at: bucket.created_at.to_rfc3339(),
            emitted_at: Some(chrono::Utc::now().to_rfc3339()),
        })?,
        idempotency_key: Some(format!(
            "bucket-metadata:{}:{}",
            tenant_scope.stream_id(),
            mutation_id
        )),
    }];

    for scope in [tenant_scope, BucketJournalScope::Global] {
        preconditions.push(
            bucket_current_coremeta_precondition(
                &core_store,
                scope,
                bucket,
                mutation,
                transaction_id,
                transaction_principal,
            )
            .await?,
        );
        operations.extend(bucket_current_coremeta_operations_with_root(
            scope,
            bucket,
            mutation,
            &transaction.scope_partition,
            transaction_id,
            row_generation,
            common_realm_id.clone(),
            common_root_key_hash.clone(),
        )?);
    }

    core_store
        .stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction_id.to_string(),
            scope_partition: transaction.scope_partition,
            committed_by_principal: transaction_principal.to_string(),
            preconditions,
            operations,
        })
        .await?;
    tracing::debug!(
        transaction_id,
        mutation_id,
        bucket_id = bucket.id,
        bucket_name = bucket.name.as_str(),
        "staged bucket metadata mutation in explicit transaction"
    );
    Ok(())
}

pub async fn read_current_bucket_by_id(
    storage: &Storage,
    bucket_id: i64,
) -> Result<Option<Bucket>> {
    let current = read_current_bucket_by_id_row(storage, bucket_id).await?;
    let Some(current) = current else {
        return Ok(None);
    };
    if current.bucket.id != bucket_id {
        return Err(anyhow!("CoreStore bucket current id row scope mismatch"));
    }
    Ok(current.into_active_bucket())
}

pub async fn next_bucket_id(storage: &Storage) -> Result<i64> {
    let max_bucket_id = read_max_bucket_id_from_current_rows(storage).await?;
    max_bucket_id
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("bucket id overflow"))
}

async fn append_bucket_mutation_to_stream(
    storage: &Storage,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    scope: BucketJournalScope,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mutation_id = uuid::Uuid::new_v4();
    let row_generation = current_unix_nanos();
    let payload = encode_bucket_journal_body(&BucketJournalBody {
        event: mutation.event_name().to_string(),
        tenant_id: bucket.tenant_id,
        bucket_id: bucket.id,
        bucket_name: bucket.name.clone(),
        region: bucket.region.clone(),
        is_public_read: bucket.is_public_read,
        mutation_id: mutation_id.to_string(),
        fence_token,
        created_at: bucket.created_at.to_rfc3339(),
        emitted_at: Some(chrono::Utc::now().to_rfc3339()),
    })?;

    let partition_id = hex::encode(scope.partition_id());
    let mut operations = vec![CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id: scope.stream_id(),
        record_kind: BUCKET_METADATA_RECORD_KIND.to_string(),
        payload,
        idempotency_key: Some(format!(
            "bucket-metadata:{}:{}",
            scope.stream_id(),
            mutation_id
        )),
    }];
    operations.extend(bucket_current_coremeta_operations(
        scope,
        bucket,
        mutation,
        &partition_id,
        &mutation_id.to_string(),
        row_generation,
    )?);

    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("bucket-metadata:{}:{}", scope.stream_id(), mutation_id),
            scope_partition: partition_id.clone(),
            committed_by_principal: scope.partition_principal(),
            preconditions: partition_precondition.into_iter().collect(),
            operations,
        })
        .await?;
    Ok(())
}

pub async fn read_current_bucket(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<Bucket>> {
    if let Some(current) =
        read_current_bucket_for_tenant_row(storage, tenant_id, bucket_name).await?
    {
        ensure_bucket_tenant_name_matches(&current.bucket, tenant_id, bucket_name)?;
        return Ok(current.into_active_bucket());
    }

    Ok(None)
}

pub async fn read_current_buckets(storage: &Storage, tenant_id: i64) -> Result<Vec<Bucket>> {
    let mut buckets = BTreeMap::new();
    overlay_current_bucket_rows(storage, BucketJournalScope::Tenant(tenant_id), &mut buckets)
        .await?;
    Ok(buckets.into_values().collect())
}

pub async fn latest_bucket_metadata_event(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<BucketMetadataEvent>> {
    Ok(
        list_bucket_metadata_events(storage, tenant_id, bucket_name, 0, 0)
            .await?
            .into_iter()
            .max_by_key(|event| event.id),
    )
}

pub async fn materialize_committed_bucket_metadata_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<Vec<BucketMetadataEvent>> {
    if transaction.state != CoreTransactionState::Committed {
        return Ok(Vec::new());
    }

    let core_store = CoreStore::new(storage.clone()).await?;
    let mut events = Vec::new();
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
        } = update
        else {
            continue;
        };
        if !stream_id.starts_with("bucket_metadata:tenant:") {
            continue;
        }
        let after_sequence = visible_sequence.saturating_sub(1);
        let records = core_store
            .read_stream(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence,
                limit: 1,
            })
            .await?;
        let record = records
            .into_iter()
            .find(|record| {
                record.sequence == *visible_sequence
                    && record.event_hash == *prepared_record_hash
                    && record.record_kind == BUCKET_METADATA_RECORD_KIND
            })
            .ok_or_else(|| {
                anyhow!("committed bucket metadata transaction stream record missing")
            })?;
        events.push(bucket_event_from_body(
            record.sequence,
            decode_bucket_journal_body(&record.payload)?,
        )?);
    }
    Ok(events)
}

pub async fn list_bucket_metadata_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<BucketMetadataEvent>> {
    let entries =
        read_bucket_journal_entries(storage, BucketJournalScope::Tenant(tenant_id)).await?;
    let mut events = Vec::new();
    for entry in entries {
        if entry.sequence <= after_cursor as u64 {
            continue;
        }
        if !bucket_name.is_empty() && entry.body.bucket_name != bucket_name {
            continue;
        }
        events.push(bucket_event_from_body(entry.sequence, entry.body)?);
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

pub async fn list_bucket_metadata_events_by_bucket_id(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<BucketMetadataEvent>> {
    let entries =
        read_bucket_journal_entries(storage, BucketJournalScope::Tenant(tenant_id)).await?;
    let mut events = Vec::new();
    for entry in entries {
        if entry.sequence <= after_cursor as u64 {
            continue;
        }
        if entry.body.bucket_id != bucket_id {
            continue;
        }
        events.push(bucket_event_from_body(entry.sequence, entry.body)?);
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

#[derive(Debug, Clone)]
struct BucketCurrentRow {
    deleted: bool,
    bucket: Bucket,
}

impl BucketCurrentRow {
    fn into_active_bucket(self) -> Option<Bucket> {
        if self.deleted {
            None
        } else {
            Some(self.bucket)
        }
    }
}

fn bucket_current_coremeta_operations(
    scope: BucketJournalScope,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    operation_partition_id: &str,
    mutation_id: &str,
    row_generation: u64,
) -> Result<Vec<CoreMutationOperation>> {
    bucket_current_coremeta_operations_with_root(
        scope,
        bucket,
        mutation,
        operation_partition_id,
        mutation_id,
        row_generation,
        scope.realm_id(),
        scope.root_key_hash(),
    )
}

fn bucket_current_coremeta_operations_with_root(
    scope: BucketJournalScope,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    operation_partition_id: &str,
    mutation_id: &str,
    row_generation: u64,
    common_realm_id: String,
    common_root_key_hash: String,
) -> Result<Vec<CoreMutationOperation>> {
    let payload = encode_bucket_current_row_with_root(
        bucket,
        mutation == BucketJournalMutation::Delete,
        mutation_id,
        row_generation,
        common_realm_id,
        common_root_key_hash,
    )?;
    let operations = match scope {
        BucketJournalScope::Tenant(tenant_id) => vec![CoreMutationOperation::CoreMetaPut {
            partition_id: operation_partition_id.to_string(),
            cf: CF_MESH.to_string(),
            table_id: TABLE_BUCKET_CURRENT_BY_NAME_ROW,
            tuple_key: tenant_bucket_name_current_tuple_key(tenant_id, &bucket.name)?,
            payload,
        }],
        BucketJournalScope::Global => vec![CoreMutationOperation::CoreMetaPut {
            partition_id: operation_partition_id.to_string(),
            cf: CF_MESH.to_string(),
            table_id: TABLE_BUCKET_CURRENT_BY_ID_ROW,
            tuple_key: global_bucket_id_current_tuple_key(bucket.id)?,
            payload,
        }],
    };
    Ok(operations)
}

async fn bucket_current_coremeta_precondition(
    core_store: &CoreStore,
    scope: BucketJournalScope,
    bucket: &Bucket,
    mutation: BucketJournalMutation,
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<CoreMutationPrecondition> {
    let (table_id, tuple_key) = scope.bucket_current_tuple_key(bucket)?;
    let current = core_store
        .read_coremeta_row_visible_to_transaction(
            CF_MESH,
            table_id,
            &tuple_key,
            transaction_id,
            transaction_principal,
        )
        .await?;
    Ok(CoreMutationPrecondition::CoreMetaRow {
        cf: CF_MESH.to_string(),
        table_id,
        tuple_key,
        expected_payload_hash: current
            .as_ref()
            .map(|payload| core_meta_payload_digest(table_id, payload)),
        require_absent: mutation == BucketJournalMutation::Create,
        require_present: mutation != BucketJournalMutation::Create,
    })
}

async fn read_current_bucket_for_tenant_row(
    storage: &Storage,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<Option<BucketCurrentRow>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let tuple_key = tenant_bucket_name_current_tuple_key(tenant_id, bucket_name)?;
    let Some(payload) =
        core_store.read_coremeta_row(CF_MESH, TABLE_BUCKET_CURRENT_BY_NAME_ROW, &tuple_key)?
    else {
        return Ok(None);
    };
    decode_bucket_current_row(&payload)
        .with_context(|| format!("decode bucket current CoreMeta row {tenant_id}/{bucket_name}"))
        .map(Some)
}

async fn read_current_bucket_by_id_row(
    storage: &Storage,
    bucket_id: i64,
) -> Result<Option<BucketCurrentRow>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let tuple_key = global_bucket_id_current_tuple_key(bucket_id)?;
    let Some(payload) =
        core_store.read_coremeta_row(CF_MESH, TABLE_BUCKET_CURRENT_BY_ID_ROW, &tuple_key)?
    else {
        return Ok(None);
    };
    decode_bucket_current_row(&payload)
        .with_context(|| format!("decode bucket current CoreMeta row id/{bucket_id}"))
        .map(Some)
}

async fn read_max_bucket_id_from_current_rows(storage: &Storage) -> Result<i64> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let prefix = global_bucket_id_current_tuple_prefix()?;
    let mut max_bucket_id = 0;
    for row in core_store.scan_coremeta_prefix(CF_MESH, TABLE_BUCKET_CURRENT_BY_ID_ROW, &prefix)? {
        let current = decode_bucket_current_row(&row.payload)
            .with_context(|| "decode bucket id CoreMeta row")?;
        max_bucket_id = max_bucket_id.max(current.bucket.id);
    }
    Ok(max_bucket_id)
}

async fn overlay_current_bucket_rows(
    storage: &Storage,
    scope: BucketJournalScope,
    buckets: &mut BTreeMap<String, Bucket>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let (table_id, prefix) = scope.bucket_current_tuple_prefix()?;
    for row in core_store.scan_coremeta_prefix(CF_MESH, table_id, &prefix)? {
        let current = decode_bucket_current_row(&row.payload)
            .with_context(|| "decode bucket list CoreMeta row")?;
        ensure_bucket_scope_matches(scope, &current.bucket)?;
        if current.deleted {
            buckets.remove(&current.bucket.name);
        } else {
            buckets.insert(current.bucket.name.clone(), current.bucket);
        }
    }
    Ok(())
}

fn encode_bucket_current_row(
    scope: BucketJournalScope,
    bucket: &Bucket,
    deleted: bool,
    mutation_id: &str,
    row_generation: u64,
) -> Result<Vec<u8>> {
    encode_bucket_current_row_with_root(
        bucket,
        deleted,
        mutation_id,
        row_generation,
        scope.realm_id(),
        scope.root_key_hash(),
    )
}

fn encode_bucket_current_row_with_root(
    bucket: &Bucket,
    deleted: bool,
    mutation_id: &str,
    row_generation: u64,
    common_realm_id: String,
    common_root_key_hash: String,
) -> Result<Vec<u8>> {
    let row = BucketCurrentRowProto {
        common: Some(core_meta_committed_row_common(
            common_realm_id,
            common_root_key_hash,
            row_generation,
            mutation_id.to_string(),
            row_generation,
        )),
        schema: BUCKET_CURRENT_ROW_SCHEMA.to_string(),
        deleted,
        bucket_id: bucket.id,
        tenant_id: bucket.tenant_id,
        bucket_name: bucket.name.clone(),
        region: bucket.region.clone(),
        created_at: bucket.created_at.to_rfc3339(),
        is_public_read: bucket.is_public_read,
    };
    encode_deterministic_proto(&row)
}

fn decode_bucket_current_row(bytes: &[u8]) -> Result<BucketCurrentRow> {
    let row = BucketCurrentRowProto::decode(bytes)?;
    ensure_deterministic_proto(&row, bytes, "bucket current row")?;
    if row.schema != BUCKET_CURRENT_ROW_SCHEMA {
        return Err(anyhow!("CoreStore bucket current row has invalid schema"));
    }
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore bucket current row missing CoreMeta common"))?;
    if common.root_key_hash.is_empty() {
        return Err(anyhow!("CoreStore bucket current row missing root hash"));
    }
    let bucket = Bucket {
        id: row.bucket_id,
        tenant_id: row.tenant_id,
        name: row.bucket_name,
        region: row.region,
        created_at: chrono::DateTime::parse_from_rfc3339(&row.created_at)?
            .with_timezone(&chrono::Utc),
        is_public_read: row.is_public_read,
    };
    Ok(BucketCurrentRow {
        deleted: row.deleted,
        bucket,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketJournalEntry {
    sequence: u64,
    body: BucketJournalBody,
}

async fn read_bucket_journal_entries(
    storage: &Storage,
    scope: BucketJournalScope,
) -> Result<Vec<BucketJournalEntry>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = core_store
        .read_stream(ReadStream {
            stream_id: scope.stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut entries = Vec::new();
    for record in records {
        if record.record_kind != BUCKET_METADATA_RECORD_KIND {
            continue;
        }
        entries.push(BucketJournalEntry {
            sequence: record.sequence,
            body: decode_bucket_journal_body(&record.payload).with_context(|| {
                format!("decode bucket metadata stream record {}", record.cursor)
            })?,
        });
    }
    Ok(entries)
}

#[derive(Debug, Clone, Copy)]
enum BucketJournalScope {
    Tenant(i64),
    Global,
}

impl BucketJournalScope {
    fn stream_id(self) -> String {
        match self {
            Self::Tenant(tenant_id) => format!("bucket_metadata:tenant:{tenant_id}"),
            Self::Global => "bucket_metadata:global".to_string(),
        }
    }

    fn partition_id(self) -> Hash32 {
        match self {
            Self::Tenant(tenant_id) => tenant_bucket_partition_id(tenant_id),
            Self::Global => global_bucket_partition_id(),
        }
    }

    fn partition_principal(self) -> String {
        match self {
            Self::Tenant(tenant_id) => {
                format!("partition-owner:bucket_metadata:tenant:{tenant_id}")
            }
            Self::Global => "partition-owner:bucket_metadata:global".to_string(),
        }
    }

    fn bucket_current_tuple_prefix(self) -> Result<(u16, Vec<u8>)> {
        match self {
            Self::Tenant(tenant_id) => Ok((
                TABLE_BUCKET_CURRENT_BY_NAME_ROW,
                tenant_bucket_name_current_tuple_prefix(tenant_id)?,
            )),
            Self::Global => Ok((
                TABLE_BUCKET_CURRENT_BY_ID_ROW,
                global_bucket_id_current_tuple_prefix()?,
            )),
        }
    }

    fn bucket_current_tuple_key(self, bucket: &Bucket) -> Result<(u16, Vec<u8>)> {
        match self {
            Self::Tenant(tenant_id) => Ok((
                TABLE_BUCKET_CURRENT_BY_NAME_ROW,
                tenant_bucket_name_current_tuple_key(tenant_id, &bucket.name)?,
            )),
            Self::Global => Ok((
                TABLE_BUCKET_CURRENT_BY_ID_ROW,
                global_bucket_id_current_tuple_key(bucket.id)?,
            )),
        }
    }

    fn realm_id(self) -> String {
        match self {
            Self::Tenant(tenant_id) => format!("tenant/{tenant_id}"),
            Self::Global => "system".to_string(),
        }
    }

    fn root_anchor_key(self) -> String {
        match self {
            Self::Tenant(tenant_id) => format!("bucket-current/tenant/{tenant_id}"),
            Self::Global => "bucket-current/global".to_string(),
        }
    }

    fn root_key_hash(self) -> String {
        core_meta_root_key_hash(&self.root_anchor_key())
    }
}

fn ensure_bucket_tenant_name_matches(
    bucket: &Bucket,
    tenant_id: i64,
    bucket_name: &str,
) -> Result<()> {
    if bucket.tenant_id != tenant_id || bucket.name != bucket_name {
        return Err(anyhow!(
            "CoreStore bucket current tenant/name row scope mismatch"
        ));
    }
    Ok(())
}

fn ensure_bucket_scope_matches(scope: BucketJournalScope, bucket: &Bucket) -> Result<()> {
    if let BucketJournalScope::Tenant(tenant_id) = scope {
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!("CoreStore bucket current list row scope mismatch"));
        }
    }
    Ok(())
}

pub fn tenant_bucket_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket_metadata").as_bytes())
}

pub fn global_bucket_partition_id() -> Hash32 {
    hash32(b"bucket_metadata/global")
}

#[cfg(test)]
pub(crate) async fn read_bucket_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
) -> Result<(Vec<u64>, Vec<u64>)> {
    let tenant = read_bucket_journal_entries(storage, BucketJournalScope::Tenant(tenant_id))
        .await?
        .into_iter()
        .map(|entry| entry.body.fence_token)
        .collect();
    let global = read_bucket_journal_entries(storage, BucketJournalScope::Global)
        .await?
        .into_iter()
        .map(|entry| entry.body.fence_token)
        .collect();
    Ok((tenant, global))
}

fn require_bucket_scope_permit(
    scope: BucketJournalScope,
    permit: &PartitionWritePermit,
) -> Result<()> {
    if permit.partition_family != "bucket_metadata"
        || permit.partition_id != hex::encode(scope.partition_id())
    {
        return Err(anyhow!(
            "partition write permit does not target this bucket metadata partition"
        ));
    }
    Ok(())
}

fn bucket_key_hash(tenant_id: i64, bucket_name: &str) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_name}").as_bytes())
}

fn tenant_bucket_name_current_tuple_key(tenant_id: i64, bucket_name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("bucket-current-by-name"),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::Hash(&hex::encode(bucket_key_hash(tenant_id, bucket_name))),
        CoreMetaTuplePart::Utf8(bucket_name),
    ])
}

fn tenant_bucket_name_current_tuple_prefix(tenant_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("bucket-current-by-name"),
        CoreMetaTuplePart::I64(tenant_id),
    ])
}

fn global_bucket_id_current_tuple_key(bucket_id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("bucket-current-by-id"),
        CoreMetaTuplePart::I64(bucket_id),
    ])
}

fn global_bucket_id_current_tuple_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("bucket-current-by-id")])
}

fn current_unix_nanos() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_secs()
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(now.subsec_nanos()))
}

fn bucket_event_from_body(sequence: u64, body: BucketJournalBody) -> Result<BucketMetadataEvent> {
    let id = i64::try_from(sequence).context("bucket metadata cursor exceeds i64")?;
    let bucket_created_at =
        chrono::DateTime::parse_from_rfc3339(&body.created_at)?.with_timezone(&chrono::Utc);
    let event_created_at = body
        .emitted_at
        .as_deref()
        .map(chrono::DateTime::parse_from_rfc3339)
        .transpose()?
        .map(|value| value.with_timezone(&chrono::Utc))
        .unwrap_or(bucket_created_at);
    let deleted = body.event == "delete";
    Ok(BucketMetadataEvent {
        id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        event_type: bucket_event_type(&body.event).to_string(),
        mutation_id: uuid::Uuid::parse_str(&body.mutation_id)?,
        bucket_metadata: bucket_metadata_json(&body, deleted),
        created_at: event_created_at,
    })
}

fn bucket_event_type(event: &str) -> &str {
    match event {
        "update" => "policy_update",
        other => other,
    }
}

fn bucket_metadata_json(body: &BucketJournalBody, deleted: bool) -> JsonValue {
    json!({
        "bucket_id": body.bucket_id,
        "name": body.bucket_name,
        "creation_date": body.created_at,
        "region": body.region,
        "is_public_read": body.is_public_read,
        "deleted": deleted,
    })
}

fn encode_bucket_journal_body(body: &BucketJournalBody) -> Result<Vec<u8>> {
    let proto = BucketJournalBodyProto {
        schema: BUCKET_METADATA_BODY_SCHEMA.to_string(),
        event: body.event.clone(),
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        region: body.region.clone(),
        is_public_read: body.is_public_read,
        mutation_id: body.mutation_id.clone(),
        fence_token: body.fence_token,
        created_at: body.created_at.clone(),
        emitted_at: body.emitted_at.clone(),
    };
    encode_deterministic_proto(&proto)
}

fn decode_bucket_journal_body(bytes: &[u8]) -> Result<BucketJournalBody> {
    let proto = BucketJournalBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "bucket metadata body")?;
    if proto.schema != BUCKET_METADATA_BODY_SCHEMA {
        return Err(anyhow!("bucket metadata body has invalid schema"));
    }
    uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("bucket metadata body has invalid mutation id"))?;
    Ok(BucketJournalBody {
        event: proto.event,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        bucket_name: proto.bucket_name,
        region: proto.region,
        is_public_read: proto.is_public_read,
        mutation_id: proto.mutation_id,
        fence_token: proto.fence_token,
        created_at: proto.created_at,
        emitted_at: proto.emitted_at,
    })
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use chrono::Utc;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"bucket metadata partition owner signing key";

    fn bucket(id: i64, name: &str, is_public_read: bool) -> Bucket {
        Bucket {
            id,
            tenant_id: 42,
            name: name.to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read,
        }
    }

    async fn ready_bucket_permit(
        storage: &Storage,
        scope: BucketJournalScope,
        owner_node_id: &str,
    ) -> PartitionWritePermit {
        let request = PartitionRecoveryAcquire {
            partition_family: "bucket_metadata".to_string(),
            partition_id: hex::encode(scope.partition_id()),
            owner_node_id: owner_node_id.to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: 100,
        };
        let recovering = acquire_partition_recovery(storage, request, PARTITION_OWNER_KEY)
            .await
            .unwrap();
        publish_partition_ready(
            storage,
            &recovering.partition_family,
            &recovering.partition_id,
            owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([2; 32]),
            200,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap()
        .write_permit()
        .unwrap()
    }

    async fn write_bucket_current_rows_without_journal(
        storage: &Storage,
        bucket: &Bucket,
        mutation: BucketJournalMutation,
    ) {
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        for scope in [
            BucketJournalScope::Tenant(bucket.tenant_id),
            BucketJournalScope::Global,
        ] {
            let partition_id = hex::encode(scope.partition_id());
            let operations = bucket_current_coremeta_operations(
                scope,
                bucket,
                mutation,
                &partition_id,
                &uuid::Uuid::new_v4().to_string(),
                current_unix_nanos(),
            )
            .unwrap();
            core_store
                .commit_mutation_batch(CoreMutationBatch {
                    transaction_id: format!(
                        "test-bucket-current:{}:{}",
                        scope.stream_id(),
                        uuid::Uuid::new_v4()
                    ),
                    scope_partition: partition_id,
                    committed_by_principal: "test-bucket-current".to_string(),
                    preconditions: Vec::new(),
                    operations,
                })
                .await
                .unwrap();
        }
    }

    async fn read_bucket_journal_payloads_for_test(
        storage: &Storage,
        scope: BucketJournalScope,
    ) -> Result<Vec<BucketJournalBodyProto>> {
        let core_store = CoreStore::new(storage.clone()).await?;
        let records = core_store
            .read_stream(ReadStream {
                stream_id: scope.stream_id(),
                after_sequence: 0,
                limit: 0,
            })
            .await?;
        let mut payloads = Vec::new();
        for record in records {
            if record.record_kind != BUCKET_METADATA_RECORD_KIND {
                continue;
            }
            let proto = BucketJournalBodyProto::decode(record.payload.as_slice())?;
            ensure_deterministic_proto(&proto, &record.payload, "bucket metadata body")?;
            payloads.push(proto);
        }
        Ok(payloads)
    }

    #[tokio::test]
    async fn bucket_journal_recovers_create_update_delete_state() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let private = bucket(1, "private-bucket", false);
        let public = bucket(1, "private-bucket", true);
        let other = bucket(2, "other-bucket", false);

        append_bucket_mutation(&storage, &private, BucketJournalMutation::Create)
            .await
            .unwrap();
        append_bucket_mutation(&storage, &public, BucketJournalMutation::Update)
            .await
            .unwrap();
        append_bucket_mutation(&storage, &other, BucketJournalMutation::Create)
            .await
            .unwrap();
        append_bucket_mutation(&storage, &other, BucketJournalMutation::Delete)
            .await
            .unwrap();

        let buckets = read_current_buckets(&storage, 42).await.unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, "private-bucket");
        assert!(buckets[0].is_public_read);
        assert!(
            read_current_bucket(&storage, 42, "other-bucket")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_current_bucket(&storage, 42, "private-bucket")
                .await
                .unwrap()
                .unwrap()
                .is_public_read
        );
    }

    #[tokio::test]
    async fn bucket_current_rows_are_sufficient_without_bucket_journal_records() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let public = bucket(7, "core-meta-bucket", true);

        write_bucket_current_rows_without_journal(&storage, &public, BucketJournalMutation::Create)
            .await;

        assert!(
            read_bucket_journal_entries(&storage, BucketJournalScope::Tenant(public.tenant_id))
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            read_bucket_journal_entries(&storage, BucketJournalScope::Global)
                .await
                .unwrap()
                .is_empty()
        );

        let buckets = read_current_buckets(&storage, public.tenant_id)
            .await
            .unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].id, public.id);
        assert_eq!(
            read_current_bucket(&storage, public.tenant_id, &public.name)
                .await
                .unwrap()
                .unwrap()
                .id,
            public.id
        );
        assert_eq!(
            read_current_bucket_by_id(&storage, public.id)
                .await
                .unwrap()
                .unwrap()
                .name,
            public.name
        );
        assert_eq!(next_bucket_id(&storage).await.unwrap(), public.id + 1);

        write_bucket_current_rows_without_journal(&storage, &public, BucketJournalMutation::Delete)
            .await;

        assert!(
            read_current_bucket(&storage, public.tenant_id, &public.name)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_current_buckets(&storage, public.tenant_id)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            read_current_bucket_by_id(&storage, public.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(next_bucket_id(&storage).await.unwrap(), public.id + 1);
    }

    #[tokio::test]
    async fn bucket_journal_lists_watch_events_from_native_log() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let private = bucket(1, "watched-bucket", false);
        let public = bucket(1, "watched-bucket", true);
        append_bucket_mutation(&storage, &private, BucketJournalMutation::Create)
            .await
            .unwrap();
        append_bucket_mutation(&storage, &public, BucketJournalMutation::Update)
            .await
            .unwrap();

        let all = list_bucket_metadata_events(&storage, 42, "", 0, 10)
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].event_type, "create");
        assert_eq!(all[1].event_type, "policy_update");
        assert!(all[1].bucket_metadata["is_public_read"].as_bool().unwrap());

        let after_first = list_bucket_metadata_events(&storage, 42, "", 1, 10)
            .await
            .unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].id, 2);

        let latest = latest_bucket_metadata_event(&storage, 42, "watched-bucket")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.id, 2);
        assert_eq!(latest.bucket_name, "watched-bucket");
    }

    #[tokio::test]
    async fn bucket_journal_permits_set_tenant_and_global_payload_fences() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = bucket(1, "fenced-bucket", false);
        let tenant_permit = ready_bucket_permit(
            &storage,
            BucketJournalScope::Tenant(bucket.tenant_id),
            "node-a",
        )
        .await;
        let global_permit =
            ready_bucket_permit(&storage, BucketJournalScope::Global, "node-a").await;

        append_bucket_mutation_with_permits(
            &storage,
            &bucket,
            BucketJournalMutation::Create,
            &tenant_permit,
            &global_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();

        let tenant_payloads = read_bucket_journal_payloads_for_test(
            &storage,
            BucketJournalScope::Tenant(bucket.tenant_id),
        )
        .await
        .unwrap();
        assert_eq!(tenant_payloads.len(), 1);
        assert_eq!(tenant_payloads[0].fence_token, tenant_permit.fence_token);
        assert!(uuid::Uuid::parse_str(&tenant_payloads[0].mutation_id).is_ok());

        let global_payloads =
            read_bucket_journal_payloads_for_test(&storage, BucketJournalScope::Global)
                .await
                .unwrap();
        assert_eq!(global_payloads.len(), 1);
        assert_eq!(global_payloads[0].fence_token, global_permit.fence_token);
        assert!(uuid::Uuid::parse_str(&global_payloads[0].mutation_id).is_ok());

        let global_records = CoreStore::new(storage.clone())
            .await
            .unwrap()
            .read_stream(ReadStream {
                stream_id: (BucketJournalScope::Global).stream_id(),
                after_sequence: 0,
                limit: 0,
            })
            .await
            .unwrap();
        assert_eq!(
            global_records[0].payload,
            global_payloads[0].encode_to_vec()
        );
    }

    #[tokio::test]
    async fn bucket_journal_rejects_stale_scope_permit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = bucket(1, "stale-bucket", false);
        let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
        let stale_tenant = ready_bucket_permit(&storage, tenant_scope, "node-a").await;
        let fresh_tenant = ready_bucket_permit(&storage, tenant_scope, "node-b").await;
        let global_permit =
            ready_bucket_permit(&storage, BucketJournalScope::Global, "node-b").await;
        assert_eq!(fresh_tenant.fence_token, stale_tenant.fence_token + 1);

        let rejected = append_bucket_mutation_with_permits(
            &storage,
            &bucket,
            BucketJournalMutation::Create,
            &stale_tenant,
            &global_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(rejected.to_string().contains("PartitionNotOwned"));

        append_bucket_mutation_with_permits(
            &storage,
            &bucket,
            BucketJournalMutation::Create,
            &fresh_tenant,
            &global_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn bucket_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = bucket(1, "stale-precondition-bucket", false);
        let tenant_scope = BucketJournalScope::Tenant(bucket.tenant_id);
        let stale_tenant = ready_bucket_permit(&storage, tenant_scope, "node-a").await;
        let stale_precondition =
            partition_write_precondition(&storage, &stale_tenant, PARTITION_OWNER_KEY)
                .await
                .unwrap();
        let fresh_tenant = ready_bucket_permit(&storage, tenant_scope, "node-b").await;
        assert_eq!(fresh_tenant.fence_token, stale_tenant.fence_token + 1);

        let rejected = append_bucket_mutation_to_stream(
            &storage,
            &bucket,
            BucketJournalMutation::Create,
            tenant_scope,
            stale_tenant.fence_token,
            Some(stale_precondition),
        )
        .await
        .unwrap_err();
        assert!(
            rejected.to_string().contains("target mismatch")
                || rejected.to_string().contains("generation mismatch"),
            "unexpected error: {rejected:?}"
        );
    }
}
