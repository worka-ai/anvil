use super::{
    OwnershipFenceRecord, OwnershipResource, PartitionOwnerState, decode_ownership_fence_record,
    decode_partition_owner_record, encode_ownership_fence_record, encode_partition_owner_record,
    ownership_resource_hash, require_nonempty, validate_hex32, validate_ownership_resource,
};
use crate::{
    core_store::{
        CF_LEASES_FENCES, CoreMetaRecord, CoreMetaStore, CoreMetaTuplePart, CoreMutationBatch,
        CoreMutationOperation, CoreMutationPrecondition, CoreStore, TABLE_OWNERSHIP_FENCE_ROW,
        TABLE_PARTITION_OWNER_ROW, core_meta_payload_digest, core_meta_record_tuple_key,
        core_meta_tuple_key, is_retryable_mutation_conflict,
    },
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use serde::{Deserialize, Serialize};

const PARTITION_OWNER_ROW_PREFIX: &str = "partition_owner";
const OWNERSHIP_FENCE_ROW_PREFIX: &str = "ownership_fence";

/// Maximum number of ordered CoreMeta rows inspected by one fence page request.
pub const MAX_PARTITION_FENCE_PAGE_SIZE: usize = 256;

/// Exclusive continuation position for an ordered partition-owner page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct PartitionOwnerPageCursor(String);

impl PartitionOwnerPageCursor {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A bounded page of partition owners in CoreMeta tuple-key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionOwnerPage {
    pub owners: Vec<PartitionOwnerState>,
    pub next_cursor: Option<PartitionOwnerPageCursor>,
}

/// Exclusive continuation position for an ordered ownership-fence page.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct OwnershipFencePageCursor(String);

impl OwnershipFencePageCursor {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A bounded page of ownership fences in CoreMeta tuple-key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipFencePage {
    pub fences: Vec<OwnershipFenceRecord>,
    pub next_cursor: Option<OwnershipFencePageCursor>,
}

/// Lists at most `limit` partition-owner rows after `cursor`.
pub async fn list_partition_owners_page(
    storage: &Storage,
    cursor: Option<&PartitionOwnerPageCursor>,
    limit: usize,
    signing_key: &[u8],
) -> Result<PartitionOwnerPage> {
    partition_owner_page(storage, cursor, limit, signing_key, |_| true)
}

/// Lists one bounded source page, retaining owners assigned to `owner_node_id`.
///
/// The limit bounds inspected CoreMeta rows, so a filtered page can contain fewer
/// owners than `limit` while still returning a continuation cursor.
pub async fn list_partition_owners_for_node_page(
    storage: &Storage,
    owner_node_id: &str,
    cursor: Option<&PartitionOwnerPageCursor>,
    limit: usize,
    signing_key: &[u8],
) -> Result<PartitionOwnerPage> {
    require_nonempty(owner_node_id, "owner node id")?;
    partition_owner_page(storage, cursor, limit, signing_key, |owner| {
        owner.owner_node_id == owner_node_id
    })
}

/// Lists at most `limit` ownership-fence rows after `cursor`.
pub async fn list_ownership_fences_page(
    storage: &Storage,
    cursor: Option<&OwnershipFencePageCursor>,
    limit: usize,
    signing_key: &[u8],
) -> Result<OwnershipFencePage> {
    ownership_fence_page(storage, cursor, limit, signing_key, |_| true)
}

/// Lists one bounded source page, retaining active node-owned fences.
///
/// The limit bounds inspected CoreMeta rows, so a filtered page can contain fewer
/// fences than `limit` while still returning a continuation cursor.
pub async fn list_active_ownership_fences_for_node_page(
    storage: &Storage,
    owner_node_id: &str,
    now_nanos: i64,
    cursor: Option<&OwnershipFencePageCursor>,
    limit: usize,
    signing_key: &[u8],
) -> Result<OwnershipFencePage> {
    require_nonempty(owner_node_id, "owner node id")?;
    if now_nanos < 0 {
        return Err(anyhow!("ownership fence timestamp must be nonnegative"));
    }
    ownership_fence_page(storage, cursor, limit, signing_key, |record| {
        record.owner.principal_kind == "node"
            && record.owner.actor_instance_id == owner_node_id
            && record.is_active_unexpired(now_nanos)
    })
}

pub(super) async fn read_ownership_fence_state(
    storage: &Storage,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<Option<(Vec<u8>, OwnershipFenceRecord)>> {
    let row_key = ownership_fence_row_key(tenant_id, resource)?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    record_point_read();
    let Some(bytes) = meta.get(CF_LEASES_FENCES, TABLE_OWNERSHIP_FENCE_ROW, &row_key)? else {
        return Ok(None);
    };
    let record = decode_ownership_fence_record(&bytes)?;
    record.verify(signing_key)?;
    if record.owner.tenant_id != tenant_id || record.resource != *resource {
        return Err(anyhow!("ownership fence row scope mismatch"));
    }
    Ok(Some((bytes, record)))
}

pub(super) async fn write_ownership_fence_state(
    storage: &Storage,
    record: &OwnershipFenceRecord,
    expected_ref: Option<&Vec<u8>>,
) -> Result<()> {
    let row_key = ownership_fence_row_key(record.owner.tenant_id, &record.resource)?;
    let payload = encode_ownership_fence_record(record)?;
    let scope_partition = ownership_resource_hash(record.owner.tenant_id, &record.resource)?;
    commit_point_put(
        storage,
        TABLE_OWNERSHIP_FENCE_ROW,
        row_key,
        expected_ref.map(Vec::as_slice),
        payload,
        scope_partition,
        "ownership-fence-cas",
    )
    .await
}

pub(super) async fn read_partition_owner_state(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    signing_key: &[u8],
) -> Result<Option<(Vec<u8>, PartitionOwnerState)>> {
    let row_key = partition_owner_row_key(partition_family, partition_id)?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    record_point_read();
    let Some(bytes) = meta.get(CF_LEASES_FENCES, TABLE_PARTITION_OWNER_ROW, &row_key)? else {
        return Ok(None);
    };
    let owner = decode_partition_owner_record(&bytes)?;
    owner.verify(signing_key)?;
    if owner.partition_family != partition_family || owner.partition_id != partition_id {
        return Err(anyhow!("partition owner row scope mismatch"));
    }
    Ok(Some((bytes, owner)))
}

pub(super) async fn write_partition_owner_state(
    storage: &Storage,
    owner: &PartitionOwnerState,
    expected_ref: Option<&Vec<u8>>,
) -> Result<()> {
    let row_key = partition_owner_row_key(&owner.partition_family, &owner.partition_id)?;
    let payload = encode_partition_owner_record(owner)?;
    commit_point_put(
        storage,
        TABLE_PARTITION_OWNER_ROW,
        row_key,
        expected_ref.map(Vec::as_slice),
        payload,
        owner.partition_id.clone(),
        "partition-owner-cas",
    )
    .await
}

pub(super) fn is_partition_fence_cas_conflict(error: &anyhow::Error) -> bool {
    is_retryable_mutation_conflict(error)
}

pub(super) fn ownership_fence_row_key(
    tenant_id: i64,
    resource: &OwnershipResource,
) -> Result<Vec<u8>> {
    validate_ownership_resource(resource)?;
    if tenant_id < 0 {
        return Err(anyhow!("ownership fence tenant id must be nonnegative"));
    }
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(OWNERSHIP_FENCE_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(&format!("tenant:{tenant_id}")),
        CoreMetaTuplePart::Utf8(resource.resource_kind.as_str()),
        CoreMetaTuplePart::Hash(&format!(
            "blake3:{}",
            ownership_resource_hash(tenant_id, resource)?
        )),
    ])
}

pub(super) fn partition_owner_row_key(
    partition_family: &str,
    partition_id: &str,
) -> Result<Vec<u8>> {
    require_nonempty(partition_family, "partition family")?;
    if partition_family.contains('\0')
        || partition_family.contains("..")
        || partition_family.contains(':')
        || partition_family.chars().any(char::is_control)
    {
        return Err(anyhow!("partition family contains an invalid component"));
    }
    validate_hex32(partition_id, "partition id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(PARTITION_OWNER_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(partition_family),
        CoreMetaTuplePart::Hash(&format!("blake3:{partition_id}")),
    ])
}

async fn commit_point_put(
    storage: &Storage,
    table_id: u16,
    tuple_key: Vec<u8>,
    expected_payload: Option<&[u8]>,
    payload: Vec<u8>,
    scope_partition: String,
    transaction_prefix: &str,
) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    wait_at_point_cas_barrier().await;
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("{transaction_prefix}:{}", uuid::Uuid::new_v4()),
            scope_partition: scope_partition.clone(),
            committed_by_principal: "partition-fence".to_string(),
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: CF_LEASES_FENCES.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash: expected_payload
                    .map(|payload| core_meta_payload_digest(table_id, payload)),
                require_absent: expected_payload.is_none(),
                require_present: expected_payload.is_some(),
            }],
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id: scope_partition,
                cf: CF_LEASES_FENCES.to_string(),
                table_id,
                tuple_key,
                payload,
            }],
        })
        .await?;
    Ok(())
}

fn partition_owner_page(
    storage: &Storage,
    cursor: Option<&PartitionOwnerPageCursor>,
    limit: usize,
    signing_key: &[u8],
    include: impl Fn(&PartitionOwnerState) -> bool,
) -> Result<PartitionOwnerPage> {
    let prefix = partition_owner_row_prefix()?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let (rows, has_more) = scan_page(
        &meta,
        TABLE_PARTITION_OWNER_ROW,
        &prefix,
        cursor.map(PartitionOwnerPageCursor::as_str),
        limit,
    )?;
    let next_cursor = if has_more {
        Some(PartitionOwnerPageCursor(cursor_for_last_row(&rows)?))
    } else {
        None
    };
    let mut owners = Vec::new();
    for row in rows {
        let owner = decode_partition_owner_record(&row.payload)?;
        owner.verify(signing_key)?;
        let tuple_key = core_meta_record_tuple_key(&row.key)?;
        let expected_key = partition_owner_row_key(&owner.partition_family, &owner.partition_id)?;
        if tuple_key != expected_key.as_slice() {
            bail!("partition owner page row key does not match its payload");
        }
        if include(&owner) {
            owners.push(owner);
        }
    }
    Ok(PartitionOwnerPage {
        owners,
        next_cursor,
    })
}

fn ownership_fence_page(
    storage: &Storage,
    cursor: Option<&OwnershipFencePageCursor>,
    limit: usize,
    signing_key: &[u8],
    include: impl Fn(&OwnershipFenceRecord) -> bool,
) -> Result<OwnershipFencePage> {
    let prefix = ownership_fence_row_prefix()?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let (rows, has_more) = scan_page(
        &meta,
        TABLE_OWNERSHIP_FENCE_ROW,
        &prefix,
        cursor.map(OwnershipFencePageCursor::as_str),
        limit,
    )?;
    let next_cursor = if has_more {
        Some(OwnershipFencePageCursor(cursor_for_last_row(&rows)?))
    } else {
        None
    };
    let mut fences = Vec::new();
    for row in rows {
        let record = decode_ownership_fence_record(&row.payload)?;
        record.verify(signing_key)?;
        let tuple_key = core_meta_record_tuple_key(&row.key)?;
        let expected_key = ownership_fence_row_key(record.owner.tenant_id, &record.resource)?;
        if tuple_key != expected_key.as_slice() {
            bail!("ownership fence page row key does not match its payload");
        }
        if include(&record) {
            fences.push(record);
        }
    }
    Ok(OwnershipFencePage {
        fences,
        next_cursor,
    })
}

fn scan_page(
    meta: &CoreMetaStore,
    table_id: u16,
    prefix: &[u8],
    cursor: Option<&str>,
    limit: usize,
) -> Result<(Vec<CoreMetaRecord>, bool)> {
    validate_page_limit(limit)?;
    let after = cursor
        .map(|cursor| decode_cursor(cursor, prefix))
        .transpose()?;
    let mut rows = meta.scan_prefix_page(
        CF_LEASES_FENCES,
        table_id,
        prefix,
        after.as_deref(),
        limit + 1,
    )?;
    let has_more = rows.len() > limit;
    if has_more {
        rows.pop();
    }
    Ok((rows, has_more))
}

fn validate_page_limit(limit: usize) -> Result<()> {
    if !(1..=MAX_PARTITION_FENCE_PAGE_SIZE).contains(&limit) {
        bail!("partition fence page limit must be between 1 and {MAX_PARTITION_FENCE_PAGE_SIZE}");
    }
    Ok(())
}

fn decode_cursor(cursor: &str, prefix: &[u8]) -> Result<Vec<u8>> {
    let tuple_key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .context("partition fence page cursor is not valid base64url")?;
    if tuple_key.len() <= prefix.len() || !tuple_key.starts_with(prefix) {
        bail!("partition fence page cursor is outside the requested key prefix");
    }
    Ok(tuple_key)
}

fn cursor_for_last_row(rows: &[CoreMetaRecord]) -> Result<String> {
    let row = rows
        .last()
        .ok_or_else(|| anyhow!("partition fence continuation page has no final row"))?;
    let tuple_key = core_meta_record_tuple_key(&row.key)?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(tuple_key))
}

fn partition_owner_row_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(PARTITION_OWNER_ROW_PREFIX)])
}

fn ownership_fence_row_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(OWNERSHIP_FENCE_ROW_PREFIX)])
}

#[cfg(test)]
struct PointCasTestGate {
    barrier: std::sync::Arc<tokio::sync::Barrier>,
    entered: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
tokio::task_local! {
    static POINT_CAS_TEST_GATE: std::sync::Arc<PointCasTestGate>;
}

#[cfg(test)]
tokio::task_local! {
    static POINT_READ_COUNT: std::cell::Cell<usize>;
}

#[cfg(test)]
pub(super) async fn with_point_cas_barrier<F>(
    barrier: std::sync::Arc<tokio::sync::Barrier>,
    future: F,
) -> F::Output
where
    F: std::future::Future,
{
    let gate = std::sync::Arc::new(PointCasTestGate {
        barrier,
        entered: std::sync::atomic::AtomicBool::new(false),
    });
    POINT_CAS_TEST_GATE.scope(gate, future).await
}

#[cfg(test)]
pub(super) async fn count_point_reads<F>(future: F) -> (F::Output, usize)
where
    F: std::future::Future,
{
    POINT_READ_COUNT
        .scope(std::cell::Cell::new(0), async move {
            let output = future.await;
            let count = POINT_READ_COUNT.with(|count| count.get());
            (output, count)
        })
        .await
}

#[cfg(test)]
async fn wait_at_point_cas_barrier() {
    use std::sync::atomic::Ordering;

    let gate = POINT_CAS_TEST_GATE.try_with(|gate| gate.clone()).ok();
    if let Some(gate) = gate
        && !gate.entered.swap(true, Ordering::SeqCst)
    {
        gate.barrier.wait().await;
    }
}

#[cfg(not(test))]
async fn wait_at_point_cas_barrier() {}

fn record_point_read() {
    #[cfg(test)]
    let _ = POINT_READ_COUNT.try_with(|count| count.set(count.get().saturating_add(1)));
}
