use crate::core_store::{
    CF_OBJECT_HEADS, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, CoreMetaTuplePart,
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, CoreTransaction,
    CoreTransactionUpdate, TABLE_MANIFEST_CAS_CURRENT_ROW, commit_coremeta_batch_for_storage,
    core_meta_committed_row_common, core_meta_payload_digest, core_meta_root_key_hash,
    core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{ManifestCasResult, MetadataMutationReceipt};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

const MANIFEST_CAS_BODY_SCHEMA: &str = "anvil.core.manifest_cas.body.v1";
const MANIFEST_CAS_CURRENT_ROW_SCHEMA: &str = "anvil.core.manifest_cas.current_row.v1";
const MANIFEST_CAS_CURRENT_ROW_KEY_PREFIX: &str = "manifest_cas_current";
const MANIFEST_CAS_CURRENT_ROW_MAX_PROTO_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestBody {
    tenant_id: i64,
    bucket_id: i64,
    object_key: String,
    revision: i64,
    manifest_hash: String,
    manifest: JsonValue,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, PartialEq, Message)]
struct ManifestBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    object_key: String,
    #[prost(int64, tag = "5")]
    revision: i64,
    #[prost(string, tag = "6")]
    manifest_hash: String,
    #[prost(bytes = "vec", tag = "7")]
    manifest_json: Vec<u8>,
    #[prost(string, tag = "8")]
    updated_at: String,
    #[prost(uint64, tag = "9")]
    fence_token: u64,
    #[prost(string, tag = "10")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct ManifestCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    object_key: String,
    #[prost(int64, tag = "6")]
    revision: i64,
    #[prost(string, tag = "7")]
    manifest_hash: String,
    #[prost(string, tag = "8")]
    updated_at: String,
}

#[derive(Debug, Clone)]
struct ManifestCurrentRow {
    tenant_id: i64,
    bucket_id: i64,
    object_key: String,
    revision: i64,
    manifest_hash: String,
    updated_at: DateTime<Utc>,
    transaction_id: String,
    created_at_unix_nanos: u64,
}

#[cfg(test)]
async fn compare_and_swap_manifest(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
) -> Result<Option<ManifestCasResult>> {
    compare_and_swap_manifest_inner(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        0,
        None,
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compare_and_swap_manifest_with_permit(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<Option<ManifestCasResult>> {
    require_manifest_cas_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    compare_and_swap_manifest_inner(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        permit.fence_token,
        Some(partition_precondition),
        None,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn compare_and_swap_manifest_with_permit_in_transaction(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
    transaction_id: &str,
    transaction_principal: &str,
) -> Result<Option<ManifestCasResult>> {
    require_manifest_cas_permit(tenant_id, bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    compare_and_swap_manifest_inner(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        expected_revision,
        manifest,
        manifest_hash,
        permit.fence_token,
        Some(partition_precondition),
        Some(transaction_id),
        Some(transaction_principal),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn compare_and_swap_manifest_inner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    expected_revision: i64,
    manifest: JsonValue,
    manifest_hash: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<Option<ManifestCasResult>> {
    let current = current_revision(
        storage,
        tenant_id,
        bucket_id,
        object_key,
        transaction_id.zip(transaction_principal),
    )
    .await?;
    if expected_revision != current {
        return Ok(None);
    }
    let revision = current
        .checked_add(1)
        .ok_or_else(|| anyhow!("manifest revision overflow"))?;
    let receipt = append_manifest(
        storage,
        ManifestBody {
            tenant_id,
            bucket_id,
            object_key: object_key.to_string(),
            revision,
            manifest_hash: manifest_hash.to_string(),
            manifest,
            updated_at: Utc::now(),
        },
        fence_token,
        partition_precondition,
        transaction_id,
        transaction_principal,
    )
    .await?;
    Ok(Some(ManifestCasResult {
        revision,
        manifest_hash: manifest_hash.to_string(),
        receipt,
    }))
}

async fn current_revision(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    transaction: Option<(&str, &str)>,
) -> Result<i64> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let payload = manifest_current_payload_for_optional_transaction(
        storage,
        &meta,
        tenant_id,
        bucket_id,
        object_key,
        transaction,
    )
    .await?;
    Ok(payload
        .map(|payload| decode_manifest_current_row(&payload))
        .transpose()?
        .map(|row| row.revision)
        .unwrap_or(0))
}

async fn append_manifest(
    storage: &Storage,
    body: ManifestBody,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
    transaction_id: Option<&str>,
    transaction_principal: Option<&str>,
) -> Result<MetadataMutationReceipt> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let stream_id = manifest_cas_stream_id(body.tenant_id, body.bucket_id);
    let mutation_id = uuid::Uuid::new_v4();
    let staged_transaction = transaction_id.is_some();
    let transaction_id = transaction_id.map(ToOwned::to_owned).unwrap_or_else(|| {
        format!(
            "manifest-cas:{}:{}:{mutation_id}",
            body.tenant_id, body.bucket_id
        )
    });
    let body_bytes = encode_manifest_body(&body, fence_token, mutation_id)?;
    let payload_hash = hex::encode(hash32(&body_bytes));
    let partition_id = hex::encode(manifest_cas_partition_id(body.tenant_id, body.bucket_id));
    let current_payload = manifest_current_payload_for_optional_transaction(
        storage,
        &meta,
        body.tenant_id,
        body.bucket_id,
        &body.object_key,
        Some(transaction_id.as_str()).zip(transaction_principal),
    )
    .await?;
    let current_update =
        manifest_current_row_update_from_payload(&body, &transaction_id, current_payload)?;
    let current_payload = encode_manifest_current_row(&current_update.row)?;
    let mut preconditions: Vec<_> = partition_precondition.into_iter().collect();
    preconditions.push(current_update.precondition.clone());
    let batch = CoreMutationBatch {
        transaction_id: transaction_id.clone(),
        scope_partition: partition_id.clone(),
        committed_by_principal: transaction_principal
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| manifest_cas_partition_principal(body.tenant_id, body.bucket_id)),
        preconditions,
        operations: vec![
            CoreMutationOperation::StreamAppend {
                partition_id: partition_id.clone(),
                stream_id,
                record_kind: "manifest_cas".to_string(),
                payload: body_bytes,
                idempotency_key: Some(format!(
                    "manifest-cas:{}:{}:{mutation_id}",
                    body.tenant_id, body.bucket_id
                )),
            },
            CoreMutationOperation::CoreMetaPut {
                partition_id,
                cf: CF_OBJECT_HEADS.to_string(),
                table_id: TABLE_MANIFEST_CAS_CURRENT_ROW,
                tuple_key: manifest_current_row_key(
                    current_update.row.tenant_id,
                    current_update.row.bucket_id,
                    &current_update.row.object_key,
                )?,
                payload: current_payload,
            },
        ],
    };
    let batch_receipt = if staged_transaction {
        core_store.stage_explicit_transaction_batch(batch).await?
    } else {
        core_store.commit_mutation_batch(batch).await?
    };
    let stream_update = batch_receipt
        .visible_updates
        .iter()
        .find_map(|update| match update {
            CoreTransactionUpdate::StreamAppend {
                visible_sequence,
                prepared_record_hash,
                ..
            } => Some((*visible_sequence, prepared_record_hash.clone())),
            CoreTransactionUpdate::CoreMetaPut { .. }
            | CoreTransactionUpdate::CoreMetaDelete { .. } => None,
        })
        .ok_or_else(|| anyhow!("manifest CAS batch did not append a stream record"))?;
    let receipt = MetadataMutationReceipt {
        mutation_id,
        payload_hash,
        record_hash: stream_update.1,
        watch_cursor: stream_update.0,
    };
    Ok(receipt)
}

#[cfg(test)]
async fn read_manifest_bodies(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<ManifestBody>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = core_store
        .read_stream(crate::core_store::ReadStream {
            stream_id: manifest_cas_stream_id(tenant_id, bucket_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    records
        .into_iter()
        .filter(|record| record.record_kind == "manifest_cas")
        .map(|record| decode_manifest_body(&record.payload))
        .collect()
}

pub async fn materialize_committed_manifest_cas_transaction(
    storage: &Storage,
    transaction: &CoreTransaction,
) -> Result<usize> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let mut materialized = 0usize;
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
        } = update
        else {
            continue;
        };
        let Some((tenant_id, bucket_id)) = parse_manifest_cas_stream_id(stream_id) else {
            continue;
        };
        let records = core_store
            .read_stream(crate::core_store::ReadStream {
                stream_id: stream_id.clone(),
                after_sequence: visible_sequence.saturating_sub(1),
                limit: 1,
            })
            .await?;
        let Some(record) = records.into_iter().find(|record| {
            record.sequence == *visible_sequence && &record.event_hash == prepared_record_hash
        }) else {
            return Err(anyhow!(
                "manifest CAS transaction {} committed stream record {stream_id}:{visible_sequence} is not readable",
                transaction.transaction_id
            ));
        };
        if record.record_kind != "manifest_cas" {
            continue;
        }
        let body = decode_manifest_body(&record.payload)?;
        if body.tenant_id != tenant_id || body.bucket_id != bucket_id {
            return Err(anyhow!(
                "manifest CAS transaction {} stream scope does not match payload",
                transaction.transaction_id
            ));
        }
        if let Some(current) =
            read_manifest_current_row(&meta, tenant_id, bucket_id, &body.object_key)?
            && current.revision == body.revision
            && current.manifest_hash == body.manifest_hash
        {
            materialized += 1;
            continue;
        }
        let row_update = manifest_current_row_update(&meta, &body, &transaction.transaction_id)?;
        write_manifest_current_row(storage, &meta, &row_update.row, &row_update.precondition)
            .await?;
        materialized += 1;
    }
    Ok(materialized)
}

pub fn manifest_cas_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas").as_bytes())
}

fn manifest_cas_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("manifest_cas:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn parse_manifest_cas_stream_id(stream_id: &str) -> Option<(i64, i64)> {
    let rest = stream_id.strip_prefix("manifest_cas:tenant:")?;
    let (tenant, bucket_part) = rest.split_once(":bucket:")?;
    Some((tenant.parse().ok()?, bucket_part.parse().ok()?))
}

fn manifest_cas_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:manifest_cas:{tenant_id}:{bucket_id}")
}

#[cfg(test)]
pub(crate) async fn read_manifest_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(crate::core_store::ReadStream {
            stream_id: manifest_cas_stream_id(tenant_id, bucket_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?
        .into_iter()
        .filter(|record| record.record_kind == "manifest_cas")
        .map(|record| decode_manifest_body_fence(&record.payload))
        .collect::<Result<Vec<_>>>()?)
}

fn encode_manifest_body(
    body: &ManifestBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let proto = ManifestBodyProto {
        schema: MANIFEST_CAS_BODY_SCHEMA.to_string(),
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        object_key: body.object_key.clone(),
        revision: body.revision,
        manifest_hash: body.manifest_hash.clone(),
        manifest_json: canonical_json_bytes(&body.manifest)?,
        updated_at: body.updated_at.to_rfc3339(),
        fence_token,
        mutation_id: mutation_id.to_string(),
    };
    encode_deterministic_proto(&proto)
}

fn decode_manifest_body(bytes: &[u8]) -> Result<ManifestBody> {
    let proto = ManifestBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "manifest CAS body")?;
    if proto.schema != MANIFEST_CAS_BODY_SCHEMA {
        return Err(anyhow!("manifest CAS body schema mismatch"));
    }
    Ok(ManifestBody {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        object_key: proto.object_key,
        revision: proto.revision,
        manifest_hash: proto.manifest_hash,
        manifest: decode_canonical_json(&proto.manifest_json, "manifest CAS body manifest")?,
        updated_at: DateTime::parse_from_rfc3339(&proto.updated_at)?.with_timezone(&Utc),
    })
}

#[cfg(test)]
fn decode_manifest_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = ManifestBodyProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "manifest CAS body")?;
    if proto.schema != MANIFEST_CAS_BODY_SCHEMA {
        return Err(anyhow!("manifest CAS body schema mismatch"));
    }
    Ok(proto.fence_token)
}

fn encode_deterministic_proto(message: &impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

#[derive(Debug, Clone)]
struct ManifestCurrentRowUpdate {
    precondition: CoreMutationPrecondition,
    row: ManifestCurrentRow,
}

fn manifest_current_row_update(
    meta: &CoreMetaStore,
    body: &ManifestBody,
    transaction_id: &str,
) -> Result<ManifestCurrentRowUpdate> {
    let current_payload =
        read_manifest_current_payload(meta, body.tenant_id, body.bucket_id, &body.object_key)?;
    manifest_current_row_update_from_payload(body, transaction_id, current_payload)
}

fn manifest_current_row_update_from_payload(
    body: &ManifestBody,
    transaction_id: &str,
    current_payload: Option<Vec<u8>>,
) -> Result<ManifestCurrentRowUpdate> {
    let current = current_payload
        .as_deref()
        .map(decode_manifest_current_row)
        .transpose()?;
    let expected_previous = body.revision.saturating_sub(1);
    if current.as_ref().map(|row| row.revision).unwrap_or(0) != expected_previous {
        return Err(anyhow!("manifest CAS current row revision mismatch"));
    }
    let key = manifest_current_row_key(body.tenant_id, body.bucket_id, &body.object_key)?;
    Ok(ManifestCurrentRowUpdate {
        precondition: CoreMutationPrecondition::CoreMetaRow {
            cf: CF_OBJECT_HEADS.to_string(),
            table_id: TABLE_MANIFEST_CAS_CURRENT_ROW,
            tuple_key: key,
            expected_payload_hash: current_payload
                .as_ref()
                .map(|payload| core_meta_payload_digest(TABLE_MANIFEST_CAS_CURRENT_ROW, payload)),
            require_absent: current_payload.is_none(),
            require_present: current_payload.is_some(),
        },
        row: ManifestCurrentRow {
            tenant_id: body.tenant_id,
            bucket_id: body.bucket_id,
            object_key: body.object_key.clone(),
            revision: body.revision,
            manifest_hash: body.manifest_hash.clone(),
            updated_at: body.updated_at,
            transaction_id: transaction_id.to_string(),
            created_at_unix_nanos: current_unix_nanos()?,
        },
    })
}

async fn manifest_current_payload_for_optional_transaction(
    storage: &Storage,
    meta: &CoreMetaStore,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
    transaction: Option<(&str, &str)>,
) -> Result<Option<Vec<u8>>> {
    let key = manifest_current_row_key(tenant_id, bucket_id, object_key)?;
    let mut current = meta.get(CF_OBJECT_HEADS, TABLE_MANIFEST_CAS_CURRENT_ROW, &key)?;
    let Some((transaction_id, transaction_principal)) = transaction else {
        return Ok(current);
    };
    let transaction = CoreStore::new(storage.clone())
        .await?
        .read_explicit_transaction_for_principal(transaction_id, transaction_principal)
        .await?;
    for update in transaction.visible_updates {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } if cf == CF_OBJECT_HEADS
                && table_id == TABLE_MANIFEST_CAS_CURRENT_ROW
                && tuple_key == key =>
            {
                current = Some(payload)
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } if cf == CF_OBJECT_HEADS
                && table_id == TABLE_MANIFEST_CAS_CURRENT_ROW
                && tuple_key == key =>
            {
                current = None
            }
            _ => {}
        }
    }
    Ok(current)
}

fn read_manifest_current_payload(
    meta: &CoreMetaStore,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
) -> Result<Option<Vec<u8>>> {
    meta.get(
        CF_OBJECT_HEADS,
        TABLE_MANIFEST_CAS_CURRENT_ROW,
        &manifest_current_row_key(tenant_id, bucket_id, object_key)?,
    )
}

fn read_manifest_current_row(
    meta: &CoreMetaStore,
    tenant_id: i64,
    bucket_id: i64,
    object_key: &str,
) -> Result<Option<ManifestCurrentRow>> {
    let Some(payload) = read_manifest_current_payload(meta, tenant_id, bucket_id, object_key)?
    else {
        return Ok(None);
    };
    let row = decode_manifest_current_row(&payload)?;
    if row.tenant_id != tenant_id || row.bucket_id != bucket_id || row.object_key != object_key {
        return Err(anyhow!("manifest CAS current CoreMeta row scope mismatch"));
    }
    Ok(Some(row))
}

async fn write_manifest_current_row(
    storage: &Storage,
    meta: &CoreMetaStore,
    row: &ManifestCurrentRow,
    precondition: &CoreMutationPrecondition,
) -> Result<()> {
    validate_manifest_current_precondition(meta, precondition)?;
    let key = manifest_current_row_key(row.tenant_id, row.bucket_id, &row.object_key)?;
    let payload = encode_manifest_current_row(row)?;
    let op = CoreMetaBatchOp {
        cf: CF_OBJECT_HEADS,
        table_id: TABLE_MANIFEST_CAS_CURRENT_ROW,
        tuple_key: &key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!(
            "manifest-current:{}:{}:{}",
            row.tenant_id, row.bucket_id, row.revision
        ),
        &[op],
    )
    .await?;
    Ok(())
}

fn validate_manifest_current_precondition(
    meta: &CoreMetaStore,
    precondition: &CoreMutationPrecondition,
) -> Result<()> {
    let CoreMutationPrecondition::CoreMetaRow {
        cf,
        table_id,
        tuple_key,
        expected_payload_hash,
        require_absent,
        require_present,
    } = precondition
    else {
        return Err(anyhow!(
            "manifest current writer received unsupported precondition"
        ));
    };
    let current = meta.get_named(cf, *table_id, tuple_key)?;
    if *require_absent && current.is_some() {
        return Err(anyhow!("manifest current CoreMeta row must be absent"));
    }
    if *require_present && current.is_none() {
        return Err(anyhow!("manifest current CoreMeta row must be present"));
    }
    if let (Some(expected), Some(current)) = (expected_payload_hash.as_ref(), current.as_ref()) {
        let actual = core_meta_payload_digest(*table_id, current);
        if actual != *expected {
            return Err(anyhow!(
                "manifest current CoreMeta row payload hash mismatch"
            ));
        }
    }
    Ok(())
}

fn encode_manifest_current_row(row: &ManifestCurrentRow) -> Result<Vec<u8>> {
    let revision = u64::try_from(row.revision)
        .map_err(|_| anyhow!("manifest current CoreMeta row revision is negative"))?;
    let proto = ManifestCurrentRowProto {
        schema: MANIFEST_CAS_CURRENT_ROW_SCHEMA.to_string(),
        common: Some(core_meta_committed_row_common(
            manifest_cas_realm_id(row.tenant_id),
            core_meta_root_key_hash(&manifest_cas_current_root_key(row.tenant_id, row.bucket_id)),
            revision,
            &row.transaction_id,
            row.created_at_unix_nanos,
        )),
        tenant_id: row.tenant_id,
        bucket_id: row.bucket_id,
        object_key: row.object_key.clone(),
        revision: row.revision,
        manifest_hash: row.manifest_hash.clone(),
        updated_at: row.updated_at.to_rfc3339(),
    };
    let bytes = encode_deterministic_proto(&proto)?;
    if bytes.len() > MANIFEST_CAS_CURRENT_ROW_MAX_PROTO_BYTES {
        return Err(anyhow!("manifest current CoreMeta row exceeds size limit"));
    }
    Ok(bytes)
}

fn decode_manifest_current_row(bytes: &[u8]) -> Result<ManifestCurrentRow> {
    if bytes.len() > MANIFEST_CAS_CURRENT_ROW_MAX_PROTO_BYTES {
        return Err(anyhow!("manifest current CoreMeta row exceeds size limit"));
    }
    let proto = ManifestCurrentRowProto::decode(bytes)?;
    ensure_deterministic_proto(&proto, bytes, "manifest CAS current row")?;
    if proto.schema != MANIFEST_CAS_CURRENT_ROW_SCHEMA {
        return Err(anyhow!("manifest CAS current row schema mismatch"));
    }
    let common = proto
        .common
        .ok_or_else(|| anyhow!("manifest CAS current row missing common metadata"))?;
    if common.realm_id != manifest_cas_realm_id(proto.tenant_id) {
        return Err(anyhow!("manifest CAS current row realm mismatch"));
    }
    if common.root_key_hash
        != core_meta_root_key_hash(&manifest_cas_current_root_key(
            proto.tenant_id,
            proto.bucket_id,
        ))
    {
        return Err(anyhow!("manifest CAS current row root mismatch"));
    }
    if common.visibility_state != crate::core_store::CoreMetaVisibilityState::Committed as i32 {
        return Err(anyhow!("manifest CAS current row is not committed"));
    }
    Ok(ManifestCurrentRow {
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        object_key: proto.object_key,
        revision: proto.revision,
        manifest_hash: proto.manifest_hash,
        updated_at: DateTime::parse_from_rfc3339(&proto.updated_at)?.with_timezone(&Utc),
        transaction_id: common.transaction_id,
        created_at_unix_nanos: common.created_at_unix_nanos,
    })
}

fn manifest_current_row_key(tenant_id: i64, bucket_id: i64, object_key: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(MANIFEST_CAS_CURRENT_ROW_KEY_PREFIX),
        CoreMetaTuplePart::I64(tenant_id),
        CoreMetaTuplePart::I64(bucket_id),
        CoreMetaTuplePart::Utf8(object_key),
    ])
}

fn manifest_cas_realm_id(tenant_id: i64) -> String {
    format!("tenant/{tenant_id}")
}

fn manifest_cas_current_root_key(tenant_id: i64, bucket_id: i64) -> String {
    format!("tenant/{tenant_id}/bucket/{bucket_id}/manifest_cas/current")
}

fn current_unix_nanos() -> Result<u64> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| anyhow!("system clock is before Unix epoch"))?;
    Ok(now
        .as_secs()
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(now.subsec_nanos())))
}

fn canonical_json_bytes(value: &JsonValue) -> Result<Vec<u8>> {
    serde_json::to_vec(&canonical_json(value)).map_err(Into::into)
}

fn decode_canonical_json(bytes: &[u8], label: &str) -> Result<JsonValue> {
    let value: JsonValue = serde_json::from_slice(bytes)?;
    if canonical_json_bytes(&value)? != bytes {
        return Err(anyhow!("{label} is not canonical JSON"));
    }
    Ok(value)
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        JsonValue::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&values[key]));
            }
            JsonValue::Object(sorted)
        }
        scalar => scalar.clone(),
    }
}

fn ensure_deterministic_proto(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    if encode_deterministic_proto(message)? != bytes {
        return Err(anyhow!("{label} is not deterministically encoded"));
    }
    Ok(())
}

fn require_manifest_cas_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id = hex::encode(manifest_cas_partition_id(tenant_id, bucket_id));
    if permit.partition_family != "manifest_cas" || permit.partition_id != expected_partition_id {
        anyhow::bail!("manifest CAS write permit targets a different partition");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    const KEY: &[u8] = b"manifest journal partition owner key";

    #[tokio::test]
    async fn manifest_journal_enforces_compare_and_swap() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 1, json!({}), "bad")
                .await
                .unwrap()
                .is_none()
        );
        let first =
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 0, json!({"a":1}), "hash-a")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(first.revision, 1);
        let second =
            compare_and_swap_manifest(&storage, 1, 2, "manifest.json", 1, json!({"a":2}), "hash-b")
                .await
                .unwrap()
                .unwrap();
        assert_eq!(second.revision, 2);
    }

    #[tokio::test]
    pub(crate) async fn manifest_cas_with_permit_writes_fenced_protobuf_record_and_current_row() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let permit = owner.write_permit().unwrap();

        let result = compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            &permit,
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.revision, 1);

        let bodies = read_manifest_bodies(&storage, 1, 2).await.unwrap();
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0].revision, 1);
        assert_eq!(bodies[0].manifest_hash, "hash-a");
        let fences = read_manifest_frame_fences_for_test(&storage, 1, 2)
            .await
            .unwrap();
        assert_eq!(fences, vec![permit.fence_token]);

        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let current = read_manifest_current_row(&meta, 1, 2, "manifest.json")
            .unwrap()
            .expect("manifest CAS current row");
        assert_eq!(current.revision, 1);
        assert_eq!(current.manifest_hash, "hash-a");
    }

    #[tokio::test]
    pub(crate) async fn manifest_cas_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            &stale_permit,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("write permit owner is not current")
        );
    }

    #[tokio::test]
    pub(crate) async fn manifest_cas_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, 1, 2, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, 1, 2, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = compare_and_swap_manifest_inner(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            stale_permit.fence_token,
            Some(stale_precondition),
            None,
            None,
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("generation mismatch") || message.contains("target mismatch"),
            "unexpected stale precondition error: {message}"
        );

        compare_and_swap_manifest_with_permit(
            &storage,
            1,
            2,
            "manifest.json",
            0,
            json!({"a":1}),
            "hash-a",
            &newer.write_permit().unwrap(),
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
    }

    async fn ready_owner(
        storage: &Storage,
        tenant_id: i64,
        bucket_id: i64,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "manifest_cas".to_string();
        let id = hex::encode(manifest_cas_partition_id(tenant_id, bucket_id));
        crate::partition_fence::ready_partition_owner_for_test(
            storage,
            family,
            id,
            owner_node_id,
            0,
            hex::encode([0; 32]),
            hex::encode([1; 32]),
            KEY,
        )
        .await
    }
}
