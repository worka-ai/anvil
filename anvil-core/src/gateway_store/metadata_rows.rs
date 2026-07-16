use super::*;
use prost::Message;

const GATEWAY_METADATA_TRANSACTION_RECORD_KIND: &str = "gateway_metadata_row";

#[derive(Clone, PartialEq, Message)]
pub(super) struct GatewayMetadataRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    row_kind: String,
    #[prost(string, tag = "4")]
    row_key: String,
    #[prost(uint64, tag = "5")]
    generation: u64,
    #[prost(bytes, tag = "6")]
    record_payload: Vec<u8>,
    #[prost(string, tag = "7")]
    record_payload_hash: String,
    #[prost(string, tag = "8")]
    updated_at: String,
}

#[derive(Debug, Clone)]
pub(super) struct GatewayStoredRecord<T> {
    pub(super) row_kind: String,
    pub(super) row_key: String,
    pub(super) generation: u64,
    pub(super) payload_hash: String,
    pub(super) updated_at: String,
    pub(super) record: T,
}

#[derive(Debug, Clone)]
pub struct GatewayStoredHandle {
    pub row_kind: String,
    pub row_key: String,
    pub generation: u64,
    pub payload_hash: String,
    pub updated_at: String,
}

impl<T> GatewayStoredRecord<T> {
    pub(super) fn stored_handle(&self) -> GatewayStoredHandle {
        GatewayStoredHandle {
            row_kind: self.row_kind.clone(),
            row_key: self.row_key.clone(),
            generation: self.generation,
            payload_hash: self.payload_hash.clone(),
            updated_at: self.updated_at.clone(),
        }
    }
}
pub(super) fn gateway_metadata_tuple_key(row_kind: &str, row_key: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("gateway"),
        CoreMetaTuplePart::Utf8(row_kind),
        CoreMetaTuplePart::Utf8(row_key),
    ])
}

pub(super) fn gateway_metadata_tuple_prefix(row_kind: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("gateway"),
        CoreMetaTuplePart::Utf8(row_kind),
    ])
}

pub(super) fn gateway_metadata_root_key_hash(row_kind: &str, row_key: &str) -> String {
    core_meta_root_key_hash(&format!("gateway/{row_kind}/{row_key}"))
}

pub(super) fn gateway_realm_id<T: GatewayRecordCodec>(record: &T) -> String {
    let payload = match encode_gateway_record(record) {
        Ok(payload) => payload,
        Err(_) => return "gateway".to_string(),
    };
    let hash = hash32(&payload);
    format!("gateway/{}", hex::encode(hash))
}

pub(crate) fn encode_gateway_metadata_row<T: GatewayRecordCodec>(
    row_kind: &str,
    row_key: &str,
    generation: u64,
    record: &T,
) -> Result<Vec<u8>> {
    let record_payload = encode_gateway_record(record)?;
    let row = GatewayMetadataRowProto {
        common: Some(core_meta_committed_row_common(
            gateway_realm_id(record),
            gateway_metadata_root_key_hash(row_kind, row_key),
            generation,
            String::new(),
            0,
        )),
        schema: GATEWAY_METADATA_ROW_SCHEMA.to_string(),
        row_kind: row_kind.to_string(),
        row_key: row_key.to_string(),
        generation,
        record_payload_hash: format!("sha256:{}", sha256_hex(&record_payload)),
        record_payload,
        updated_at: now_rfc3339(),
    };
    Ok(encode_deterministic_proto(&row))
}

pub(super) fn decode_gateway_metadata_row<T: GatewayRecordCodec>(
    row_kind: &str,
    row_key: &str,
    bytes: &[u8],
) -> Result<GatewayStoredRecord<T>> {
    let row = decode_deterministic_proto::<GatewayMetadataRowProto>(bytes, "gateway metadata row")?;
    if row.schema != GATEWAY_METADATA_ROW_SCHEMA
        || row.row_kind != row_kind
        || row.row_key != row_key
        || row.generation == 0
    {
        bail!("gateway metadata row scope mismatch");
    }
    let payload_hash = format!("sha256:{}", sha256_hex(&row.record_payload));
    if row.record_payload_hash != payload_hash {
        bail!("gateway metadata row payload hash mismatch");
    }
    Ok(GatewayStoredRecord {
        row_kind: row.row_kind,
        row_key: row.row_key,
        generation: row.generation,
        payload_hash,
        updated_at: row.updated_at,
        record: decode_gateway_record(&row.record_payload)?,
    })
}

pub(super) async fn read_record_row<T: GatewayRecordCodec>(
    storage: &Storage,
    row_kind: &str,
    row_key: &str,
) -> Result<Option<GatewayStoredRecord<T>>> {
    let Some(bytes) = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_REGISTRY,
        TABLE_GATEWAY_METADATA_ROW,
        &gateway_metadata_tuple_key(row_kind, row_key)?,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_gateway_metadata_row(
        row_kind, row_key, &bytes,
    )?))
}

pub(super) async fn put_record_row<T: GatewayRecordCodec>(
    storage: &Storage,
    row_kind: &str,
    row_key: &str,
    record: &T,
    require_absent: bool,
    expected_generation: Option<u64>,
) -> Result<GatewayStoredRecord<T>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let tuple_key = gateway_metadata_tuple_key(row_kind, row_key)?;
    let current_bytes = meta.get(CF_REGISTRY, TABLE_GATEWAY_METADATA_ROW, &tuple_key)?;
    let expected_payload_hash = current_bytes.as_ref().map(|bytes| {
        crate::core_store::core_meta_payload_digest(TABLE_GATEWAY_METADATA_ROW, bytes)
    });
    let current = current_bytes
        .map(|bytes| decode_gateway_metadata_row::<T>(row_kind, row_key, &bytes))
        .transpose()?;
    if require_absent && current.is_some() {
        bail!("gateway metadata row {row_kind}/{row_key} already exists");
    }
    if let Some(expected_generation) = expected_generation {
        let actual = current.as_ref().map(|value| value.generation);
        if actual != Some(expected_generation) {
            bail!("gateway metadata row {row_kind}/{row_key} generation mismatch");
        }
    }
    let generation = current
        .as_ref()
        .map(|value| value.generation + 1)
        .unwrap_or(1);
    let expected_payload_hash = current.as_ref().map(|value| value.payload_hash.clone());
    let payload = encode_gateway_metadata_row(row_kind, row_key, generation, record)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let op = CoreMetaBatchOp {
        cf: CF_REGISTRY,
        table_id: TABLE_GATEWAY_METADATA_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    core_store
        .commit_coremeta_batch_by_embedded_roots(
            &format!("gateway-row:{row_kind}:{generation}"),
            &[op],
        )
        .await?;
    decode_gateway_metadata_row(row_kind, row_key, &payload)
}

pub(super) async fn put_record_row_in_transaction<T: GatewayRecordCodec>(
    storage: &Storage,
    row_kind: &str,
    row_key: &str,
    record: &T,
    require_absent: bool,
    expected_generation: Option<u64>,
    transaction_id: &str,
    principal: &str,
) -> Result<GatewayStoredRecord<T>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let transaction = core_store
        .read_explicit_transaction_for_principal(transaction_id, principal)
        .await?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let tuple_key = gateway_metadata_tuple_key(row_kind, row_key)?;
    let mut current = meta
        .get(CF_REGISTRY, TABLE_GATEWAY_METADATA_ROW, &tuple_key)?
        .map(|bytes| decode_gateway_metadata_row::<T>(row_kind, row_key, &bytes))
        .transpose()?;
    let stream_id = gateway_metadata_transaction_stream_id(row_kind, row_key);
    for update in transaction.visible_updates.iter().rev() {
        let crate::core_store::CoreTransactionUpdate::StreamAppend {
            stream_id: update_stream_id,
            visible_sequence,
            prepared_record_hash,
        } = update
        else {
            continue;
        };
        if update_stream_id != &stream_id {
            continue;
        }
        if let Some(record) = core_store
            .read_raw_stream_record(update_stream_id, *visible_sequence, prepared_record_hash)
            .await?
        {
            if record.record_kind == GATEWAY_METADATA_TRANSACTION_RECORD_KIND {
                current = Some(decode_gateway_metadata_row::<T>(
                    row_kind,
                    row_key,
                    &record.payload,
                )?);
                break;
            }
        }
    }
    if require_absent && current.is_some() {
        bail!("gateway metadata row {row_kind}/{row_key} already exists");
    }
    if let Some(expected_generation) = expected_generation {
        let actual = current.as_ref().map(|value| value.generation);
        if actual != Some(expected_generation) {
            bail!("gateway metadata row {row_kind}/{row_key} generation mismatch");
        }
    }
    let generation = current
        .as_ref()
        .map(|value| value.generation + 1)
        .unwrap_or(1);
    let expected_payload_hash = current.as_ref().map(|value| value.payload_hash.clone());
    let payload = encode_gateway_metadata_row(row_kind, row_key, generation, record)?;
    let scope_partition = transaction.scope_partition.clone();
    core_store
        .stage_explicit_transaction_batch(crate::core_store::CoreMutationBatch {
            transaction_id: transaction_id.to_string(),
            scope_partition: scope_partition.clone(),
            committed_by_principal: principal.to_string(),
            preconditions: vec![crate::core_store::CoreMutationPrecondition::CoreMetaRow {
                cf: CF_REGISTRY.to_string(),
                table_id: TABLE_GATEWAY_METADATA_ROW,
                tuple_key,
                expected_payload_hash,
                require_absent,
                require_present: expected_generation.is_some(),
            }],
            operations: vec![crate::core_store::CoreMutationOperation::StreamAppend {
                partition_id: scope_partition,
                stream_id: gateway_metadata_transaction_stream_id(row_kind, row_key),
                record_kind: GATEWAY_METADATA_TRANSACTION_RECORD_KIND.to_string(),
                payload: payload.clone(),
                idempotency_key: Some(format!(
                    "gateway-metadata-row:{row_kind}:{row_key}:{generation}"
                )),
            }],
        })
        .await?;
    decode_gateway_metadata_row(row_kind, row_key, &payload)
}

pub(super) async fn read_record_row_in_transaction<T: GatewayRecordCodec>(
    storage: &Storage,
    row_kind: &str,
    row_key: &str,
    transaction_id: &str,
    principal: &str,
) -> Result<Option<GatewayStoredRecord<T>>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let transaction = core_store
        .read_explicit_transaction_for_principal(transaction_id, principal)
        .await?;
    let stream_id = gateway_metadata_transaction_stream_id(row_kind, row_key);
    for update in transaction.visible_updates.iter().rev() {
        let crate::core_store::CoreTransactionUpdate::StreamAppend {
            stream_id: update_stream_id,
            visible_sequence,
            prepared_record_hash,
        } = update
        else {
            continue;
        };
        if update_stream_id != &stream_id {
            continue;
        }
        let Some(record) = core_store
            .read_raw_stream_record(update_stream_id, *visible_sequence, prepared_record_hash)
            .await?
        else {
            continue;
        };
        if record.record_kind != GATEWAY_METADATA_TRANSACTION_RECORD_KIND {
            continue;
        }
        return Ok(Some(decode_gateway_metadata_row::<T>(
            row_kind,
            row_key,
            &record.payload,
        )?));
    }
    Ok(None)
}

pub async fn materialize_committed_gateway_transaction(
    storage: &Storage,
    transaction: &crate::core_store::CoreTransaction,
) -> Result<usize> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut materialized = 0usize;
    for update in &transaction.visible_updates {
        let crate::core_store::CoreTransactionUpdate::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
        } = update
        else {
            continue;
        };
        let records = core_store
            .read_stream(ReadStream {
                stream_id: stream_id.clone(),
                after_sequence: visible_sequence.saturating_sub(1),
                limit: 1,
            })
            .await?;
        let Some(record) = records.into_iter().find(|record| {
            record.sequence == *visible_sequence
                && &record.event_hash == prepared_record_hash
                && record.record_kind == GATEWAY_METADATA_TRANSACTION_RECORD_KIND
        }) else {
            continue;
        };
        let row = decode_deterministic_proto::<GatewayMetadataRowProto>(
            &record.payload,
            "gateway metadata transaction row",
        )?;
        let tuple_key = gateway_metadata_tuple_key(&row.row_kind, &row.row_key)?;
        let op = CoreMetaBatchOp {
            cf: CF_REGISTRY,
            table_id: TABLE_GATEWAY_METADATA_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&record.payload),
        };
        core_store
            .commit_coremeta_batch_by_embedded_roots(
                &format!(
                    "gateway-materialize:{}:{}:{}",
                    row.row_kind, row.row_key, row.generation
                ),
                &[op],
            )
            .await?;
        materialize_gateway_side_effects(storage, &row).await?;
        materialized = materialized.saturating_add(1);
    }
    Ok(materialized)
}

async fn materialize_gateway_side_effects(
    storage: &Storage,
    row: &GatewayMetadataRowProto,
) -> Result<()> {
    match row.row_kind.as_str() {
        GATEWAY_ROW_BLOB => {
            let record: GatewayBlobRecord = decode_gateway_record(&row.record_payload)?;
            coremeta::write_registry_blob_locator_row_from_record(storage, &record).await?;
        }
        GATEWAY_ROW_TAG => {
            let record: GatewayTagRecord = decode_gateway_record(&row.record_payload)?;
            if let Some(blob) = coremeta::read_registry_blob_locator_row(
                storage,
                record.tenant_id,
                &record.gateway,
                &record.registry_instance_id,
                &record.target_digest,
            )? {
                coremeta::write_registry_version_row_for_tag(
                    storage,
                    &record,
                    &blob,
                    row.generation,
                )
                .await?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn gateway_metadata_transaction_stream_id(row_kind: &str, row_key: &str) -> String {
    let row_hash = hash32(format!("{row_kind}\0{row_key}").as_bytes());
    format!("gateway_metadata_row:{row_kind}:{}", hex::encode(row_hash))
}

pub(super) async fn put_upload_session_start_rows(
    storage: &Storage,
    session_key: &str,
    idempotency_key: &str,
    record: &GatewayUploadSessionRecord,
) -> Result<GatewayStoredRecord<GatewayUploadSessionRecord>> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let session_tuple_key = gateway_metadata_tuple_key(GATEWAY_ROW_UPLOAD_SESSION, session_key)?;
    let idempotency_tuple_key =
        gateway_metadata_tuple_key(GATEWAY_ROW_UPLOAD_IDEMPOTENCY, idempotency_key)?;
    if meta
        .get(CF_REGISTRY, TABLE_GATEWAY_METADATA_ROW, &session_tuple_key)?
        .is_some()
        || meta
            .get(
                CF_REGISTRY,
                TABLE_GATEWAY_METADATA_ROW,
                &idempotency_tuple_key,
            )?
            .is_some()
    {
        bail!("gateway upload session metadata row already exists");
    }
    let session_payload =
        encode_gateway_metadata_row(GATEWAY_ROW_UPLOAD_SESSION, session_key, 1, record)?;
    let idempotency_payload =
        encode_gateway_metadata_row(GATEWAY_ROW_UPLOAD_IDEMPOTENCY, idempotency_key, 1, record)?;
    let core_store = CoreStore::new(storage.clone()).await?;
    core_store
        .commit_coremeta_batch_by_embedded_roots(
            &format!("gateway-upload-session:{session_key}"),
            &[
                CoreMetaBatchOp {
                    cf: CF_REGISTRY,
                    table_id: TABLE_GATEWAY_METADATA_ROW,
                    tuple_key: &session_tuple_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&session_payload),
                },
                CoreMetaBatchOp {
                    cf: CF_REGISTRY,
                    table_id: TABLE_GATEWAY_METADATA_ROW,
                    tuple_key: &idempotency_tuple_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&idempotency_payload),
                },
            ],
        )
        .await?;
    decode_gateway_metadata_row(GATEWAY_ROW_UPLOAD_SESSION, session_key, &session_payload)
}

pub(super) async fn list_record_rows<T: GatewayRecordCodec>(
    storage: &Storage,
    row_kind: &str,
) -> Result<Vec<GatewayStoredRecord<T>>> {
    CoreMetaStore::open(storage.core_store_meta_path())?
        .scan_prefix(
            CF_REGISTRY,
            TABLE_GATEWAY_METADATA_ROW,
            &gateway_metadata_tuple_prefix(row_kind)?,
        )?
        .into_iter()
        .map(|record| {
            let row = decode_deterministic_proto::<GatewayMetadataRowProto>(
                &record.payload,
                "gateway metadata row",
            )?;
            decode_gateway_metadata_row(row_kind, &row.row_key, &record.payload)
        })
        .collect::<Result<Vec<_>>>()
}
