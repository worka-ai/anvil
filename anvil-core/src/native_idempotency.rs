use crate::{
    anvil_api::NativeMutationContext,
    core_store::{
        CF_TRANSACTIONS, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaRowCommonProto,
        CoreMetaStore, CoreMetaTuplePart, CoreMetaVisibilityState, CoreStore, CoreTransactionState,
        CoreTransactionUpdate, TABLE_NATIVE_IDEMPOTENCY_ROW, commit_coremeta_batch_for_storage,
        core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
    },
    storage::Storage,
};
use prost::Message;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use tonic::Status;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NativeIdempotencyTarget {
    pub operation: String,
    pub bucket_name: String,
    pub object_key: String,
    #[serde(default)]
    pub parameters: JsonValue,
}

impl NativeIdempotencyTarget {
    pub fn new(
        operation: impl Into<String>,
        bucket_name: impl Into<String>,
        object_key: impl Into<String>,
    ) -> Self {
        Self {
            operation: operation.into(),
            bucket_name: bucket_name.into(),
            object_key: object_key.into(),
            parameters: JsonValue::Null,
        }
    }

    pub fn with_parameters(mut self, parameters: JsonValue) -> Self {
        self.parameters = parameters;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NativeIdempotencyRecord {
    format_version: u16,
    tenant_id: i64,
    bucket_id: i64,
    principal: String,
    idempotency_key: String,
    transaction_id: Option<String>,
    request_id: String,
    target: NativeIdempotencyTarget,
    response_json: JsonValue,
    response_hash: String,
    record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct NativeIdempotencyTargetProto {
    #[prost(string, tag = "1")]
    operation: String,
    #[prost(string, tag = "2")]
    bucket_name: String,
    #[prost(string, tag = "3")]
    object_key: String,
    #[prost(bytes, tag = "4")]
    parameters_json: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct NativeIdempotencyRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(uint32, tag = "2")]
    format_version: u32,
    #[prost(int64, tag = "3")]
    tenant_id: i64,
    #[prost(int64, tag = "4")]
    bucket_id: i64,
    #[prost(string, tag = "5")]
    principal: String,
    #[prost(string, tag = "6")]
    idempotency_key: String,
    #[prost(string, tag = "7")]
    request_id: String,
    #[prost(message, optional, tag = "8")]
    target: Option<NativeIdempotencyTargetProto>,
    #[prost(bytes, tag = "9")]
    response_json: Vec<u8>,
    #[prost(string, tag = "10")]
    response_hash: String,
    #[prost(string, tag = "11")]
    record_hash: String,
    #[prost(string, optional, tag = "12")]
    transaction_id: Option<String>,
}

pub async fn load_response<T>(
    storage: &Storage,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
) -> Result<Option<T>, Status>
where
    T: DeserializeOwned,
{
    let Some(record) = read_record(storage, context).await? else {
        return Ok(None);
    };
    validate_record_context(&record, context, target)?;
    let response = serde_json::from_value(record.response_json)
        .map_err(|e| Status::internal(format!("Invalid native idempotency response: {e}")))?;
    Ok(Some(response))
}

pub async fn store_response<T>(
    storage: &Storage,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
    response: &T,
) -> Result<(), Status>
where
    T: Serialize,
{
    if let Some(record) = read_record(storage, context).await? {
        validate_record_context(&record, context, target)?;
        return Ok(());
    }

    let response_json = serde_json::to_value(response)
        .map_err(|e| Status::internal(format!("Serialize native idempotency response: {e}")))?;
    let response_hash = blake3::hash(&serde_json::to_vec(&response_json).map_err(|e| {
        Status::internal(format!("Serialize native idempotency response hash: {e}"))
    })?)
    .to_hex()
    .to_string();
    let mut record = NativeIdempotencyRecord {
        format_version: 1,
        tenant_id: context.tenant_id,
        bucket_id: context.bucket_id,
        principal: context.principal.clone(),
        idempotency_key: context.idempotency_key.clone(),
        transaction_id: context.transaction_id.clone(),
        request_id: context.request_id.clone(),
        target: target.clone(),
        response_json,
        response_hash,
        record_hash: String::new(),
    };
    record.record_hash = record_hash(&record)?;

    let bytes = encode_record(&record)?;
    let row_key = record_tuple_key(context)?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())
        .map_err(|e| Status::internal(e.to_string()))?;

    if let Err(error) = put_record_if_absent(storage, &meta, &row_key, &record, &bytes).await {
        let existing = read_record(storage, context)
            .await?
            .ok_or_else(|| Status::internal(error.to_string()))?;
        validate_record_context(&existing, context, target)?;
    }
    Ok(())
}

async fn read_record(
    storage: &Storage,
    context: &NativeMutationContext,
) -> Result<Option<NativeIdempotencyRecord>, Status> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())
        .map_err(|e| Status::internal(e.to_string()))?;
    let row_key = record_tuple_key(context)?;
    let Some(bytes) = meta
        .get(CF_TRANSACTIONS, TABLE_NATIVE_IDEMPOTENCY_ROW, &row_key)
        .map_err(|e| Status::internal(e.to_string()))?
    else {
        return read_staged_record(storage, context, &row_key).await;
    };
    decode_record(&bytes).map(Some)
}

fn validate_record_context(
    record: &NativeIdempotencyRecord,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
) -> Result<(), Status> {
    if record.tenant_id != context.tenant_id
        || record.bucket_id != context.bucket_id
        || record.principal != context.principal
        || record.idempotency_key != context.idempotency_key
        || record.transaction_id != context.transaction_id
    {
        return Err(Status::permission_denied(
            "Native idempotency record context mismatch",
        ));
    }
    if &record.target != target {
        return Err(Status::failed_precondition(
            "Native idempotency key already used for a different mutation target",
        ));
    }
    Ok(())
}

fn record_key_hash(context: &NativeMutationContext) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&context.tenant_id.to_le_bytes());
    hasher.update(&context.bucket_id.to_le_bytes());
    hasher.update(context.principal.as_bytes());
    hasher.update(&[0]);
    hasher.update(context.idempotency_key.as_bytes());
    if let Some(transaction_id) = context.transaction_id.as_deref() {
        hasher.update(&[0]);
        hasher.update(transaction_id.as_bytes());
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn record_tuple_key(context: &NativeMutationContext) -> Result<Vec<u8>, Status> {
    let hash = record_key_hash(context);
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("native_idempotency"),
        CoreMetaTuplePart::I64(context.tenant_id),
        CoreMetaTuplePart::I64(context.bucket_id),
        CoreMetaTuplePart::Hash(&hash),
    ])
    .map_err(|e| Status::internal(e.to_string()))
}

async fn read_staged_record(
    storage: &Storage,
    context: &NativeMutationContext,
    row_key: &[u8],
) -> Result<Option<NativeIdempotencyRecord>, Status> {
    let Some(transaction_id) = context.transaction_id.as_deref() else {
        return Ok(None);
    };
    let core_store = CoreStore::new(storage.clone())
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    let transaction = core_store
        .read_explicit_transaction_for_principal(
            transaction_id,
            &native_transaction_principal_from_context(context),
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    if transaction.state != CoreTransactionState::Open {
        return Ok(None);
    }
    for update in transaction.visible_updates.iter().rev() {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                payload,
                ..
            } if cf == CF_TRANSACTIONS
                && *table_id == TABLE_NATIVE_IDEMPOTENCY_ROW
                && tuple_key == row_key =>
            {
                return decode_record(payload).map(Some);
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                ..
            } if cf == CF_TRANSACTIONS
                && *table_id == TABLE_NATIVE_IDEMPOTENCY_ROW
                && tuple_key == row_key =>
            {
                return Ok(None);
            }
            _ => {}
        }
    }
    Ok(None)
}

async fn put_record_if_absent(
    storage: &Storage,
    meta: &CoreMetaStore,
    row_key: &[u8],
    record: &NativeIdempotencyRecord,
    payload: &[u8],
) -> Result<(), Status> {
    if meta
        .get(CF_TRANSACTIONS, TABLE_NATIVE_IDEMPOTENCY_ROW, row_key)
        .map_err(|e| Status::internal(e.to_string()))?
        .is_some()
    {
        return Err(Status::already_exists("NativeIdempotencyRecordExists"));
    }

    if let Some(transaction_id) = record.transaction_id.as_deref() {
        let core_store = CoreStore::new(storage.clone())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        return core_store
            .stage_coremeta_put_in_transaction(
                transaction_id,
                &native_transaction_principal(record),
                CF_TRANSACTIONS,
                TABLE_NATIVE_IDEMPOTENCY_ROW,
                row_key.to_vec(),
                payload.to_vec(),
                None,
                true,
                false,
            )
            .await
            .map(|_| ())
            .map_err(|e| Status::internal(e.to_string()));
    }

    let op = CoreMetaBatchOp {
        cf: CF_TRANSACTIONS,
        table_id: TABLE_NATIVE_IDEMPOTENCY_ROW,
        tuple_key: row_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(payload),
    };
    commit_coremeta_batch_for_storage(storage, &record.idempotency_key, &[op])
        .await
        .map(|_| ())
        .map_err(|e| Status::internal(e.to_string()))
}

fn native_transaction_principal(record: &NativeIdempotencyRecord) -> String {
    format!("tenant/{}/principal/{}", record.tenant_id, record.principal)
}

fn native_transaction_principal_from_context(context: &NativeMutationContext) -> String {
    format!(
        "tenant/{}/principal/{}",
        context.tenant_id, context.principal
    )
}

fn encode_record(record: &NativeIdempotencyRecord) -> Result<Vec<u8>, Status> {
    let proto = record_to_proto(record)?;
    let mut bytes = Vec::new();
    proto
        .encode(&mut bytes)
        .map_err(|e| Status::internal(format!("Encode native idempotency record: {e}")))?;
    Ok(bytes)
}

fn decode_record(bytes: &[u8]) -> Result<NativeIdempotencyRecord, Status> {
    let proto = NativeIdempotencyRecordProto::decode(bytes)
        .map_err(|e| Status::internal(format!("Invalid native idempotency record: {e}")))?;
    let record = record_from_proto(proto)?;
    if record.record_hash != record_hash(&record)? {
        return Err(Status::data_loss("Native idempotency record hash mismatch"));
    }
    Ok(record)
}

fn record_to_proto(
    record: &NativeIdempotencyRecord,
) -> Result<NativeIdempotencyRecordProto, Status> {
    Ok(NativeIdempotencyRecordProto {
        common: Some(native_idempotency_common(record)),
        format_version: u32::from(record.format_version),
        tenant_id: record.tenant_id,
        bucket_id: record.bucket_id,
        principal: record.principal.clone(),
        idempotency_key: record.idempotency_key.clone(),
        transaction_id: record.transaction_id.clone(),
        request_id: record.request_id.clone(),
        target: Some(target_to_proto(&record.target)?),
        response_json: json_to_vec(&record.response_json, "native idempotency response")?,
        response_hash: record.response_hash.clone(),
        record_hash: record.record_hash.clone(),
    })
}

fn record_from_proto(
    proto: NativeIdempotencyRecordProto,
) -> Result<NativeIdempotencyRecord, Status> {
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| Status::data_loss("Native idempotency record missing CoreMeta common"))?;
    validate_native_idempotency_common(&proto, common)?;
    Ok(NativeIdempotencyRecord {
        format_version: proto
            .format_version
            .try_into()
            .map_err(|_| Status::internal("Native idempotency format version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        principal: proto.principal,
        idempotency_key: proto.idempotency_key,
        transaction_id: proto.transaction_id,
        request_id: proto.request_id,
        target: target_from_proto(
            proto
                .target
                .ok_or_else(|| Status::internal("Native idempotency target missing"))?,
        )?,
        response_json: vec_to_json(&proto.response_json, "native idempotency response")?,
        response_hash: proto.response_hash,
        record_hash: proto.record_hash,
    })
}

fn native_idempotency_common(record: &NativeIdempotencyRecord) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        format!("tenant/{}", record.tenant_id),
        native_idempotency_root_key_hash(record.tenant_id, record.bucket_id),
        1,
        record
            .transaction_id
            .clone()
            .unwrap_or_else(|| record.idempotency_key.clone()),
        0,
    )
}

fn validate_native_idempotency_common(
    proto: &NativeIdempotencyRecordProto,
    common: &CoreMetaRowCommonProto,
) -> Result<(), Status> {
    if common.realm_id != format!("tenant/{}", proto.tenant_id) {
        return Err(Status::data_loss(
            "Native idempotency CoreMeta realm mismatch",
        ));
    }
    if common.root_key_hash != native_idempotency_root_key_hash(proto.tenant_id, proto.bucket_id) {
        return Err(Status::data_loss(
            "Native idempotency CoreMeta root mismatch",
        ));
    }
    let expected_transaction_id = proto
        .transaction_id
        .clone()
        .unwrap_or_else(|| proto.idempotency_key.clone());
    if common.transaction_id != expected_transaction_id {
        return Err(Status::data_loss(
            "Native idempotency CoreMeta transaction mismatch",
        ));
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        return Err(Status::data_loss(
            "Native idempotency CoreMeta row is not committed",
        ));
    }
    Ok(())
}

fn native_idempotency_root_key_hash(tenant_id: i64, bucket_id: i64) -> String {
    core_meta_root_key_hash(&format!(
        "native-idempotency/tenant/{tenant_id}/bucket/{bucket_id}"
    ))
}

fn target_to_proto(
    target: &NativeIdempotencyTarget,
) -> Result<NativeIdempotencyTargetProto, Status> {
    Ok(NativeIdempotencyTargetProto {
        operation: target.operation.clone(),
        bucket_name: target.bucket_name.clone(),
        object_key: target.object_key.clone(),
        parameters_json: json_to_vec(&target.parameters, "native idempotency target parameters")?,
    })
}

fn target_from_proto(
    proto: NativeIdempotencyTargetProto,
) -> Result<NativeIdempotencyTarget, Status> {
    Ok(NativeIdempotencyTarget {
        operation: proto.operation,
        bucket_name: proto.bucket_name,
        object_key: proto.object_key,
        parameters: vec_to_json(
            &proto.parameters_json,
            "native idempotency target parameters",
        )?,
    })
}

fn json_to_vec(value: &JsonValue, label: &str) -> Result<Vec<u8>, Status> {
    serde_json::to_vec(value).map_err(|e| Status::internal(format!("Serialize {label}: {e}")))
}

fn vec_to_json(bytes: &[u8], label: &str) -> Result<JsonValue, Status> {
    serde_json::from_slice(bytes).map_err(|e| Status::internal(format!("Invalid {label}: {e}")))
}

fn record_hash(record: &NativeIdempotencyRecord) -> Result<String, Status> {
    let mut input = record_to_proto(record)?;
    input.record_hash.clear();
    let mut bytes = Vec::new();
    input
        .encode(&mut bytes)
        .map_err(|e| Status::internal(format!("Hash native idempotency record: {e}")))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn context() -> NativeMutationContext {
        NativeMutationContext {
            tenant_id: 7,
            bucket_id: 42,
            principal: "user:alice".to_string(),
            request_id: "req-1".to_string(),
            precondition: String::new(),
            authz_zookie_optional: String::new(),
            idempotency_key: "idem-1".to_string(),
            transaction_id: None,
            saga_operation: None,
            saga_compensation_operation: None,
            write_visibility: None,
        }
    }

    #[tokio::test]
    async fn native_idempotency_records_are_coremeta_rows_not_corestore_refs() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let context = context();
        let target = NativeIdempotencyTarget::new("PutObject", "docs", "a.txt")
            .with_parameters(json!({"mode": "create"}));
        let response = json!({"version_id": "v1", "committed": true});

        store_response(&storage, &context, &target, &response)
            .await
            .unwrap();

        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let row = meta
            .get(
                CF_TRANSACTIONS,
                TABLE_NATIVE_IDEMPOTENCY_ROW,
                &record_tuple_key(&context).unwrap(),
            )
            .unwrap()
            .expect("native idempotency record must be stored in CoreMeta");
        assert_eq!(decode_record(&row).unwrap().response_json, response);

        let replay: serde_json::Value = load_response(&storage, &context, &target)
            .await
            .unwrap()
            .expect("native idempotency replay");
        assert_eq!(replay, response);
    }

    #[tokio::test]
    async fn native_idempotency_rejects_target_reuse_from_coremeta_record() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let context = context();
        let original = NativeIdempotencyTarget::new("PutObject", "docs", "a.txt");
        let changed = NativeIdempotencyTarget::new("DeleteObject", "docs", "a.txt");

        store_response(&storage, &context, &original, &json!({"ok": true}))
            .await
            .unwrap();
        let error = store_response(&storage, &context, &changed, &json!({"ok": true}))
            .await
            .expect_err("reusing an idempotency key for a different target must fail");
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn native_idempotency_keys_are_scoped_by_transaction_id() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(tmp.path()).await.unwrap();
        let mut tx_context = context();
        tx_context.transaction_id = Some(
            begin_native_idempotency_transaction(&storage, &tx_context, "native-idem-tx-1")
                .await
                .transaction_id,
        );
        let target = NativeIdempotencyTarget::new("PutObject", "docs", "a.txt");

        store_response(&storage, &tx_context, &target, &json!({"state": "staged"}))
            .await
            .unwrap();

        let mut other_tx_context = tx_context.clone();
        other_tx_context.transaction_id = Some(
            begin_native_idempotency_transaction(&storage, &other_tx_context, "native-idem-tx-2")
                .await
                .transaction_id,
        );
        assert!(
            load_response::<serde_json::Value>(&storage, &other_tx_context, &target)
                .await
                .unwrap()
                .is_none()
        );

        let replay = load_response::<serde_json::Value>(&storage, &tx_context, &target)
            .await
            .unwrap()
            .expect("same transaction idempotency replay");
        assert_eq!(replay, json!({"state": "staged"}));
    }

    async fn begin_native_idempotency_transaction(
        storage: &Storage,
        context: &NativeMutationContext,
        idempotency_key: &str,
    ) -> crate::core_store::CoreTransaction {
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let root_anchor_key = format!(
            "native-idempotency/tenant/{}/bucket/{}",
            context.tenant_id, context.bucket_id
        );
        core_store
            .begin_explicit_transaction(crate::core_store::CoreBeginTransaction {
                idempotency_key: idempotency_key.to_string(),
                root_key_hash: CoreStore::root_key_hash_for_anchor(&root_anchor_key),
                root_anchor_key: root_anchor_key.clone(),
                scope_partition: root_anchor_key,
                ttl_ms: 30_000,
                purpose: "native-idempotency-test".to_string(),
                principal: native_transaction_principal_from_context(context),
                preconditions_hash: format!("sha256:{}", "0".repeat(64)),
            })
            .await
            .unwrap()
    }
}
