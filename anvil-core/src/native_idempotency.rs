use crate::{
    anvil_api::NativeMutationContext,
    core_store::{
        CompareAndSwapRef, CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob,
        WriteLogicalFileRequest,
    },
    storage::Storage,
};
use base64::Engine;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use tonic::Status;

const NATIVE_IDEMPOTENCY_REF_PREFIX: &str = "native_idempotency:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
    request_id: String,
    target: NativeIdempotencyTarget,
    response_json: JsonValue,
    response_hash: String,
    record_hash: String,
}

#[derive(Debug, Serialize)]
struct NativeIdempotencyRecordHashInput<'a> {
    format_version: u16,
    tenant_id: i64,
    bucket_id: i64,
    principal: &'a str,
    idempotency_key: &'a str,
    request_id: &'a str,
    target: &'a NativeIdempotencyTarget,
    response_json: &'a JsonValue,
    response_hash: &'a str,
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
        request_id: context.request_id.clone(),
        target: target.clone(),
        response_json,
        response_hash,
        record_hash: String::new(),
    };
    record.record_hash = record_hash(&record)?;

    let bytes = serde_json::to_vec(&record)
        .map_err(|e| Status::internal(format!("Serialize native idempotency record: {e}")))?;
    let ref_name = record_ref_name(context);
    let store = CoreStore::new(storage.clone())
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "idempotency".to_string(),
            generation: 1,
            logical_file_id: ref_name.clone(),
            source: bytes,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!("native-idempotency:{}", context.request_id),
            region_id: "local".to_string(),
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    if let Err(error) = store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name,
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)
                .map_err(|e| Status::internal(e.to_string()))?,
            transaction_id: None,
        })
        .await
    {
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
    let store = CoreStore::new(storage.clone())
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    let Some(ref_value) = store
        .read_ref(&record_ref_name(context))
        .await
        .map_err(|e| Status::internal(e.to_string()))?
    else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)
                .map_err(|e| Status::internal(e.to_string()))?,
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    let record: NativeIdempotencyRecord = serde_json::from_slice(&bytes)
        .map_err(|e| Status::internal(format!("Invalid native idempotency record: {e}")))?;
    if record.record_hash != record_hash(&record)? {
        return Err(Status::data_loss("Native idempotency record hash mismatch"));
    }
    Ok(Some(record))
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
    hasher.finalize().to_hex().to_string()
}

fn record_ref_name(context: &NativeMutationContext) -> String {
    format!(
        "{NATIVE_IDEMPOTENCY_REF_PREFIX}tenant:{}:bucket:{}:hash:{}",
        context.tenant_id,
        context.bucket_id,
        record_key_hash(context)
    )
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String, Status> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(object_ref).map_err(|e| Status::internal(e.to_string()))?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef, Status> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| Status::internal("CoreStore ref target is not a CoreObjectRef"))?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|e| Status::internal(e.to_string()))?;
    serde_json::from_slice(&bytes).map_err(|e| Status::internal(e.to_string()))
}

fn record_hash(record: &NativeIdempotencyRecord) -> Result<String, Status> {
    let input = NativeIdempotencyRecordHashInput {
        format_version: record.format_version,
        tenant_id: record.tenant_id,
        bucket_id: record.bucket_id,
        principal: &record.principal,
        idempotency_key: &record.idempotency_key,
        request_id: &record.request_id,
        target: &record.target,
        response_json: &record.response_json,
        response_hash: &record.response_hash,
    };
    let bytes = serde_json::to_vec(&input)
        .map_err(|e| Status::internal(format!("Hash native idempotency record: {e}")))?;
    Ok(blake3::hash(&bytes).to_hex().to_string())
}
