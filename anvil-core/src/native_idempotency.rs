use crate::{anvil_api::NativeMutationContext, storage::Storage};
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

    let path = storage
        .native_idempotency_record_path(
            context.tenant_id,
            context.bucket_id,
            &record_key_hash(context),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
    }
    let temp_path = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(&record)
        .map_err(|e| Status::internal(format!("Serialize native idempotency record: {e}")))?;
    let mut file = tokio::fs::File::create(&temp_path)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    use tokio::io::AsyncWriteExt;
    file.write_all(&bytes)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    file.sync_data()
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

    if tokio::fs::metadata(&path).await.is_ok() {
        let _ = tokio::fs::remove_file(&temp_path).await;
        let existing = read_record(storage, context)
            .await?
            .ok_or_else(|| Status::internal("Native idempotency record disappeared"))?;
        validate_record_context(&existing, context, target)?;
        return Ok(());
    }
    tokio::fs::rename(&temp_path, &path)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    Ok(())
}

async fn read_record(
    storage: &Storage,
    context: &NativeMutationContext,
) -> Result<Option<NativeIdempotencyRecord>, Status> {
    let path = storage
        .native_idempotency_record_path(
            context.tenant_id,
            context.bucket_id,
            &record_key_hash(context),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Status::internal(error.to_string())),
    };
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
