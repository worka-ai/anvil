use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::IndexDiagnostic;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use prost::{Message, Oneof};
use serde_json::Value as JsonValue;

const INDEX_DIAGNOSTIC_BODY_SCHEMA: &str = "anvil.core.index_diagnostic.journal_body.v1";

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticBodyProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(message, optional, tag = "2")]
    diagnostic: Option<IndexDiagnosticProto>,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(int64, tag = "3")]
    bucket_id: i64,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(int64, optional, tag = "5")]
    index_id: Option<i64>,
    #[prost(string, tag = "6")]
    index_name: String,
    #[prost(string, tag = "7")]
    object_key: String,
    #[prost(string, optional, tag = "8")]
    version_id: Option<String>,
    #[prost(string, tag = "9")]
    severity: String,
    #[prost(string, tag = "10")]
    code: String,
    #[prost(string, tag = "11")]
    message: String,
    #[prost(message, optional, tag = "12")]
    details: Option<IndexDiagnosticJsonValueProto>,
    #[prost(int64, tag = "13")]
    created_at_unix_nanos: i64,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticJsonValueProto {
    #[prost(
        oneof = "index_diagnostic_json_value_proto::Kind",
        tags = "1, 2, 3, 4, 5, 6, 7, 8"
    )]
    kind: Option<index_diagnostic_json_value_proto::Kind>,
}

mod index_diagnostic_json_value_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(bool, tag = "1")]
        Null(bool),
        #[prost(bool, tag = "2")]
        Bool(bool),
        #[prost(int64, tag = "3")]
        I64(i64),
        #[prost(uint64, tag = "4")]
        U64(u64),
        #[prost(double, tag = "5")]
        F64(f64),
        #[prost(string, tag = "6")]
        String(String),
        #[prost(message, tag = "7")]
        Array(super::IndexDiagnosticJsonArrayProto),
        #[prost(message, tag = "8")]
        Object(super::IndexDiagnosticJsonObjectProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticJsonArrayProto {
    #[prost(message, repeated, tag = "1")]
    values: Vec<IndexDiagnosticJsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticJsonObjectProto {
    #[prost(message, repeated, tag = "1")]
    entries: Vec<IndexDiagnosticJsonObjectEntryProto>,
}

#[derive(Clone, PartialEq, Message)]
struct IndexDiagnosticJsonObjectEntryProto {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<IndexDiagnosticJsonValueProto>,
}

#[cfg(test)]
async fn write_index_diagnostic(
    storage: &Storage,
    diagnostic: IndexDiagnostic,
) -> Result<IndexDiagnostic> {
    write_index_diagnostic_inner(storage, diagnostic, 0, None).await
}

pub(crate) async fn write_index_diagnostic_with_permit(
    storage: &Storage,
    diagnostic: IndexDiagnostic,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<IndexDiagnostic> {
    require_index_diagnostic_permit(diagnostic.tenant_id, diagnostic.bucket_id, permit)?;
    let partition_precondition =
        partition_write_precondition(storage, permit, partition_owner_signing_key).await?;
    write_index_diagnostic_inner(
        storage,
        diagnostic,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn write_index_diagnostic_inner(
    storage: &Storage,
    mut diagnostic: IndexDiagnostic,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<IndexDiagnostic> {
    let cursor = read_index_diagnostics(
        storage,
        diagnostic.tenant_id,
        diagnostic.bucket_id,
        "",
        "",
        0,
        0,
    )
    .await?
    .into_iter()
    .map(|record| record.id)
    .max()
    .unwrap_or(0)
    .checked_add(1)
    .ok_or_else(|| anyhow!("index diagnostic cursor overflow"))?;
    diagnostic.id = cursor;
    append_diagnostic(storage, &diagnostic, fence_token, partition_precondition).await?;
    Ok(diagnostic)
}

pub async fn read_index_diagnostics(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_name: &str,
    severity: &str,
    after_cursor: i64,
    limit: usize,
) -> Result<Vec<IndexDiagnostic>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let records = core_store
        .read_stream(ReadStream {
            stream_id: index_diagnostic_stream_id(tenant_id, bucket_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut diagnostics = Vec::new();
    for record in records {
        if record.record_kind != "index_diagnostic" {
            continue;
        }
        let diagnostic = decode_index_diagnostic_body(&record.payload)?;
        if !index_name.is_empty() && diagnostic.index_name != index_name {
            continue;
        }
        if !severity.is_empty() && diagnostic.severity != severity {
            continue;
        }
        if diagnostic.id <= after_cursor {
            continue;
        }
        diagnostics.push(diagnostic);
    }
    diagnostics.sort_by_key(|diagnostic| diagnostic.id);
    if limit > 0 && diagnostics.len() > limit {
        diagnostics.truncate(limit);
    }
    Ok(diagnostics)
}

async fn append_diagnostic(
    storage: &Storage,
    diagnostic: &IndexDiagnostic,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = index_diagnostic_stream_id(diagnostic.tenant_id, diagnostic.bucket_id);
    let mutation_id = uuid::Uuid::new_v4();
    let payload = encode_index_diagnostic_body(diagnostic, fence_token, mutation_id)?;
    let partition_id = hex::encode(index_diagnostic_partition_id(
        diagnostic.tenant_id,
        diagnostic.bucket_id,
    ));
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "index-diagnostic:{}:{}:{mutation_id}",
                diagnostic.tenant_id, diagnostic.bucket_id,
            ),
            scope_partition: partition_id.clone(),
            committed_by_principal: index_diagnostic_partition_principal(
                diagnostic.tenant_id,
                diagnostic.bucket_id,
            ),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind: "index_diagnostic".to_string(),
                payload,
                idempotency_key: Some(format!(
                    "index-diagnostic:{}:{}:{mutation_id}",
                    diagnostic.tenant_id, diagnostic.bucket_id
                )),
            }],
        })
        .await?;
    Ok(())
}

pub fn index_diagnostic_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index_diagnostic").as_bytes())
}

fn index_diagnostic_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("index_diagnostic:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn index_diagnostic_partition_principal(tenant_id: i64, bucket_id: i64) -> String {
    format!("partition-owner:index_diagnostic:{tenant_id}:{bucket_id}")
}

#[cfg(test)]
pub(crate) async fn read_index_diagnostic_frame_fences_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
) -> Result<Vec<u64>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(ReadStream {
            stream_id: index_diagnostic_stream_id(tenant_id, bucket_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?
        .into_iter()
        .filter(|record| record.record_kind == "index_diagnostic")
        .map(|record| decode_index_diagnostic_body_fence(&record.payload))
        .collect::<Result<Vec<_>>>()?)
}

fn require_index_diagnostic_permit(
    tenant_id: i64,
    bucket_id: i64,
    permit: &PartitionWritePermit,
) -> Result<()> {
    if permit.partition_family != "index_diagnostic"
        || permit.partition_id != hex::encode(index_diagnostic_partition_id(tenant_id, bucket_id))
    {
        return Err(anyhow!(
            "partition write permit does not target this index diagnostic partition"
        ));
    }
    Ok(())
}

fn encode_index_diagnostic_body(
    diagnostic: &IndexDiagnostic,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    encode_deterministic_proto(
        &IndexDiagnosticBodyProto {
            schema: INDEX_DIAGNOSTIC_BODY_SCHEMA.to_string(),
            diagnostic: Some(index_diagnostic_to_proto(diagnostic)?),
            fence_token,
            mutation_id: mutation_id.to_string(),
        },
        "index diagnostic body",
    )
}

fn decode_index_diagnostic_body(bytes: &[u8]) -> Result<IndexDiagnostic> {
    let body =
        decode_deterministic_proto::<IndexDiagnosticBodyProto>(bytes, "index diagnostic body")?;
    if body.schema != INDEX_DIAGNOSTIC_BODY_SCHEMA {
        return Err(anyhow!("index diagnostic body schema mismatch"));
    }
    let diagnostic = index_diagnostic_from_proto(
        body.diagnostic
            .ok_or_else(|| anyhow!("index diagnostic body is missing diagnostic"))?,
    )?;
    Ok(diagnostic)
}

#[cfg(test)]
fn decode_index_diagnostic_body_fence(bytes: &[u8]) -> Result<u64> {
    let body =
        decode_deterministic_proto::<IndexDiagnosticBodyProto>(bytes, "index diagnostic body")?;
    if body.schema != INDEX_DIAGNOSTIC_BODY_SCHEMA {
        return Err(anyhow!("index diagnostic body schema mismatch"));
    }
    Ok(body.fence_token)
}

fn index_diagnostic_to_proto(diagnostic: &IndexDiagnostic) -> Result<IndexDiagnosticProto> {
    Ok(IndexDiagnosticProto {
        id: diagnostic.id,
        tenant_id: diagnostic.tenant_id,
        bucket_id: diagnostic.bucket_id,
        bucket_name: diagnostic.bucket_name.clone(),
        index_id: diagnostic.index_id,
        index_name: diagnostic.index_name.clone(),
        object_key: diagnostic.object_key.clone(),
        version_id: diagnostic.version_id.as_ref().map(ToString::to_string),
        severity: diagnostic.severity.clone(),
        code: diagnostic.code.clone(),
        message: diagnostic.message.clone(),
        details: Some(json_value_to_proto(&diagnostic.details)?),
        created_at_unix_nanos: diagnostic.created_at.timestamp_nanos_opt().ok_or_else(|| {
            anyhow!("index diagnostic timestamp cannot be represented in nanoseconds")
        })?,
    })
}

fn index_diagnostic_from_proto(proto: IndexDiagnosticProto) -> Result<IndexDiagnostic> {
    Ok(IndexDiagnostic {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        bucket_name: proto.bucket_name,
        index_id: proto.index_id,
        index_name: proto.index_name,
        object_key: proto.object_key,
        version_id: proto
            .version_id
            .map(|value| {
                uuid::Uuid::parse_str(&value).context("index diagnostic version_id is not a UUID")
            })
            .transpose()?,
        severity: proto.severity,
        code: proto.code,
        message: proto.message,
        details: json_value_from_proto(
            proto
                .details
                .ok_or_else(|| anyhow!("index diagnostic body is missing details"))?,
        )?,
        created_at: chrono::DateTime::from_timestamp_nanos(proto.created_at_unix_nanos),
    })
}

fn json_value_to_proto(value: &JsonValue) -> Result<IndexDiagnosticJsonValueProto> {
    let kind = match value {
        JsonValue::Null => index_diagnostic_json_value_proto::Kind::Null(true),
        JsonValue::Bool(value) => index_diagnostic_json_value_proto::Kind::Bool(*value),
        JsonValue::Number(number) => {
            if let Some(value) = number.as_i64() {
                index_diagnostic_json_value_proto::Kind::I64(value)
            } else if let Some(value) = number.as_u64() {
                index_diagnostic_json_value_proto::Kind::U64(value)
            } else {
                index_diagnostic_json_value_proto::Kind::F64(number.as_f64().ok_or_else(|| {
                    anyhow!("index diagnostic JSON number cannot be represented deterministically")
                })?)
            }
        }
        JsonValue::String(value) => index_diagnostic_json_value_proto::Kind::String(value.clone()),
        JsonValue::Array(values) => {
            index_diagnostic_json_value_proto::Kind::Array(IndexDiagnosticJsonArrayProto {
                values: values
                    .iter()
                    .map(json_value_to_proto)
                    .collect::<Result<Vec<_>>>()?,
            })
        }
        JsonValue::Object(map) => {
            let mut entries = map
                .iter()
                .map(|(key, value)| {
                    Ok(IndexDiagnosticJsonObjectEntryProto {
                        key: key.clone(),
                        value: Some(json_value_to_proto(value)?),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            entries.sort_by(|left, right| left.key.cmp(&right.key));
            index_diagnostic_json_value_proto::Kind::Object(IndexDiagnosticJsonObjectProto {
                entries,
            })
        }
    };
    Ok(IndexDiagnosticJsonValueProto { kind: Some(kind) })
}

fn json_value_from_proto(proto: IndexDiagnosticJsonValueProto) -> Result<JsonValue> {
    let kind = proto
        .kind
        .ok_or_else(|| anyhow!("index diagnostic JSON value is missing kind"))?;
    Ok(match kind {
        index_diagnostic_json_value_proto::Kind::Null(marker) => {
            if !marker {
                return Err(anyhow!("index diagnostic JSON null marker must be true"));
            }
            JsonValue::Null
        }
        index_diagnostic_json_value_proto::Kind::Bool(value) => JsonValue::Bool(value),
        index_diagnostic_json_value_proto::Kind::I64(value) => JsonValue::Number(value.into()),
        index_diagnostic_json_value_proto::Kind::U64(value) => JsonValue::Number(value.into()),
        index_diagnostic_json_value_proto::Kind::F64(value) => JsonValue::Number(
            serde_json::Number::from_f64(value)
                .ok_or_else(|| anyhow!("index diagnostic JSON f64 is not finite"))?,
        ),
        index_diagnostic_json_value_proto::Kind::String(value) => JsonValue::String(value),
        index_diagnostic_json_value_proto::Kind::Array(array) => JsonValue::Array(
            array
                .values
                .into_iter()
                .map(json_value_from_proto)
                .collect::<Result<Vec<_>>>()?,
        ),
        index_diagnostic_json_value_proto::Kind::Object(object) => {
            let mut previous_key: Option<String> = None;
            let mut map = serde_json::Map::new();
            for entry in object.entries {
                if previous_key
                    .as_ref()
                    .is_some_and(|previous| previous >= &entry.key)
                {
                    return Err(anyhow!(
                        "index diagnostic JSON object entries are not strictly sorted"
                    ));
                }
                previous_key = Some(entry.key.clone());
                let value = entry.value.ok_or_else(|| {
                    anyhow!("index diagnostic JSON object entry is missing value")
                })?;
                map.insert(entry.key, json_value_from_proto(value)?);
            }
            JsonValue::Object(map)
        }
    })
}

fn encode_deterministic_proto<M>(message: &M, label: &str) -> Result<Vec<u8>>
where
    M: Message + Default,
{
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    let decoded = M::decode(bytes.as_slice())?;
    let mut canonical = Vec::with_capacity(decoded.encoded_len());
    decoded.encode(&mut canonical)?;
    if canonical != bytes {
        return Err(anyhow!("{label} is not deterministic protobuf"));
    }
    Ok(bytes)
}

fn decode_deterministic_proto<M>(bytes: &[u8], label: &str) -> Result<M>
where
    M: Message + Default,
{
    let value = M::decode(bytes)?;
    let mut canonical = Vec::with_capacity(value.encoded_len());
    value.encode(&mut canonical)?;
    if canonical != bytes {
        return Err(anyhow!("{label} is not deterministic protobuf"));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"index diagnostic partition owner signing key";

    fn diagnostic(index_name: &str, severity: &str) -> IndexDiagnostic {
        IndexDiagnostic {
            id: 0,
            tenant_id: 42,
            bucket_id: 7,
            bucket_name: "docs".to_string(),
            index_id: Some(10),
            index_name: index_name.to_string(),
            object_key: "doc.txt".to_string(),
            version_id: None,
            severity: severity.to_string(),
            code: "parse_failed".to_string(),
            message: "parse failed".to_string(),
            details: json!({"line": 1}),
            created_at: Utc::now(),
        }
    }

    fn assert_same_diagnostic(left: &IndexDiagnostic, right: &IndexDiagnostic) {
        assert_eq!(left.id, right.id);
        assert_eq!(left.tenant_id, right.tenant_id);
        assert_eq!(left.bucket_id, right.bucket_id);
        assert_eq!(left.bucket_name, right.bucket_name);
        assert_eq!(left.index_id, right.index_id);
        assert_eq!(left.index_name, right.index_name);
        assert_eq!(left.object_key, right.object_key);
        assert_eq!(left.version_id, right.version_id);
        assert_eq!(left.severity, right.severity);
        assert_eq!(left.code, right.code);
        assert_eq!(left.message, right.message);
        assert_eq!(left.details, right.details);
        assert_eq!(left.created_at, right.created_at);
    }

    fn push_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push((value as u8) | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
    }

    fn append_length_delimited(out: &mut Vec<u8>, field_number: u64, value: &[u8]) {
        push_varint(out, (field_number << 3) | 2);
        push_varint(out, value.len() as u64);
        out.extend_from_slice(value);
    }

    async fn ready_diagnostic_permit(
        storage: &Storage,
        owner_node_id: &str,
    ) -> PartitionWritePermit {
        crate::partition_fence::ready_partition_owner_for_test(
            storage,
            "index_diagnostic".to_string(),
            hex::encode(index_diagnostic_partition_id(42, 7)),
            owner_node_id,
            0,
            hex::encode([0; 32]),
            hex::encode([5; 32]),
            PARTITION_OWNER_KEY,
        )
        .await
        .write_permit()
        .unwrap()
    }

    #[tokio::test]
    async fn index_diagnostic_journal_replays_and_filters() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_index_diagnostic(&storage, diagnostic("a", "warning"))
            .await
            .unwrap();
        write_index_diagnostic(&storage, diagnostic("b", "error"))
            .await
            .unwrap();

        let all = read_index_diagnostics(&storage, 42, 7, "", "", 0, 10)
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].id, 1);
        assert_eq!(all[1].id, 2);
        assert_eq!(
            read_index_diagnostics(&storage, 42, 7, "b", "error", 0, 10)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn index_diagnostic_frame_body_is_deterministic_protobuf() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut input = diagnostic("a", "warning");
        input.version_id = Some(uuid::Uuid::from_u128(
            0x12345678_9abc_def0_1234_56789abcdef0,
        ));
        input.details = json!({
            "z": [1, true, null],
            "a": {"nested": "value"},
        });

        let written = write_index_diagnostic(&storage, input).await.unwrap();
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let records = core_store
            .read_stream(ReadStream {
                stream_id: index_diagnostic_stream_id(42, 7),
                after_sequence: 0,
                limit: 0,
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].record_kind, "index_diagnostic");
        assert!(
            !records[0].payload.starts_with(b"{"),
            "index diagnostic stream payload must not use JSON"
        );
        let decoded = decode_index_diagnostic_body(&records[0].payload).unwrap();
        assert_same_diagnostic(&decoded, &written);
    }

    #[test]
    fn index_diagnostic_body_rejects_reencoded_protobuf() {
        let mut value = diagnostic("a", "warning");
        value.id = 1;
        let diagnostic = index_diagnostic_to_proto(&value).unwrap();
        let diagnostic_bytes = encode_deterministic_proto(&diagnostic, "test diagnostic").unwrap();
        let mut reencoded = Vec::new();
        append_length_delimited(&mut reencoded, 2, &diagnostic_bytes);
        append_length_delimited(&mut reencoded, 1, INDEX_DIAGNOSTIC_BODY_SCHEMA.as_bytes());

        let mutation_id = uuid::Uuid::from_u128(0xfeed_beef_feed_beef_feed_beef_feed_beef);
        assert_ne!(
            reencoded,
            encode_index_diagnostic_body(&value, 0, mutation_id).unwrap()
        );
        let err = decode_index_diagnostic_body(&reencoded).unwrap_err();
        assert!(
            err.to_string().contains("not deterministic protobuf"),
            "unexpected re-encoded body error: {err}"
        );
    }

    #[test]
    fn index_diagnostic_body_rejects_schema_mismatch() {
        let mut value = diagnostic("a", "warning");
        value.id = 1;
        let bytes = encode_deterministic_proto(
            &IndexDiagnosticBodyProto {
                schema: "anvil.core.index_diagnostic.wrong.v1".to_string(),
                diagnostic: Some(index_diagnostic_to_proto(&value).unwrap()),
                fence_token: 0,
                mutation_id: uuid::Uuid::nil().to_string(),
            },
            "test index diagnostic body",
        )
        .unwrap();

        let err = decode_index_diagnostic_body(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("schema mismatch"),
            "unexpected schema mismatch error: {err}"
        );
    }

    #[tokio::test]
    async fn index_diagnostic_permit_sets_frame_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_diagnostic_permit(&storage, "node-a").await;

        let written = write_index_diagnostic_with_permit(
            &storage,
            diagnostic("a", "warning"),
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        assert_eq!(written.id, 1);
        let fences = read_index_diagnostic_frame_fences_for_test(&storage, 42, 7)
            .await
            .unwrap();
        assert_eq!(fences, vec![permit.fence_token]);
    }

    #[tokio::test]
    async fn index_diagnostic_rejects_stale_partition_permit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_diagnostic_permit(&storage, "node-a").await;
        let fresh = ready_diagnostic_permit(&storage, "node-b").await;
        assert!(fresh.fence_token > stale.fence_token);

        let rejected = write_index_diagnostic_with_permit(
            &storage,
            diagnostic("a", "warning"),
            &stale,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(rejected.to_string().contains("PartitionNotOwned"));

        write_index_diagnostic_with_permit(
            &storage,
            diagnostic("a", "warning"),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn index_diagnostic_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_diagnostic_permit(&storage, "node-a").await;
        let stale_precondition =
            partition_write_precondition(&storage, &stale, PARTITION_OWNER_KEY)
                .await
                .unwrap();
        let fresh = ready_diagnostic_permit(&storage, "node-b").await;
        assert!(fresh.fence_token > stale.fence_token);

        let rejected = write_index_diagnostic_inner(
            &storage,
            diagnostic("a", "warning"),
            stale.fence_token,
            Some(stale_precondition),
        )
        .await
        .unwrap_err();
        let message = rejected.to_string();
        assert!(
            message.contains("generation mismatch") || message.contains("target mismatch"),
            "unexpected stale precondition error: {message}"
        );

        write_index_diagnostic_with_permit(
            &storage,
            diagnostic("a", "warning"),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }
}
