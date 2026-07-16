use crate::{
    core_store::{
        AuthzScopeRef, CORE_META_MAX_INLINE_PAYLOAD_BYTES, CoreByteRange,
        CoreMetaInlineOrLocatorProto, CoreMetaLocatorProto, CoreMetaRowCommonProto,
        CorePipelinePolicy, CorePrefetchPolicy, CoreStore, CoreTraceContext,
        ReadLogicalRangeRequest, WriteLogicalFileRequest, core_meta_locator_from_manifest_locator,
        core_meta_locator_to_manifest_locator, encode_deterministic_proto, sha256_hex,
    },
    formats::writer::WriterFamily,
    storage::Storage,
};
use anyhow::{Result, anyhow, bail};
use prost::Message;

const AUTHZ_STORED_PAYLOAD_ROW_SCHEMA: &str = "anvil.authz.coremeta_payload_row.v1";

#[derive(Clone, PartialEq, Message)]
struct AuthzStoredPayloadRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    payload_kind: String,
    #[prost(string, tag = "4")]
    payload_hash: String,
    #[prost(uint64, tag = "5")]
    payload_length: u64,
    #[prost(message, optional, tag = "6")]
    payload: Option<CoreMetaInlineOrLocatorProto>,
}

pub(crate) async fn encode_authz_payload_row(
    storage: &Storage,
    common: CoreMetaRowCommonProto,
    payload_kind: &str,
    stable_name: &str,
    generation: u64,
    transaction_id: &str,
    payload: Vec<u8>,
) -> Result<Vec<u8>> {
    validate_payload_args(payload_kind, stable_name, transaction_id, generation)?;
    let payload_hash = format!("sha256:{}", sha256_hex(&payload));
    let payload_length = payload.len() as u64;
    let payload = if payload.len() <= CORE_META_MAX_INLINE_PAYLOAD_BYTES {
        CoreMetaInlineOrLocatorProto {
            inline_payload: payload,
            locator: None,
        }
    } else {
        CoreMetaInlineOrLocatorProto {
            inline_payload: Vec::new(),
            locator: Some(
                write_authz_payload_locator(
                    storage,
                    payload_kind,
                    stable_name,
                    generation,
                    transaction_id,
                    payload,
                )
                .await?,
            ),
        }
    };
    Ok(encode_deterministic_proto(&AuthzStoredPayloadRowProto {
        common: Some(common),
        schema: AUTHZ_STORED_PAYLOAD_ROW_SCHEMA.to_string(),
        payload_kind: payload_kind.to_string(),
        payload_hash,
        payload_length,
        payload: Some(payload),
    }))
}

pub(crate) async fn decode_authz_payload_row(
    storage: &Storage,
    tenant_id: i64,
    row_bytes: &[u8],
    expected_payload_kind: &str,
) -> Result<Vec<u8>> {
    let row = crate::core_store::decode_deterministic_proto::<AuthzStoredPayloadRowProto>(
        row_bytes,
        "authorization CoreMeta payload row",
    )?;
    if row.schema != AUTHZ_STORED_PAYLOAD_ROW_SCHEMA {
        bail!("authorization CoreMeta payload row has unsupported schema");
    }
    if row.payload_kind != expected_payload_kind {
        bail!("authorization CoreMeta payload row kind mismatch");
    }
    row.common
        .as_ref()
        .ok_or_else(|| anyhow!("authorization CoreMeta payload row missing common"))?;
    let payload_ref = row
        .payload
        .ok_or_else(|| anyhow!("authorization CoreMeta payload row missing payload"))?;
    let payload = match (payload_ref.inline_payload.is_empty(), payload_ref.locator) {
        (false, None) => payload_ref.inline_payload,
        (true, Some(locator)) => read_authz_payload_locator(storage, tenant_id, &locator).await?,
        (false, Some(_)) => bail!("authorization CoreMeta payload row mixes inline and locator"),
        (true, None) => Vec::new(),
    };
    if payload.len() as u64 != row.payload_length {
        bail!("authorization CoreMeta payload length mismatch");
    }
    if format!("sha256:{}", sha256_hex(&payload)) != row.payload_hash {
        bail!("authorization CoreMeta payload hash mismatch");
    }
    Ok(payload)
}

fn validate_payload_args(
    payload_kind: &str,
    stable_name: &str,
    transaction_id: &str,
    generation: u64,
) -> Result<()> {
    if payload_kind.is_empty()
        || stable_name.is_empty()
        || transaction_id.is_empty()
        || payload_kind.contains('\0')
        || stable_name.contains('\0')
        || transaction_id.contains('\0')
        || payload_kind.contains("..")
        || stable_name.contains("..")
        || transaction_id.contains("..")
    {
        bail!("authorization CoreMeta payload identity is invalid");
    }
    if generation == 0 {
        bail!("authorization CoreMeta payload generation must be nonzero");
    }
    Ok(())
}

async fn write_authz_payload_locator(
    storage: &Storage,
    payload_kind: &str,
    stable_name: &str,
    generation: u64,
    transaction_id: &str,
    payload: Vec<u8>,
) -> Result<CoreMetaLocatorProto> {
    let store = CoreStore::new(storage.clone()).await?;
    let logical_file_id = format!("authz/{payload_kind}/{stable_name}/{generation}");
    let write = store
        .write_logical_file_with_locator(WriteLogicalFileRequest {
            writer_family: WriterFamily::Authz.as_str().to_string(),
            generation,
            logical_file_id,
            source: payload,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: transaction_id.to_string(),
            region_id: "local".to_string(),
        })
        .await?;
    core_meta_locator_from_manifest_locator(&write.locator)
}

async fn read_authz_payload_locator(
    storage: &Storage,
    tenant_id: i64,
    locator: &CoreMetaLocatorProto,
) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let manifest_locator = core_meta_locator_to_manifest_locator(locator)?;
    let manifest = store.read_logical_file_manifest(&manifest_locator).await?;
    store
        .read_logical_range(ReadLogicalRangeRequest {
            ranges: vec![CoreByteRange {
                start: 0,
                end_exclusive: manifest.logical_size,
            }],
            manifest,
            authz_scope: AuthzScopeRef {
                anvil_storage_tenant_id: tenant_id.to_string(),
                authz_realm_id: format!("tenant/{tenant_id}"),
            },
            expected_boundary: None,
            prefetch_policy: CorePrefetchPolicy::default(),
            trace_context: CoreTraceContext::default(),
        })
        .await
}
