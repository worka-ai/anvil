use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde_json::Value as JsonValue;

use crate::core_store::{decode_core_object_ref_target, decode_manifest_locator_proto};

pub(super) fn object_data_target_kind(value: &JsonValue) -> Result<String> {
    if value.get("schema").and_then(JsonValue::as_str) != Some("anvil.core.object_data_target.v1") {
        return Err(anyhow!(
            "object metadata shard map is not a canonical CoreStore object data target"
        ));
    }
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("object metadata shard map kind is missing"))?;
    match kind {
        "logical_file" | "object_ref" => Ok(kind.to_string()),
        other => Err(anyhow!("unsupported object data target kind {other}")),
    }
}

pub(super) fn object_data_target_bytes(value: &JsonValue) -> Result<Vec<u8>> {
    let kind = object_data_target_kind(value)?;
    let target = value
        .get("target")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("object metadata shard map target is missing"))?;
    match kind.as_str() {
        "logical_file" => {
            let bytes = URL_SAFE_NO_PAD
                .decode(target)
                .context("object metadata logical-file target is not base64url")?;
            decode_manifest_locator_proto(&bytes)?;
            Ok(bytes)
        }
        "object_ref" => {
            decode_core_object_ref_target(target)?;
            Ok(target.as_bytes().to_vec())
        }
        other => Err(anyhow!("unsupported object data target kind {other}")),
    }
}

pub(super) fn shard_map_from_object_data_target(kind: &str, target: &[u8]) -> Result<JsonValue> {
    match kind {
        "logical_file" => {
            decode_manifest_locator_proto(target)?;
            Ok(serde_json::json!({
                "schema": "anvil.core.object_data_target.v1",
                "kind": "logical_file",
                "target": URL_SAFE_NO_PAD.encode(target),
            }))
        }
        "object_ref" => {
            let target = std::str::from_utf8(target)
                .context("object metadata object-ref target is not UTF-8")?;
            decode_core_object_ref_target(target)?;
            Ok(serde_json::json!({
                "schema": "anvil.core.object_data_target.v1",
                "kind": "object_ref",
                "target": target,
            }))
        }
        other => Err(anyhow!("unsupported object data target kind {other}")),
    }
}
