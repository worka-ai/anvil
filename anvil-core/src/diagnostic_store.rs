use crate::{
    core_store::{
        CF_OBSERVABILITY, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, CoreMetaTuplePart,
        TABLE_DIAGNOSTIC_ROW, commit_coremeta_batch_for_storage, core_meta_committed_row_common,
        core_meta_root_key_hash, core_meta_tuple_key, decode_deterministic_proto,
        encode_deterministic_proto,
    },
    formats::hash32,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
const DIAGNOSTIC_REF_PREFIX: &str = "diagnostic:";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiagnosticObjectRef {
    pub bucket_id: Option<i64>,
    pub object_key: Option<String>,
    pub version_id: Option<String>,
    pub content_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticObject {
    pub format_version: u16,
    pub diagnostic_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub source: String,
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: String,
    pub object_ref: Option<DiagnosticObjectRef>,
    pub details: serde_json::Value,
    pub created_at_nanos: i64,
    pub diagnostic_hash: Option<String>,
    pub diagnostic_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiagnosticWrite {
    pub diagnostic_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub source: String,
    pub severity: DiagnosticSeverity,
    pub code: String,
    pub message: String,
    pub object_ref: Option<DiagnosticObjectRef>,
    pub details: serde_json::Value,
    pub created_at_nanos: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum DiagnosticSeverityProto {
    Unspecified = 0,
    Info = 1,
    Warning = 2,
    Error = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum DiagnosticJsonKindProto {
    Unspecified = 0,
    Null = 1,
    Bool = 2,
    Number = 3,
    String = 4,
    Array = 5,
    Object = 6,
}

#[derive(Clone, PartialEq, Message)]
struct DiagnosticJsonValueProto {
    #[prost(enumeration = "DiagnosticJsonKindProto", tag = "1")]
    kind: i32,
    #[prost(bool, tag = "2")]
    bool_value: bool,
    #[prost(string, tag = "3")]
    number_value: String,
    #[prost(string, tag = "4")]
    string_value: String,
    #[prost(message, repeated, tag = "5")]
    array_values: Vec<DiagnosticJsonValueProto>,
    #[prost(string, repeated, tag = "6")]
    object_keys: Vec<String>,
    #[prost(message, repeated, tag = "7")]
    object_values: Vec<DiagnosticJsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct DiagnosticObjectProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(uint32, tag = "2")]
    format_version: u32,
    #[prost(string, tag = "3")]
    diagnostic_id: String,
    #[prost(string, tag = "4")]
    scope_kind: String,
    #[prost(string, tag = "5")]
    scope_id: String,
    #[prost(string, tag = "6")]
    source: String,
    #[prost(enumeration = "DiagnosticSeverityProto", tag = "7")]
    severity: i32,
    #[prost(string, tag = "8")]
    code: String,
    #[prost(string, tag = "9")]
    message: String,
    #[prost(message, optional, tag = "10")]
    object_ref: Option<DiagnosticObjectRefProto>,
    #[prost(message, optional, tag = "11")]
    details: Option<DiagnosticJsonValueProto>,
    #[prost(int64, tag = "12")]
    created_at_nanos: i64,
    #[prost(string, optional, tag = "13")]
    diagnostic_hash: Option<String>,
    #[prost(string, optional, tag = "14")]
    diagnostic_signature: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct DiagnosticObjectRefProto {
    #[prost(int64, optional, tag = "1")]
    bucket_id: Option<i64>,
    #[prost(string, optional, tag = "2")]
    object_key: Option<String>,
    #[prost(string, optional, tag = "3")]
    version_id: Option<String>,
    #[prost(string, optional, tag = "4")]
    content_hash: Option<String>,
}

impl DiagnosticObject {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_diagnostic(&self)?;
        let hash = hash_diagnostic_object(&self)?;
        let signature = sign_diagnostic_hash(
            signing_key,
            &hash,
            &[
                &self.scope_kind,
                &self.scope_id,
                &self.source,
                &self.diagnostic_id,
            ],
        )?;
        self.diagnostic_hash = Some(hash);
        self.diagnostic_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_diagnostic(self)?;
        let expected_hash = hash_diagnostic_object(self)?;
        if self.diagnostic_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("diagnostic object hash mismatch"));
        }
        let expected_signature = sign_diagnostic_hash(
            signing_key,
            &expected_hash,
            &[
                &self.scope_kind,
                &self.scope_id,
                &self.source,
                &self.diagnostic_id,
            ],
        )?;
        if self.diagnostic_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("diagnostic object signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_diagnostic_object(diagnostic: &DiagnosticObject) -> Result<String> {
    let mut unsigned = diagnostic.clone();
    unsigned.diagnostic_hash = None;
    unsigned.diagnostic_signature = None;
    Ok(hex::encode(hash32(&encode_diagnostic_object(&unsigned)?)))
}

pub async fn write_diagnostic_object(
    storage: &Storage,
    diagnostic: DiagnosticWrite,
    signing_key: &[u8],
) -> Result<DiagnosticObject> {
    validate_write(&diagnostic)?;
    let sealed = DiagnosticObject {
        format_version: 1,
        diagnostic_id: diagnostic.diagnostic_id,
        scope_kind: diagnostic.scope_kind,
        scope_id: diagnostic.scope_id,
        source: diagnostic.source,
        severity: diagnostic.severity,
        code: diagnostic.code,
        message: diagnostic.message,
        object_ref: diagnostic.object_ref,
        details: diagnostic.details,
        created_at_nanos: diagnostic.created_at_nanos,
        diagnostic_hash: None,
        diagnostic_signature: None,
    }
    .seal(signing_key)?;
    write_diagnostic_ref(storage, &sealed).await?;
    Ok(sealed)
}

pub async fn read_diagnostic_object(
    storage: &Storage,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
    signing_key: &[u8],
) -> Result<Option<DiagnosticObject>> {
    let Some(diagnostic) = read_diagnostic_ref(
        storage,
        &diagnostic_ref_name(scope_kind, scope_id, source, diagnostic_id)?,
    )
    .await?
    else {
        return Ok(None);
    };
    diagnostic.verify(signing_key)?;
    if diagnostic.scope_kind != scope_kind
        || diagnostic.scope_id != scope_id
        || diagnostic.source != source
        || diagnostic.diagnostic_id != diagnostic_id
    {
        return Err(anyhow!("diagnostic object path scope mismatch"));
    }
    Ok(Some(diagnostic))
}

pub async fn list_diagnostic_objects(
    storage: &Storage,
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    min_severity: Option<DiagnosticSeverity>,
    signing_key: &[u8],
) -> Result<Vec<DiagnosticObject>> {
    let mut diagnostics = Vec::new();
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    for record in meta.scan_prefix(
        CF_OBSERVABILITY,
        TABLE_DIAGNOSTIC_ROW,
        &diagnostic_tuple_prefix(scope_kind, scope_id, source)?,
    )? {
        let diagnostic = decode_diagnostic_object(&record.payload)?;
        diagnostic.verify(signing_key)?;
        if diagnostic.scope_kind != scope_kind
            || diagnostic.scope_id != scope_id
            || diagnostic.source != source
        {
            return Err(anyhow!("diagnostic object path scope mismatch"));
        }
        if min_severity
            .map(|minimum| severity_rank(diagnostic.severity) < severity_rank(minimum))
            .unwrap_or(false)
        {
            continue;
        }
        diagnostics.push(diagnostic);
    }
    diagnostics.sort_by(|left, right| {
        left.created_at_nanos
            .cmp(&right.created_at_nanos)
            .then(left.diagnostic_id.cmp(&right.diagnostic_id))
    });
    Ok(diagnostics)
}

async fn write_diagnostic_ref(storage: &Storage, diagnostic: &DiagnosticObject) -> Result<()> {
    let tuple_key = diagnostic_tuple_key(
        &diagnostic.scope_kind,
        &diagnostic.scope_id,
        &diagnostic.source,
        &diagnostic.diagnostic_id,
    )?;
    let payload = encode_diagnostic_object(diagnostic)?;
    let op = CoreMetaBatchOp {
        cf: CF_OBSERVABILITY,
        table_id: TABLE_DIAGNOSTIC_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!(
            "diagnostic:{}:{}:{}",
            diagnostic.scope_kind, diagnostic.scope_id, diagnostic.diagnostic_id
        ),
        &[op],
    )
    .await?;
    Ok(())
}

async fn read_diagnostic_ref(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<DiagnosticObject>> {
    let (scope_kind, scope_id, source, diagnostic_id) = parse_diagnostic_ref_name(ref_name)?;
    let tuple_key = diagnostic_tuple_key(&scope_kind, &scope_id, &source, &diagnostic_id)?;
    let Some(bytes) = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_OBSERVABILITY,
        TABLE_DIAGNOSTIC_ROW,
        &tuple_key,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_diagnostic_object(&bytes)?))
}

fn encode_diagnostic_object(diagnostic: &DiagnosticObject) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&diagnostic_to_proto(
        diagnostic,
    )?))
}

fn decode_diagnostic_object(bytes: &[u8]) -> Result<DiagnosticObject> {
    diagnostic_from_proto(decode_deterministic_proto::<DiagnosticObjectProto>(
        bytes,
        "diagnostic object",
    )?)
}

fn diagnostic_to_proto(diagnostic: &DiagnosticObject) -> Result<DiagnosticObjectProto> {
    Ok(DiagnosticObjectProto {
        common: Some(core_meta_committed_row_common(
            "system",
            core_meta_root_key_hash(&format!(
                "diagnostic/{}/{}/{}",
                diagnostic.scope_kind, diagnostic.scope_id, diagnostic.source
            )),
            diagnostic.created_at_nanos.max(0) as u64,
            diagnostic.diagnostic_id.clone(),
            diagnostic.created_at_nanos.max(0) as u64,
        )),
        format_version: u32::from(diagnostic.format_version),
        diagnostic_id: diagnostic.diagnostic_id.clone(),
        scope_kind: diagnostic.scope_kind.clone(),
        scope_id: diagnostic.scope_id.clone(),
        source: diagnostic.source.clone(),
        severity: severity_to_proto(diagnostic.severity) as i32,
        code: diagnostic.code.clone(),
        message: diagnostic.message.clone(),
        object_ref: diagnostic.object_ref.as_ref().map(diagnostic_ref_to_proto),
        details: Some(json_value_to_proto(&diagnostic.details)),
        created_at_nanos: diagnostic.created_at_nanos,
        diagnostic_hash: diagnostic.diagnostic_hash.clone(),
        diagnostic_signature: diagnostic.diagnostic_signature.clone(),
    })
}

fn diagnostic_from_proto(proto: DiagnosticObjectProto) -> Result<DiagnosticObject> {
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("diagnostic object missing CoreMeta common"))?;
    Ok(DiagnosticObject {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("diagnostic object version exceeds u16"))?,
        diagnostic_id: proto.diagnostic_id,
        scope_kind: proto.scope_kind,
        scope_id: proto.scope_id,
        source: proto.source,
        severity: severity_from_proto(proto.severity)?,
        code: proto.code,
        message: proto.message,
        object_ref: proto.object_ref.map(diagnostic_ref_from_proto),
        details: json_value_from_proto(
            proto
                .details
                .ok_or_else(|| anyhow!("diagnostic object missing details"))?,
        )?,
        created_at_nanos: proto.created_at_nanos,
        diagnostic_hash: proto.diagnostic_hash,
        diagnostic_signature: proto.diagnostic_signature,
    })
}

fn diagnostic_ref_to_proto(object_ref: &DiagnosticObjectRef) -> DiagnosticObjectRefProto {
    DiagnosticObjectRefProto {
        bucket_id: object_ref.bucket_id,
        object_key: object_ref.object_key.clone(),
        version_id: object_ref.version_id.clone(),
        content_hash: object_ref.content_hash.clone(),
    }
}

fn diagnostic_ref_from_proto(proto: DiagnosticObjectRefProto) -> DiagnosticObjectRef {
    DiagnosticObjectRef {
        bucket_id: proto.bucket_id,
        object_key: proto.object_key,
        version_id: proto.version_id,
        content_hash: proto.content_hash,
    }
}

fn severity_to_proto(severity: DiagnosticSeverity) -> DiagnosticSeverityProto {
    match severity {
        DiagnosticSeverity::Info => DiagnosticSeverityProto::Info,
        DiagnosticSeverity::Warning => DiagnosticSeverityProto::Warning,
        DiagnosticSeverity::Error => DiagnosticSeverityProto::Error,
    }
}

fn severity_from_proto(severity: i32) -> Result<DiagnosticSeverity> {
    match DiagnosticSeverityProto::try_from(severity)
        .map_err(|_| anyhow!("diagnostic object severity is invalid"))?
    {
        DiagnosticSeverityProto::Info => Ok(DiagnosticSeverity::Info),
        DiagnosticSeverityProto::Warning => Ok(DiagnosticSeverity::Warning),
        DiagnosticSeverityProto::Error => Ok(DiagnosticSeverity::Error),
        DiagnosticSeverityProto::Unspecified => {
            Err(anyhow!("diagnostic object severity is unspecified"))
        }
    }
}

fn json_value_to_proto(value: &serde_json::Value) -> DiagnosticJsonValueProto {
    match value {
        serde_json::Value::Null => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::Null as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Bool(value) => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::Bool as i32,
            bool_value: *value,
            number_value: String::new(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Number(value) => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::Number as i32,
            bool_value: false,
            number_value: value.to_string(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::String(value) => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::String as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: value.clone(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Array(values) => DiagnosticJsonValueProto {
            kind: DiagnosticJsonKindProto::Array as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: String::new(),
            array_values: values.iter().map(json_value_to_proto).collect(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            DiagnosticJsonValueProto {
                kind: DiagnosticJsonKindProto::Object as i32,
                bool_value: false,
                number_value: String::new(),
                string_value: String::new(),
                array_values: Vec::new(),
                object_keys: entries.iter().map(|(key, _)| (*key).clone()).collect(),
                object_values: entries
                    .into_iter()
                    .map(|(_, value)| json_value_to_proto(value))
                    .collect(),
            }
        }
    }
}

fn json_value_from_proto(proto: DiagnosticJsonValueProto) -> Result<serde_json::Value> {
    match DiagnosticJsonKindProto::try_from(proto.kind)
        .map_err(|_| anyhow!("diagnostic detail value kind is invalid"))?
    {
        DiagnosticJsonKindProto::Null => Ok(serde_json::Value::Null),
        DiagnosticJsonKindProto::Bool => Ok(serde_json::Value::Bool(proto.bool_value)),
        DiagnosticJsonKindProto::Number => {
            let parsed = serde_json::from_str::<serde_json::Value>(&proto.number_value)?;
            if !parsed.is_number() {
                return Err(anyhow!("diagnostic detail number is invalid"));
            }
            Ok(parsed)
        }
        DiagnosticJsonKindProto::String => Ok(serde_json::Value::String(proto.string_value)),
        DiagnosticJsonKindProto::Array => Ok(serde_json::Value::Array(
            proto
                .array_values
                .into_iter()
                .map(json_value_from_proto)
                .collect::<Result<Vec<_>>>()?,
        )),
        DiagnosticJsonKindProto::Object => {
            if proto.object_keys.len() != proto.object_values.len() {
                return Err(anyhow!("diagnostic detail object key/value mismatch"));
            }
            if proto
                .object_keys
                .windows(2)
                .any(|window| window[0] >= window[1])
            {
                return Err(anyhow!("diagnostic detail object keys are not canonical"));
            }
            let mut object = serde_json::Map::new();
            for (key, value) in proto.object_keys.into_iter().zip(proto.object_values) {
                object.insert(key, json_value_from_proto(value)?);
            }
            Ok(serde_json::Value::Object(object))
        }
        DiagnosticJsonKindProto::Unspecified => {
            Err(anyhow!("diagnostic detail value kind is unspecified"))
        }
    }
}

fn validate_write(diagnostic: &DiagnosticWrite) -> Result<()> {
    let unsigned = DiagnosticObject {
        format_version: 1,
        diagnostic_id: diagnostic.diagnostic_id.clone(),
        scope_kind: diagnostic.scope_kind.clone(),
        scope_id: diagnostic.scope_id.clone(),
        source: diagnostic.source.clone(),
        severity: diagnostic.severity,
        code: diagnostic.code.clone(),
        message: diagnostic.message.clone(),
        object_ref: diagnostic.object_ref.clone(),
        details: diagnostic.details.clone(),
        created_at_nanos: diagnostic.created_at_nanos,
        diagnostic_hash: None,
        diagnostic_signature: None,
    };
    validate_unsigned_diagnostic(&unsigned)
}

fn validate_unsigned_diagnostic(diagnostic: &DiagnosticObject) -> Result<()> {
    if diagnostic.format_version != 1 {
        return Err(anyhow!("unsupported diagnostic object version"));
    }
    require_safe_component(&diagnostic.diagnostic_id, "diagnostic_id")?;
    require_safe_component(&diagnostic.scope_kind, "scope_kind")?;
    require_safe_component(&diagnostic.scope_id, "scope_id")?;
    require_safe_component(&diagnostic.source, "source")?;
    require_nonempty(&diagnostic.code, "code")?;
    require_nonempty(&diagnostic.message, "message")?;
    if diagnostic.created_at_nanos < 0 {
        return Err(anyhow!("diagnostic object timestamp must be nonnegative"));
    }
    if let Some(object_ref) = diagnostic.object_ref.as_ref() {
        if let Some(content_hash) = object_ref.content_hash.as_ref() {
            validate_optional_hash(content_hash, "content_hash")?;
        }
    }
    Ok(())
}

fn severity_rank(severity: DiagnosticSeverity) -> u8 {
    match severity {
        DiagnosticSeverity::Info => 0,
        DiagnosticSeverity::Warning => 1,
        DiagnosticSeverity::Error => 2,
    }
}

fn sign_diagnostic_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("diagnostic object signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"diagnostic_object");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn validate_optional_hash(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe path component"));
    }
    Ok(())
}

fn diagnostic_ref_prefix(scope_kind: &str, scope_id: &str, source: &str) -> Result<String> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(source, "source")?;
    Ok(format!(
        "{DIAGNOSTIC_REF_PREFIX}scope:{scope_kind}:id:{scope_id}:source:{source}:"
    ))
}

fn diagnostic_ref_name(
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
) -> Result<String> {
    require_safe_component(diagnostic_id, "diagnostic_id")?;
    Ok(format!(
        "{}diagnostic:{diagnostic_id}",
        diagnostic_ref_prefix(scope_kind, scope_id, source)?
    ))
}

fn diagnostic_tuple_prefix(scope_kind: &str, scope_id: &str, source: &str) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(source, "source")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("diagnostic"),
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
        CoreMetaTuplePart::Utf8(source),
    ])
}

fn diagnostic_tuple_key(
    scope_kind: &str,
    scope_id: &str,
    source: &str,
    diagnostic_id: &str,
) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(source, "source")?;
    require_safe_component(diagnostic_id, "diagnostic_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("diagnostic"),
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
        CoreMetaTuplePart::Utf8(source),
        CoreMetaTuplePart::Utf8(diagnostic_id),
    ])
}

fn parse_diagnostic_ref_name(ref_name: &str) -> Result<(String, String, String, String)> {
    let parts = ref_name.split(':').collect::<Vec<_>>();
    if parts.len() != 9
        || parts[0] != DIAGNOSTIC_REF_PREFIX.trim_end_matches(':')
        || parts[1] != "scope"
        || parts[3] != "id"
        || parts[5] != "source"
        || parts[7] != "diagnostic"
    {
        return Err(anyhow!("diagnostic CoreMeta ref name has invalid shape"));
    }
    Ok((
        parts[2].to_string(),
        parts[4].to_string(),
        parts[6].to_string(),
        parts[8].to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"diagnostic object signing key";

    #[tokio::test]
    async fn diagnostic_objects_write_read_and_list_from_coremeta_rows() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = write_diagnostic_object(
            &storage,
            diagnostic("diag-001", 10, DiagnosticSeverity::Info),
            KEY,
        )
        .await
        .unwrap();
        let second = write_diagnostic_object(
            &storage,
            diagnostic("diag-002", 20, DiagnosticSeverity::Error),
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(
            diagnostic_ref_name("bucket", "tenant-1-bucket-2", "full-text", "diag-001").unwrap(),
            "diagnostic:scope:bucket:id:tenant-1-bucket-2:source:full-text:diagnostic:diag-001"
        );

        assert_eq!(
            read_diagnostic_object(
                &storage,
                "bucket",
                "tenant-1-bucket-2",
                "full-text",
                "diag-001",
                KEY,
            )
            .await
            .unwrap()
            .unwrap(),
            first
        );
        assert_eq!(
            list_diagnostic_objects(
                &storage,
                "bucket",
                "tenant-1-bucket-2",
                "full-text",
                None,
                KEY
            )
            .await
            .unwrap(),
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            list_diagnostic_objects(
                &storage,
                "bucket",
                "tenant-1-bucket-2",
                "full-text",
                Some(DiagnosticSeverity::Warning),
                KEY,
            )
            .await
            .unwrap(),
            vec![second]
        );
    }

    #[tokio::test]
    async fn diagnostic_objects_reject_tamper_and_scope_mismatch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_diagnostic_object(
            &storage,
            diagnostic("diag-001", 10, DiagnosticSeverity::Warning),
            KEY,
        )
        .await
        .unwrap();
        let ref_name =
            diagnostic_ref_name("bucket", "tenant-1-bucket-2", "full-text", "diag-001").unwrap();
        let (scope_kind, scope_id, source, diagnostic_id) =
            parse_diagnostic_ref_name(&ref_name).unwrap();
        let tuple_key =
            diagnostic_tuple_key(&scope_kind, &scope_id, &source, &diagnostic_id).unwrap();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let mut value = meta
            .get(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &tuple_key)
            .unwrap()
            .unwrap();
        *value
            .last_mut()
            .expect("stored diagnostic bytes are not empty") ^= 0x01;
        meta.put(CF_OBSERVABILITY, TABLE_DIAGNOSTIC_ROW, &tuple_key, &value)
            .unwrap();
        assert!(
            read_diagnostic_object(
                &storage,
                "bucket",
                "tenant-1-bucket-2",
                "full-text",
                "diag-001",
                KEY,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn diagnostic_objects_reject_unsafe_paths_and_invalid_payloads() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(diagnostic_ref_name("../bucket", "tenant", "source", "diag").is_err());
        assert!(diagnostic_ref_name("bucket", "tenant", "../source", "diag").is_err());
        let mut invalid = diagnostic("diag-001", 10, DiagnosticSeverity::Info);
        invalid.message.clear();
        assert!(
            write_diagnostic_object(&storage, invalid, KEY)
                .await
                .is_err()
        );
        let mut invalid_hash = diagnostic("diag-002", 10, DiagnosticSeverity::Info);
        invalid_hash.object_ref = Some(DiagnosticObjectRef {
            bucket_id: Some(2),
            object_key: Some("a".to_string()),
            version_id: Some(uuid::Uuid::new_v4().to_string()),
            content_hash: Some("not-hex".to_string()),
        });
        assert!(
            write_diagnostic_object(&storage, invalid_hash, KEY)
                .await
                .is_err()
        );
    }

    fn diagnostic(
        id: &str,
        created_at_nanos: i64,
        severity: DiagnosticSeverity,
    ) -> DiagnosticWrite {
        DiagnosticWrite {
            diagnostic_id: id.to_string(),
            scope_kind: "bucket".to_string(),
            scope_id: "tenant-1-bucket-2".to_string(),
            source: "full-text".to_string(),
            severity,
            code: "IndexLag".to_string(),
            message: "index partition is behind source watch cursor".to_string(),
            object_ref: Some(DiagnosticObjectRef {
                bucket_id: Some(2),
                object_key: Some("docs/a.txt".to_string()),
                version_id: Some(uuid::Uuid::new_v4().to_string()),
                content_hash: Some(hex::encode([3; 32])),
            }),
            details: serde_json::json!({"source_cursor": 50, "processed_cursor": 40}),
            created_at_nanos,
        }
    }
}
