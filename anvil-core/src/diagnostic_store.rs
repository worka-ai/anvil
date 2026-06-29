use crate::{formats::hash32, storage::Storage};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::io::ErrorKind;
use std::path::Path;

type HmacSha256 = Hmac<Sha256>;

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
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
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
    let path = storage.diagnostic_object_path(
        &sealed.scope_kind,
        &sealed.scope_id,
        &sealed.source,
        &sealed.diagnostic_id,
    )?;
    write_json_atomically(&path, &sealed).await?;
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
    let path = storage.diagnostic_object_path(scope_kind, scope_id, source, diagnostic_id)?;
    let Some(diagnostic) = read_json_optional::<DiagnosticObject>(&path).await? else {
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
    let dir = storage.diagnostic_source_dir(scope_kind, scope_id, source)?;
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).with_context(|| format!("read {}", dir.display())),
    };
    let mut diagnostics = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let diagnostic: DiagnosticObject = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
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

async fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| format!("write temporary diagnostic object {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish diagnostic object {}", path.display()))?;
    Ok(())
}

async fn read_json_optional<T>(path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    Ok(Some(serde_json::from_slice(&bytes)?))
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
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe path component"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"diagnostic object signing key";

    #[tokio::test]
    async fn diagnostic_objects_write_read_and_list_from_internal_paths() {
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
        let path = storage
            .diagnostic_object_path("bucket", "tenant-1-bucket-2", "full-text", "diag-001")
            .unwrap();
        assert!(
            path.ends_with("_anvil/diagnostics/bucket/tenant-1-bucket-2/full-text/diag-001.json")
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
        let path = storage
            .diagnostic_object_path("bucket", "tenant-1-bucket-2", "full-text", "diag-001")
            .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["message"] = serde_json::json!("changed");
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
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
        assert!(
            storage
                .diagnostic_object_path("../bucket", "tenant", "source", "diag")
                .is_err()
        );
        assert!(
            storage
                .diagnostic_object_path("bucket", "tenant", "../source", "diag")
                .is_err()
        );
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
