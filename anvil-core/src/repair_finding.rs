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
pub enum RepairFindingSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairFindingStatus {
    Open,
    RebuiltDerivedIndex,
    RepairedManifest,
    RequiresOperatorReview,
    Irreparable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairActionKind {
    VerifyOnly,
    RebuildDerivedIndex,
    RebuildDirectoryIndex,
    RepairManifestFromSegments,
    SynthesizeCommittedObjectVersion,
    SynthesizePersonalDbCommit,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepairSubjectRef {
    pub subject_kind: String,
    pub subject_id: String,
    pub generation: Option<u64>,
    pub cursor: Option<u128>,
    pub expected_hash: Option<String>,
    pub actual_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairFinding {
    pub format_version: u16,
    pub finding_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub repair_task_id: String,
    pub lease_fence_token: u64,
    pub severity: RepairFindingSeverity,
    pub status: RepairFindingStatus,
    pub code: String,
    pub message: String,
    pub subjects: Vec<RepairSubjectRef>,
    pub proposed_action: RepairActionKind,
    pub evidence: serde_json::Value,
    pub created_at_nanos: i64,
    pub finding_hash: Option<String>,
    pub finding_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepairFindingWrite {
    pub finding_id: String,
    pub scope_kind: String,
    pub scope_id: String,
    pub repair_task_id: String,
    pub lease_fence_token: u64,
    pub severity: RepairFindingSeverity,
    pub status: RepairFindingStatus,
    pub code: String,
    pub message: String,
    pub subjects: Vec<RepairSubjectRef>,
    pub proposed_action: RepairActionKind,
    pub evidence: serde_json::Value,
    pub created_at_nanos: i64,
}

impl RepairFinding {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_finding(&self)?;
        let hash = hash_repair_finding(&self)?;
        let signature = sign_finding_hash(
            signing_key,
            &hash,
            &[
                &self.scope_kind,
                &self.scope_id,
                &self.repair_task_id,
                &self.finding_id,
            ],
        )?;
        self.finding_hash = Some(hash);
        self.finding_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_finding(self)?;
        let expected_hash = hash_repair_finding(self)?;
        if self.finding_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("repair finding hash mismatch"));
        }
        let expected_signature = sign_finding_hash(
            signing_key,
            &expected_hash,
            &[
                &self.scope_kind,
                &self.scope_id,
                &self.repair_task_id,
                &self.finding_id,
            ],
        )?;
        if self.finding_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("repair finding signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_repair_finding(finding: &RepairFinding) -> Result<String> {
    let mut unsigned = finding.clone();
    unsigned.finding_hash = None;
    unsigned.finding_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub async fn write_repair_finding(
    storage: &Storage,
    finding: RepairFindingWrite,
    signing_key: &[u8],
) -> Result<RepairFinding> {
    validate_write(&finding)?;
    let sealed = RepairFinding {
        format_version: 1,
        finding_id: finding.finding_id,
        scope_kind: finding.scope_kind,
        scope_id: finding.scope_id,
        repair_task_id: finding.repair_task_id,
        lease_fence_token: finding.lease_fence_token,
        severity: finding.severity,
        status: finding.status,
        code: finding.code,
        message: finding.message,
        subjects: finding.subjects,
        proposed_action: finding.proposed_action,
        evidence: finding.evidence,
        created_at_nanos: finding.created_at_nanos,
        finding_hash: None,
        finding_signature: None,
    }
    .seal(signing_key)?;
    let path =
        storage.repair_finding_path(&sealed.scope_kind, &sealed.scope_id, &sealed.finding_id)?;
    write_json_atomically(&path, &sealed).await?;
    Ok(sealed)
}

pub async fn read_repair_finding(
    storage: &Storage,
    scope_kind: &str,
    scope_id: &str,
    finding_id: &str,
    signing_key: &[u8],
) -> Result<Option<RepairFinding>> {
    let path = storage.repair_finding_path(scope_kind, scope_id, finding_id)?;
    let Some(finding) = read_json_optional::<RepairFinding>(&path).await? else {
        return Ok(None);
    };
    finding.verify(signing_key)?;
    if finding.scope_kind != scope_kind
        || finding.scope_id != scope_id
        || finding.finding_id != finding_id
    {
        return Err(anyhow!("repair finding path scope mismatch"));
    }
    Ok(Some(finding))
}

pub async fn list_repair_findings(
    storage: &Storage,
    scope_kind: &str,
    scope_id: &str,
    signing_key: &[u8],
) -> Result<Vec<RepairFinding>> {
    let dir = storage.repair_finding_dir(scope_kind, scope_id)?;
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err).with_context(|| format!("read {}", dir.display())),
    };
    let mut findings = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        if entry.file_type().await?.is_dir() {
            continue;
        }
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let finding: RepairFinding = serde_json::from_slice(&tokio::fs::read(&path).await?)?;
        finding.verify(signing_key)?;
        if finding.scope_kind != scope_kind || finding.scope_id != scope_id {
            return Err(anyhow!("repair finding path scope mismatch"));
        }
        findings.push(finding);
    }
    findings.sort_by(|left, right| {
        left.created_at_nanos
            .cmp(&right.created_at_nanos)
            .then(left.finding_id.cmp(&right.finding_id))
    });
    Ok(findings)
}

pub fn validate_repair_action(action: RepairActionKind) -> Result<()> {
    match action {
        RepairActionKind::SynthesizeCommittedObjectVersion
        | RepairActionKind::SynthesizePersonalDbCommit => Err(anyhow!(
            "repair action cannot synthesize committed object versions or PersonalDB commits"
        )),
        RepairActionKind::VerifyOnly
        | RepairActionKind::RebuildDerivedIndex
        | RepairActionKind::RebuildDirectoryIndex
        | RepairActionKind::RepairManifestFromSegments => Ok(()),
    }
}

fn validate_write(finding: &RepairFindingWrite) -> Result<()> {
    let unsigned = RepairFinding {
        format_version: 1,
        finding_id: finding.finding_id.clone(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        repair_task_id: finding.repair_task_id.clone(),
        lease_fence_token: finding.lease_fence_token,
        severity: finding.severity,
        status: finding.status,
        code: finding.code.clone(),
        message: finding.message.clone(),
        subjects: finding.subjects.clone(),
        proposed_action: finding.proposed_action,
        evidence: finding.evidence.clone(),
        created_at_nanos: finding.created_at_nanos,
        finding_hash: None,
        finding_signature: None,
    };
    validate_unsigned_finding(&unsigned)
}

fn validate_unsigned_finding(finding: &RepairFinding) -> Result<()> {
    if finding.format_version != 1 {
        return Err(anyhow!("unsupported repair finding version"));
    }
    require_safe_component(&finding.finding_id, "finding_id")?;
    require_safe_component(&finding.scope_kind, "scope_kind")?;
    require_safe_component(&finding.scope_id, "scope_id")?;
    require_safe_component(&finding.repair_task_id, "repair_task_id")?;
    if finding.lease_fence_token == 0 {
        return Err(anyhow!("repair finding lease fence token must be nonzero"));
    }
    require_nonempty(&finding.code, "code")?;
    require_nonempty(&finding.message, "message")?;
    if finding.subjects.is_empty() {
        return Err(anyhow!("repair finding must include at least one subject"));
    }
    for subject in &finding.subjects {
        validate_subject(subject)?;
    }
    validate_repair_action(finding.proposed_action)?;
    if finding.created_at_nanos < 0 {
        return Err(anyhow!("repair finding timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_subject(subject: &RepairSubjectRef) -> Result<()> {
    require_safe_component(&subject.subject_kind, "subject_kind")?;
    require_nonempty(&subject.subject_id, "subject_id")?;
    if let Some(expected_hash) = subject.expected_hash.as_ref() {
        validate_hex32(expected_hash, "expected_hash")?;
    }
    if let Some(actual_hash) = subject.actual_hash.as_ref() {
        validate_hex32(actual_hash, "actual_hash")?;
    }
    Ok(())
}

fn sign_finding_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("repair finding signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"repair_finding");
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
        .with_context(|| format!("write temporary repair finding {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish repair finding {}", path.display()))?;
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

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
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

    const KEY: &[u8] = b"repair finding signing key";

    #[tokio::test]
    async fn repair_findings_write_read_and_list() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = write_repair_finding(&storage, finding("finding-001", 10), KEY)
            .await
            .unwrap();
        let second = write_repair_finding(&storage, finding("finding-002", 20), KEY)
            .await
            .unwrap();
        let path = storage
            .repair_finding_path("bucket", "tenant-1-bucket-2", "finding-001")
            .unwrap();
        assert!(path.ends_with("_anvil/repair/findings/bucket/tenant-1-bucket-2/finding-001.json"));
        assert_eq!(
            read_repair_finding(&storage, "bucket", "tenant-1-bucket-2", "finding-001", KEY)
                .await
                .unwrap()
                .unwrap(),
            first
        );
        assert_eq!(
            list_repair_findings(&storage, "bucket", "tenant-1-bucket-2", KEY)
                .await
                .unwrap(),
            vec![first, second]
        );
    }

    #[tokio::test]
    async fn repair_findings_reject_tamper_and_unsafe_paths() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_repair_finding(&storage, finding("finding-001", 10), KEY)
            .await
            .unwrap();
        let path = storage
            .repair_finding_path("bucket", "tenant-1-bucket-2", "finding-001")
            .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["message"] = serde_json::json!("changed");
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();
        assert!(
            read_repair_finding(&storage, "bucket", "tenant-1-bucket-2", "finding-001", KEY)
                .await
                .is_err()
        );
        assert!(
            storage
                .repair_finding_path("../bucket", "scope", "finding")
                .is_err()
        );
        assert!(
            storage
                .repair_finding_path("bucket", "scope", "../finding")
                .is_err()
        );
    }

    #[test]
    fn repair_actions_reject_synthesis_of_committed_state() {
        assert!(validate_repair_action(RepairActionKind::VerifyOnly).is_ok());
        assert!(validate_repair_action(RepairActionKind::RebuildDerivedIndex).is_ok());
        assert!(validate_repair_action(RepairActionKind::RepairManifestFromSegments).is_ok());
        assert!(
            validate_repair_action(RepairActionKind::SynthesizeCommittedObjectVersion).is_err()
        );
        assert!(validate_repair_action(RepairActionKind::SynthesizePersonalDbCommit).is_err());
    }

    #[tokio::test]
    async fn repair_findings_reject_invalid_payloads() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut invalid = finding("finding-001", 10);
        invalid.subjects.clear();
        assert!(write_repair_finding(&storage, invalid, KEY).await.is_err());
        let mut invalid_hash = finding("finding-002", 10);
        invalid_hash.subjects[0].actual_hash = Some("not-hex".to_string());
        assert!(
            write_repair_finding(&storage, invalid_hash, KEY)
                .await
                .is_err()
        );
        let mut prohibited = finding("finding-003", 10);
        prohibited.proposed_action = RepairActionKind::SynthesizePersonalDbCommit;
        assert!(
            write_repair_finding(&storage, prohibited, KEY)
                .await
                .is_err()
        );
    }

    fn finding(id: &str, created_at_nanos: i64) -> RepairFindingWrite {
        RepairFindingWrite {
            finding_id: id.to_string(),
            scope_kind: "bucket".to_string(),
            scope_id: "tenant-1-bucket-2".to_string(),
            repair_task_id: "repair-task-a".to_string(),
            lease_fence_token: 7,
            severity: RepairFindingSeverity::Error,
            status: RepairFindingStatus::Open,
            code: "SegmentHashMismatch".to_string(),
            message: "segment hash does not match manifest entry".to_string(),
            subjects: vec![RepairSubjectRef {
                subject_kind: "metadata_segment".to_string(),
                subject_id: "generation-7".to_string(),
                generation: Some(7),
                cursor: Some(42),
                expected_hash: Some(hex::encode([1; 32])),
                actual_hash: Some(hex::encode([2; 32])),
            }],
            proposed_action: RepairActionKind::RebuildDerivedIndex,
            evidence: serde_json::json!({"manifest_generation": 7}),
            created_at_nanos,
        }
    }
}
