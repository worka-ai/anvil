use crate::{
    core_store::{
        CF_MESH, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaRowCommonProto, CoreMetaStore,
        CoreMetaTuplePart, TABLE_REPAIR_FINDING_ROW, commit_coremeta_batch_for_storage,
        core_meta_committed_row_common, core_meta_root_key_hash, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
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
const REPAIR_FINDING_REF_PREFIX: &str = "repair_finding:";

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

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RepairFindingSeverityProto {
    Unspecified = 0,
    Info = 1,
    Warning = 2,
    Error = 3,
    Critical = 4,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RepairFindingStatusProto {
    Unspecified = 0,
    Open = 1,
    RebuiltDerivedIndex = 2,
    RepairedManifest = 3,
    RequiresOperatorReview = 4,
    Irreparable = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RepairActionKindProto {
    Unspecified = 0,
    VerifyOnly = 1,
    RebuildDerivedIndex = 2,
    RebuildDirectoryIndex = 3,
    RepairManifestFromSegments = 4,
    SynthesizeCommittedObjectVersion = 5,
    SynthesizePersonalDbCommit = 6,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RepairJsonKindProto {
    Unspecified = 0,
    Null = 1,
    Bool = 2,
    Number = 3,
    String = 4,
    Array = 5,
    Object = 6,
}

#[derive(Clone, PartialEq, Message)]
struct RepairJsonValueProto {
    #[prost(enumeration = "RepairJsonKindProto", tag = "1")]
    kind: i32,
    #[prost(bool, tag = "2")]
    bool_value: bool,
    #[prost(string, tag = "3")]
    number_value: String,
    #[prost(string, tag = "4")]
    string_value: String,
    #[prost(message, repeated, tag = "5")]
    array_values: Vec<RepairJsonValueProto>,
    #[prost(string, repeated, tag = "6")]
    object_keys: Vec<String>,
    #[prost(message, repeated, tag = "7")]
    object_values: Vec<RepairJsonValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct RepairFindingProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(uint32, tag = "2")]
    format_version: u32,
    #[prost(string, tag = "3")]
    finding_id: String,
    #[prost(string, tag = "4")]
    scope_kind: String,
    #[prost(string, tag = "5")]
    scope_id: String,
    #[prost(string, tag = "6")]
    repair_task_id: String,
    #[prost(uint64, tag = "7")]
    lease_fence_token: u64,
    #[prost(enumeration = "RepairFindingSeverityProto", tag = "8")]
    severity: i32,
    #[prost(enumeration = "RepairFindingStatusProto", tag = "9")]
    status: i32,
    #[prost(string, tag = "10")]
    code: String,
    #[prost(string, tag = "11")]
    message: String,
    #[prost(message, repeated, tag = "12")]
    subjects: Vec<RepairSubjectRefProto>,
    #[prost(enumeration = "RepairActionKindProto", tag = "13")]
    proposed_action: i32,
    #[prost(message, optional, tag = "14")]
    evidence: Option<RepairJsonValueProto>,
    #[prost(int64, tag = "15")]
    created_at_nanos: i64,
    #[prost(string, optional, tag = "16")]
    finding_hash: Option<String>,
    #[prost(string, optional, tag = "17")]
    finding_signature: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct RepairSubjectRefProto {
    #[prost(string, tag = "1")]
    subject_kind: String,
    #[prost(string, tag = "2")]
    subject_id: String,
    #[prost(uint64, optional, tag = "3")]
    generation: Option<u64>,
    #[prost(string, optional, tag = "4")]
    cursor: Option<String>,
    #[prost(string, optional, tag = "5")]
    expected_hash: Option<String>,
    #[prost(string, optional, tag = "6")]
    actual_hash: Option<String>,
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
    Ok(hex::encode(hash32(&encode_repair_finding(&unsigned)?)))
}

pub async fn write_repair_finding(
    storage: &Storage,
    finding: RepairFindingWrite,
    signing_key: &[u8],
) -> Result<RepairFinding> {
    let repair_started_at = std::time::Instant::now();
    validate_write(&finding)?;
    let metric_scope_kind = finding.scope_kind.clone();
    let metric_status = repair_finding_status_name(finding.status);
    let metric_severity = repair_finding_severity_name(finding.severity);
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
    write_repair_finding_ref(storage, &sealed).await?;
    crate::perf::record_repair_duration(
        sealed.code.as_str(),
        sealed.scope_kind.as_str(),
        metric_status,
        repair_started_at.elapsed(),
    );
    crate::perf::record_anti_entropy_findings_total(
        sealed.code.as_str(),
        metric_scope_kind.as_str(),
        metric_severity,
        1,
    );
    Ok(sealed)
}

pub async fn read_repair_finding(
    storage: &Storage,
    scope_kind: &str,
    scope_id: &str,
    finding_id: &str,
    signing_key: &[u8],
) -> Result<Option<RepairFinding>> {
    let Some(finding) = read_repair_finding_ref(
        storage,
        &repair_finding_ref_name(scope_kind, scope_id, finding_id)?,
    )
    .await?
    else {
        return Ok(None);
    };
    finding.verify(signing_key)?;
    if finding.scope_kind != scope_kind
        || finding.scope_id != scope_id
        || finding.finding_id != finding_id
    {
        return Err(anyhow!("repair finding ref scope mismatch"));
    }
    Ok(Some(finding))
}

pub async fn list_repair_findings(
    storage: &Storage,
    scope_kind: &str,
    scope_id: &str,
    signing_key: &[u8],
) -> Result<Vec<RepairFinding>> {
    let mut findings = Vec::new();
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    for record in meta.scan_prefix(
        CF_MESH,
        TABLE_REPAIR_FINDING_ROW,
        &repair_finding_tuple_prefix(scope_kind, scope_id)?,
    )? {
        let finding = decode_repair_finding(&record.payload)?;
        finding.verify(signing_key)?;
        if finding.scope_kind != scope_kind || finding.scope_id != scope_id {
            return Err(anyhow!("repair finding ref scope mismatch"));
        }
        findings.push(finding);
    }
    findings.sort_by(|left, right| {
        left.created_at_nanos
            .cmp(&right.created_at_nanos)
            .then(left.finding_id.cmp(&right.finding_id))
    });
    let open_depth = findings
        .iter()
        .filter(|finding| {
            matches!(
                finding.status,
                RepairFindingStatus::Open | RepairFindingStatus::RequiresOperatorReview
            )
        })
        .count() as u64;
    crate::perf::record_repair_queue_depth(scope_kind, "local", "local", open_depth);
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

fn repair_finding_severity_name(severity: RepairFindingSeverity) -> &'static str {
    match severity {
        RepairFindingSeverity::Info => "info",
        RepairFindingSeverity::Warning => "warning",
        RepairFindingSeverity::Error => "error",
        RepairFindingSeverity::Critical => "critical",
    }
}

fn repair_finding_status_name(status: RepairFindingStatus) -> &'static str {
    match status {
        RepairFindingStatus::Open => "open",
        RepairFindingStatus::RebuiltDerivedIndex => "rebuilt_derived_index",
        RepairFindingStatus::RepairedManifest => "repaired_manifest",
        RepairFindingStatus::RequiresOperatorReview => "requires_operator_review",
        RepairFindingStatus::Irreparable => "irreparable",
    }
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
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe path component"));
    }
    Ok(())
}

async fn write_repair_finding_ref(storage: &Storage, finding: &RepairFinding) -> Result<()> {
    let tuple_key =
        repair_finding_tuple_key(&finding.scope_kind, &finding.scope_id, &finding.finding_id)?;
    let payload = encode_repair_finding(finding)?;
    let op = CoreMetaBatchOp {
        cf: CF_MESH,
        table_id: TABLE_REPAIR_FINDING_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!(
            "repair-finding:{}:{}:{}",
            finding.scope_kind, finding.scope_id, finding.finding_id
        ),
        &[op],
    )
    .await?;
    Ok(())
}

async fn read_repair_finding_ref(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<RepairFinding>> {
    let (scope_kind, scope_id, finding_id) = parse_repair_finding_ref_name(ref_name)?;
    let tuple_key = repair_finding_tuple_key(&scope_kind, &scope_id, &finding_id)?;
    let Some(bytes) = CoreMetaStore::open(storage.core_store_meta_path())?.get(
        CF_MESH,
        TABLE_REPAIR_FINDING_ROW,
        &tuple_key,
    )?
    else {
        return Ok(None);
    };
    Ok(Some(decode_repair_finding(&bytes)?))
}

fn encode_repair_finding(finding: &RepairFinding) -> Result<Vec<u8>> {
    Ok(encode_deterministic_proto(&repair_finding_to_proto(
        finding,
    )?))
}

fn decode_repair_finding(bytes: &[u8]) -> Result<RepairFinding> {
    repair_finding_from_proto(decode_deterministic_proto::<RepairFindingProto>(
        bytes,
        "repair finding",
    )?)
}

fn repair_finding_to_proto(finding: &RepairFinding) -> Result<RepairFindingProto> {
    Ok(RepairFindingProto {
        common: Some(repair_finding_common(finding)?),
        format_version: u32::from(finding.format_version),
        finding_id: finding.finding_id.clone(),
        scope_kind: finding.scope_kind.clone(),
        scope_id: finding.scope_id.clone(),
        repair_task_id: finding.repair_task_id.clone(),
        lease_fence_token: finding.lease_fence_token,
        severity: severity_to_proto(finding.severity) as i32,
        status: status_to_proto(finding.status) as i32,
        code: finding.code.clone(),
        message: finding.message.clone(),
        subjects: finding.subjects.iter().map(subject_to_proto).collect(),
        proposed_action: action_to_proto(finding.proposed_action) as i32,
        evidence: Some(json_value_to_proto(&finding.evidence)),
        created_at_nanos: finding.created_at_nanos,
        finding_hash: finding.finding_hash.clone(),
        finding_signature: finding.finding_signature.clone(),
    })
}

fn repair_finding_from_proto(proto: RepairFindingProto) -> Result<RepairFinding> {
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("repair finding missing CoreMeta common row fields"))?;
    validate_repair_finding_common(&proto, &common)?;
    Ok(RepairFinding {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("repair finding version exceeds u16"))?,
        finding_id: proto.finding_id,
        scope_kind: proto.scope_kind,
        scope_id: proto.scope_id,
        repair_task_id: proto.repair_task_id,
        lease_fence_token: proto.lease_fence_token,
        severity: severity_from_proto(proto.severity)?,
        status: status_from_proto(proto.status)?,
        code: proto.code,
        message: proto.message,
        subjects: proto
            .subjects
            .into_iter()
            .map(subject_from_proto)
            .collect::<Result<Vec<_>>>()?,
        proposed_action: action_from_proto(proto.proposed_action)?,
        evidence: json_value_from_proto(
            proto
                .evidence
                .ok_or_else(|| anyhow!("repair finding missing evidence"))?,
        )?,
        created_at_nanos: proto.created_at_nanos,
        finding_hash: proto.finding_hash,
        finding_signature: proto.finding_signature,
    })
}

fn repair_finding_common(finding: &RepairFinding) -> Result<CoreMetaRowCommonProto> {
    let created_at_unix_nanos = u64::try_from(finding.created_at_nanos)
        .map_err(|_| anyhow!("repair finding timestamp must be nonnegative"))?;
    Ok(core_meta_committed_row_common(
        format!("repair/{}/{}", finding.scope_kind, finding.scope_id),
        repair_finding_root_key_hash(&finding.scope_kind, &finding.scope_id),
        created_at_unix_nanos,
        format!("{}/{}", finding.repair_task_id, finding.finding_id),
        created_at_unix_nanos,
    ))
}

fn validate_repair_finding_common(
    proto: &RepairFindingProto,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    if common.realm_id != format!("repair/{}/{}", proto.scope_kind, proto.scope_id) {
        return Err(anyhow!("repair finding CoreMeta realm mismatch"));
    }
    if common.root_key_hash != repair_finding_root_key_hash(&proto.scope_kind, &proto.scope_id) {
        return Err(anyhow!("repair finding CoreMeta root mismatch"));
    }
    if common.transaction_id != format!("{}/{}", proto.repair_task_id, proto.finding_id) {
        return Err(anyhow!("repair finding CoreMeta transaction mismatch"));
    }
    if common.visibility_state_enum() != crate::core_store::CoreMetaVisibilityState::Committed {
        return Err(anyhow!("repair finding CoreMeta row is not committed"));
    }
    Ok(())
}

fn repair_finding_root_key_hash(scope_kind: &str, scope_id: &str) -> String {
    core_meta_root_key_hash(&format!("repair/{scope_kind}/{scope_id}"))
}

fn subject_to_proto(subject: &RepairSubjectRef) -> RepairSubjectRefProto {
    RepairSubjectRefProto {
        subject_kind: subject.subject_kind.clone(),
        subject_id: subject.subject_id.clone(),
        generation: subject.generation,
        cursor: subject.cursor.map(|value| value.to_string()),
        expected_hash: subject.expected_hash.clone(),
        actual_hash: subject.actual_hash.clone(),
    }
}

fn subject_from_proto(proto: RepairSubjectRefProto) -> Result<RepairSubjectRef> {
    Ok(RepairSubjectRef {
        subject_kind: proto.subject_kind,
        subject_id: proto.subject_id,
        generation: proto.generation,
        cursor: proto
            .cursor
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| anyhow!("repair finding subject cursor is not u128"))
            })
            .transpose()?,
        expected_hash: proto.expected_hash,
        actual_hash: proto.actual_hash,
    })
}

fn severity_to_proto(severity: RepairFindingSeverity) -> RepairFindingSeverityProto {
    match severity {
        RepairFindingSeverity::Info => RepairFindingSeverityProto::Info,
        RepairFindingSeverity::Warning => RepairFindingSeverityProto::Warning,
        RepairFindingSeverity::Error => RepairFindingSeverityProto::Error,
        RepairFindingSeverity::Critical => RepairFindingSeverityProto::Critical,
    }
}

fn severity_from_proto(severity: i32) -> Result<RepairFindingSeverity> {
    match RepairFindingSeverityProto::try_from(severity)
        .map_err(|_| anyhow!("repair finding severity is invalid"))?
    {
        RepairFindingSeverityProto::Info => Ok(RepairFindingSeverity::Info),
        RepairFindingSeverityProto::Warning => Ok(RepairFindingSeverity::Warning),
        RepairFindingSeverityProto::Error => Ok(RepairFindingSeverity::Error),
        RepairFindingSeverityProto::Critical => Ok(RepairFindingSeverity::Critical),
        RepairFindingSeverityProto::Unspecified => {
            Err(anyhow!("repair finding severity is unspecified"))
        }
    }
}

fn status_to_proto(status: RepairFindingStatus) -> RepairFindingStatusProto {
    match status {
        RepairFindingStatus::Open => RepairFindingStatusProto::Open,
        RepairFindingStatus::RebuiltDerivedIndex => RepairFindingStatusProto::RebuiltDerivedIndex,
        RepairFindingStatus::RepairedManifest => RepairFindingStatusProto::RepairedManifest,
        RepairFindingStatus::RequiresOperatorReview => {
            RepairFindingStatusProto::RequiresOperatorReview
        }
        RepairFindingStatus::Irreparable => RepairFindingStatusProto::Irreparable,
    }
}

fn status_from_proto(status: i32) -> Result<RepairFindingStatus> {
    match RepairFindingStatusProto::try_from(status)
        .map_err(|_| anyhow!("repair finding status is invalid"))?
    {
        RepairFindingStatusProto::Open => Ok(RepairFindingStatus::Open),
        RepairFindingStatusProto::RebuiltDerivedIndex => {
            Ok(RepairFindingStatus::RebuiltDerivedIndex)
        }
        RepairFindingStatusProto::RepairedManifest => Ok(RepairFindingStatus::RepairedManifest),
        RepairFindingStatusProto::RequiresOperatorReview => {
            Ok(RepairFindingStatus::RequiresOperatorReview)
        }
        RepairFindingStatusProto::Irreparable => Ok(RepairFindingStatus::Irreparable),
        RepairFindingStatusProto::Unspecified => {
            Err(anyhow!("repair finding status is unspecified"))
        }
    }
}

fn action_to_proto(action: RepairActionKind) -> RepairActionKindProto {
    match action {
        RepairActionKind::VerifyOnly => RepairActionKindProto::VerifyOnly,
        RepairActionKind::RebuildDerivedIndex => RepairActionKindProto::RebuildDerivedIndex,
        RepairActionKind::RebuildDirectoryIndex => RepairActionKindProto::RebuildDirectoryIndex,
        RepairActionKind::RepairManifestFromSegments => {
            RepairActionKindProto::RepairManifestFromSegments
        }
        RepairActionKind::SynthesizeCommittedObjectVersion => {
            RepairActionKindProto::SynthesizeCommittedObjectVersion
        }
        RepairActionKind::SynthesizePersonalDbCommit => {
            RepairActionKindProto::SynthesizePersonalDbCommit
        }
    }
}

fn action_from_proto(action: i32) -> Result<RepairActionKind> {
    match RepairActionKindProto::try_from(action)
        .map_err(|_| anyhow!("repair action kind is invalid"))?
    {
        RepairActionKindProto::VerifyOnly => Ok(RepairActionKind::VerifyOnly),
        RepairActionKindProto::RebuildDerivedIndex => Ok(RepairActionKind::RebuildDerivedIndex),
        RepairActionKindProto::RebuildDirectoryIndex => Ok(RepairActionKind::RebuildDirectoryIndex),
        RepairActionKindProto::RepairManifestFromSegments => {
            Ok(RepairActionKind::RepairManifestFromSegments)
        }
        RepairActionKindProto::SynthesizeCommittedObjectVersion => {
            Ok(RepairActionKind::SynthesizeCommittedObjectVersion)
        }
        RepairActionKindProto::SynthesizePersonalDbCommit => {
            Ok(RepairActionKind::SynthesizePersonalDbCommit)
        }
        RepairActionKindProto::Unspecified => Err(anyhow!("repair action kind is unspecified")),
    }
}

fn json_value_to_proto(value: &serde_json::Value) -> RepairJsonValueProto {
    match value {
        serde_json::Value::Null => RepairJsonValueProto {
            kind: RepairJsonKindProto::Null as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Bool(value) => RepairJsonValueProto {
            kind: RepairJsonKindProto::Bool as i32,
            bool_value: *value,
            number_value: String::new(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Number(value) => RepairJsonValueProto {
            kind: RepairJsonKindProto::Number as i32,
            bool_value: false,
            number_value: value.to_string(),
            string_value: String::new(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::String(value) => RepairJsonValueProto {
            kind: RepairJsonKindProto::String as i32,
            bool_value: false,
            number_value: String::new(),
            string_value: value.clone(),
            array_values: Vec::new(),
            object_keys: Vec::new(),
            object_values: Vec::new(),
        },
        serde_json::Value::Array(values) => RepairJsonValueProto {
            kind: RepairJsonKindProto::Array as i32,
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
            RepairJsonValueProto {
                kind: RepairJsonKindProto::Object as i32,
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

fn json_value_from_proto(proto: RepairJsonValueProto) -> Result<serde_json::Value> {
    match RepairJsonKindProto::try_from(proto.kind)
        .map_err(|_| anyhow!("repair finding evidence value kind is invalid"))?
    {
        RepairJsonKindProto::Null => Ok(serde_json::Value::Null),
        RepairJsonKindProto::Bool => Ok(serde_json::Value::Bool(proto.bool_value)),
        RepairJsonKindProto::Number => {
            let parsed = serde_json::from_str::<serde_json::Value>(&proto.number_value)?;
            if !parsed.is_number() {
                return Err(anyhow!("repair finding evidence number is invalid"));
            }
            Ok(parsed)
        }
        RepairJsonKindProto::String => Ok(serde_json::Value::String(proto.string_value)),
        RepairJsonKindProto::Array => Ok(serde_json::Value::Array(
            proto
                .array_values
                .into_iter()
                .map(json_value_from_proto)
                .collect::<Result<Vec<_>>>()?,
        )),
        RepairJsonKindProto::Object => {
            if proto.object_keys.len() != proto.object_values.len() {
                return Err(anyhow!("repair finding evidence object key/value mismatch"));
            }
            if proto
                .object_keys
                .windows(2)
                .any(|window| window[0] >= window[1])
            {
                return Err(anyhow!(
                    "repair finding evidence object keys are not canonical"
                ));
            }
            let mut object = serde_json::Map::new();
            for (key, value) in proto.object_keys.into_iter().zip(proto.object_values) {
                object.insert(key, json_value_from_proto(value)?);
            }
            Ok(serde_json::Value::Object(object))
        }
        RepairJsonKindProto::Unspecified => {
            Err(anyhow!("repair finding evidence value kind is unspecified"))
        }
    }
}

fn repair_finding_ref_prefix(scope_kind: &str, scope_id: &str) -> Result<String> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    Ok(format!(
        "{REPAIR_FINDING_REF_PREFIX}scope_kind:{scope_kind}:scope_id:{scope_id}:finding:"
    ))
}

fn repair_finding_ref_name(scope_kind: &str, scope_id: &str, finding_id: &str) -> Result<String> {
    require_safe_component(finding_id, "finding_id")?;
    Ok(format!(
        "{}{finding_id}",
        repair_finding_ref_prefix(scope_kind, scope_id)?
    ))
}

fn repair_finding_tuple_prefix(scope_kind: &str, scope_id: &str) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("repair_finding"),
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
    ])
}

fn repair_finding_tuple_key(scope_kind: &str, scope_id: &str, finding_id: &str) -> Result<Vec<u8>> {
    require_safe_component(scope_kind, "scope_kind")?;
    require_safe_component(scope_id, "scope_id")?;
    require_safe_component(finding_id, "finding_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("repair_finding"),
        CoreMetaTuplePart::Utf8(scope_kind),
        CoreMetaTuplePart::Utf8(scope_id),
        CoreMetaTuplePart::Utf8(finding_id),
    ])
}

fn parse_repair_finding_ref_name(ref_name: &str) -> Result<(String, String, String)> {
    let parts = ref_name.split(':').collect::<Vec<_>>();
    if parts.len() != 7
        || parts[0] != REPAIR_FINDING_REF_PREFIX.trim_end_matches(':')
        || parts[1] != "scope_kind"
        || parts[3] != "scope_id"
        || parts[5] != "finding"
    {
        return Err(anyhow!(
            "repair finding CoreMeta ref name has invalid shape"
        ));
    }
    Ok((
        parts[2].to_string(),
        parts[4].to_string(),
        parts[6].to_string(),
    ))
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
        let tuple_key =
            repair_finding_tuple_key("bucket", "tenant-1-bucket-2", "finding-001").unwrap();
        assert!(
            CoreMetaStore::open(storage.core_store_meta_path())
                .unwrap()
                .get(CF_MESH, TABLE_REPAIR_FINDING_ROW, &tuple_key)
                .unwrap()
                .is_some()
        );
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
        let tuple_key =
            repair_finding_tuple_key("bucket", "tenant-1-bucket-2", "finding-001").unwrap();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let mut value = meta
            .get(CF_MESH, TABLE_REPAIR_FINDING_ROW, &tuple_key)
            .unwrap()
            .unwrap();
        *value
            .last_mut()
            .expect("stored repair finding bytes are not empty") ^= 0x01;
        meta.put(CF_MESH, TABLE_REPAIR_FINDING_ROW, &tuple_key, &value)
            .unwrap();
        assert!(
            read_repair_finding(&storage, "bucket", "tenant-1-bucket-2", "finding-001", KEY)
                .await
                .is_err()
        );
        assert!(repair_finding_ref_name("../bucket", "scope", "finding").is_err());
        assert!(repair_finding_ref_name("bucket", "scope", "../finding").is_err());
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
