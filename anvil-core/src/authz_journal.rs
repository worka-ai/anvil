use crate::authz_segment;
use crate::authz_userset_index::{
    DEFAULT_DERIVED_USERSET_INDEX_ID, lookup_derived_userset_index_at_revision,
};
use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::partition_fence::{PartitionWritePermit, validate_partition_write};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize)]
struct AuthzJournalHeader<'a> {
    tenant_id: String,
    partition_family: &'static str,
    partition_id: String,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthzTupleBody {
    revision: i64,
    tenant_id: i64,
    namespace: String,
    object_id: String,
    relation: String,
    subject_kind: String,
    subject_id: String,
    caveat_hash: String,
    operation: String,
    written_by: String,
    reason: String,
    record_hash: String,
    written_at: String,
}

pub struct AuthzTupleWrite<'a> {
    pub tenant_id: i64,
    pub namespace: &'a str,
    pub object_id: &'a str,
    pub relation: &'a str,
    pub subject_kind: &'a str,
    pub subject_id: &'a str,
    pub caveat_hash: &'a str,
    pub operation: &'a str,
    pub written_by: &'a str,
    pub reason: &'a str,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthzTupleFilter {
    pub namespace: Option<String>,
    pub object_id: Option<String>,
    pub relation: Option<String>,
    pub subject_kind: Option<String>,
    pub subject_id: Option<String>,
    pub caveat_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AuthzSubjectRef {
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
}

pub(crate) async fn write_authz_tuple_with_permit(
    storage: &Storage,
    input: AuthzTupleWrite<'_>,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<AuthzTupleRecord> {
    require_authz_permit(input.tenant_id, permit)?;
    validate_optional_caveat_hash(input.caveat_hash)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    write_authz_tuple_inner(storage, input, permit.fence_token).await
}

async fn write_authz_tuple_inner(
    storage: &Storage,
    input: AuthzTupleWrite<'_>,
    fence_token: u64,
) -> Result<AuthzTupleRecord> {
    validate_optional_caveat_hash(input.caveat_hash)?;
    let revision = latest_authz_revision(storage, input.tenant_id)
        .await?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("authz revision overflow"))?;
    let written_at = chrono::Utc::now();
    let mutation_id = uuid::Uuid::new_v4();
    let record_hash = authz_record_hash(AuthzRecordHashInput {
        tenant_id: input.tenant_id,
        namespace: input.namespace,
        object_id: input.object_id,
        relation: input.relation,
        subject_kind: input.subject_kind,
        subject_id: input.subject_id,
        caveat_hash: input.caveat_hash,
        operation: input.operation,
        written_by: input.written_by,
        reason: input.reason,
    });
    let record = AuthzTupleRecord {
        revision,
        tenant_id: input.tenant_id,
        namespace: input.namespace.to_string(),
        object_id: input.object_id.to_string(),
        relation: input.relation.to_string(),
        subject_kind: input.subject_kind.to_string(),
        subject_id: input.subject_id.to_string(),
        caveat_hash: input.caveat_hash.to_string(),
        operation: input.operation.to_string(),
        written_by: input.written_by.to_string(),
        reason: input.reason.to_string(),
        mutation_id,
        record_hash,
        written_at,
    };
    append_authz_tuple_record_inner(storage, &record, fence_token).await?;
    Ok(record)
}

#[cfg(test)]
async fn append_authz_tuple_record(storage: &Storage, record: &AuthzTupleRecord) -> Result<()> {
    append_authz_tuple_record_inner(storage, record, 0).await
}

#[cfg(test)]
pub(crate) async fn append_authz_tuple_record_with_permit(
    storage: &Storage,
    record: &AuthzTupleRecord,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_authz_permit(record.tenant_id, permit)?;
    validate_partition_write(storage, permit, partition_owner_signing_key).await?;
    append_authz_tuple_record_inner(storage, record, permit.fence_token).await
}

async fn append_authz_tuple_record_inner(
    storage: &Storage,
    record: &AuthzTupleRecord,
    fence_token: u64,
) -> Result<()> {
    let path = storage.authz_tuple_journal_path(record.tenant_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path, record.tenant_id, fence_token).await?;

    let previous = read_authz_journal_frames_at_path(path.as_path())
        .await
        .unwrap_or_default();
    let sequence = previous
        .last()
        .map(|frame| frame.partition_sequence + 1)
        .unwrap_or(1);
    let previous_hash = previous
        .last()
        .map(|frame| frame.record_hash)
        .unwrap_or([0; 32]);
    let body = serde_json::to_vec(&AuthzTupleBody {
        revision: record.revision,
        tenant_id: record.tenant_id,
        namespace: record.namespace.clone(),
        object_id: record.object_id.clone(),
        relation: record.relation.clone(),
        subject_kind: record.subject_kind.clone(),
        subject_id: record.subject_id.clone(),
        caveat_hash: record.caveat_hash.clone(),
        operation: record.operation.clone(),
        written_by: record.written_by.clone(),
        reason: record.reason.clone(),
        record_hash: record.record_hash.clone(),
        written_at: record.written_at.to_rfc3339(),
    })?;
    let frame = JournalFrame::new(
        JournalRecordKind::AuthzTuple,
        sequence,
        fence_token,
        *record.mutation_id.as_bytes(),
        tuple_key_hash(record),
        previous_hash,
        body,
    );

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open authz tuple journal {}", path.display()))?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    let records = read_all_authz_tuple_records_from_journal(storage, record.tenant_id).await?;
    authz_segment::write_authz_tuple_segment_with_fence(
        storage,
        record.tenant_id,
        &records,
        fence_token,
    )
    .await?;
    Ok(())
}

pub async fn latest_authz_revision(storage: &Storage, tenant_id: i64) -> Result<i64> {
    Ok(read_all_authz_tuple_records(storage, tenant_id)
        .await?
        .into_iter()
        .map(|record| record.revision)
        .max()
        .unwrap_or(0))
}

#[allow(clippy::too_many_arguments)]
pub async fn check_authz_tuple(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
) -> Result<Option<AuthzTupleRecord>> {
    check_authz_tuple_at_revision(
        storage,
        tenant_id,
        namespace,
        object_id,
        relation,
        subject_kind,
        subject_id,
        caveat_hash,
        i64::MAX,
    )
    .await
}

pub fn validate_optional_caveat_hash(value: &str) -> Result<()> {
    if value.is_empty() {
        return Ok(());
    }
    if value.len() == 64 && value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        Ok(())
    } else {
        Err(anyhow!("caveat_hash must be empty or hex32"))
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn check_authz_tuple_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<Option<AuthzTupleRecord>> {
    Ok(read_all_authz_tuple_records(storage, tenant_id)
        .await?
        .into_iter()
        .filter(|record| {
            record.revision <= revision
                && record.namespace == namespace
                && record.object_id == object_id
                && record.relation == relation
                && record.subject_kind == subject_kind
                && record.subject_id == subject_id
                && record.caveat_hash == caveat_hash
        })
        .max_by_key(|record| record.revision))
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_permission_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<bool> {
    if revision >= 0
        && let Some(allowed) = lookup_derived_userset_index_at_revision(
            storage,
            tenant_id,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
            revision as u64,
        )
        .await?
    {
        return Ok(allowed);
    }

    let current = current_authz_view_at_revision(storage, tenant_id, revision).await?;
    let subject = SubjectRef {
        kind: subject_kind.to_string(),
        id: subject_id.to_string(),
        caveat_hash: caveat_hash.to_string(),
    };
    let userset = UsersetRef {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
    };
    let mut visited = BTreeSet::new();
    resolve_userset(&current, &userset, &subject, &mut visited)
}

pub async fn list_authz_tuple_log(
    storage: &Storage,
    tenant_id: i64,
    after_revision: i64,
    namespace: &str,
    limit: usize,
) -> Result<Vec<AuthzTupleRecord>> {
    let mut records = read_all_authz_tuple_records(storage, tenant_id).await?;
    records.retain(|record| {
        record.revision > after_revision && (namespace.is_empty() || record.namespace == namespace)
    });
    records.sort_by_key(|record| record.revision);
    if limit > 0 && records.len() > limit {
        records.truncate(limit);
    }
    Ok(records)
}

pub async fn read_current_authz_tuples_at_revision(
    storage: &Storage,
    tenant_id: i64,
    filter: AuthzTupleFilter,
    revision: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    let mut records: Vec<_> = current_authz_view_at_revision(storage, tenant_id, revision)
        .await?
        .into_values()
        .filter(|record| record.operation == "add")
        .filter(|record| matches_authz_tuple_filter(record, &filter))
        .collect();
    records.sort_by(|left, right| {
        (
            &left.namespace,
            &left.object_id,
            &left.relation,
            &left.subject_kind,
            &left.subject_id,
            &left.caveat_hash,
        )
            .cmp(&(
                &right.namespace,
                &right.object_id,
                &right.relation,
                &right.subject_kind,
                &right.subject_id,
                &right.caveat_hash,
            ))
    });
    Ok(records)
}

pub async fn list_current_authz_objects_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    caveat_hash: &str,
    revision: i64,
) -> Result<Vec<String>> {
    let filter = AuthzTupleFilter {
        namespace: Some(namespace.to_string()),
        relation: Some(relation.to_string()),
        subject_kind: Some(subject_kind.to_string()),
        subject_id: Some(subject_id.to_string()),
        caveat_hash: Some(caveat_hash.to_string()),
        ..AuthzTupleFilter::default()
    };
    let records =
        read_current_authz_tuples_at_revision(storage, tenant_id, filter, revision).await?;
    Ok(records
        .into_iter()
        .map(|record| record.object_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

pub async fn list_current_authz_subjects_at_revision(
    storage: &Storage,
    tenant_id: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: Option<&str>,
    revision: i64,
) -> Result<Vec<AuthzSubjectRef>> {
    let filter = AuthzTupleFilter {
        namespace: Some(namespace.to_string()),
        object_id: Some(object_id.to_string()),
        relation: Some(relation.to_string()),
        subject_kind: subject_kind.map(str::to_string),
        ..AuthzTupleFilter::default()
    };
    let records =
        read_current_authz_tuples_at_revision(storage, tenant_id, filter, revision).await?;
    Ok(records
        .into_iter()
        .map(|record| AuthzSubjectRef {
            subject_kind: record.subject_kind,
            subject_id: record.subject_id,
            caveat_hash: record.caveat_hash,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect())
}

fn matches_authz_tuple_filter(record: &AuthzTupleRecord, filter: &AuthzTupleFilter) -> bool {
    filter
        .namespace
        .as_ref()
        .is_none_or(|value| record.namespace == *value)
        && filter
            .object_id
            .as_ref()
            .is_none_or(|value| record.object_id == *value)
        && filter
            .relation
            .as_ref()
            .is_none_or(|value| record.relation == *value)
        && filter
            .subject_kind
            .as_ref()
            .is_none_or(|value| record.subject_kind == *value)
        && filter
            .subject_id
            .as_ref()
            .is_none_or(|value| record.subject_id == *value)
        && filter
            .caveat_hash
            .as_ref()
            .is_none_or(|value| record.caveat_hash == *value)
}

async fn current_authz_view_at_revision(
    storage: &Storage,
    tenant_id: i64,
    revision: i64,
) -> Result<BTreeMap<TupleViewKey, AuthzTupleRecord>> {
    let mut records = read_all_authz_tuple_records(storage, tenant_id).await?;
    records.retain(|record| record.revision <= revision);
    records.sort_by_key(|record| record.revision);
    let mut current = BTreeMap::new();
    for record in records {
        current.insert(TupleViewKey::from(&record), record);
    }
    Ok(current)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TupleViewKey {
    namespace: String,
    object_id: String,
    relation: String,
    subject_kind: String,
    subject_id: String,
    caveat_hash: String,
}

impl From<&AuthzTupleRecord> for TupleViewKey {
    fn from(record: &AuthzTupleRecord) -> Self {
        Self {
            namespace: record.namespace.clone(),
            object_id: record.object_id.clone(),
            relation: record.relation.clone(),
            subject_kind: record.subject_kind.clone(),
            subject_id: record.subject_id.clone(),
            caveat_hash: record.caveat_hash.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SubjectRef {
    kind: String,
    id: String,
    caveat_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct UsersetRef {
    namespace: String,
    object_id: String,
    relation: String,
}

fn resolve_userset(
    current: &BTreeMap<TupleViewKey, AuthzTupleRecord>,
    userset: &UsersetRef,
    subject: &SubjectRef,
    visited: &mut BTreeSet<UsersetRef>,
) -> Result<bool> {
    if !visited.insert(userset.clone()) {
        return Ok(false);
    }

    let direct_key = TupleViewKey {
        namespace: userset.namespace.clone(),
        object_id: userset.object_id.clone(),
        relation: userset.relation.clone(),
        subject_kind: subject.kind.clone(),
        subject_id: subject.id.clone(),
        caveat_hash: subject.caveat_hash.clone(),
    };
    if current
        .get(&direct_key)
        .is_some_and(|record| record.operation == "add")
    {
        visited.remove(userset);
        return Ok(true);
    }

    for record in current.values() {
        if record.namespace != userset.namespace
            || record.object_id != userset.object_id
            || record.relation != userset.relation
            || record.subject_kind != "userset"
            || record.operation != "add"
            || !record.caveat_hash.is_empty()
        {
            continue;
        }
        let Some(next) = parse_userset_subject(&record.subject_id)? else {
            continue;
        };
        if resolve_userset(current, &next, subject, visited)? {
            visited.remove(userset);
            return Ok(true);
        }
    }

    visited.remove(userset);
    Ok(false)
}

fn parse_userset_subject(value: &str) -> Result<Option<UsersetRef>> {
    let Some((namespace, rest)) = value.split_once('/') else {
        return Ok(None);
    };
    let Some((object_id, relation)) = rest.rsplit_once('#') else {
        return Ok(None);
    };
    if namespace.is_empty()
        || object_id.is_empty()
        || relation.is_empty()
        || namespace.chars().any(char::is_control)
        || object_id.chars().any(char::is_control)
        || relation.chars().any(char::is_control)
    {
        return Err(anyhow!("invalid userset subject reference"));
    }
    Ok(Some(UsersetRef {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
    }))
}

async fn read_all_authz_tuple_records(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    if let Some(segment) =
        authz_segment::read_latest_authz_tuple_segment(storage, tenant_id).await?
    {
        return Ok(segment
            .records
            .into_iter()
            .map(|mut record| {
                record.tenant_id = tenant_id;
                record
            })
            .collect());
    }
    read_all_authz_tuple_records_from_journal(storage, tenant_id).await
}

async fn read_all_authz_tuple_records_from_journal(
    storage: &Storage,
    tenant_id: i64,
) -> Result<Vec<AuthzTupleRecord>> {
    let frames =
        read_authz_journal_frames_at_path(&storage.authz_tuple_journal_path(tenant_id)).await?;
    let mut records = Vec::new();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::AuthzTuple {
            continue;
        }
        let body: AuthzTupleBody = serde_json::from_slice(&frame.body)?;
        records.push(AuthzTupleRecord {
            revision: body.revision,
            tenant_id: body.tenant_id,
            namespace: body.namespace,
            object_id: body.object_id,
            relation: body.relation,
            subject_kind: body.subject_kind,
            subject_id: body.subject_id,
            caveat_hash: body.caveat_hash,
            operation: body.operation,
            written_by: body.written_by,
            reason: body.reason,
            mutation_id: uuid::Uuid::from_bytes(frame.mutation_id),
            record_hash: body.record_hash,
            written_at: chrono::DateTime::parse_from_rfc3339(&body.written_at)?
                .with_timezone(&chrono::Utc),
        });
    }
    Ok(records)
}

async fn read_authz_journal_frames_at_path(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read authz tuple journal {}", path.display()))?;
    decode_journal_file(&bytes)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("authz tuple journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated authz tuple journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow::anyhow!("invalid authz tuple journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated authz tuple journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

async fn ensure_journal_header(path: &Path, tenant_id: i64, fence_token: u64) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = chrono::Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&AuthzJournalHeader {
        tenant_id: tenant_id.to_string(),
        partition_family: "authz_tuple",
        partition_id: hex::encode(authz_partition_id(tenant_id)),
        fence_token,
        first_sequence: 1,
        created_at: &created_at,
        codec: "none",
    })?;
    let header = BinaryEnvelopeHeader::new(FileFamily::MetadataJournal, 0, 0, header_json);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .with_context(|| format!("create authz tuple journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

pub fn authz_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/authz_tuple").as_bytes())
}

fn require_authz_permit(tenant_id: i64, permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "authz_tuple"
        || permit.partition_id != hex::encode(authz_partition_id(tenant_id))
    {
        return Err(anyhow!(
            "partition write permit does not target this authorization tuple partition"
        ));
    }
    Ok(())
}

fn tuple_key_hash(record: &AuthzTupleRecord) -> Hash32 {
    hash32(
        format!(
            "tenant/{}/authz/{}/{}/{}/{}/{}/{}",
            record.tenant_id,
            record.namespace,
            record.object_id,
            record.relation,
            record.subject_kind,
            record.subject_id,
            record.caveat_hash
        )
        .as_bytes(),
    )
}

struct AuthzRecordHashInput<'a> {
    tenant_id: i64,
    namespace: &'a str,
    object_id: &'a str,
    relation: &'a str,
    subject_kind: &'a str,
    subject_id: &'a str,
    caveat_hash: &'a str,
    operation: &'a str,
    written_by: &'a str,
    reason: &'a str,
}

fn authz_record_hash(input: AuthzRecordHashInput<'_>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&input.tenant_id.to_le_bytes());
    for part in [
        input.namespace,
        input.object_id,
        input.relation,
        input.subject_kind,
        input.subject_id,
        input.caveat_hash,
        input.operation,
        input.written_by,
        input.reason,
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    hasher.finalize().to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use chrono::Utc;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"authorization tuple partition owner signing key";

    fn record(revision: i64, operation: &str) -> AuthzTupleRecord {
        AuthzTupleRecord {
            revision,
            tenant_id: 42,
            namespace: "document".to_string(),
            object_id: "alpha".to_string(),
            relation: "viewer".to_string(),
            subject_kind: "user".to_string(),
            subject_id: "alice".to_string(),
            caveat_hash: String::new(),
            operation: operation.to_string(),
            written_by: "tester".to_string(),
            reason: "test".to_string(),
            mutation_id: uuid::Uuid::new_v4(),
            record_hash: hex::encode(hash32(format!("record-{revision}").as_bytes())),
            written_at: Utc::now(),
        }
    }

    fn tuple(
        revision: i64,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        operation: &str,
    ) -> AuthzTupleRecord {
        AuthzTupleRecord {
            revision,
            tenant_id: 42,
            namespace: namespace.to_string(),
            object_id: object_id.to_string(),
            relation: relation.to_string(),
            subject_kind: subject_kind.to_string(),
            subject_id: subject_id.to_string(),
            caveat_hash: String::new(),
            operation: operation.to_string(),
            written_by: "tester".to_string(),
            reason: "test".to_string(),
            mutation_id: uuid::Uuid::new_v4(),
            record_hash: hex::encode(hash32(
                format!(
                    "record-{revision}-{namespace}-{object_id}-{relation}-{subject_kind}-{subject_id}-{operation}"
                )
                .as_bytes(),
            )),
            written_at: Utc::now(),
        }
    }

    async fn ready_authz_permit(
        storage: &Storage,
        tenant_id: i64,
        owner_node_id: &str,
    ) -> PartitionWritePermit {
        let request = PartitionRecoveryAcquire {
            partition_family: "authz_tuple".to_string(),
            partition_id: hex::encode(authz_partition_id(tenant_id)),
            owner_node_id: owner_node_id.to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: 100,
        };
        let recovering = acquire_partition_recovery(storage, request, PARTITION_OWNER_KEY)
            .await
            .unwrap();
        publish_partition_ready(
            storage,
            &recovering.partition_family,
            &recovering.partition_id,
            owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([3; 32]),
            200,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap()
        .write_permit()
        .unwrap()
    }

    #[tokio::test]
    async fn authz_journal_recovers_latest_exact_and_watch_ranges() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_authz_tuple_record(&storage, &record(1, "add"))
            .await
            .unwrap();
        append_authz_tuple_record(&storage, &record(2, "remove"))
            .await
            .unwrap();

        assert_eq!(latest_authz_revision(&storage, 42).await.unwrap(), 2);
        assert_eq!(
            check_authz_tuple(
                &storage, 42, "document", "alpha", "viewer", "user", "alice", ""
            )
            .await
            .unwrap()
            .unwrap()
            .operation,
            "remove"
        );
        assert_eq!(
            check_authz_tuple_at_revision(
                &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 1
            )
            .await
            .unwrap()
            .unwrap()
            .operation,
            "add"
        );
        let watched = list_authz_tuple_log(&storage, 42, 0, "document", 10)
            .await
            .unwrap();
        assert_eq!(watched.len(), 2);
        assert_eq!(watched[1].revision, 2);
    }

    #[test]
    fn caveat_hash_validation_accepts_empty_or_hex32_only() {
        validate_optional_caveat_hash("").unwrap();
        validate_optional_caveat_hash(&hex::encode([7; 32])).unwrap();
        validate_optional_caveat_hash("not-hex32").unwrap_err();
        validate_optional_caveat_hash(&hex::encode([7; 31])).unwrap_err();
    }

    #[tokio::test]
    async fn authz_resolves_direct_and_nested_userset_tuples() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for record in [
            tuple(1, "group", "engineering", "member", "user", "alice", "add"),
            tuple(
                2,
                "folder",
                "platform",
                "viewer",
                "userset",
                "group/engineering#member",
                "add",
            ),
            tuple(
                3,
                "document",
                "alpha",
                "viewer",
                "userset",
                "folder/platform#viewer",
                "add",
            ),
        ] {
            append_authz_tuple_record(&storage, &record).await.unwrap();
        }

        assert!(
            resolve_permission_at_revision(
                &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 3
            )
            .await
            .unwrap()
        );
        assert!(
            !resolve_permission_at_revision(
                &storage, 42, "document", "alpha", "viewer", "user", "bob", "", 3
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn authz_userset_removal_and_cycles_do_not_grant_access() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for record in [
            tuple(1, "group", "engineering", "member", "user", "alice", "add"),
            tuple(
                2,
                "folder",
                "platform",
                "viewer",
                "userset",
                "group/engineering#member",
                "add",
            ),
            tuple(
                3,
                "document",
                "alpha",
                "viewer",
                "userset",
                "folder/platform#viewer",
                "add",
            ),
            tuple(
                4,
                "folder",
                "platform",
                "viewer",
                "userset",
                "group/engineering#member",
                "remove",
            ),
            tuple(
                5,
                "group",
                "a",
                "member",
                "userset",
                "group/b#member",
                "add",
            ),
            tuple(
                6,
                "group",
                "b",
                "member",
                "userset",
                "group/a#member",
                "add",
            ),
        ] {
            append_authz_tuple_record(&storage, &record).await.unwrap();
        }

        assert!(
            resolve_permission_at_revision(
                &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 3
            )
            .await
            .unwrap()
        );
        assert!(
            !resolve_permission_at_revision(
                &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 4
            )
            .await
            .unwrap()
        );
        assert!(
            !resolve_permission_at_revision(
                &storage, 42, "group", "a", "member", "user", "alice", "", 6
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn authz_current_tuple_reads_filter_active_adds_only() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        for record in [
            tuple(1, "document", "alpha", "viewer", "user", "alice", "add"),
            tuple(2, "document", "beta", "viewer", "user", "alice", "add"),
            tuple(3, "document", "beta", "viewer", "user", "alice", "remove"),
            tuple(4, "document", "alpha", "editor", "user", "bob", "add"),
        ] {
            append_authz_tuple_record(&storage, &record).await.unwrap();
        }

        let active_viewers = read_current_authz_tuples_at_revision(
            &storage,
            42,
            AuthzTupleFilter {
                namespace: Some("document".to_string()),
                relation: Some("viewer".to_string()),
                subject_kind: Some("user".to_string()),
                subject_id: Some("alice".to_string()),
                caveat_hash: Some(String::new()),
                ..AuthzTupleFilter::default()
            },
            4,
        )
        .await
        .unwrap();
        assert_eq!(active_viewers.len(), 1);
        assert_eq!(active_viewers[0].object_id, "alpha");

        let historical_viewers = read_current_authz_tuples_at_revision(
            &storage,
            42,
            AuthzTupleFilter {
                namespace: Some("document".to_string()),
                relation: Some("viewer".to_string()),
                subject_kind: Some("user".to_string()),
                subject_id: Some("alice".to_string()),
                caveat_hash: Some(String::new()),
                ..AuthzTupleFilter::default()
            },
            2,
        )
        .await
        .unwrap();
        assert_eq!(
            historical_viewers
                .iter()
                .map(|record| record.object_id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta"]
        );

        assert_eq!(
            list_current_authz_objects_at_revision(
                &storage, 42, "document", "viewer", "user", "alice", "", 4
            )
            .await
            .unwrap(),
            vec!["alpha".to_string()]
        );
        assert_eq!(
            list_current_authz_subjects_at_revision(
                &storage,
                42,
                "document",
                "alpha",
                "editor",
                Some("user"),
                4
            )
            .await
            .unwrap(),
            vec![AuthzSubjectRef {
                subject_kind: "user".to_string(),
                subject_id: "bob".to_string(),
                caveat_hash: String::new(),
            }]
        );
    }

    #[tokio::test]
    async fn authz_journal_permit_sets_frame_and_segment_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_authz_permit(&storage, 42, "node-a").await;

        append_authz_tuple_record_with_permit(
            &storage,
            &record(1, "add"),
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();

        let frames = read_authz_journal_frames_at_path(&storage.authz_tuple_journal_path(42))
            .await
            .unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].fence_token, permit.fence_token);

        let segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(segment.header.source_fence_token, permit.fence_token);
    }

    #[tokio::test]
    async fn authz_journal_rejects_stale_partition_permit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let stale = ready_authz_permit(&storage, 42, "node-a").await;
        let fresh = ready_authz_permit(&storage, 42, "node-b").await;
        assert_eq!(fresh.fence_token, stale.fence_token + 1);

        let rejected = append_authz_tuple_record_with_permit(
            &storage,
            &record(1, "add"),
            &stale,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(rejected.to_string().contains("PartitionNotOwned"));

        append_authz_tuple_record_with_permit(
            &storage,
            &record(1, "add"),
            &fresh,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn authz_journal_rejects_wrong_partition_scope_before_write() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let valid = ready_authz_permit(&storage, 42, "node-a").await;

        let wrong_family = PartitionWritePermit {
            partition_family: "object_metadata".to_string(),
            partition_id: valid.partition_id.clone(),
            owner_node_id: valid.owner_node_id.clone(),
            fence_token: valid.fence_token,
        };
        let rejected = append_authz_tuple_record_with_permit(
            &storage,
            &record(1, "add"),
            &wrong_family,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(
            rejected
                .to_string()
                .contains("does not target this authorization tuple partition")
        );

        let wrong_tenant_partition = PartitionWritePermit {
            partition_family: valid.partition_family.clone(),
            partition_id: hex::encode(authz_partition_id(43)),
            owner_node_id: valid.owner_node_id,
            fence_token: valid.fence_token,
        };
        let rejected = append_authz_tuple_record_with_permit(
            &storage,
            &record(1, "add"),
            &wrong_tenant_partition,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(
            rejected
                .to_string()
                .contains("does not target this authorization tuple partition")
        );
        assert!(
            !tokio::fs::try_exists(storage.authz_tuple_journal_path(42))
                .await
                .unwrap(),
            "wrong-scope internal authz writes must fail before journal creation"
        );
    }

    #[tokio::test]
    pub(crate) async fn authz_write_with_permit_allocates_revision_under_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let permit = ready_authz_permit(&storage, 42, "node-a").await;

        let written = write_authz_tuple_with_permit(
            &storage,
            AuthzTupleWrite {
                tenant_id: 42,
                namespace: "document",
                object_id: "beta",
                relation: "editor",
                subject_kind: "user",
                subject_id: "bob",
                caveat_hash: "",
                operation: "add",
                written_by: "tester",
                reason: "test",
            },
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        assert_eq!(written.revision, 1);
        let frames = read_authz_journal_frames_at_path(&storage.authz_tuple_journal_path(42))
            .await
            .unwrap();
        assert_eq!(frames[0].fence_token, permit.fence_token);
    }
}
