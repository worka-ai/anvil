use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::persistence::AuthzTupleRecord;
use crate::storage::Storage;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
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

pub async fn append_authz_tuple_record(storage: &Storage, record: &AuthzTupleRecord) -> Result<()> {
    let path = storage.authz_tuple_journal_path(record.tenant_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path, record.tenant_id).await?;

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
    let mutation_id = uuid::Uuid::new_v4();
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
        0,
        *mutation_id.as_bytes(),
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

async fn read_all_authz_tuple_records(
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

async fn ensure_journal_header(path: &Path, tenant_id: i64) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = chrono::Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&AuthzJournalHeader {
        tenant_id: tenant_id.to_string(),
        partition_family: "authz_tuple",
        partition_id: hex::encode(authz_partition_id(tenant_id)),
        fence_token: 0,
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

fn authz_partition_id(tenant_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/authz_tuple").as_bytes())
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::tempdir;

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
            record_hash: format!("hash-{revision}"),
            written_at: Utc::now(),
        }
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
}
