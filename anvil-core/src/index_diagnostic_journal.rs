use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::persistence::IndexDiagnostic;
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize)]
struct IndexDiagnosticJournalHeader<'a> {
    tenant_id: String,
    bucket_id: String,
    partition_family: &'static str,
    partition_id: String,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexDiagnosticBody {
    diagnostic: IndexDiagnostic,
}

pub async fn write_index_diagnostic(
    storage: &Storage,
    mut diagnostic: IndexDiagnostic,
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
    append_diagnostic(storage, &diagnostic).await?;
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
    let frames = read_index_diagnostic_frames_at_path(
        &storage.index_diagnostic_journal_path(tenant_id, bucket_id),
    )
    .await?;
    let mut diagnostics = Vec::new();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::IndexDiagnostic {
            continue;
        }
        let body: IndexDiagnosticBody = serde_json::from_slice(&frame.body)?;
        let diagnostic = body.diagnostic;
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

async fn append_diagnostic(storage: &Storage, diagnostic: &IndexDiagnostic) -> Result<()> {
    let path = storage.index_diagnostic_journal_path(diagnostic.tenant_id, diagnostic.bucket_id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path, diagnostic.tenant_id, diagnostic.bucket_id).await?;
    let previous = read_index_diagnostic_frames_at_path(path.as_path())
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
    let frame = JournalFrame::new(
        JournalRecordKind::IndexDiagnostic,
        sequence,
        0,
        *mutation_id.as_bytes(),
        diagnostic_key_hash(diagnostic),
        previous_hash,
        serde_json::to_vec(&IndexDiagnosticBody {
            diagnostic: diagnostic.clone(),
        })?,
    );
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open index diagnostic journal {}", path.display()))?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn ensure_journal_header(path: &Path, tenant_id: i64, bucket_id: i64) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = chrono::Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&IndexDiagnosticJournalHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: bucket_id.to_string(),
        partition_family: "index_diagnostic",
        partition_id: hex::encode(index_diagnostic_partition_id(tenant_id, bucket_id)),
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
        .with_context(|| format!("create index diagnostic journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn read_index_diagnostic_frames_at_path(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read index diagnostic journal {}", path.display()))?;
    decode_journal_file(&bytes)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("index diagnostic journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated index diagnostic journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("invalid index diagnostic journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated index diagnostic journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

fn index_diagnostic_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    hash32(format!("tenant/{tenant_id}/bucket/{bucket_id}/index_diagnostic").as_bytes())
}

fn diagnostic_key_hash(diagnostic: &IndexDiagnostic) -> Hash32 {
    hash32(
        format!(
            "tenant/{}/bucket/{}/index/{}/diagnostic/{}",
            diagnostic.tenant_id, diagnostic.bucket_id, diagnostic.index_name, diagnostic.id
        )
        .as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use tempfile::tempdir;

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
}
