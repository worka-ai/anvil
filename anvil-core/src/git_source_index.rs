use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    Hash32,
    git::{GitHashAlgorithm, GitSourceRecord},
    hash32,
};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitSourceIndexHeader {
    pub tenant_id: String,
    pub repository_id: String,
    pub generation: u64,
    pub source_hash: String,
    pub hash_algorithm: String,
    pub key_order: String,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedGitSourceIndex {
    pub header: GitSourceIndexHeader,
    pub records: Vec<GitSourceRecord>,
}

#[derive(Debug, Clone)]
pub struct GitSourceIndexWrite<'a> {
    pub tenant_id: i64,
    pub repository_id: &'a str,
    pub generation: u64,
    pub source_hash: Hash32,
    pub hash_algorithm: GitHashAlgorithm,
    pub records: &'a [GitSourceRecord],
}

pub async fn write_git_source_index(
    storage: &Storage,
    input: GitSourceIndexWrite<'_>,
) -> Result<PathBuf> {
    let mut records = input.records.to_vec();
    ensure_record_algorithms(input.hash_algorithm, &records)?;
    records.sort_by(compare_git_source_records);
    let body = encode_git_source_body(&records);
    let path = storage.git_source_index_path(
        input.tenant_id,
        input.repository_id,
        input.generation,
        &hex::encode(input.source_hash),
    )?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let header = GitSourceIndexHeader {
        tenant_id: input.tenant_id.to_string(),
        repository_id: input.repository_id.to_string(),
        generation: input.generation,
        source_hash: hex::encode(input.source_hash),
        hash_algorithm: hash_algorithm_name(input.hash_algorithm).to_string(),
        key_order: "repository_commit_tree_path_object".to_string(),
        codec: "none".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let header_json = serde_json::to_vec(&header)?;
    let envelope = BinaryEnvelopeHeader::new(FileFamily::GitSourceIndex, 0, 0, header_json);
    let encoded_header = envelope.encode();
    let (first_hash, last_hash) = record_hash_bounds(&records);
    let footer = BinaryFileFooter::new(
        &encoded_header,
        &body,
        records.len() as u64,
        first_hash,
        last_hash,
    );

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await
        .with_context(|| format!("create git source index {}", path.display()))?;
    file.write_all(&encoded_header).await?;
    file.write_all(&body).await?;
    file.write_all(&footer.encode()).await?;
    file.sync_data().await?;
    Ok(path)
}

pub async fn read_git_source_index(path: impl Into<PathBuf>) -> Result<DecodedGitSourceIndex> {
    let path = path.into();
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("read git source index {}", path.display()))?;
    decode_git_source_index(&bytes)
}

pub fn decode_git_source_index(bytes: &[u8]) -> Result<DecodedGitSourceIndex> {
    let envelope = BinaryEnvelopeHeader::decode(bytes)?;
    if envelope.family != FileFamily::GitSourceIndex {
        return Err(anyhow!("git source index file family mismatch"));
    }
    if bytes.len() < COMMON_FOOTER_LEN {
        return Err(anyhow!("git source index is shorter than footer"));
    }
    let header_end = COMMON_HEADER_LEN
        .checked_add(envelope.header_json.len())
        .ok_or_else(|| anyhow!("git source index header length overflow"))?;
    let footer_start = bytes
        .len()
        .checked_sub(COMMON_FOOTER_LEN)
        .ok_or_else(|| anyhow!("git source index footer length overflow"))?;
    if footer_start < header_end {
        return Err(anyhow!("git source index body overlaps header"));
    }
    let encoded_header = &bytes[..header_end];
    let body = &bytes[header_end..footer_start];
    let footer = BinaryFileFooter::decode(&bytes[footer_start..])?;
    footer.verify(encoded_header, body)?;
    let header: GitSourceIndexHeader = serde_json::from_slice(&envelope.header_json)?;
    let hash_algorithm = parse_hash_algorithm(&header.hash_algorithm)?;
    let records = decode_git_source_body(body, hash_algorithm)?;
    ensure_sorted(&records)?;
    Ok(DecodedGitSourceIndex { header, records })
}

fn encode_git_source_body(records: &[GitSourceRecord]) -> Vec<u8> {
    let len = records.iter().map(|record| record.encode().len()).sum();
    let mut out = Vec::with_capacity(len);
    for record in records {
        out.extend_from_slice(&record.encode());
    }
    out
}

fn decode_git_source_body(
    mut input: &[u8],
    hash_algorithm: GitHashAlgorithm,
) -> Result<Vec<GitSourceRecord>> {
    let mut records = Vec::new();
    while !input.is_empty() {
        let (record, used) = GitSourceRecord::decode(input, hash_algorithm)?;
        records.push(record);
        input = &input[used..];
    }
    Ok(records)
}

fn ensure_record_algorithms(
    hash_algorithm: GitHashAlgorithm,
    records: &[GitSourceRecord],
) -> Result<()> {
    let len = hash_algorithm.object_id_len();
    if records
        .iter()
        .all(|record| record.commit_id.len() == len && record.object_id.len() == len)
    {
        Ok(())
    } else {
        Err(anyhow!(
            "git source records do not match declared hash algorithm"
        ))
    }
}

fn ensure_sorted(records: &[GitSourceRecord]) -> Result<()> {
    if records
        .windows(2)
        .all(|pair| compare_git_source_records(&pair[0], &pair[1]).is_le())
    {
        Ok(())
    } else {
        Err(anyhow!("git source records are not sorted"))
    }
}

fn compare_git_source_records(
    left: &GitSourceRecord,
    right: &GitSourceRecord,
) -> std::cmp::Ordering {
    left.repository_id
        .cmp(&right.repository_id)
        .then_with(|| left.commit_id.cmp(&right.commit_id))
        .then_with(|| hash32(&left.tree_path).cmp(&hash32(&right.tree_path)))
        .then_with(|| left.object_id.cmp(&right.object_id))
}

fn hash_algorithm_name(hash_algorithm: GitHashAlgorithm) -> &'static str {
    match hash_algorithm {
        GitHashAlgorithm::Sha1 => "sha1",
        GitHashAlgorithm::Sha256 => "sha256",
    }
}

fn parse_hash_algorithm(value: &str) -> Result<GitHashAlgorithm> {
    match value {
        "sha1" => Ok(GitHashAlgorithm::Sha1),
        "sha256" => Ok(GitHashAlgorithm::Sha256),
        other => Err(anyhow!("unsupported git hash algorithm {other}")),
    }
}

fn record_hash_bounds(records: &[GitSourceRecord]) -> (Hash32, Hash32) {
    let first = records
        .first()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    let last = records
        .last()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn record(commit: u8, path: &str, object: u8) -> GitSourceRecord {
        GitSourceRecord::new(
            GitHashAlgorithm::Sha1,
            b"repo-alpha".to_vec(),
            vec![commit; 20],
            vec![object; 20],
            path.as_bytes().to_vec(),
            u64::from(object) * 100,
            44,
            [9; 16],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn git_source_index_round_trips_sorted_records() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let records = vec![
            record(2, "src/lib.rs", 2),
            record(1, "README.md", 9),
            record(1, "src/main.rs", 1),
        ];

        let path = write_git_source_index(
            &storage,
            GitSourceIndexWrite {
                tenant_id: 5,
                repository_id: "repo-alpha",
                generation: 3,
                source_hash: [8; 32],
                hash_algorithm: GitHashAlgorithm::Sha1,
                records: &records,
            },
        )
        .await
        .unwrap();
        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".angit"))
        );

        let decoded = read_git_source_index(path).await.unwrap();
        assert_eq!(decoded.header.repository_id, "repo-alpha");
        assert_eq!(decoded.header.hash_algorithm, "sha1");
        assert_eq!(decoded.records.len(), 3);
        ensure_sorted(&decoded.records).unwrap();
    }

    #[tokio::test]
    async fn git_source_index_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let path = write_git_source_index(
            &storage,
            GitSourceIndexWrite {
                tenant_id: 5,
                repository_id: "repo-alpha",
                generation: 3,
                source_hash: [8; 32],
                hash_algorithm: GitHashAlgorithm::Sha1,
                records: &[record(1, "README.md", 1)],
            },
        )
        .await
        .unwrap();
        let mut bytes = tokio::fs::read(path).await.unwrap();
        bytes[COMMON_HEADER_LEN + 1] ^= 1;
        assert!(decode_git_source_index(&bytes).is_err());
    }

    #[test]
    fn git_source_index_rejects_wrong_record_algorithm() {
        let record = GitSourceRecord::new(
            GitHashAlgorithm::Sha1,
            b"repo-alpha".to_vec(),
            vec![1; 20],
            vec![2; 20],
            b"README.md".to_vec(),
            0,
            12,
            [4; 16],
        )
        .unwrap();
        assert!(ensure_record_algorithms(GitHashAlgorithm::Sha256, &[record]).is_err());
    }
}
