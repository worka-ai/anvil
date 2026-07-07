use crate::{
    core_store::{CompareAndSwapRef, CoreObjectRef, CoreStore, GetBlob, PutBlob},
    formats::{
        BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
        Hash32,
        git::{GitHashAlgorithm, GitSourceRecord},
        hash32,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use serde::{Deserialize, Serialize};

const GIT_SOURCE_INDEX_REF_PREFIX: &str = "git_source_index:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
) -> Result<String> {
    let mut records = input.records.to_vec();
    ensure_record_algorithms(input.hash_algorithm, &records)?;
    records.sort_by(compare_git_source_records);
    let body = encode_git_source_body(&records);
    let source_hash = hex::encode(input.source_hash);
    let ref_name = git_source_index_ref_name(
        input.tenant_id,
        input.repository_id,
        input.generation,
        &source_hash,
    )?;

    let header = GitSourceIndexHeader {
        tenant_id: input.tenant_id.to_string(),
        repository_id: input.repository_id.to_string(),
        generation: input.generation,
        source_hash,
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

    let mut bytes = Vec::with_capacity(encoded_header.len() + body.len() + COMMON_FOOTER_LEN);
    bytes.extend_from_slice(&encoded_header);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&footer.encode());
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.clone(),
            bytes,
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: format!(
                "git-source-index:{}:{}:{}",
                input.tenant_id, input.repository_id, input.generation
            ),
        })
        .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.clone(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(ref_name)
}

pub async fn read_git_source_index(
    storage: &Storage,
    index_ref: &str,
) -> Result<DecodedGitSourceIndex> {
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(index_ref)
        .await?
        .ok_or_else(|| anyhow!("git source index ref is missing"))?;
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    decode_git_source_index(&bytes)
}

pub async fn read_git_source_index_bytes(storage: &Storage, index_ref: &str) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(index_ref)
        .await?
        .ok_or_else(|| anyhow!("git source index ref is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await
}

pub async fn latest_git_source_index_ref(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<String>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut refs = store
        .list_ref_names(&git_source_index_ref_prefix(tenant_id, repository_id)?)
        .await?;
    refs.sort_by_key(|value| generation_from_ref(value).unwrap_or(0));
    Ok(refs.pop())
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

fn git_source_index_ref_prefix(tenant_id: i64, repository_id: &str) -> Result<String> {
    require_safe_component(repository_id, "git repository id")?;
    Ok(format!(
        "{GIT_SOURCE_INDEX_REF_PREFIX}tenant:{tenant_id}:repository:{repository_id}:"
    ))
}

fn git_source_index_ref_name(
    tenant_id: i64,
    repository_id: &str,
    generation: u64,
    source_hash: &str,
) -> Result<String> {
    validate_hex32(source_hash, "git source hash")?;
    Ok(format!(
        "{}generation:{generation:020}:source:{source_hash}",
        git_source_index_ref_prefix(tenant_id, repository_id)?
    ))
}

fn generation_from_ref(ref_name: &str) -> Option<u64> {
    ref_name
        .rsplit_once(":generation:")?
        .1
        .split(':')
        .next()?
        .parse()
        .ok()
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded)?,
    )?)
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

        let index_ref = write_git_source_index(
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
        assert!(index_ref.starts_with("git_source_index:tenant:5:repository:repo-alpha:"));

        let decoded = read_git_source_index(&storage, &index_ref).await.unwrap();
        assert_eq!(decoded.header.repository_id, "repo-alpha");
        assert_eq!(decoded.header.hash_algorithm, "sha1");
        assert_eq!(decoded.records.len(), 3);
        ensure_sorted(&decoded.records).unwrap();
    }

    #[tokio::test]
    async fn git_source_index_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let index_ref = write_git_source_index(
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
        let mut bytes = read_git_source_index_bytes(&storage, &index_ref)
            .await
            .unwrap();
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
