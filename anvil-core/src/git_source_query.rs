use crate::{
    formats::git::GitSourceRecord,
    git_source_index::{DecodedGitSourceIndex, read_git_source_index},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitObjectLookup {
    pub repository_id: String,
    pub commit_id: Vec<u8>,
    pub object_id: Vec<u8>,
    pub tree_path: String,
    pub blob_start: u64,
    pub blob_len: u64,
    pub pack_object_version_id: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitTreeEntry {
    pub tree_path: String,
    pub object_id: Vec<u8>,
    pub blob_start: u64,
    pub blob_len: u64,
    pub pack_object_version_id: [u8; 16],
}

pub async fn read_latest_git_source_index(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<DecodedGitSourceIndex>> {
    let Some(path) = latest_git_source_index_path(storage, tenant_id, repository_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_git_source_index(path).await?))
}

pub async fn latest_git_source_index_path(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<PathBuf>> {
    let dir = storage.git_source_index_dir(tenant_id, repository_id)?;
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", dir.display())),
    };
    let mut best: Option<(u64, PathBuf)> = None;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("angit") {
            continue;
        }
        let Some(generation) = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(parse_generation)
        else {
            continue;
        };
        if best
            .as_ref()
            .is_none_or(|(best_generation, _)| generation > *best_generation)
        {
            best = Some((generation, path));
        }
    }
    Ok(best.map(|(_, path)| path))
}

pub fn get_git_object(
    index: &DecodedGitSourceIndex,
    object_id: &[u8],
) -> Result<Vec<GitObjectLookup>> {
    let mut matches = index
        .records
        .iter()
        .filter(|record| record.object_id == object_id)
        .map(record_to_lookup)
        .collect::<Result<Vec<_>>>()?;
    matches.sort_by(|left, right| {
        left.commit_id
            .cmp(&right.commit_id)
            .then_with(|| left.tree_path.cmp(&right.tree_path))
    });
    Ok(matches)
}

pub fn get_git_blob_by_path(
    index: &DecodedGitSourceIndex,
    commit_id: &[u8],
    tree_path: &str,
) -> Result<Option<GitObjectLookup>> {
    let normalized = normalize_tree_path(tree_path)?;
    index
        .records
        .iter()
        .find(|record| record.commit_id == commit_id && record.tree_path == normalized.as_bytes())
        .map(record_to_lookup)
        .transpose()
}

pub fn list_git_tree(
    index: &DecodedGitSourceIndex,
    commit_id: &[u8],
    prefix: &str,
) -> Result<Vec<GitTreeEntry>> {
    let normalized_prefix = normalize_prefix(prefix)?;
    let mut entries = index
        .records
        .iter()
        .filter(|record| {
            record.commit_id == commit_id
                && std::str::from_utf8(&record.tree_path)
                    .is_ok_and(|path| path.starts_with(&normalized_prefix))
        })
        .map(record_to_tree_entry)
        .collect::<Result<Vec<_>>>()?;
    entries.sort_by(|left, right| left.tree_path.cmp(&right.tree_path));
    Ok(entries)
}

fn record_to_lookup(record: &GitSourceRecord) -> Result<GitObjectLookup> {
    Ok(GitObjectLookup {
        repository_id: String::from_utf8(record.repository_id.clone())?,
        commit_id: record.commit_id.clone(),
        object_id: record.object_id.clone(),
        tree_path: tree_path_string(record)?,
        blob_start: record.blob_start,
        blob_len: record.blob_len,
        pack_object_version_id: record.pack_object_version_id,
    })
}

fn record_to_tree_entry(record: &GitSourceRecord) -> Result<GitTreeEntry> {
    Ok(GitTreeEntry {
        tree_path: tree_path_string(record)?,
        object_id: record.object_id.clone(),
        blob_start: record.blob_start,
        blob_len: record.blob_len,
        pack_object_version_id: record.pack_object_version_id,
    })
}

fn tree_path_string(record: &GitSourceRecord) -> Result<String> {
    Ok(std::str::from_utf8(&record.tree_path)?.to_string())
}

fn normalize_tree_path(path: &str) -> Result<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() || trimmed.contains("..") || trimmed.contains('\\') {
        return Err(anyhow!("invalid git tree path"));
    }
    Ok(trimmed.to_string())
}

fn normalize_prefix(prefix: &str) -> Result<String> {
    if prefix.is_empty() || prefix == "/" {
        return Ok(String::new());
    }
    let normalized = normalize_tree_path(prefix)?;
    Ok(if normalized.ends_with('/') {
        normalized
    } else {
        format!("{normalized}/")
    })
}

fn parse_generation(name: &str) -> Option<u64> {
    name.strip_prefix("generation-")?
        .split('-')
        .next()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        formats::git::{GitHashAlgorithm, GitSourceRecord},
        git_source_index::{GitSourceIndexWrite, write_git_source_index},
    };
    use tempfile::tempdir;

    #[tokio::test]
    async fn latest_git_source_index_selects_highest_generation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_git_source_index(
            &storage,
            GitSourceIndexWrite {
                tenant_id: 8,
                repository_id: "repo-alpha",
                generation: 1,
                source_hash: [1; 32],
                hash_algorithm: GitHashAlgorithm::Sha1,
                records: &[record(1, "README.md", 1)],
            },
        )
        .await
        .unwrap();
        write_git_source_index(
            &storage,
            GitSourceIndexWrite {
                tenant_id: 8,
                repository_id: "repo-alpha",
                generation: 2,
                source_hash: [2; 32],
                hash_algorithm: GitHashAlgorithm::Sha1,
                records: &[record(2, "src/lib.rs", 2)],
            },
        )
        .await
        .unwrap();

        let latest = read_latest_git_source_index(&storage, 8, "repo-alpha")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.header.generation, 2);
        assert_eq!(latest.records[0].tree_path, b"src/lib.rs".to_vec());
    }

    #[tokio::test]
    async fn git_source_queries_find_object_path_and_tree() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let records = vec![
            record(1, "src/lib.rs", 1),
            record(1, "src/main.rs", 2),
            record(1, "README.md", 3),
            record(2, "src/lib.rs", 4),
        ];
        let path = write_git_source_index(
            &storage,
            GitSourceIndexWrite {
                tenant_id: 8,
                repository_id: "repo-alpha",
                generation: 1,
                source_hash: [3; 32],
                hash_algorithm: GitHashAlgorithm::Sha1,
                records: &records,
            },
        )
        .await
        .unwrap();
        let index = read_git_source_index(path).await.unwrap();

        let blob = get_git_blob_by_path(&index, &[1; 20], "/src/lib.rs")
            .unwrap()
            .unwrap();
        assert_eq!(blob.object_id, vec![1; 20]);
        assert_eq!(blob.blob_start, 100);

        let tree = list_git_tree(&index, &[1; 20], "src").unwrap();
        assert_eq!(
            tree.iter()
                .map(|entry| entry.tree_path.as_str())
                .collect::<Vec<_>>(),
            vec!["src/lib.rs", "src/main.rs"]
        );

        let objects = get_git_object(&index, &[1; 20]).unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].tree_path, "src/lib.rs");
    }

    #[test]
    fn git_source_queries_reject_unsafe_paths() {
        assert!(normalize_tree_path("../secret").is_err());
        assert!(normalize_tree_path("src\\main.rs").is_err());
        assert!(normalize_prefix("/").unwrap().is_empty());
    }

    fn record(commit: u8, path: &str, object: u8) -> GitSourceRecord {
        GitSourceRecord::new(
            GitHashAlgorithm::Sha1,
            b"repo-alpha".to_vec(),
            vec![commit; 20],
            vec![object; 20],
            path.as_bytes().to_vec(),
            u64::from(object) * 100,
            44,
            [object; 16],
        )
        .unwrap()
    }
}
