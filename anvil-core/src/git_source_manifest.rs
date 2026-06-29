use crate::storage::Storage;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitSourceRepositoryManifest {
    pub format_version: u32,
    pub tenant_id: i64,
    pub repository_id: String,
    pub bucket_name: String,
    pub object_key: String,
    pub pack_object_version_id: String,
    pub source_hash: String,
    pub generation: u64,
    pub record_count: u64,
    pub index_path: String,
    pub updated_at: String,
}

pub async fn write_git_source_repository_manifest(
    storage: &Storage,
    manifest: &GitSourceRepositoryManifest,
) -> Result<()> {
    validate_manifest(manifest)?;
    let path = storage.git_source_manifest_path(manifest.tenant_id, &manifest.repository_id)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::write(&tmp, bytes).await?;
    tokio::fs::rename(tmp, path).await?;
    Ok(())
}

pub async fn read_git_source_repository_manifest(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<GitSourceRepositoryManifest>> {
    let path = storage.git_source_manifest_path(tenant_id, repository_id)?;
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let manifest: GitSourceRepositoryManifest = serde_json::from_slice(&bytes)?;
    validate_manifest(&manifest)?;
    if manifest.tenant_id != tenant_id || manifest.repository_id != repository_id {
        return Err(anyhow!("git source manifest path scope mismatch"));
    }
    Ok(Some(manifest))
}

fn validate_manifest(manifest: &GitSourceRepositoryManifest) -> Result<()> {
    if manifest.format_version != 1 {
        return Err(anyhow!("unsupported git source manifest version"));
    }
    if manifest.tenant_id <= 0 {
        return Err(anyhow!("git source manifest tenant id must be positive"));
    }
    require_nonempty(&manifest.repository_id, "repository_id")?;
    require_nonempty(&manifest.bucket_name, "bucket_name")?;
    require_nonempty(&manifest.object_key, "object_key")?;
    require_nonempty(&manifest.pack_object_version_id, "pack_object_version_id")?;
    require_nonempty(&manifest.source_hash, "source_hash")?;
    require_nonempty(&manifest.index_path, "index_path")?;
    if manifest.generation == 0 {
        return Err(anyhow!("git source manifest generation must be positive"));
    }
    if !manifest.index_path.starts_with("_anvil/") {
        return Err(anyhow!(
            "git source manifest index path must be storage-relative"
        ));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("git source manifest {field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn git_source_manifest_round_trips_and_validates_scope() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let manifest = GitSourceRepositoryManifest {
            format_version: 1,
            tenant_id: 3,
            repository_id: "repo-alpha".to_string(),
            bucket_name: "source-packs".to_string(),
            object_key: "git-source/repo-alpha/packs/pack.pack".to_string(),
            pack_object_version_id: "00000000-0000-0000-0000-000000000001".to_string(),
            source_hash: hex::encode([4; 32]),
            generation: 7,
            record_count: 12,
            index_path: "_anvil/git/tenants/tenant-3/repositories/repo-alpha/indexes/generation-00000000000000000007-source.angit".to_string(),
            updated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        };
        write_git_source_repository_manifest(&storage, &manifest)
            .await
            .unwrap();
        let read = read_git_source_repository_manifest(&storage, 3, "repo-alpha")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, manifest);
        assert!(
            read_git_source_repository_manifest(&storage, 3, "repo-beta")
                .await
                .unwrap()
                .is_none()
        );
    }
}
