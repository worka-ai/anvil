use crate::{
    core_store::{CompareAndSwapRef, CoreObjectRef, CoreStore, GetBlob, PutBlob},
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use serde::{Deserialize, Serialize};

const GIT_SOURCE_MANIFEST_REF_PREFIX: &str = "git_source_manifest:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
    let ref_name = manifest_ref_name(manifest.tenant_id, &manifest.repository_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.clone(),
            bytes: serde_json::to_vec(manifest)?,
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: format!(
                "git-source-manifest:{}:{}",
                manifest.tenant_id, manifest.repository_id
            ),
        })
        .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name,
            expected_generation: None,
            expected_target: None,
            require_absent: false,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(())
}

pub async fn read_git_source_repository_manifest(
    storage: &Storage,
    tenant_id: i64,
    repository_id: &str,
) -> Result<Option<GitSourceRepositoryManifest>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store
        .read_ref(&manifest_ref_name(tenant_id, repository_id)?)
        .await?
    else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
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
    if !manifest.index_path.starts_with("git_source_index:") {
        return Err(anyhow!(
            "git source manifest index path must be a CoreStore git source index ref"
        ));
    }
    Ok(())
}

fn manifest_ref_name(tenant_id: i64, repository_id: &str) -> Result<String> {
    require_safe_component(repository_id, "repository_id")?;
    Ok(format!(
        "{GIT_SOURCE_MANIFEST_REF_PREFIX}tenant:{tenant_id}:repository:{repository_id}"
    ))
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!(
            "git source manifest {field} is not a safe component"
        ));
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
            index_path: format!(
                "git_source_index:tenant:3:repository:repo-alpha:generation:00000000000000000007:source:{}",
                hex::encode([4; 32])
            ),
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
