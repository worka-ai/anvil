use crate::{
    formats::{Hash32, hash32},
    personaldb_control::PersonalDbSnapshotManifest,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use serde::{Serialize, de::DeserializeOwned};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotWriteResult {
    pub object_path: PathBuf,
    pub manifest_path: PathBuf,
}

pub async fn write_personaldb_snapshot(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    compressed_sqlite_bytes: &[u8],
    manifest: &PersonalDbSnapshotManifest,
    signing_key: &[u8],
) -> Result<PersonalDbSnapshotWriteResult> {
    manifest.verify(signing_key)?;
    ensure_manifest_scope(tenant_id, database_id, manifest)?;
    let state_hash = decode_hex32(&manifest.state_hash, "state_hash")?;
    let snapshot_object_hash =
        decode_hex32(&manifest.snapshot_object_hash, "snapshot_object_hash")?;
    if hash32(compressed_sqlite_bytes) != snapshot_object_hash {
        return Err(anyhow!("personaldb snapshot object hash mismatch"));
    }

    let object_path = storage.personaldb_snapshot_object_path(
        tenant_id,
        database_id,
        manifest.log_index,
        &manifest.state_hash,
    )?;
    let expected_object_relative = storage.relative_storage_path(&object_path)?;
    if manifest.snapshot_object_key != expected_object_relative {
        return Err(anyhow!(
            "personaldb snapshot object key does not match storage path"
        ));
    }

    write_once(&object_path, compressed_sqlite_bytes).await?;
    let manifest_path = storage.personaldb_snapshot_manifest_path(
        tenant_id,
        database_id,
        manifest.log_index,
        &hex::encode(state_hash),
    )?;
    write_json_atomically(&manifest_path, manifest).await?;
    Ok(PersonalDbSnapshotWriteResult {
        object_path,
        manifest_path,
    })
}

pub async fn read_personaldb_snapshot_manifest(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    state_hash: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbSnapshotManifest>> {
    let path =
        storage.personaldb_snapshot_manifest_path(tenant_id, database_id, log_index, state_hash)?;
    let Some(manifest) = read_json_optional::<PersonalDbSnapshotManifest>(&path).await? else {
        return Ok(None);
    };
    manifest.verify(signing_key)?;
    ensure_manifest_scope(tenant_id, database_id, &manifest)?;
    if manifest.log_index != log_index || manifest.state_hash != state_hash {
        return Err(anyhow!("personaldb snapshot manifest path scope mismatch"));
    }
    Ok(Some(manifest))
}

pub async fn read_personaldb_snapshot_object(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    manifest: &PersonalDbSnapshotManifest,
    signing_key: &[u8],
) -> Result<Option<Vec<u8>>> {
    manifest.verify(signing_key)?;
    ensure_manifest_scope(tenant_id, database_id, manifest)?;
    let path = storage.personaldb_snapshot_object_path(
        tenant_id,
        database_id,
        manifest.log_index,
        &manifest.state_hash,
    )?;
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    if hash32(&bytes) != decode_hex32(&manifest.snapshot_object_hash, "snapshot_object_hash")? {
        return Err(anyhow!("personaldb snapshot object hash mismatch"));
    }
    Ok(Some(bytes))
}

fn ensure_manifest_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    manifest: &PersonalDbSnapshotManifest,
) -> Result<()> {
    if manifest.tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("personaldb snapshot tenant scope mismatch"));
    }
    if manifest.database_id != expected_database_id {
        return Err(anyhow!("personaldb snapshot database scope mismatch"));
    }
    Ok(())
}

async fn write_once(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
    {
        Ok(mut file) => {
            use tokio::io::AsyncWriteExt;
            file.write_all(bytes).await?;
            file.sync_data().await?;
            Ok(())
        }
        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
            let existing = tokio::fs::read(path).await?;
            if existing == bytes {
                Ok(())
            } else {
                Err(anyhow!("personaldb immutable snapshot path collision"))
            }
        }
        Err(err) => Err(err).with_context(|| format!("write {}", path.display())),
    }
}

async fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| {
            format!(
                "write temporary personaldb snapshot manifest {}",
                tmp.display()
            )
        })?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish personaldb snapshot manifest {}", path.display()))?;
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

fn decode_hex32(value: &str, field: &'static str) -> Result<Hash32> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow!("{field} must be hex32"))?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"personaldb snapshot signing key";

    #[tokio::test]
    async fn snapshot_object_and_manifest_round_trip_at_spec_paths() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bytes = b"zstd sqlite snapshot bytes";
        let manifest = sample_manifest(&storage, bytes).seal(KEY).unwrap();

        let result = write_personaldb_snapshot(&storage, 6, "db-alpha", bytes, &manifest, KEY)
            .await
            .unwrap();
        assert!(result.object_path.ends_with(format!(
            "_anvil/personaldb/tenants/tenant-6/groups/db-alpha/snapshots/objects/00000000000000001024-{}.sqlite.zst",
            manifest.state_hash
        )));
        assert!(result.manifest_path.ends_with(format!(
            "_anvil/personaldb/tenants/tenant-6/groups/db-alpha/snapshots/manifests/00000000000000001024-{}.json",
            manifest.state_hash
        )));

        let read_manifest = read_personaldb_snapshot_manifest(
            &storage,
            6,
            "db-alpha",
            1024,
            &manifest.state_hash,
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read_manifest, manifest);
        let read_bytes = read_personaldb_snapshot_object(&storage, 6, "db-alpha", &manifest, KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_bytes, bytes);
    }

    #[tokio::test]
    async fn snapshot_write_rejects_hash_scope_and_path_mismatch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bytes = b"zstd sqlite snapshot bytes";
        let manifest = sample_manifest(&storage, bytes).seal(KEY).unwrap();
        assert!(
            write_personaldb_snapshot(&storage, 6, "db-alpha", b"wrong", &manifest, KEY)
                .await
                .is_err()
        );

        let wrong_scope = PersonalDbSnapshotManifest {
            database_id: "db-beta".to_string(),
            ..sample_manifest(&storage, bytes)
        }
        .seal(KEY)
        .unwrap();
        assert!(
            write_personaldb_snapshot(&storage, 6, "db-alpha", bytes, &wrong_scope, KEY)
                .await
                .is_err()
        );

        let wrong_path = PersonalDbSnapshotManifest {
            snapshot_object_key: "_anvil/personaldb/wrong.sqlite.zst".to_string(),
            ..sample_manifest(&storage, bytes)
        }
        .seal(KEY)
        .unwrap();
        assert!(
            write_personaldb_snapshot(&storage, 6, "db-alpha", bytes, &wrong_path, KEY)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn snapshot_reader_rejects_tampered_object() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bytes = b"zstd sqlite snapshot bytes";
        let manifest = sample_manifest(&storage, bytes).seal(KEY).unwrap();
        write_personaldb_snapshot(&storage, 6, "db-alpha", bytes, &manifest, KEY)
            .await
            .unwrap();
        let path = storage
            .personaldb_snapshot_object_path(6, "db-alpha", 1024, &manifest.state_hash)
            .unwrap();
        tokio::fs::write(path, b"corrupt").await.unwrap();
        assert!(
            read_personaldb_snapshot_object(&storage, 6, "db-alpha", &manifest, KEY)
                .await
                .is_err()
        );
    }

    fn sample_manifest(storage: &Storage, bytes: &[u8]) -> PersonalDbSnapshotManifest {
        let state_hash = hex::encode([1; 32]);
        let object_path = storage
            .personaldb_snapshot_object_path(6, "db-alpha", 1024, &state_hash)
            .unwrap();
        PersonalDbSnapshotManifest {
            format_version: 1,
            tenant_id: "6".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 1024,
            log_hash: hex::encode([2; 32]),
            state_hash,
            schema_hash: hex::encode([3; 32]),
            snapshot_object_key: storage.relative_storage_path(&object_path).unwrap(),
            snapshot_object_hash: hex::encode(hash32(bytes)),
            source_segment_start: 1,
            source_segment_end: 1024,
            row_index_generation: 4,
            created_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            created_by_node: "node-a".to_string(),
            manifest_hash: None,
            manifest_signature: None,
        }
    }
}
