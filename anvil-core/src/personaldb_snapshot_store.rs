use crate::{
    core_store::{CompareAndSwapRef, CoreObjectRef, CoreStore, GetBlob, PutBlob},
    formats::{Hash32, hash32},
    personaldb_control::PersonalDbSnapshotManifest,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

const PERSONALDB_SNAPSHOT_OBJECT_REF_PREFIX: &str = "personaldb_snapshot_object:";
const PERSONALDB_SNAPSHOT_MANIFEST_REF_PREFIX: &str = "personaldb_snapshot_manifest:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbSnapshotWriteResult {
    pub object_ref: String,
    pub manifest_ref: String,
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

    let object_ref = personaldb_snapshot_object_ref_name(
        tenant_id,
        database_id,
        manifest.log_index,
        &manifest.state_hash,
    )?;
    if manifest.snapshot_object_key != object_ref {
        return Err(anyhow!(
            "personaldb snapshot object key does not match CoreStore ref"
        ));
    }

    let store = CoreStore::new(storage.clone()).await?;
    put_immutable_ref_bytes(
        &store,
        &object_ref,
        compressed_sqlite_bytes,
        "personaldb-snapshot-object",
        &format!(
            "personaldb-snapshot-object:{tenant_id}:{database_id}:{}",
            manifest.log_index
        ),
    )
    .await?;
    let manifest_ref = personaldb_snapshot_manifest_ref_name(
        tenant_id,
        database_id,
        manifest.log_index,
        &hex::encode(state_hash),
    )?;
    let manifest_bytes = serde_json::to_vec_pretty(manifest)?;
    put_immutable_ref_bytes(
        &store,
        &manifest_ref,
        &manifest_bytes,
        "personaldb-snapshot-manifest",
        &format!(
            "personaldb-snapshot-manifest:{tenant_id}:{database_id}:{}",
            manifest.log_index
        ),
    )
    .await?;
    Ok(PersonalDbSnapshotWriteResult {
        object_ref,
        manifest_ref,
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
    let manifest_ref =
        personaldb_snapshot_manifest_ref_name(tenant_id, database_id, log_index, state_hash)?;
    let Some(manifest) =
        read_personaldb_snapshot_manifest_by_ref(storage, &manifest_ref, signing_key).await?
    else {
        return Ok(None);
    };
    ensure_manifest_scope(tenant_id, database_id, &manifest)?;
    if manifest.log_index != log_index || manifest.state_hash != state_hash {
        return Err(anyhow!("personaldb snapshot manifest ref scope mismatch"));
    }
    Ok(Some(manifest))
}

pub async fn read_personaldb_snapshot_manifest_by_ref(
    storage: &Storage,
    manifest_ref: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbSnapshotManifest>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(manifest_ref).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    let manifest: PersonalDbSnapshotManifest = serde_json::from_slice(&bytes)?;
    manifest.verify(signing_key)?;
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
    let expected_object_ref = personaldb_snapshot_object_ref_name(
        tenant_id,
        database_id,
        manifest.log_index,
        &manifest.state_hash,
    )?;
    if manifest.snapshot_object_key != expected_object_ref {
        return Err(anyhow!(
            "personaldb snapshot object key does not match CoreStore ref"
        ));
    }
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&manifest.snapshot_object_key).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
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

fn decode_hex32(value: &str, field: &'static str) -> Result<Hash32> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(hex::decode(value)?
        .try_into()
        .map_err(|_| anyhow!("{field} must be hex32"))?)
}

pub fn personaldb_snapshot_object_ref_name(
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    state_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(state_hash, "state_hash")?;
    Ok(format!(
        "{PERSONALDB_SNAPSHOT_OBJECT_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:log:{log_index:020}:state:{state_hash}"
    ))
}

pub fn personaldb_snapshot_manifest_ref_name(
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    state_hash: &str,
) -> Result<String> {
    validate_scope_component(tenant_id, database_id)?;
    decode_hex32(state_hash, "state_hash")?;
    Ok(format!(
        "{PERSONALDB_SNAPSHOT_MANIFEST_REF_PREFIX}tenant:{tenant_id}:database:{database_id}:log:{log_index:020}:state:{state_hash}"
    ))
}

fn validate_scope_component(tenant_id: i64, database_id: &str) -> Result<()> {
    if tenant_id < 0 {
        return Err(anyhow!("personaldb snapshot tenant id must be nonnegative"));
    }
    if database_id.is_empty()
        || database_id == "."
        || database_id == ".."
        || database_id.contains('/')
        || database_id.contains('\\')
        || database_id.contains(':')
        || database_id.chars().any(char::is_control)
    {
        return Err(anyhow!("database_id is not a safe component"));
    }
    Ok(())
}

async fn put_immutable_ref_bytes(
    store: &CoreStore,
    ref_name: &str,
    bytes: &[u8],
    logical_name: &str,
    mutation_id: &str,
) -> Result<()> {
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: format!("{logical_name}:{ref_name}"),
            bytes: bytes.to_vec(),
            region_id: "local".to_string(),
            mutation_id: mutation_id.to_string(),
        })
        .await?;
    let target = encode_core_object_ref_target(&object_ref)?;
    match store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.to_string(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: target,
            transaction_id: None,
        })
        .await
    {
        Ok(_) => Ok(()),
        Err(err) => {
            let Some(existing) = store.read_ref(ref_name).await? else {
                return Err(err);
            };
            let existing_bytes = store
                .get_blob(GetBlob {
                    object_ref: decode_core_object_ref_target(&existing.target)?,
                })
                .await?;
            if existing_bytes == bytes {
                Ok(())
            } else {
                Err(anyhow!("personaldb immutable snapshot ref collision"))
            }
        }
    }
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"personaldb snapshot signing key";

    #[tokio::test]
    async fn snapshot_object_and_manifest_round_trip_through_corestore_refs() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bytes = b"zstd sqlite snapshot bytes";
        let manifest = sample_manifest(&storage, bytes).seal(KEY).unwrap();

        let result = write_personaldb_snapshot(&storage, 6, "db-alpha", bytes, &manifest, KEY)
            .await
            .unwrap();
        assert_eq!(result.object_ref, manifest.snapshot_object_key);
        assert!(
            result
                .object_ref
                .starts_with("personaldb_snapshot_object:tenant:6:database:db-alpha:")
        );
        assert!(
            result
                .manifest_ref
                .starts_with("personaldb_snapshot_manifest:tenant:6:database:db-alpha:")
        );

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
            snapshot_object_key: "personaldb_snapshot_object:wrong".to_string(),
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
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let corrupt = store
            .put_blob(PutBlob {
                logical_name: "corrupt-snapshot".to_string(),
                bytes: b"corrupt".to_vec(),
                region_id: "local".to_string(),
                mutation_id: "corrupt-snapshot".to_string(),
            })
            .await
            .unwrap();
        let current = store
            .read_ref(&manifest.snapshot_object_key)
            .await
            .unwrap()
            .expect("snapshot object ref exists");
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: manifest.snapshot_object_key.clone(),
                expected_generation: Some(current.generation),
                expected_target: Some(current.target),
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&corrupt).unwrap(),
                transaction_id: None,
            })
            .await
            .unwrap();
        assert!(
            read_personaldb_snapshot_object(&storage, 6, "db-alpha", &manifest, KEY)
                .await
                .is_err()
        );
    }

    fn sample_manifest(_storage: &Storage, bytes: &[u8]) -> PersonalDbSnapshotManifest {
        let state_hash = hex::encode([1; 32]);
        PersonalDbSnapshotManifest {
            format_version: 1,
            tenant_id: "6".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 1024,
            log_hash: hex::encode([2; 32]),
            state_hash: state_hash.clone(),
            schema_hash: hex::encode([3; 32]),
            snapshot_object_key: personaldb_snapshot_object_ref_name(
                6,
                "db-alpha",
                1024,
                &state_hash,
            )
            .unwrap(),
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
