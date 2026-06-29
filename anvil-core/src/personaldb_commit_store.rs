use crate::{
    formats::{Hash32, hash32},
    personaldb_control::PersonalDbCommitCertificate,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use serde::{Serialize, de::DeserializeOwned};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalDbChangesetPayloadPaths {
    pub by_index_path: PathBuf,
    pub by_hash_path: PathBuf,
}

pub async fn write_personaldb_changeset_payload(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    expected_payload_hash: Hash32,
    changeset_bytes: &[u8],
) -> Result<PersonalDbChangesetPayloadPaths> {
    let actual_hash = hash32(changeset_bytes);
    if actual_hash != expected_payload_hash {
        return Err(anyhow!("personaldb changeset payload hash mismatch"));
    }

    let payload_hash_hex = hex::encode(expected_payload_hash);
    let by_hash_path = storage.personaldb_changeset_payload_by_hash_path(
        tenant_id,
        database_id,
        &payload_hash_hex,
    )?;
    let by_index_path = storage.personaldb_changeset_payload_by_index_path(
        tenant_id,
        database_id,
        log_index,
        &payload_hash_hex,
    )?;

    write_once(&by_hash_path, changeset_bytes).await?;
    link_or_copy(&by_hash_path, &by_index_path).await?;

    Ok(PersonalDbChangesetPayloadPaths {
        by_index_path,
        by_hash_path,
    })
}

pub async fn read_personaldb_changeset_payload_by_hash(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let path = storage.personaldb_changeset_payload_by_hash_path(
        tenant_id,
        database_id,
        &hex::encode(payload_hash),
    )?;
    read_payload_optional(&path, payload_hash).await
}

pub async fn read_personaldb_changeset_payload_by_index(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let path = storage.personaldb_changeset_payload_by_index_path(
        tenant_id,
        database_id,
        log_index,
        &hex::encode(payload_hash),
    )?;
    read_payload_optional(&path, payload_hash).await
}

pub async fn write_personaldb_commit_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    certificate: &PersonalDbCommitCertificate,
    signing_key: &[u8],
) -> Result<PathBuf> {
    certificate.verify(signing_key)?;
    ensure_scope(
        tenant_id,
        database_id,
        &certificate.tenant_id,
        &certificate.database_id,
    )?;
    let path = storage.personaldb_commit_certificate_path(
        tenant_id,
        database_id,
        certificate.log_index,
        &certificate.entry_hash,
    )?;
    write_json_atomically(&path, certificate).await?;
    Ok(path)
}

pub async fn read_personaldb_commit_certificate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    log_index: u64,
    entry_hash: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbCommitCertificate>> {
    let path = storage.personaldb_commit_certificate_path(
        tenant_id,
        database_id,
        log_index,
        entry_hash,
    )?;
    let Some(certificate) = read_json_optional::<PersonalDbCommitCertificate>(&path).await? else {
        return Ok(None);
    };
    certificate.verify(signing_key)?;
    ensure_scope(
        tenant_id,
        database_id,
        &certificate.tenant_id,
        &certificate.database_id,
    )?;
    if certificate.log_index != log_index || certificate.entry_hash != entry_hash {
        return Err(anyhow!("personaldb commit certificate path scope mismatch"));
    }
    Ok(Some(certificate))
}

async fn read_payload_optional(
    path: &Path,
    expected_payload_hash: Hash32,
) -> Result<Option<Vec<u8>>> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    if hash32(&bytes) != expected_payload_hash {
        return Err(anyhow!("personaldb changeset payload hash mismatch"));
    }
    Ok(Some(bytes))
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
                Err(anyhow!("personaldb immutable payload path collision"))
            }
        }
        Err(err) => Err(err).with_context(|| format!("write {}", path.display())),
    }
}

async fn link_or_copy(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if tokio::fs::hard_link(source, destination).await.is_ok() {
        return Ok(());
    }
    let bytes = tokio::fs::read(source).await?;
    write_once(destination, &bytes).await
}

async fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
    let bytes = serde_json::to_vec_pretty(value)?;
    tokio::fs::write(&tmp, bytes)
        .await
        .with_context(|| format!("write temporary personaldb JSON file {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish personaldb JSON file {}", path.display()))?;
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

fn ensure_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    actual_tenant_id: &str,
    actual_database_id: &str,
) -> Result<()> {
    if actual_tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("personaldb commit tenant scope mismatch"));
    }
    if actual_database_id != expected_database_id {
        return Err(anyhow!("personaldb commit database scope mismatch"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"personaldb commit signing key";

    #[tokio::test]
    async fn changeset_payload_is_written_by_hash_and_index() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload = b"sqlite changeset bytes";
        let payload_hash = hash32(payload);

        let paths =
            write_personaldb_changeset_payload(&storage, 9, "db-alpha", 42, payload_hash, payload)
                .await
                .unwrap();

        assert!(paths.by_index_path.ends_with(format!(
            "_anvil/personaldb/tenants/tenant-9/groups/db-alpha/log/payloads/by-index/00000000000000000042-{}.sqlite-changeset",
            hex::encode(payload_hash)
        )));
        assert!(paths.by_hash_path.ends_with(format!(
            "_anvil/personaldb/tenants/tenant-9/groups/db-alpha/log/payloads/by-hash/{}.sqlite-changeset",
            hex::encode(payload_hash)
        )));

        let by_hash =
            read_personaldb_changeset_payload_by_hash(&storage, 9, "db-alpha", payload_hash)
                .await
                .unwrap()
                .unwrap();
        let by_index =
            read_personaldb_changeset_payload_by_index(&storage, 9, "db-alpha", 42, payload_hash)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(by_hash, payload);
        assert_eq!(by_index, payload);
    }

    #[tokio::test]
    async fn changeset_payload_rejects_hash_mismatch_and_path_collision() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let payload_hash = hash32(b"good");

        assert!(
            write_personaldb_changeset_payload(&storage, 9, "db-alpha", 1, payload_hash, b"bad")
                .await
                .is_err()
        );

        write_personaldb_changeset_payload(&storage, 9, "db-alpha", 1, payload_hash, b"good")
            .await
            .unwrap();
        let by_hash_path = storage
            .personaldb_changeset_payload_by_hash_path(9, "db-alpha", &hex::encode(payload_hash))
            .unwrap();
        tokio::fs::write(by_hash_path, b"corrupt").await.unwrap();
        assert!(
            read_personaldb_changeset_payload_by_hash(&storage, 9, "db-alpha", payload_hash)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn commit_certificate_round_trips_at_spec_path() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let certificate = sample_certificate().seal(KEY).unwrap();

        let path = write_personaldb_commit_certificate(&storage, 9, "db-alpha", &certificate, KEY)
            .await
            .unwrap();

        assert!(path.ends_with(format!(
            "_anvil/personaldb/tenants/tenant-9/groups/db-alpha/log/certificates/00000000000000000042-{}.certificate.json",
            certificate.entry_hash
        )));

        let read = read_personaldb_commit_certificate(
            &storage,
            9,
            "db-alpha",
            42,
            &certificate.entry_hash,
            KEY,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read, certificate);
    }

    #[tokio::test]
    async fn commit_certificate_tamper_and_scope_mismatch_are_rejected() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let certificate = sample_certificate().seal(KEY).unwrap();
        write_personaldb_commit_certificate(&storage, 9, "db-alpha", &certificate, KEY)
            .await
            .unwrap();

        let path = storage
            .personaldb_commit_certificate_path(9, "db-alpha", 42, &certificate.entry_hash)
            .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["authz_revision"] = serde_json::json!(99);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();
        assert!(
            read_personaldb_commit_certificate(
                &storage,
                9,
                "db-alpha",
                42,
                &certificate.entry_hash,
                KEY,
            )
            .await
            .is_err()
        );

        let wrong_scope = PersonalDbCommitCertificate {
            database_id: "db-beta".to_string(),
            ..sample_certificate()
        }
        .seal(KEY)
        .unwrap();
        assert!(
            write_personaldb_commit_certificate(&storage, 9, "db-alpha", &wrong_scope, KEY)
                .await
                .is_err()
        );
    }

    fn sample_certificate() -> PersonalDbCommitCertificate {
        PersonalDbCommitCertificate {
            format_version: 1,
            tenant_id: "9".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 42,
            previous_log_hash: hex::encode([0; 32]),
            entry_hash: hex::encode([1; 32]),
            changeset_payload_hash: hex::encode(hash32(b"sqlite changeset bytes")),
            verified_envelope_hash: hex::encode([3; 32]),
            client_log_epoch: 1,
            membership_epoch: 2,
            policy_epoch: 3,
            leader_replica_id: "leader-a".to_string(),
            voter_acks_hash: hex::encode([4; 32]),
            authz_revision: 5,
            witness_node_id: "node-a".to_string(),
            witnessed_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            certificate_hash: None,
            witness_signature: None,
        }
    }
}
