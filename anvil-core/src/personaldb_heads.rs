use crate::{formats::hash32, personaldb_control::PersonalDbGroupManifest, storage::Storage};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::io::ErrorKind;
use std::path::Path;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbCommittedHead {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub log_index: u64,
    pub log_hash: String,
    pub segment_path: String,
    pub row_index_generation: u64,
    pub policy_epoch: u64,
    pub membership_epoch: u64,
    pub schema_hash: String,
    pub updated_at: String,
    pub updated_by_node: String,
    pub head_hash: Option<String>,
    pub head_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbSnapshotsHead {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub latest_snapshot_log_index: u64,
    pub latest_snapshot_log_hash: String,
    pub latest_snapshot_manifest_path: String,
    pub retained_snapshot_count: u32,
    pub updated_at: String,
    pub updated_by_node: String,
    pub head_hash: Option<String>,
    pub head_signature: Option<String>,
}

impl PersonalDbCommittedHead {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_committed_head_unsigned(&self)?;
        let hash = hash_committed_head(&self)?;
        let signature = sign_head_hash(
            signing_key,
            "personaldb_committed_head",
            &hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.log_index.to_string(),
            ],
        )?;
        self.head_hash = Some(hash);
        self.head_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_committed_head_unsigned(self)?;
        let expected_hash = hash_committed_head(self)?;
        if self.head_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb committed head hash mismatch"));
        }
        let expected_signature = sign_head_hash(
            signing_key,
            "personaldb_committed_head",
            &expected_hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.log_index.to_string(),
            ],
        )?;
        if self.head_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("personaldb committed head signature mismatch"));
        }
        Ok(())
    }
}

impl PersonalDbSnapshotsHead {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_snapshots_head_unsigned(&self)?;
        let hash = hash_snapshots_head(&self)?;
        let signature = sign_head_hash(
            signing_key,
            "personaldb_snapshots_head",
            &hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.latest_snapshot_log_index.to_string(),
            ],
        )?;
        self.head_hash = Some(hash);
        self.head_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_snapshots_head_unsigned(self)?;
        let expected_hash = hash_snapshots_head(self)?;
        if self.head_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb snapshots head hash mismatch"));
        }
        let expected_signature = sign_head_hash(
            signing_key,
            "personaldb_snapshots_head",
            &expected_hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.latest_snapshot_log_index.to_string(),
            ],
        )?;
        if self.head_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("personaldb snapshots head signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_committed_head(head: &PersonalDbCommittedHead) -> Result<String> {
    let mut unsigned = head.clone();
    unsigned.head_hash = None;
    unsigned.head_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub fn hash_snapshots_head(head: &PersonalDbSnapshotsHead) -> Result<String> {
    let mut unsigned = head.clone();
    unsigned.head_hash = None;
    unsigned.head_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub async fn write_personaldb_group_manifest(
    storage: &Storage,
    tenant_id: i64,
    manifest: &PersonalDbGroupManifest,
    signing_key: &[u8],
) -> Result<()> {
    manifest.verify(signing_key)?;
    ensure_head_scope(
        tenant_id,
        &manifest.database_id,
        &manifest.tenant_id,
        &manifest.database_id,
    )?;
    let path = storage.personaldb_group_manifest_path(tenant_id, &manifest.database_id)?;
    write_json_atomically(&path, manifest).await
}

pub async fn read_personaldb_group_manifest(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbGroupManifest>> {
    let path = storage.personaldb_group_manifest_path(tenant_id, database_id)?;
    let Some(manifest) = read_json_optional::<PersonalDbGroupManifest>(&path).await? else {
        return Ok(None);
    };
    manifest.verify(signing_key)?;
    ensure_head_scope(
        tenant_id,
        database_id,
        &manifest.tenant_id,
        &manifest.database_id,
    )?;
    Ok(Some(manifest))
}

pub async fn write_personaldb_committed_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbCommittedHead,
    signing_key: &[u8],
) -> Result<()> {
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    let path = storage.personaldb_committed_head_path(tenant_id, database_id)?;
    write_json_atomically(&path, head).await
}

pub async fn read_personaldb_committed_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbCommittedHead>> {
    let path = storage.personaldb_committed_head_path(tenant_id, database_id)?;
    let Some(head) = read_json_optional::<PersonalDbCommittedHead>(&path).await? else {
        return Ok(None);
    };
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    Ok(Some(head))
}

pub async fn write_personaldb_snapshots_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    head: &PersonalDbSnapshotsHead,
    signing_key: &[u8],
) -> Result<()> {
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    let path = storage.personaldb_snapshots_head_path(tenant_id, database_id)?;
    write_json_atomically(&path, head).await
}

pub async fn read_personaldb_snapshots_head(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    signing_key: &[u8],
) -> Result<Option<PersonalDbSnapshotsHead>> {
    let path = storage.personaldb_snapshots_head_path(tenant_id, database_id)?;
    let Some(head) = read_json_optional::<PersonalDbSnapshotsHead>(&path).await? else {
        return Ok(None);
    };
    head.verify(signing_key)?;
    ensure_head_scope(tenant_id, database_id, &head.tenant_id, &head.database_id)?;
    Ok(Some(head))
}

fn validate_committed_head_unsigned(head: &PersonalDbCommittedHead) -> Result<()> {
    if head.format_version != 1 {
        return Err(anyhow!("unsupported personaldb committed head version"));
    }
    validate_hex32(&head.log_hash, "log_hash")?;
    validate_hex32(&head.schema_hash, "schema_hash")?;
    require_nonempty(&head.tenant_id, "tenant_id")?;
    require_nonempty(&head.database_id, "database_id")?;
    require_nonempty(&head.segment_path, "segment_path")?;
    require_nonempty(&head.updated_at, "updated_at")?;
    require_nonempty(&head.updated_by_node, "updated_by_node")?;
    Ok(())
}

fn validate_snapshots_head_unsigned(head: &PersonalDbSnapshotsHead) -> Result<()> {
    if head.format_version != 1 {
        return Err(anyhow!("unsupported personaldb snapshots head version"));
    }
    validate_hex32(&head.latest_snapshot_log_hash, "latest_snapshot_log_hash")?;
    require_nonempty(&head.tenant_id, "tenant_id")?;
    require_nonempty(&head.database_id, "database_id")?;
    require_nonempty(
        &head.latest_snapshot_manifest_path,
        "latest_snapshot_manifest_path",
    )?;
    require_nonempty(&head.updated_at, "updated_at")?;
    require_nonempty(&head.updated_by_node, "updated_by_node")?;
    Ok(())
}

fn ensure_head_scope(
    expected_tenant_id: i64,
    expected_database_id: &str,
    actual_tenant_id: &str,
    actual_database_id: &str,
) -> Result<()> {
    if actual_tenant_id != expected_tenant_id.to_string() {
        return Err(anyhow!("personaldb head tenant scope mismatch"));
    }
    if actual_database_id != expected_database_id {
        return Err(anyhow!("personaldb head database scope mismatch"));
    }
    Ok(())
}

fn sign_head_hash(
    signing_key: &[u8],
    domain: &str,
    hash: &str,
    scope_parts: &[&str],
) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("personaldb head signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(domain.as_bytes());
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

async fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension("json.tmp");
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
    T: for<'de> Deserialize<'de>,
{
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be 32 bytes encoded as hex"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"personaldb head signing key";

    #[tokio::test]
    async fn committed_head_round_trips_at_spec_path() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let head = sample_committed_head().seal(KEY).unwrap();

        write_personaldb_committed_head(&storage, 7, "db-alpha", &head, KEY)
            .await
            .unwrap();

        let path = storage
            .personaldb_committed_head_path(7, "db-alpha")
            .unwrap();
        assert!(
            path.ends_with(
                "_anvil/personaldb/tenants/tenant-7/groups/db-alpha/heads/committed.json"
            )
        );

        let read = read_personaldb_committed_head(&storage, 7, "db-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, head);
        read.verify(KEY).unwrap();
    }

    #[tokio::test]
    async fn snapshots_head_round_trips_at_spec_path() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let head = sample_snapshots_head().seal(KEY).unwrap();

        write_personaldb_snapshots_head(&storage, 7, "db-alpha", &head, KEY)
            .await
            .unwrap();

        let path = storage
            .personaldb_snapshots_head_path(7, "db-alpha")
            .unwrap();
        assert!(
            path.ends_with(
                "_anvil/personaldb/tenants/tenant-7/groups/db-alpha/heads/snapshots.json"
            )
        );

        let read = read_personaldb_snapshots_head(&storage, 7, "db-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, head);
        read.verify(KEY).unwrap();
    }

    #[tokio::test]
    async fn group_manifest_round_trips_at_spec_path() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let manifest = sample_group_manifest().seal(KEY).unwrap();

        write_personaldb_group_manifest(&storage, 7, &manifest, KEY)
            .await
            .unwrap();

        let path = storage
            .personaldb_group_manifest_path(7, "db-alpha")
            .unwrap();
        assert!(path.ends_with("_anvil/personaldb/tenants/tenant-7/groups/db-alpha/manifest.json"));

        let read = read_personaldb_group_manifest(&storage, 7, "db-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read, manifest);
    }

    #[tokio::test]
    async fn missing_heads_return_none() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        assert!(
            read_personaldb_committed_head(&storage, 7, "db-alpha", KEY)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_personaldb_snapshots_head(&storage, 7, "db-alpha", KEY)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn tampered_committed_head_is_rejected() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let head = sample_committed_head().seal(KEY).unwrap();
        write_personaldb_committed_head(&storage, 7, "db-alpha", &head, KEY)
            .await
            .unwrap();

        let path = storage
            .personaldb_committed_head_path(7, "db-alpha")
            .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["log_index"] = serde_json::json!(head.log_index + 1);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();

        assert!(
            read_personaldb_committed_head(&storage, 7, "db-alpha", KEY)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_hashes_and_scope_are_rejected() {
        let invalid_hash = PersonalDbCommittedHead {
            log_hash: "not-hex".to_string(),
            ..sample_committed_head()
        };
        assert!(invalid_hash.seal(KEY).is_err());

        let wrong_scope = PersonalDbCommittedHead {
            database_id: "db-beta".to_string(),
            ..sample_committed_head()
        }
        .seal(KEY)
        .unwrap();
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        assert!(
            write_personaldb_committed_head(&storage, 7, "db-alpha", &wrong_scope, KEY)
                .await
                .is_err()
        );
        assert!(
            storage
                .personaldb_committed_head_path(7, "../escape")
                .is_err()
        );
    }

    fn sample_committed_head() -> PersonalDbCommittedHead {
        PersonalDbCommittedHead {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            log_index: 42,
            log_hash: hex::encode([1; 32]),
            segment_path: "_anvil/personaldb/tenants/tenant-7/groups/db-alpha/log/segments/00000000000000000001-00000000000000000042-segment.pdbseg".to_string(),
            row_index_generation: 3,
            policy_epoch: 5,
            membership_epoch: 8,
            schema_hash: hex::encode([2; 32]),
            updated_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            updated_by_node: "node-a".to_string(),
            head_hash: None,
            head_signature: None,
        }
    }

    fn sample_snapshots_head() -> PersonalDbSnapshotsHead {
        PersonalDbSnapshotsHead {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            latest_snapshot_log_index: 1024,
            latest_snapshot_log_hash: hex::encode([3; 32]),
            latest_snapshot_manifest_path: "_anvil/personaldb/tenants/tenant-7/groups/db-alpha/snapshots/manifests/00000000000000001024-state.json".to_string(),
            retained_snapshot_count: 2,
            updated_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            updated_by_node: "node-a".to_string(),
            head_hash: None,
            head_signature: None,
        }
    }

    fn sample_group_manifest() -> PersonalDbGroupManifest {
        PersonalDbGroupManifest {
            format_version: 1,
            tenant_id: "7".to_string(),
            database_id: "db-alpha".to_string(),
            schema_hash: hex::encode([1; 32]),
            genesis_hash: hex::encode([2; 32]),
            created_at: "2026-06-27T00:00:00.000000000Z".to_string(),
            created_by: "principal-a".to_string(),
            consistency_policy: "StrictWitnessed".to_string(),
            object_layout_version: 1,
            active_membership_epoch: 8,
            active_policy_epoch: 5,
            current_row_index_generation: 3,
            current_projection_generation: 0,
            manifest_hash: None,
            manifest_signature: None,
        }
    }
}
