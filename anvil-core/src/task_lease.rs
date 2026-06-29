use crate::{formats::hash32, storage::Storage};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::io::ErrorKind;
use std::path::Path;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskLease {
    pub format_version: u16,
    pub task_id: String,
    pub task_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence_token: u64,
    pub source_cursor: u128,
    pub checkpoint_cursor: u128,
    pub lease_epoch: u64,
    pub acquired_at_nanos: i64,
    pub expires_at_nanos: i64,
    pub updated_at_nanos: i64,
    pub lease_hash: Option<String>,
    pub lease_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLeaseAcquire {
    pub task_id: String,
    pub task_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub source_cursor: u128,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
}

impl TaskLease {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_lease(&self)?;
        let hash = hash_task_lease(&self)?;
        let signature = sign_lease_hash(
            signing_key,
            &hash,
            &[
                &self.task_id,
                &self.owner_node_id,
                &self.fence_token.to_string(),
            ],
        )?;
        self.lease_hash = Some(hash);
        self.lease_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_lease(self)?;
        let expected_hash = hash_task_lease(self)?;
        if self.lease_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("task lease hash mismatch"));
        }
        let expected_signature = sign_lease_hash(
            signing_key,
            &expected_hash,
            &[
                &self.task_id,
                &self.owner_node_id,
                &self.fence_token.to_string(),
            ],
        )?;
        if self.lease_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("task lease signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_task_lease(lease: &TaskLease) -> Result<String> {
    let mut unsigned = lease.clone();
    unsigned.lease_hash = None;
    unsigned.lease_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub async fn acquire_task_lease(
    storage: &Storage,
    request: TaskLeaseAcquire,
    signing_key: &[u8],
) -> Result<TaskLease> {
    validate_acquire_request(&request)?;
    let existing = read_task_lease(storage, &request.task_id, signing_key).await?;
    if let Some(existing) = existing.as_ref() {
        if existing.expires_at_nanos > request.now_nanos
            && existing.owner_node_id != request.owner_node_id
        {
            return Err(anyhow!("task lease is owned by another active node"));
        }
    }
    let fence_token = existing
        .as_ref()
        .map(|lease| lease.fence_token.saturating_add(1))
        .unwrap_or(1);
    let lease_epoch = existing
        .as_ref()
        .map(|lease| lease.lease_epoch.saturating_add(1))
        .unwrap_or(1);
    let checkpoint_cursor = existing
        .as_ref()
        .map(|lease| lease.checkpoint_cursor)
        .unwrap_or(0)
        .max(request.source_cursor);
    let lease = TaskLease {
        format_version: 1,
        task_id: request.task_id,
        task_kind: request.task_kind,
        partition_family: request.partition_family,
        partition_id: request.partition_id,
        owner_node_id: request.owner_node_id,
        fence_token,
        source_cursor: request.source_cursor,
        checkpoint_cursor,
        lease_epoch,
        acquired_at_nanos: request.now_nanos,
        expires_at_nanos: request.now_nanos.saturating_add(request.ttl_nanos),
        updated_at_nanos: request.now_nanos,
        lease_hash: None,
        lease_signature: None,
    }
    .seal(signing_key)?;
    write_task_lease(storage, &lease).await?;
    Ok(lease)
}

pub async fn checkpoint_task_lease(
    storage: &Storage,
    task_id: &str,
    owner_node_id: &str,
    fence_token: u64,
    checkpoint_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let Some(mut lease) = read_task_lease(storage, task_id, signing_key).await? else {
        return Err(anyhow!("task lease does not exist"));
    };
    lease.verify(signing_key)?;
    if lease.owner_node_id != owner_node_id || lease.fence_token != fence_token {
        return Err(anyhow!("task lease fence token mismatch"));
    }
    if lease.expires_at_nanos <= now_nanos {
        return Err(anyhow!("task lease expired"));
    }
    if checkpoint_cursor < lease.checkpoint_cursor {
        return Err(anyhow!("task lease checkpoint cannot move backwards"));
    }
    lease.checkpoint_cursor = checkpoint_cursor;
    lease.updated_at_nanos = now_nanos;
    lease = lease.seal(signing_key)?;
    write_task_lease(storage, &lease).await?;
    Ok(lease)
}

pub async fn read_task_lease(
    storage: &Storage,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    let path = storage.task_lease_path(task_id)?;
    let Some(lease) = read_json_optional::<TaskLease>(&path).await? else {
        return Ok(None);
    };
    lease.verify(signing_key)?;
    if lease.task_id != task_id {
        return Err(anyhow!("task lease path scope mismatch"));
    }
    Ok(Some(lease))
}

async fn write_task_lease(storage: &Storage, lease: &TaskLease) -> Result<()> {
    let path = storage.task_lease_path(&lease.task_id)?;
    write_json_atomically(&path, lease).await
}

fn validate_acquire_request(request: &TaskLeaseAcquire) -> Result<()> {
    require_nonempty(&request.task_id, "task_id")?;
    require_nonempty(&request.task_kind, "task_kind")?;
    require_nonempty(&request.partition_family, "partition_family")?;
    validate_hex32(&request.partition_id, "partition_id")?;
    require_nonempty(&request.owner_node_id, "owner_node_id")?;
    if request.ttl_nanos <= 0 {
        return Err(anyhow!("task lease ttl must be positive"));
    }
    if request.now_nanos < 0 {
        return Err(anyhow!("task lease timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_unsigned_lease(lease: &TaskLease) -> Result<()> {
    if lease.format_version != 1 {
        return Err(anyhow!("unsupported task lease version"));
    }
    require_nonempty(&lease.task_id, "task_id")?;
    require_nonempty(&lease.task_kind, "task_kind")?;
    require_nonempty(&lease.partition_family, "partition_family")?;
    validate_hex32(&lease.partition_id, "partition_id")?;
    require_nonempty(&lease.owner_node_id, "owner_node_id")?;
    if lease.fence_token == 0 || lease.lease_epoch == 0 {
        return Err(anyhow!("task lease fence and epoch must be nonzero"));
    }
    if lease.expires_at_nanos <= lease.acquired_at_nanos {
        return Err(anyhow!("task lease expiry must be after acquisition"));
    }
    if lease.updated_at_nanos < lease.acquired_at_nanos {
        return Err(anyhow!("task lease update timestamp is before acquisition"));
    }
    Ok(())
}

fn sign_lease_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("task lease signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"task_lease");
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
    let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| format!("write temporary task lease {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish task lease {}", path.display()))?;
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

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
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

    const KEY: &[u8] = b"task lease signing key";

    #[tokio::test]
    async fn task_lease_acquires_reads_and_checkpoints() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let lease = acquire_task_lease(&storage, acquire("node-a", 100, 500), KEY)
            .await
            .unwrap();
        assert_eq!(lease.fence_token, 1);
        assert_eq!(lease.lease_epoch, 1);
        assert_eq!(lease.checkpoint_cursor, 10);
        assert!(lease.lease_hash.as_deref().unwrap().len() == 64);
        let path = storage.task_lease_path("index-build-alpha").unwrap();
        assert!(path.ends_with("_anvil/tasks/leases/index-build-alpha.json"));

        let checkpointed = checkpoint_task_lease(
            &storage,
            "index-build-alpha",
            "node-a",
            lease.fence_token,
            99,
            200,
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(checkpointed.checkpoint_cursor, 99);
        assert_eq!(
            read_task_lease(&storage, "index-build-alpha", KEY)
                .await
                .unwrap()
                .unwrap(),
            checkpointed
        );
    }

    #[tokio::test]
    async fn active_lease_blocks_other_owner_until_expired_then_fences() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = acquire_task_lease(&storage, acquire("node-a", 100, 500), KEY)
            .await
            .unwrap();
        assert!(
            acquire_task_lease(&storage, acquire("node-b", 200, 500), KEY)
                .await
                .is_err()
        );

        let second = acquire_task_lease(&storage, acquire("node-b", 700, 500), KEY)
            .await
            .unwrap();
        assert_eq!(second.fence_token, first.fence_token + 1);
        assert_eq!(second.lease_epoch, first.lease_epoch + 1);
        assert!(
            checkpoint_task_lease(
                &storage,
                "index-build-alpha",
                "node-a",
                first.fence_token,
                20,
                750,
                KEY,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn task_lease_rejects_tamper_and_invalid_inputs() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        acquire_task_lease(&storage, acquire("node-a", 100, 500), KEY)
            .await
            .unwrap();
        let path = storage.task_lease_path("index-build-alpha").unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["checkpoint_cursor"] = serde_json::json!(1234);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();
        assert!(
            read_task_lease(&storage, "index-build-alpha", KEY)
                .await
                .is_err()
        );
        assert!(storage.task_lease_path("../escape").is_err());
        let mut invalid = acquire("node-a", 100, 0);
        invalid.partition_id = "not-hex".to_string();
        assert!(acquire_task_lease(&storage, invalid, KEY).await.is_err());
    }

    fn acquire(owner_node_id: &str, now_nanos: i64, ttl_nanos: i64) -> TaskLeaseAcquire {
        TaskLeaseAcquire {
            task_id: "index-build-alpha".to_string(),
            task_kind: "full_text_index_build".to_string(),
            partition_family: "full_text_index".to_string(),
            partition_id: hex::encode([4; 32]),
            owner_node_id: owner_node_id.to_string(),
            source_cursor: 10,
            now_nanos,
            ttl_nanos,
        }
    }
}
