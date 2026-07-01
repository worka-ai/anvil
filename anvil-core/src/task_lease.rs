use crate::{formats::hash32, storage::Storage};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::fs::OpenOptions;

pub const LEASE_HELD: &str = "LeaseHeld";
pub const LEASE_EXPIRED: &str = "LeaseExpired";
pub const STALE_FENCE: &str = "StaleFence";
pub const LEASE_OWNER_MISMATCH: &str = "LeaseOwnerMismatch";
pub const LEASE_CAS_CONFLICT: &str = "LeaseCasConflict";

const LOCK_RETRY_ATTEMPTS: usize = 200;
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskLeaseOwner {
    pub tenant_id: i64,
    pub principal_kind: String,
    pub principal_id: String,
    pub actor_instance_id: String,
    pub display_name: String,
}

impl TaskLeaseOwner {
    pub fn node(owner_node_id: impl Into<String>) -> Self {
        let owner_node_id = owner_node_id.into();
        Self {
            tenant_id: 0,
            principal_kind: "node".to_string(),
            principal_id: owner_node_id.clone(),
            actor_instance_id: owner_node_id.clone(),
            display_name: owner_node_id,
        }
    }

    pub fn same_security_owner(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
            && self.principal_kind == other.principal_kind
            && self.principal_id == other.principal_id
            && self.actor_instance_id == other.actor_instance_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskLease {
    pub format_version: u16,
    pub task_id: String,
    pub task_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub owner: TaskLeaseOwner,
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

impl TaskLease {
    pub fn owner_node_id(&self) -> &str {
        &self.owner.display_name
    }

    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_lease(&self)?;
        let hash = hash_task_lease(&self)?;
        let signature = sign_lease_hash(
            signing_key,
            &hash,
            &[
                &self.task_id,
                &self.owner.tenant_id.to_string(),
                &self.owner.principal_kind,
                &self.owner.principal_id,
                &self.owner.actor_instance_id,
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
                &self.owner.tenant_id.to_string(),
                &self.owner.principal_kind,
                &self.owner.principal_id,
                &self.owner.actor_instance_id,
                &self.fence_token.to_string(),
            ],
        )?;
        if self.lease_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("task lease signature mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLeaseAcquire {
    pub task_id: String,
    pub task_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub owner: TaskLeaseOwner,
    pub source_cursor: u128,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
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
    let _guard =
        TaskLeaseWriteGuard::acquire(storage, request.owner.tenant_id, &request.task_id).await?;
    let existing = read_task_lease_unlocked(
        storage,
        request.owner.tenant_id,
        &request.task_id,
        signing_key,
    )
    .await?;

    let active_same_owner = existing.as_ref().is_some_and(|lease| {
        lease.expires_at_nanos > request.now_nanos
            && lease.owner.same_security_owner(&request.owner)
    });
    if let Some(existing) = existing.as_ref()
        && existing.expires_at_nanos > request.now_nanos
        && !existing.owner.same_security_owner(&request.owner)
    {
        return Err(anyhow!(
            "{LEASE_HELD}: task lease is owned by another active principal"
        ));
    }

    let fence_token = if active_same_owner {
        existing
            .as_ref()
            .map(|lease| lease.fence_token)
            .unwrap_or(1)
    } else {
        existing
            .as_ref()
            .map(|lease| lease.fence_token.saturating_add(1))
            .unwrap_or(1)
    };
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
        format_version: 2,
        task_id: request.task_id,
        task_kind: request.task_kind,
        partition_family: request.partition_family,
        partition_id: request.partition_id,
        owner: request.owner,
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
    write_task_lease_unlocked(storage, &lease).await?;
    Ok(lease)
}

pub async fn checkpoint_task_lease(
    storage: &Storage,
    task_id: &str,
    owner: &TaskLeaseOwner,
    fence_token: u64,
    checkpoint_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let _guard = TaskLeaseWriteGuard::acquire(storage, owner.tenant_id, task_id).await?;
    let Some(mut lease) =
        read_task_lease_unlocked(storage, owner.tenant_id, task_id, signing_key).await?
    else {
        return Err(anyhow!("task lease does not exist"));
    };
    lease.verify(signing_key)?;
    if !lease.owner.same_security_owner(owner) {
        return Err(anyhow!("{LEASE_OWNER_MISMATCH}: task lease owner mismatch"));
    }
    if lease.fence_token != fence_token {
        return Err(anyhow!("{STALE_FENCE}: task lease fence token mismatch"));
    }
    if lease.expires_at_nanos <= now_nanos {
        return Err(anyhow!("{LEASE_EXPIRED}: task lease expired"));
    }
    if checkpoint_cursor < lease.checkpoint_cursor {
        return Err(anyhow!(
            "{STALE_FENCE}: task lease checkpoint cannot move backwards"
        ));
    }
    lease.checkpoint_cursor = checkpoint_cursor;
    lease.updated_at_nanos = now_nanos;
    lease = lease.seal(signing_key)?;
    write_task_lease_unlocked(storage, &lease).await?;
    Ok(lease)
}

pub async fn commit_task_lease(
    storage: &Storage,
    task_id: &str,
    owner: &TaskLeaseOwner,
    fence_token: u64,
    committed_cursor: u128,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<TaskLease> {
    let _guard = TaskLeaseWriteGuard::acquire(storage, owner.tenant_id, task_id).await?;
    let Some(mut lease) =
        read_task_lease_unlocked(storage, owner.tenant_id, task_id, signing_key).await?
    else {
        return Err(anyhow!("task lease does not exist"));
    };
    lease.verify(signing_key)?;
    if !lease.owner.same_security_owner(owner) {
        return Err(anyhow!("{LEASE_OWNER_MISMATCH}: task lease owner mismatch"));
    }
    if lease.fence_token != fence_token {
        return Err(anyhow!("{STALE_FENCE}: task lease fence token mismatch"));
    }
    if lease.expires_at_nanos <= now_nanos {
        return Err(anyhow!("{LEASE_EXPIRED}: task lease expired"));
    }
    if committed_cursor < lease.checkpoint_cursor {
        return Err(anyhow!(
            "{STALE_FENCE}: task lease commit cannot move backwards"
        ));
    }
    lease.checkpoint_cursor = committed_cursor;
    lease.updated_at_nanos = now_nanos;
    let committed = lease.seal(signing_key)?;
    let path = storage.task_lease_path(owner.tenant_id, task_id)?;
    match tokio::fs::remove_file(&path).await {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("remove {}", path.display())),
    }
    Ok(committed)
}

pub async fn read_task_lease(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    read_task_lease_unlocked(storage, tenant_id, task_id, signing_key).await
}

pub async fn force_release_task_lease(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    let _guard = TaskLeaseWriteGuard::acquire(storage, tenant_id, task_id).await?;
    let lease = read_task_lease_unlocked(storage, tenant_id, task_id, signing_key).await?;
    let path = storage.task_lease_path(tenant_id, task_id)?;
    match tokio::fs::remove_file(&path).await {
        Ok(_) => {}
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => return Err(err).with_context(|| format!("remove {}", path.display())),
    }
    Ok(lease)
}

async fn read_task_lease_unlocked(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    let path = storage.task_lease_path(tenant_id, task_id)?;
    let Some(lease) = read_json_optional::<TaskLease>(&path).await? else {
        return Ok(None);
    };
    lease.verify(signing_key)?;
    if lease.task_id != task_id {
        return Err(anyhow!("task lease path scope mismatch"));
    }
    Ok(Some(lease))
}

async fn write_task_lease_unlocked(storage: &Storage, lease: &TaskLease) -> Result<()> {
    let path = storage.task_lease_path(lease.owner.tenant_id, &lease.task_id)?;
    write_json_atomically(&path, lease).await
}

struct TaskLeaseWriteGuard {
    path: PathBuf,
}

impl TaskLeaseWriteGuard {
    async fn acquire(storage: &Storage, tenant_id: i64, task_id: &str) -> Result<Self> {
        let lease_path = storage.task_lease_path(tenant_id, task_id)?;
        let lock_path = lease_path.with_extension("json.lock");
        if let Some(parent) = lock_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        for _ in 0..LOCK_RETRY_ATTEMPTS {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .await
            {
                Ok(_) => return Ok(Self { path: lock_path }),
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                    tokio::time::sleep(LOCK_RETRY_DELAY).await;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("create task lease CAS lock {}", lock_path.display())
                    });
                }
            }
        }
        Err(anyhow!(
            "{LEASE_CAS_CONFLICT}: task lease CAS lock was not acquired"
        ))
    }
}

impl Drop for TaskLeaseWriteGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn validate_acquire_request(request: &TaskLeaseAcquire) -> Result<()> {
    require_nonempty(&request.task_id, "task_id")?;
    require_nonempty(&request.task_kind, "task_kind")?;
    require_nonempty(&request.partition_family, "partition_family")?;
    validate_hex32(&request.partition_id, "partition_id")?;
    validate_owner(&request.owner)?;
    if request.ttl_nanos <= 0 {
        return Err(anyhow!("task lease ttl must be positive"));
    }
    if request.now_nanos < 0 {
        return Err(anyhow!("task lease timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_unsigned_lease(lease: &TaskLease) -> Result<()> {
    if lease.format_version != 2 {
        return Err(anyhow!("unsupported task lease version"));
    }
    require_nonempty(&lease.task_id, "task_id")?;
    require_nonempty(&lease.task_kind, "task_kind")?;
    require_nonempty(&lease.partition_family, "partition_family")?;
    validate_hex32(&lease.partition_id, "partition_id")?;
    validate_owner(&lease.owner)?;
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

fn validate_owner(owner: &TaskLeaseOwner) -> Result<()> {
    if owner.tenant_id < 0 {
        return Err(anyhow!("task lease owner tenant_id must be nonnegative"));
    }
    require_nonempty(&owner.principal_kind, "owner.principal_kind")?;
    require_nonempty(&owner.principal_id, "owner.principal_id")?;
    require_nonempty(&owner.actor_instance_id, "owner.actor_instance_id")?;
    require_nonempty(&owner.display_name, "owner.display_name")?;
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
        let owner = TaskLeaseOwner::node("node-a");
        let lease = acquire_task_lease(&storage, acquire(owner.clone(), 100, 500), KEY)
            .await
            .unwrap();
        assert_eq!(lease.fence_token, 1);
        assert_eq!(lease.lease_epoch, 1);
        assert_eq!(lease.checkpoint_cursor, 10);
        assert_eq!(lease.owner_node_id(), "node-a");
        assert!(lease.lease_hash.as_deref().unwrap().len() == 64);
        let path = storage.task_lease_path(0, "index-build-alpha").unwrap();
        assert!(path.ends_with("_anvil/tasks/leases/tenant-0/index-build-alpha.json"));

        let checkpointed = checkpoint_task_lease(
            &storage,
            "index-build-alpha",
            &owner,
            lease.fence_token,
            99,
            200,
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(checkpointed.checkpoint_cursor, 99);
        assert_eq!(
            read_task_lease(&storage, 0, "index-build-alpha", KEY)
                .await
                .unwrap()
                .unwrap(),
            checkpointed
        );
    }

    #[tokio::test]
    async fn task_lease_commit_requires_owner_and_fence_then_removes_lease() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = TaskLeaseOwner::node("node-a");
        let other = TaskLeaseOwner::node("node-b");
        let lease = acquire_task_lease(&storage, acquire(owner.clone(), 100, 500), KEY)
            .await
            .unwrap();

        let wrong_owner = commit_task_lease(
            &storage,
            "index-build-alpha",
            &other,
            lease.fence_token,
            10,
            200,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(wrong_owner.to_string().contains(LEASE_OWNER_MISMATCH));

        let stale = commit_task_lease(
            &storage,
            "index-build-alpha",
            &owner,
            lease.fence_token + 1,
            10,
            200,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(stale.to_string().contains(STALE_FENCE));

        let committed = commit_task_lease(
            &storage,
            "index-build-alpha",
            &owner,
            lease.fence_token,
            12,
            200,
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(committed.checkpoint_cursor, 12);
        assert!(
            read_task_lease(&storage, 0, "index-build-alpha", KEY)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn active_lease_blocks_other_owner_until_expired_then_fences() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner_a = TaskLeaseOwner::node("node-a");
        let owner_b = TaskLeaseOwner::node("node-b");
        let first = acquire_task_lease(&storage, acquire(owner_a.clone(), 100, 500), KEY)
            .await
            .unwrap();
        assert!(
            acquire_task_lease(&storage, acquire(owner_b.clone(), 200, 500), KEY)
                .await
                .unwrap_err()
                .to_string()
                .contains(LEASE_HELD)
        );

        let second = acquire_task_lease(&storage, acquire(owner_b, 700, 500), KEY)
            .await
            .unwrap();
        assert_eq!(second.fence_token, first.fence_token + 1);
        assert_eq!(second.lease_epoch, first.lease_epoch + 1);
        assert!(
            checkpoint_task_lease(
                &storage,
                "index-build-alpha",
                &owner_a,
                first.fence_token,
                20,
                750,
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(LEASE_OWNER_MISMATCH)
        );
    }

    #[tokio::test]
    async fn same_owner_renews_without_changing_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = TaskLeaseOwner::node("node-a");
        let first = acquire_task_lease(&storage, acquire(owner.clone(), 100, 500), KEY)
            .await
            .unwrap();
        let renewed = acquire_task_lease(&storage, acquire(owner, 200, 500), KEY)
            .await
            .unwrap();
        assert_eq!(renewed.fence_token, first.fence_token);
        assert_eq!(renewed.lease_epoch, first.lease_epoch + 1);
        assert_eq!(renewed.expires_at_nanos, 700);
    }

    #[tokio::test]
    async fn concurrent_acquire_has_exactly_one_active_owner() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut tasks = Vec::new();
        for idx in 0..16 {
            let storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                acquire_task_lease(
                    &storage,
                    acquire(TaskLeaseOwner::node(format!("node-{idx}")), 100, 500),
                    KEY,
                )
                .await
            }));
        }
        let mut successes = 0;
        let mut held = 0;
        for task in tasks {
            match task.await.unwrap() {
                Ok(_) => successes += 1,
                Err(err) if err.to_string().contains(LEASE_HELD) => held += 1,
                Err(err) => panic!("unexpected lease error: {err}"),
            }
        }
        assert_eq!(successes, 1);
        assert_eq!(held, 15);
    }

    #[tokio::test]
    async fn task_lease_rejects_tamper_and_invalid_inputs() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        acquire_task_lease(
            &storage,
            acquire(TaskLeaseOwner::node("node-a"), 100, 500),
            KEY,
        )
        .await
        .unwrap();
        let path = storage.task_lease_path(0, "index-build-alpha").unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["checkpoint_cursor"] = serde_json::json!(1234);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();
        assert!(
            read_task_lease(&storage, 0, "index-build-alpha", KEY)
                .await
                .is_err()
        );
        assert!(storage.task_lease_path(0, "../escape").is_err());
        let mut invalid = acquire(TaskLeaseOwner::node("node-a"), 100, 0);
        invalid.partition_id = "not-hex".to_string();
        assert!(acquire_task_lease(&storage, invalid, KEY).await.is_err());
    }

    fn acquire(owner: TaskLeaseOwner, now_nanos: i64, ttl_nanos: i64) -> TaskLeaseAcquire {
        TaskLeaseAcquire {
            task_id: "index-build-alpha".to_string(),
            task_kind: "full_text_index_build".to_string(),
            partition_family: "full_text_index".to_string(),
            partition_id: hex::encode([4; 32]),
            owner,
            source_cursor: 10,
            now_nanos,
            ttl_nanos,
        }
    }
}
