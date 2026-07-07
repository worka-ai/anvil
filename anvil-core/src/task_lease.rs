use crate::{
    core_store::{CompareAndSwapRef, CoreObjectRef, CoreRefValue, CoreStore, GetBlob, PutBlob},
    formats::hash32,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

pub const LEASE_HELD: &str = "LeaseHeld";
pub const LEASE_EXPIRED: &str = "LeaseExpired";
pub const STALE_FENCE: &str = "StaleFence";
pub const LEASE_OWNER_MISMATCH: &str = "LeaseOwnerMismatch";
pub const LEASE_CAS_CONFLICT: &str = "LeaseCasConflict";

const LOCK_RETRY_ATTEMPTS: usize = 200;
const TASK_LEASE_REF_PREFIX: &str = "task_lease:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let existing = read_task_lease_state(
            storage,
            request.owner.tenant_id,
            &request.task_id,
            signing_key,
        )
        .await?;
        let existing_lease = existing.as_ref().map(|(_, lease)| lease);
        let active_same_owner = existing_lease.is_some_and(|lease| {
            lease.expires_at_nanos > request.now_nanos
                && lease.owner.same_security_owner(&request.owner)
        });
        if let Some(existing) = existing_lease
            && existing.expires_at_nanos > request.now_nanos
            && !existing.owner.same_security_owner(&request.owner)
        {
            return Err(anyhow!(
                "{LEASE_HELD}: task lease is owned by another active principal"
            ));
        }

        let fence_token = if active_same_owner {
            existing_lease.map(|lease| lease.fence_token).unwrap_or(1)
        } else {
            existing_lease
                .map(|lease| lease.fence_token.saturating_add(1))
                .unwrap_or(1)
        };
        let lease_epoch = existing_lease
            .map(|lease| lease.lease_epoch.saturating_add(1))
            .unwrap_or(1);
        let checkpoint_cursor = existing_lease
            .map(|lease| lease.checkpoint_cursor)
            .unwrap_or(0)
            .max(request.source_cursor);
        let lease = TaskLease {
            format_version: 2,
            task_id: request.task_id.clone(),
            task_kind: request.task_kind.clone(),
            partition_family: request.partition_family.clone(),
            partition_id: request.partition_id.clone(),
            owner: request.owner.clone(),
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
        match write_task_lease_state(
            storage,
            &lease,
            existing.as_ref().map(|(ref_value, _)| ref_value),
        )
        .await
        {
            Ok(()) => return Ok(lease),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease CAS retries exhausted"
    ))
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
    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let Some((ref_value, mut lease)) =
            read_task_lease_state(storage, owner.tenant_id, task_id, signing_key).await?
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
        match write_task_lease_state(storage, &lease, Some(&ref_value)).await {
            Ok(()) => return Ok(lease),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease CAS retries exhausted"
    ))
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
    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let Some((ref_value, mut lease)) =
            read_task_lease_state(storage, owner.tenant_id, task_id, signing_key).await?
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
        let store = CoreStore::new(storage.clone()).await?;
        match store
            .delete_ref(
                &task_lease_ref_name(owner.tenant_id, task_id)?,
                Some(ref_value.generation),
                Some(&ref_value.target),
                true,
            )
            .await
        {
            Ok(_) => return Ok(committed),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease CAS retries exhausted"
    ))
}

pub async fn read_task_lease(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    Ok(
        read_task_lease_state(storage, tenant_id, task_id, signing_key)
            .await?
            .map(|(_, lease)| lease),
    )
}

pub async fn list_active_task_leases_for_node(
    storage: &Storage,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<Vec<TaskLease>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut out = Vec::new();
    for ref_name in store.list_ref_names(TASK_LEASE_REF_PREFIX).await? {
        let Some((tenant_id, task_id)) = parse_task_lease_ref_name(&ref_name)? else {
            continue;
        };
        let Some((_, lease)) =
            read_task_lease_state(storage, tenant_id, &task_id, signing_key).await?
        else {
            continue;
        };
        if lease.owner_node_id() == owner_node_id && lease.expires_at_nanos > now_nanos {
            out.push(lease);
        }
    }
    out.sort_by(|left, right| left.task_id.cmp(&right.task_id));
    Ok(out)
}

pub async fn force_release_task_lease(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<TaskLease>> {
    for _ in 0..LOCK_RETRY_ATTEMPTS {
        let Some((ref_value, lease)) =
            read_task_lease_state(storage, tenant_id, task_id, signing_key).await?
        else {
            return Ok(None);
        };
        let store = CoreStore::new(storage.clone()).await?;
        match store
            .delete_ref(
                &task_lease_ref_name(tenant_id, task_id)?,
                Some(ref_value.generation),
                Some(&ref_value.target),
                true,
            )
            .await
        {
            Ok(_) => return Ok(Some(lease)),
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{LEASE_CAS_CONFLICT}: task lease CAS retries exhausted"
    ))
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

async fn read_task_lease_state(
    storage: &Storage,
    tenant_id: i64,
    task_id: &str,
    signing_key: &[u8],
) -> Result<Option<(CoreRefValue, TaskLease)>> {
    let ref_name = task_lease_ref_name(tenant_id, task_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&ref_name).await? else {
        return Ok(None);
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    let lease: TaskLease = serde_json::from_slice(&bytes)?;
    lease.verify(signing_key)?;
    if lease.owner.tenant_id != tenant_id || lease.task_id != task_id {
        return Err(anyhow!("task lease ref scope mismatch"));
    }
    Ok(Some((ref_value, lease)))
}

async fn write_task_lease_state(
    storage: &Storage,
    lease: &TaskLease,
    expected_ref: Option<&CoreRefValue>,
) -> Result<()> {
    let ref_name = task_lease_ref_name(lease.owner.tenant_id, &lease.task_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.clone(),
            bytes: serde_json::to_vec_pretty(lease)?,
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: format!(
                "task-lease:{}:{}:{}",
                lease.owner.tenant_id, lease.task_id, lease.lease_epoch
            ),
        })
        .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name,
            expected_generation: expected_ref.map(|value| value.generation),
            expected_target: expected_ref.map(|value| value.target.clone()),
            require_absent: expected_ref.is_none(),
            require_present: expected_ref.is_some(),
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(())
}

fn task_lease_ref_name(tenant_id: i64, task_id: &str) -> Result<String> {
    if tenant_id < 0 {
        return Err(anyhow!("task lease tenant id must be nonnegative"));
    }
    require_nonempty(task_id, "task_id")?;
    if task_id.contains('\0') || task_id.contains("..") || task_id.chars().any(char::is_control) {
        return Err(anyhow!("task_id contains an invalid component"));
    }
    Ok(format!(
        "{TASK_LEASE_REF_PREFIX}tenant:{tenant_id}:task:{task_id}"
    ))
}

fn parse_task_lease_ref_name(ref_name: &str) -> Result<Option<(i64, String)>> {
    let Some(rest) = ref_name.strip_prefix(TASK_LEASE_REF_PREFIX) else {
        return Ok(None);
    };
    let Some(rest) = rest.strip_prefix("tenant:") else {
        return Ok(None);
    };
    let Some((tenant, task)) = rest.split_once(":task:") else {
        return Ok(None);
    };
    let tenant_id = tenant.parse::<i64>()?;
    Ok(Some((tenant_id, task.to_string())))
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

fn is_core_ref_cas_conflict(err: &anyhow::Error) -> bool {
    let message = err.to_string();
    message.contains("generation mismatch")
        || message.contains("target mismatch")
        || message.contains("must be absent")
        || message.contains("must be present")
        || message.contains("CAS lock was not acquired")
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
        let store = CoreStore::new(storage.clone()).await.unwrap();
        assert!(
            store
                .read_ref(&task_lease_ref_name(0, "index-build-alpha").unwrap())
                .await
                .unwrap()
                .is_some()
        );

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
        let (ref_value, _) = read_task_lease_state(&storage, 0, "index-build-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let object_ref = decode_core_object_ref_target(&ref_value.target).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&store.get_blob(GetBlob { object_ref }).await.unwrap()).unwrap();
        value["checkpoint_cursor"] = serde_json::json!(1234);
        let tampered = store
            .put_blob(PutBlob {
                logical_name: "task-lease-tamper".to_string(),
                bytes: serde_json::to_vec_pretty(&value).unwrap(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "task-lease-tamper".to_string(),
            })
            .await
            .unwrap();
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: task_lease_ref_name(0, "index-build-alpha").unwrap(),
                expected_generation: Some(ref_value.generation),
                expected_target: Some(ref_value.target),
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&tampered).unwrap(),
                transaction_id: None,
            })
            .await
            .unwrap();
        assert!(
            read_task_lease(&storage, 0, "index-build-alpha", KEY)
                .await
                .is_err()
        );
        assert!(task_lease_ref_name(0, "../escape").is_err());
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
