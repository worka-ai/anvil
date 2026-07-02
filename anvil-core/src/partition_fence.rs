use crate::{
    error_codes::AnvilErrorCode,
    formats::{JournalFrame, hash32},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::{
    fmt,
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::fs::OpenOptions;

type HmacSha256 = Hmac<Sha256>;

pub const OWNERSHIP_HELD: &str = "OwnershipHeld";
pub const OWNERSHIP_EXPIRED: &str = "OwnershipExpired";
pub const OWNERSHIP_NOT_FOUND: &str = "OwnershipNotFound";
pub const OWNERSHIP_OWNER_MISMATCH: &str = "OwnershipOwnerMismatch";
pub const OWNERSHIP_STALE_FENCE: &str = "StaleFence";
pub const OWNERSHIP_CAS_CONFLICT: &str = "OwnershipCasConflict";
pub const MAX_OWNERSHIP_LEASE_MS: u64 = 120_000;

const OWNERSHIP_LOCK_RETRY_ATTEMPTS: usize = 200;
const OWNERSHIP_LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionOwnerStatus {
    Recovering,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionOwnerState {
    pub format_version: u16,
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence_token: u64,
    pub recovery_epoch: u64,
    pub status: PartitionOwnerStatus,
    pub recovered_through_sequence: u64,
    pub recovered_manifest_hash: String,
    pub updated_at_nanos: i64,
    pub owner_hash: Option<String>,
    pub owner_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRecoveryAcquire {
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub recovered_through_sequence: u64,
    pub recovered_manifest_hash: String,
    pub now_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWritePermit {
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence_token: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceRejection {
    pub code: AnvilErrorCode,
    pub reason: &'static str,
}

impl fmt::Display for FenceRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.reason)
    }
}

impl std::error::Error for FenceRejection {}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipResourceKind {
    ControlPartition,
    BucketPrimary,
    ObjectPartition,
    IndexPartition,
    PersonalDbGroup,
    TaskQueue,
    WatchPartition,
}

impl OwnershipResourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ControlPartition => "control_partition",
            Self::BucketPrimary => "bucket_primary",
            Self::ObjectPartition => "object_partition",
            Self::IndexPartition => "index_partition",
            Self::PersonalDbGroup => "personaldb_group",
            Self::TaskQueue => "task_queue",
            Self::WatchPartition => "watch_partition",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipResource {
    pub resource_kind: OwnershipResourceKind,
    pub resource_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipPrincipal {
    pub tenant_id: i64,
    pub principal_kind: String,
    pub principal_id: String,
    pub actor_instance_id: String,
    pub display_name: String,
    pub region: String,
    pub cell: String,
}

impl OwnershipPrincipal {
    pub fn node(owner_node_id: impl Into<String>) -> Self {
        let owner_node_id = owner_node_id.into();
        Self {
            tenant_id: 0,
            principal_kind: "node".to_string(),
            principal_id: owner_node_id.clone(),
            actor_instance_id: owner_node_id.clone(),
            display_name: owner_node_id,
            region: "default".to_string(),
            cell: "default".to_string(),
        }
    }

    pub fn same_security_owner(&self, other: &Self) -> bool {
        self.tenant_id == other.tenant_id
            && self.principal_kind == other.principal_kind
            && self.principal_id == other.principal_id
            && self.actor_instance_id == other.actor_instance_id
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OwnershipFenceState {
    Active,
    Transferring,
    Draining,
    Expired,
    Released,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipFenceRecord {
    pub format_version: u16,
    pub resource: OwnershipResource,
    pub owner: OwnershipPrincipal,
    pub fence: u64,
    pub state: OwnershipFenceState,
    pub lease_expires_at_nanos: i64,
    pub last_heartbeat_at_nanos: i64,
    pub generation: u64,
    pub last_operation: Option<String>,
    pub last_idempotency_key: Option<String>,
    #[serde(default)]
    pub last_actor: Option<OwnershipPrincipal>,
    pub ownership_hash: Option<String>,
    pub ownership_signature: Option<String>,
}

impl OwnershipFenceRecord {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_ownership_fence(&self)?;
        let hash = hash_ownership_fence(&self)?;
        let signature = sign_ownership_hash(
            signing_key,
            &hash,
            &[
                &self.owner.tenant_id.to_string(),
                self.resource.resource_kind.as_str(),
                &self.resource.resource_id,
                &self.owner.principal_kind,
                &self.owner.principal_id,
                &self.owner.actor_instance_id,
                &self.fence.to_string(),
            ],
        )?;
        self.ownership_hash = Some(hash);
        self.ownership_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_ownership_fence(self)?;
        let expected_hash = hash_ownership_fence(self)?;
        if self.ownership_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("ownership fence hash mismatch"));
        }
        let expected_signature = sign_ownership_hash(
            signing_key,
            &expected_hash,
            &[
                &self.owner.tenant_id.to_string(),
                self.resource.resource_kind.as_str(),
                &self.resource.resource_id,
                &self.owner.principal_kind,
                &self.owner.principal_id,
                &self.owner.actor_instance_id,
                &self.fence.to_string(),
            ],
        )?;
        if self.ownership_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("ownership fence signature mismatch"));
        }
        Ok(())
    }

    pub fn is_active_unexpired(&self, now_nanos: i64) -> bool {
        matches!(
            self.state,
            OwnershipFenceState::Active
                | OwnershipFenceState::Transferring
                | OwnershipFenceState::Draining
        ) && self.lease_expires_at_nanos > now_nanos
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipFenceOutcome {
    pub record: OwnershipFenceRecord,
    pub idempotent_replay: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquireOwnership {
    pub request_id: String,
    pub idempotency_key: String,
    pub resource: OwnershipResource,
    pub owner: OwnershipPrincipal,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenewOwnership {
    pub request_id: String,
    pub resource: OwnershipResource,
    pub owner: OwnershipPrincipal,
    pub current_fence: u64,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferOwnership {
    pub request_id: String,
    pub idempotency_key: String,
    pub resource: OwnershipResource,
    pub current_owner: OwnershipPrincipal,
    pub new_owner: OwnershipPrincipal,
    pub current_fence: u64,
    pub now_nanos: i64,
    pub ttl_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseOwnership {
    pub request_id: String,
    pub idempotency_key: String,
    pub resource: OwnershipResource,
    pub owner: OwnershipPrincipal,
    pub current_fence: u64,
    pub administrative_force: bool,
    pub now_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceExpireOwnership {
    pub request_id: String,
    pub idempotency_key: String,
    pub resource: OwnershipResource,
    pub admin: OwnershipPrincipal,
    pub reason: String,
    pub now_nanos: i64,
}

impl PartitionOwnerState {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_owner(&self)?;
        let hash = hash_partition_owner(&self)?;
        let signature = sign_owner_hash(
            signing_key,
            &hash,
            &[
                &self.partition_family,
                &self.partition_id,
                &self.owner_node_id,
                &self.fence_token.to_string(),
            ],
        )?;
        self.owner_hash = Some(hash);
        self.owner_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_owner(self)?;
        let expected_hash = hash_partition_owner(self)?;
        if self.owner_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("partition owner hash mismatch"));
        }
        let expected_signature = sign_owner_hash(
            signing_key,
            &expected_hash,
            &[
                &self.partition_family,
                &self.partition_id,
                &self.owner_node_id,
                &self.fence_token.to_string(),
            ],
        )?;
        if self.owner_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("partition owner signature mismatch"));
        }
        Ok(())
    }

    pub fn write_permit(&self) -> Result<PartitionWritePermit, FenceRejection> {
        if self.status != PartitionOwnerStatus::Ready {
            return Err(FenceRejection {
                code: AnvilErrorCode::PartitionNotOwned,
                reason: "partition owner has not completed recovery",
            });
        }
        Ok(PartitionWritePermit {
            partition_family: self.partition_family.clone(),
            partition_id: self.partition_id.clone(),
            owner_node_id: self.owner_node_id.clone(),
            fence_token: self.fence_token,
        })
    }
}

pub fn hash_partition_owner(owner: &PartitionOwnerState) -> Result<String> {
    let mut unsigned = owner.clone();
    unsigned.owner_hash = None;
    unsigned.owner_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub fn hash_ownership_fence(record: &OwnershipFenceRecord) -> Result<String> {
    let mut unsigned = record.clone();
    unsigned.ownership_hash = None;
    unsigned.ownership_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub async fn acquire_ownership(
    storage: &Storage,
    request: AcquireOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_acquire_ownership(&request)?;
    let _guard =
        OwnershipFenceWriteGuard::acquire(storage, request.owner.tenant_id, &request.resource)
            .await?;
    let existing = read_ownership_fence_unlocked(
        storage,
        request.owner.tenant_id,
        &request.resource,
        signing_key,
    )
    .await?;

    if let Some(existing) = existing.as_ref() {
        if ownership_idempotency_matches(
            existing,
            "acquire",
            &request.idempotency_key,
            &request.owner,
        ) && existing.is_active_unexpired(request.now_nanos)
        {
            return Ok(OwnershipFenceOutcome {
                record: existing.clone(),
                idempotent_replay: true,
            });
        }
        if existing.is_active_unexpired(request.now_nanos) {
            return Err(anyhow!(
                "{OWNERSHIP_HELD}: ownership fence is held by an active principal"
            ));
        }
    }

    let fence = existing
        .as_ref()
        .map(|record| record.fence.saturating_add(1))
        .unwrap_or(1);
    let record = OwnershipFenceRecord {
        format_version: 1,
        resource: request.resource,
        owner: request.owner.clone(),
        fence,
        state: OwnershipFenceState::Active,
        lease_expires_at_nanos: request.now_nanos.saturating_add(request.ttl_nanos),
        last_heartbeat_at_nanos: request.now_nanos,
        generation: fence,
        last_operation: Some("acquire".to_string()),
        last_idempotency_key: nonempty_idempotency_key(request.idempotency_key),
        last_actor: Some(request.owner),
        ownership_hash: None,
        ownership_signature: None,
    }
    .seal(signing_key)?;
    write_ownership_fence_unlocked(storage, &record).await?;
    Ok(OwnershipFenceOutcome {
        record,
        idempotent_replay: false,
    })
}

pub async fn renew_ownership(
    storage: &Storage,
    request: RenewOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_renew_ownership(&request)?;
    let _guard =
        OwnershipFenceWriteGuard::acquire(storage, request.owner.tenant_id, &request.resource)
            .await?;
    let Some(mut record) = read_ownership_fence_unlocked(
        storage,
        request.owner.tenant_id,
        &request.resource,
        signing_key,
    )
    .await?
    else {
        return Err(anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"));
    };
    require_current_owner_and_fence(&record, &request.owner, request.current_fence)?;
    if !record.is_active_unexpired(request.now_nanos) {
        return Err(anyhow!(
            "{OWNERSHIP_EXPIRED}: ownership fence is not active"
        ));
    }
    record.lease_expires_at_nanos = request.now_nanos.saturating_add(request.ttl_nanos);
    record.last_heartbeat_at_nanos = request.now_nanos;
    record.last_operation = Some("renew".to_string());
    record.last_idempotency_key = None;
    record.last_actor = Some(request.owner);
    record = record.seal(signing_key)?;
    write_ownership_fence_unlocked(storage, &record).await?;
    Ok(OwnershipFenceOutcome {
        record,
        idempotent_replay: false,
    })
}

pub async fn transfer_ownership(
    storage: &Storage,
    request: TransferOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_transfer_ownership(&request)?;
    if request.new_owner.tenant_id != request.current_owner.tenant_id {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: transfer target is outside the owner tenant"
        ));
    }
    let _guard = OwnershipFenceWriteGuard::acquire(
        storage,
        request.current_owner.tenant_id,
        &request.resource,
    )
    .await?;
    let Some(mut record) = read_ownership_fence_unlocked(
        storage,
        request.current_owner.tenant_id,
        &request.resource,
        signing_key,
    )
    .await?
    else {
        return Err(anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"));
    };
    if ownership_idempotency_matches(
        &record,
        "transfer",
        &request.idempotency_key,
        &request.current_owner,
    ) {
        return Ok(OwnershipFenceOutcome {
            record,
            idempotent_replay: true,
        });
    }
    require_current_owner_and_fence(&record, &request.current_owner, request.current_fence)?;
    if !record.is_active_unexpired(request.now_nanos) {
        return Err(anyhow!(
            "{OWNERSHIP_EXPIRED}: ownership fence is not active"
        ));
    }

    record.state = OwnershipFenceState::Transferring;
    record.fence = record.fence.saturating_add(1);
    record.generation = record.fence;
    record.owner = request.new_owner;
    record.state = OwnershipFenceState::Active;
    record.lease_expires_at_nanos = request.now_nanos.saturating_add(request.ttl_nanos);
    record.last_heartbeat_at_nanos = request.now_nanos;
    record.last_operation = Some("transfer".to_string());
    record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key);
    record.last_actor = Some(request.current_owner);
    record = record.seal(signing_key)?;
    write_ownership_fence_unlocked(storage, &record).await?;
    Ok(OwnershipFenceOutcome {
        record,
        idempotent_replay: false,
    })
}

pub async fn release_ownership(
    storage: &Storage,
    request: ReleaseOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_release_ownership(&request)?;
    let _guard =
        OwnershipFenceWriteGuard::acquire(storage, request.owner.tenant_id, &request.resource)
            .await?;
    let Some(mut record) = read_ownership_fence_unlocked(
        storage,
        request.owner.tenant_id,
        &request.resource,
        signing_key,
    )
    .await?
    else {
        return Err(anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"));
    };
    if ownership_idempotency_matches(&record, "release", &request.idempotency_key, &request.owner) {
        return Ok(OwnershipFenceOutcome {
            record,
            idempotent_replay: true,
        });
    }
    if !request.administrative_force {
        require_current_owner_and_fence(&record, &request.owner, request.current_fence)?;
    }
    record.fence = record.fence.saturating_add(1);
    record.generation = record.fence;
    record.state = OwnershipFenceState::Released;
    record.lease_expires_at_nanos = request.now_nanos;
    record.last_heartbeat_at_nanos = request.now_nanos;
    record.last_operation = Some("release".to_string());
    record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key);
    record.last_actor = Some(request.owner);
    record = record.seal(signing_key)?;
    write_ownership_fence_unlocked(storage, &record).await?;
    Ok(OwnershipFenceOutcome {
        record,
        idempotent_replay: false,
    })
}

pub async fn force_expire_ownership(
    storage: &Storage,
    request: ForceExpireOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_force_expire_ownership(&request)?;
    let _guard =
        OwnershipFenceWriteGuard::acquire(storage, request.admin.tenant_id, &request.resource)
            .await?;
    let Some(mut record) = read_ownership_fence_unlocked(
        storage,
        request.admin.tenant_id,
        &request.resource,
        signing_key,
    )
    .await?
    else {
        return Err(anyhow!("{OWNERSHIP_NOT_FOUND}: ownership fence is absent"));
    };
    if ownership_idempotency_matches(
        &record,
        "force_expire",
        &request.idempotency_key,
        &request.admin,
    ) {
        return Ok(OwnershipFenceOutcome {
            record,
            idempotent_replay: true,
        });
    }
    record.fence = record.fence.saturating_add(1);
    record.generation = record.fence;
    record.state = OwnershipFenceState::Expired;
    record.lease_expires_at_nanos = request.now_nanos;
    record.last_heartbeat_at_nanos = request.now_nanos;
    record.last_operation = Some("force_expire".to_string());
    record.last_idempotency_key = nonempty_idempotency_key(request.idempotency_key);
    record.last_actor = Some(request.admin);
    record = record.seal(signing_key)?;
    write_ownership_fence_unlocked(storage, &record).await?;
    Ok(OwnershipFenceOutcome {
        record,
        idempotent_replay: false,
    })
}

pub async fn read_ownership_fence(
    storage: &Storage,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<Option<OwnershipFenceRecord>> {
    read_ownership_fence_unlocked(storage, tenant_id, resource, signing_key).await
}

pub async fn acquire_partition_recovery(
    storage: &Storage,
    request: PartitionRecoveryAcquire,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    validate_recovery_acquire(&request)?;
    let existing = read_partition_owner(
        storage,
        &request.partition_family,
        &request.partition_id,
        signing_key,
    )
    .await?;
    let fence_token = existing
        .as_ref()
        .map(|state| state.fence_token.saturating_add(1))
        .unwrap_or(1);
    let recovery_epoch = existing
        .as_ref()
        .map(|state| state.recovery_epoch.saturating_add(1))
        .unwrap_or(1);
    let state = PartitionOwnerState {
        format_version: 1,
        partition_family: request.partition_family,
        partition_id: request.partition_id,
        owner_node_id: request.owner_node_id,
        fence_token,
        recovery_epoch,
        status: PartitionOwnerStatus::Recovering,
        recovered_through_sequence: request.recovered_through_sequence,
        recovered_manifest_hash: request.recovered_manifest_hash,
        updated_at_nanos: request.now_nanos,
        owner_hash: None,
        owner_signature: None,
    }
    .seal(signing_key)?;
    write_partition_owner(storage, &state).await?;
    Ok(state)
}

pub async fn publish_partition_ready(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    owner_node_id: &str,
    fence_token: u64,
    recovered_through_sequence: u64,
    recovered_manifest_hash: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    let Some(mut state) =
        read_partition_owner(storage, partition_family, partition_id, signing_key).await?
    else {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner state is absent",
        }
        .into());
    };
    validate_write_permit_for_state(
        &state,
        &PartitionWritePermit {
            partition_family: partition_family.to_string(),
            partition_id: partition_id.to_string(),
            owner_node_id: owner_node_id.to_string(),
            fence_token,
        },
        false,
    )?;
    validate_hex32(recovered_manifest_hash, "recovered manifest hash")?;
    if now_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    state.status = PartitionOwnerStatus::Ready;
    state.recovered_through_sequence = recovered_through_sequence;
    state.recovered_manifest_hash = recovered_manifest_hash.to_string();
    state.updated_at_nanos = now_nanos;
    state = state.seal(signing_key)?;
    write_partition_owner(storage, &state).await?;
    Ok(state)
}

pub async fn validate_partition_write(
    storage: &Storage,
    permit: &PartitionWritePermit,
    signing_key: &[u8],
) -> Result<(), FenceRejection> {
    let owner = read_partition_owner(
        storage,
        &permit.partition_family,
        &permit.partition_id,
        signing_key,
    )
    .await
    .map_err(|_| FenceRejection {
        code: AnvilErrorCode::PartitionNotOwned,
        reason: "partition owner state cannot be read",
    })?;
    let Some(owner) = owner else {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner state is absent",
        });
    };
    validate_write_permit_for_state(&owner, permit, true)
}

pub fn validate_write_permit_for_state(
    owner: &PartitionOwnerState,
    permit: &PartitionWritePermit,
    require_ready: bool,
) -> Result<(), FenceRejection> {
    if owner.partition_family != permit.partition_family
        || owner.partition_id != permit.partition_id
    {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "write permit targets a different partition",
        });
    }
    if require_ready && owner.status != PartitionOwnerStatus::Ready {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner has not completed recovery",
        });
    }
    if owner.owner_node_id != permit.owner_node_id {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "write permit owner is not current",
        });
    }
    if owner.fence_token != permit.fence_token {
        return Err(FenceRejection {
            code: AnvilErrorCode::StaleFenceToken,
            reason: "write permit fence token is stale",
        });
    }
    Ok(())
}

pub async fn read_partition_owner(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    signing_key: &[u8],
) -> Result<Option<PartitionOwnerState>> {
    let path = storage.partition_owner_path(partition_family, partition_id)?;
    let Some(owner) = read_json_optional::<PartitionOwnerState>(&path).await? else {
        return Ok(None);
    };
    owner.verify(signing_key)?;
    if owner.partition_family != partition_family || owner.partition_id != partition_id {
        return Err(anyhow!("partition owner path scope mismatch"));
    }
    Ok(Some(owner))
}

pub fn frames_for_recovered_fence(
    frames: &[JournalFrame],
    manifest_checkpoint_sequence: u64,
    current_fence_token: u64,
) -> Vec<JournalFrame> {
    frames
        .iter()
        .filter(|frame| {
            frame.partition_sequence > manifest_checkpoint_sequence
                && frame.fence_token == current_fence_token
        })
        .cloned()
        .collect()
}

pub fn reject_stale_frames_after_checkpoint(
    frames: &[JournalFrame],
    manifest_checkpoint_sequence: u64,
    current_fence_token: u64,
) -> Result<(), FenceRejection> {
    if frames.iter().any(|frame| {
        frame.partition_sequence > manifest_checkpoint_sequence
            && frame.fence_token != current_fence_token
    }) {
        return Err(FenceRejection {
            code: AnvilErrorCode::StaleFenceToken,
            reason: "journal contains stale-fence records after manifest checkpoint",
        });
    }
    Ok(())
}

async fn read_ownership_fence_unlocked(
    storage: &Storage,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<Option<OwnershipFenceRecord>> {
    validate_ownership_resource(resource)?;
    if tenant_id < 0 {
        return Err(anyhow!("ownership fence tenant id must be nonnegative"));
    }
    let path = ownership_fence_path(storage, tenant_id, resource)?;
    let Some(record) = read_json_optional::<OwnershipFenceRecord>(&path).await? else {
        return Ok(None);
    };
    record.verify(signing_key)?;
    if record.owner.tenant_id != tenant_id || record.resource != *resource {
        return Err(anyhow!("ownership fence path scope mismatch"));
    }
    Ok(Some(record))
}

async fn write_ownership_fence_unlocked(
    storage: &Storage,
    record: &OwnershipFenceRecord,
) -> Result<()> {
    let path = ownership_fence_path(storage, record.owner.tenant_id, &record.resource)?;
    write_json_atomically(&path, record).await
}

fn ownership_fence_path(
    storage: &Storage,
    tenant_id: i64,
    resource: &OwnershipResource,
) -> Result<PathBuf> {
    validate_ownership_resource(resource)?;
    if tenant_id < 0 {
        return Err(anyhow!("ownership fence tenant id must be nonnegative"));
    }
    let family = format!("ownership-{}", resource.resource_kind.as_str());
    let partition_id = ownership_resource_hash(tenant_id, resource)?;
    storage.partition_owner_path(&family, &partition_id)
}

fn ownership_resource_hash(tenant_id: i64, resource: &OwnershipResource) -> Result<String> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(tenant_id.to_string().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(resource.resource_kind.as_str().as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(resource.resource_id.as_bytes());
    Ok(hex::encode(hash32(&bytes)))
}

struct OwnershipFenceWriteGuard {
    path: PathBuf,
}

impl OwnershipFenceWriteGuard {
    async fn acquire(
        storage: &Storage,
        tenant_id: i64,
        resource: &OwnershipResource,
    ) -> Result<Self> {
        let fence_path = ownership_fence_path(storage, tenant_id, resource)?;
        let lock_path = fence_path.with_extension("json.lock");
        if let Some(parent) = lock_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        for _ in 0..OWNERSHIP_LOCK_RETRY_ATTEMPTS {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .await
            {
                Ok(_) => return Ok(Self { path: lock_path }),
                Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                    tokio::time::sleep(OWNERSHIP_LOCK_RETRY_DELAY).await;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("create ownership fence CAS lock {}", lock_path.display())
                    });
                }
            }
        }
        Err(anyhow!(
            "{OWNERSHIP_CAS_CONFLICT}: ownership fence CAS lock was not acquired"
        ))
    }
}

impl Drop for OwnershipFenceWriteGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn write_partition_owner(storage: &Storage, owner: &PartitionOwnerState) -> Result<()> {
    let path = storage.partition_owner_path(&owner.partition_family, &owner.partition_id)?;
    write_json_atomically(&path, owner).await
}

fn validate_acquire_ownership(request: &AcquireOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.owner)?;
    validate_ownership_time(request.now_nanos, request.ttl_nanos)?;
    Ok(())
}

fn validate_renew_ownership(request: &RenewOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.owner)?;
    validate_ownership_fence_value(request.current_fence)?;
    validate_ownership_time(request.now_nanos, request.ttl_nanos)?;
    Ok(())
}

fn validate_transfer_ownership(request: &TransferOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    require_nonempty(&request.idempotency_key, "idempotency_key")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.current_owner)?;
    validate_ownership_principal(&request.new_owner)?;
    validate_ownership_fence_value(request.current_fence)?;
    validate_ownership_time(request.now_nanos, request.ttl_nanos)?;
    Ok(())
}

fn validate_release_ownership(request: &ReleaseOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.owner)?;
    if !request.administrative_force {
        validate_ownership_fence_value(request.current_fence)?;
    }
    if request.now_nanos < 0 {
        return Err(anyhow!("ownership fence timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_force_expire_ownership(request: &ForceExpireOwnership) -> Result<()> {
    require_nonempty(&request.request_id, "request_id")?;
    validate_ownership_resource(&request.resource)?;
    validate_ownership_principal(&request.admin)?;
    if request.now_nanos < 0 {
        return Err(anyhow!("ownership fence timestamp must be nonnegative"));
    }
    if request.reason.chars().any(char::is_control) {
        return Err(anyhow!(
            "ownership force-expire reason must not contain control characters"
        ));
    }
    Ok(())
}

fn validate_ownership_resource(resource: &OwnershipResource) -> Result<()> {
    require_nonempty(&resource.resource_id, "resource_id")?;
    if resource
        .resource_id
        .chars()
        .any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("resource_id must not contain control characters"));
    }
    Ok(())
}

fn validate_ownership_principal(owner: &OwnershipPrincipal) -> Result<()> {
    if owner.tenant_id < 0 {
        return Err(anyhow!("ownership owner tenant_id must be nonnegative"));
    }
    require_nonempty(&owner.principal_kind, "owner.principal_kind")?;
    require_nonempty(&owner.principal_id, "owner.principal_id")?;
    require_nonempty(&owner.actor_instance_id, "owner.actor_instance_id")?;
    require_nonempty(&owner.display_name, "owner.display_name")?;
    require_nonempty(&owner.region, "owner.region")?;
    require_nonempty(&owner.cell, "owner.cell")?;
    validate_optional_label(&owner.display_name, "owner.display_name")?;
    validate_optional_label(&owner.region, "owner.region")?;
    validate_optional_label(&owner.cell, "owner.cell")?;
    Ok(())
}

fn validate_ownership_time(now_nanos: i64, ttl_nanos: i64) -> Result<()> {
    if now_nanos < 0 {
        return Err(anyhow!("ownership fence timestamp must be nonnegative"));
    }
    if ttl_nanos <= 0 {
        return Err(anyhow!("ownership fence ttl must be positive"));
    }
    Ok(())
}

fn validate_ownership_fence_value(fence: u64) -> Result<()> {
    if fence == 0 {
        return Err(anyhow!("ownership fence token must be nonzero"));
    }
    Ok(())
}

fn validate_optional_label(value: &str, field: &'static str) -> Result<()> {
    if value.chars().any(|ch| ch == '\0' || ch.is_control()) {
        return Err(anyhow!("{field} must not contain control characters"));
    }
    Ok(())
}

fn validate_unsigned_ownership_fence(record: &OwnershipFenceRecord) -> Result<()> {
    if record.format_version != 1 {
        return Err(anyhow!("unsupported ownership fence version"));
    }
    validate_ownership_resource(&record.resource)?;
    validate_ownership_principal(&record.owner)?;
    validate_ownership_fence_value(record.fence)?;
    if record.generation == 0 {
        return Err(anyhow!("ownership fence generation must be nonzero"));
    }
    if record.last_heartbeat_at_nanos < 0 || record.lease_expires_at_nanos < 0 {
        return Err(anyhow!("ownership fence timestamps must be nonnegative"));
    }
    if matches!(
        record.state,
        OwnershipFenceState::Active
            | OwnershipFenceState::Transferring
            | OwnershipFenceState::Draining
    ) && record.lease_expires_at_nanos <= record.last_heartbeat_at_nanos
    {
        return Err(anyhow!(
            "active ownership fence expiry must be after heartbeat"
        ));
    }
    Ok(())
}

fn require_current_owner_and_fence(
    record: &OwnershipFenceRecord,
    owner: &OwnershipPrincipal,
    current_fence: u64,
) -> Result<()> {
    if !record.owner.same_security_owner(owner) {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: ownership fence owner mismatch"
        ));
    }
    if record.fence != current_fence {
        return Err(anyhow!(
            "{OWNERSHIP_STALE_FENCE}: ownership fence token mismatch"
        ));
    }
    Ok(())
}

fn ownership_idempotency_matches(
    record: &OwnershipFenceRecord,
    operation: &str,
    idempotency_key: &str,
    owner: &OwnershipPrincipal,
) -> bool {
    !idempotency_key.is_empty()
        && record.last_operation.as_deref() == Some(operation)
        && record.last_idempotency_key.as_deref() == Some(idempotency_key)
        && record
            .last_actor
            .as_ref()
            .unwrap_or(&record.owner)
            .same_security_owner(owner)
}

fn nonempty_idempotency_key(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn validate_recovery_acquire(request: &PartitionRecoveryAcquire) -> Result<()> {
    require_nonempty(&request.partition_family, "partition family")?;
    validate_hex32(&request.partition_id, "partition id")?;
    require_nonempty(&request.owner_node_id, "owner node id")?;
    validate_hex32(&request.recovered_manifest_hash, "recovered manifest hash")?;
    if request.now_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_unsigned_owner(owner: &PartitionOwnerState) -> Result<()> {
    if owner.format_version != 1 {
        return Err(anyhow!("unsupported partition owner version"));
    }
    require_nonempty(&owner.partition_family, "partition family")?;
    validate_hex32(&owner.partition_id, "partition id")?;
    require_nonempty(&owner.owner_node_id, "owner node id")?;
    validate_hex32(&owner.recovered_manifest_hash, "recovered manifest hash")?;
    if owner.fence_token == 0 || owner.recovery_epoch == 0 {
        return Err(anyhow!("partition owner fence and epoch must be nonzero"));
    }
    if owner.updated_at_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    Ok(())
}

fn sign_owner_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("partition owner signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"partition_owner");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn sign_ownership_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("ownership fence signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"ownership_fence");
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
        .with_context(|| format!("write temporary partition owner {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish partition owner {}", path.display()))?;
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
    use crate::formats::JournalRecordKind;
    use tempfile::tempdir;

    const KEY: &[u8] = b"partition owner signing key";

    #[tokio::test]
    async fn recovery_acquire_blocks_writes_until_owner_ready() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let recovering = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
            .await
            .unwrap();
        assert_eq!(recovering.fence_token, 1);
        assert_eq!(recovering.status, PartitionOwnerStatus::Recovering);

        let permit = PartitionWritePermit {
            partition_family: recovering.partition_family.clone(),
            partition_id: recovering.partition_id.clone(),
            owner_node_id: "node-a".to_string(),
            fence_token: recovering.fence_token,
        };
        let rejected = validate_partition_write(&storage, &permit, KEY)
            .await
            .unwrap_err();
        assert_eq!(rejected.code, AnvilErrorCode::PartitionNotOwned);

        let ready = publish_partition_ready(
            &storage,
            &recovering.partition_family,
            &recovering.partition_id,
            "node-a",
            recovering.fence_token,
            77,
            &hex::encode([9; 32]),
            200,
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(ready.status, PartitionOwnerStatus::Ready);
        assert_eq!(ready.recovered_through_sequence, 77);
        validate_partition_write(&storage, &ready.write_permit().unwrap(), KEY)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn owner_handoff_rejects_stale_fence_token() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
            .await
            .unwrap();
        let first = publish_partition_ready(
            &storage,
            &first.partition_family,
            &first.partition_id,
            "node-a",
            first.fence_token,
            10,
            &hex::encode([3; 32]),
            150,
            KEY,
        )
        .await
        .unwrap();
        let stale_permit = first.write_permit().unwrap();

        let second = acquire_partition_recovery(&storage, acquire("node-b", 300), KEY)
            .await
            .unwrap();
        assert_eq!(second.fence_token, first.fence_token + 1);
        let stale_rejection = validate_partition_write(&storage, &stale_permit, KEY)
            .await
            .unwrap_err();
        assert_eq!(stale_rejection.code, AnvilErrorCode::PartitionNotOwned);

        let second = publish_partition_ready(
            &storage,
            &second.partition_family,
            &second.partition_id,
            "node-b",
            second.fence_token,
            20,
            &hex::encode([4; 32]),
            350,
            KEY,
        )
        .await
        .unwrap();
        validate_partition_write(&storage, &second.write_permit().unwrap(), KEY)
            .await
            .unwrap();

        let mut stale_same_owner = second.write_permit().unwrap();
        stale_same_owner.fence_token -= 1;
        let stale_rejection = validate_partition_write(&storage, &stale_same_owner, KEY)
            .await
            .unwrap_err();
        assert_eq!(stale_rejection.code, AnvilErrorCode::StaleFenceToken);
    }

    #[test]
    fn recovery_replay_keeps_current_fence_after_manifest_checkpoint() {
        let stale_before = frame(9, 1, [0; 32]);
        let current_after = frame(11, 2, stale_before.record_hash);
        let stale_after = frame(12, 1, current_after.record_hash);
        let frames = vec![stale_before, current_after.clone(), stale_after];

        assert_eq!(
            frames_for_recovered_fence(&frames, 10, 2),
            vec![current_after]
        );
        let rejection = reject_stale_frames_after_checkpoint(&frames, 10, 2).unwrap_err();
        assert_eq!(rejection.code, AnvilErrorCode::StaleFenceToken);
        reject_stale_frames_after_checkpoint(&frames[..2], 10, 2).unwrap();
    }

    #[tokio::test]
    async fn partition_owner_state_is_signed_and_path_scoped() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
            .await
            .unwrap();
        let path = storage
            .partition_owner_path(&owner.partition_family, &owner.partition_id)
            .unwrap();
        assert!(path.ends_with(format!(
            "_anvil/control/partition-owners/object_metadata/{}.json",
            owner.partition_id
        )));

        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["fence_token"] = serde_json::json!(99);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();
        assert!(
            read_partition_owner(&storage, &owner.partition_family, &owner.partition_id, KEY)
                .await
                .is_err()
        );
        assert!(
            storage
                .partition_owner_path("../escape", &owner.partition_id)
                .is_err()
        );
    }

    #[tokio::test]
    async fn ownership_label_is_not_security_identity() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner_a = principal("app-a", "token-a", "node-shared");
        let owner_b = principal("app-b", "token-b", "node-shared");
        let first = acquire_ownership(
            &storage,
            ownership_acquire(owner_a.clone(), 100, 500, "acquire-a"),
            KEY,
        )
        .await
        .unwrap()
        .record;

        assert_eq!(first.owner.display_name, "node-shared");
        assert_eq!(first.owner.principal_id, "app-a");
        assert!(
            renew_ownership(
                &storage,
                RenewOwnership {
                    request_id: "renew-b".to_string(),
                    resource: ownership_resource(),
                    owner: owner_b.clone(),
                    current_fence: first.fence,
                    now_nanos: 200,
                    ttl_nanos: 500,
                },
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(OWNERSHIP_OWNER_MISMATCH)
        );
        assert!(
            release_ownership(
                &storage,
                ReleaseOwnership {
                    request_id: "release-b".to_string(),
                    idempotency_key: "release-b".to_string(),
                    resource: ownership_resource(),
                    owner: owner_b,
                    current_fence: first.fence,
                    administrative_force: false,
                    now_nanos: 250,
                },
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(OWNERSHIP_OWNER_MISMATCH)
        );
    }

    #[tokio::test]
    async fn expired_ownership_can_be_acquired_and_increments_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = acquire_ownership(
            &storage,
            ownership_acquire(
                principal("app-a", "token-a", "node-a"),
                100,
                50,
                "acquire-a",
            ),
            KEY,
        )
        .await
        .unwrap()
        .record;

        let second = acquire_ownership(
            &storage,
            ownership_acquire(
                principal("app-b", "token-b", "node-b"),
                200,
                50,
                "acquire-b",
            ),
            KEY,
        )
        .await
        .unwrap()
        .record;

        assert_eq!(second.fence, first.fence + 1);
        assert_eq!(second.owner.principal_id, "app-b");
        assert_eq!(second.state, OwnershipFenceState::Active);
    }

    #[tokio::test]
    async fn ownership_operations_reject_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = principal("app-a", "token-a", "node-a");
        let first = acquire_ownership(
            &storage,
            ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
            KEY,
        )
        .await
        .unwrap()
        .record;
        let stale_fence = first.fence + 1;

        assert!(
            renew_ownership(
                &storage,
                RenewOwnership {
                    request_id: "renew-stale".to_string(),
                    resource: ownership_resource(),
                    owner: owner.clone(),
                    current_fence: stale_fence,
                    now_nanos: 200,
                    ttl_nanos: 500,
                },
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(OWNERSHIP_STALE_FENCE)
        );
        assert!(
            transfer_ownership(
                &storage,
                TransferOwnership {
                    request_id: "transfer-stale".to_string(),
                    idempotency_key: "transfer-stale".to_string(),
                    resource: ownership_resource(),
                    current_owner: owner.clone(),
                    new_owner: principal("app-b", "token-b", "node-b"),
                    current_fence: stale_fence,
                    now_nanos: 220,
                    ttl_nanos: 500,
                },
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(OWNERSHIP_STALE_FENCE)
        );
        assert!(
            release_ownership(
                &storage,
                ReleaseOwnership {
                    request_id: "release-stale".to_string(),
                    idempotency_key: "release-stale".to_string(),
                    resource: ownership_resource(),
                    owner,
                    current_fence: stale_fence,
                    administrative_force: false,
                    now_nanos: 240,
                },
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(OWNERSHIP_STALE_FENCE)
        );
    }

    #[tokio::test]
    async fn concurrent_ownership_acquires_have_one_winner() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mut tasks = Vec::new();
        for idx in 0..16 {
            let storage = storage.clone();
            tasks.push(tokio::spawn(async move {
                acquire_ownership(
                    &storage,
                    ownership_acquire(
                        principal(
                            format!("app-{idx}"),
                            format!("token-{idx}"),
                            format!("node-{idx}"),
                        ),
                        100,
                        500,
                        format!("acquire-{idx}"),
                    ),
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
                Err(err) if err.to_string().contains(OWNERSHIP_HELD) => held += 1,
                Err(err) => panic!("unexpected ownership error: {err}"),
            }
        }
        assert_eq!(successes, 1);
        assert_eq!(held, 15);
    }

    #[tokio::test]
    async fn force_expire_increments_fence_and_blocks_stale_owner() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = principal("app-a", "token-a", "node-a");
        let first = acquire_ownership(
            &storage,
            ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
            KEY,
        )
        .await
        .unwrap()
        .record;

        let expired = force_expire_ownership(
            &storage,
            ForceExpireOwnership {
                request_id: "force-expire".to_string(),
                idempotency_key: "force-expire".to_string(),
                resource: ownership_resource(),
                admin: principal("admin", "admin-token", "admin"),
                reason: "test failover".to_string(),
                now_nanos: 200,
            },
            KEY,
        )
        .await
        .unwrap()
        .record;
        assert_eq!(expired.state, OwnershipFenceState::Expired);
        assert_eq!(expired.fence, first.fence + 1);

        assert!(
            renew_ownership(
                &storage,
                RenewOwnership {
                    request_id: "stale-renew".to_string(),
                    resource: ownership_resource(),
                    owner,
                    current_fence: first.fence,
                    now_nanos: 220,
                    ttl_nanos: 500,
                },
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(OWNERSHIP_STALE_FENCE)
        );

        let replacement = acquire_ownership(
            &storage,
            ownership_acquire(
                principal("app-b", "token-b", "node-b"),
                250,
                500,
                "acquire-b",
            ),
            KEY,
        )
        .await
        .unwrap()
        .record;
        assert_eq!(replacement.fence, expired.fence + 1);
    }

    #[tokio::test]
    async fn transfer_moves_to_explicit_target_identity_and_is_idempotent() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = principal("app-a", "token-a", "node-a");
        let new_owner = principal("app-b", "token-b", "node-b");
        let first = acquire_ownership(
            &storage,
            ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
            KEY,
        )
        .await
        .unwrap()
        .record;

        let transferred = transfer_ownership(
            &storage,
            TransferOwnership {
                request_id: "transfer".to_string(),
                idempotency_key: "transfer-key".to_string(),
                resource: ownership_resource(),
                current_owner: owner.clone(),
                new_owner: new_owner.clone(),
                current_fence: first.fence,
                now_nanos: 200,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(transferred.record.fence, first.fence + 1);
        assert!(transferred.record.owner.same_security_owner(&new_owner));
        assert!(!transferred.record.owner.same_security_owner(&owner));
        assert!(!transferred.idempotent_replay);

        let replay = transfer_ownership(
            &storage,
            TransferOwnership {
                request_id: "transfer-replay".to_string(),
                idempotency_key: "transfer-key".to_string(),
                resource: ownership_resource(),
                current_owner: owner.clone(),
                new_owner,
                current_fence: first.fence,
                now_nanos: 220,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap();
        assert!(replay.idempotent_replay);
        assert_eq!(replay.record.fence, transferred.record.fence);

        assert!(
            renew_ownership(
                &storage,
                RenewOwnership {
                    request_id: "old-owner-renew".to_string(),
                    resource: ownership_resource(),
                    owner,
                    current_fence: transferred.record.fence,
                    now_nanos: 230,
                    ttl_nanos: 500,
                },
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(OWNERSHIP_OWNER_MISMATCH)
        );
    }

    #[tokio::test]
    async fn release_requires_owner_and_fence_unless_force() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = principal("app-a", "token-a", "node-a");
        let other = principal("app-b", "token-b", "node-b");
        let first = acquire_ownership(
            &storage,
            ownership_acquire(owner, 100, 500, "acquire-a"),
            KEY,
        )
        .await
        .unwrap()
        .record;

        assert!(
            release_ownership(
                &storage,
                ReleaseOwnership {
                    request_id: "release-other".to_string(),
                    idempotency_key: "release-other".to_string(),
                    resource: ownership_resource(),
                    owner: other.clone(),
                    current_fence: first.fence,
                    administrative_force: false,
                    now_nanos: 200,
                },
                KEY,
            )
            .await
            .unwrap_err()
            .to_string()
            .contains(OWNERSHIP_OWNER_MISMATCH)
        );

        let released = release_ownership(
            &storage,
            ReleaseOwnership {
                request_id: "release-force".to_string(),
                idempotency_key: "release-force".to_string(),
                resource: ownership_resource(),
                owner: other,
                current_fence: 0,
                administrative_force: true,
                now_nanos: 220,
            },
            KEY,
        )
        .await
        .unwrap()
        .record;
        assert_eq!(released.state, OwnershipFenceState::Released);
        assert_eq!(released.fence, first.fence + 1);
    }

    fn acquire(owner_node_id: &str, now_nanos: i64) -> PartitionRecoveryAcquire {
        PartitionRecoveryAcquire {
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode([7; 32]),
            owner_node_id: owner_node_id.to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos,
        }
    }

    fn ownership_acquire(
        owner: OwnershipPrincipal,
        now_nanos: i64,
        ttl_nanos: i64,
        idempotency_key: impl Into<String>,
    ) -> AcquireOwnership {
        AcquireOwnership {
            request_id: format!("req-{}", now_nanos),
            idempotency_key: idempotency_key.into(),
            resource: ownership_resource(),
            owner,
            now_nanos,
            ttl_nanos,
        }
    }

    fn ownership_resource() -> OwnershipResource {
        OwnershipResource {
            resource_kind: OwnershipResourceKind::BucketPrimary,
            resource_id: "tenant-acme/releases".to_string(),
        }
    }

    fn principal(
        principal_id: impl Into<String>,
        actor_instance_id: impl Into<String>,
        display_name: impl Into<String>,
    ) -> OwnershipPrincipal {
        OwnershipPrincipal {
            tenant_id: 1,
            principal_kind: "app".to_string(),
            principal_id: principal_id.into(),
            actor_instance_id: actor_instance_id.into(),
            display_name: display_name.into(),
            region: "eu-west-1".to_string(),
            cell: "cell-a".to_string(),
        }
    }

    fn frame(sequence: u64, fence_token: u64, previous_hash: [u8; 32]) -> JournalFrame {
        JournalFrame::new(
            JournalRecordKind::ObjectVersion,
            sequence,
            fence_token,
            [sequence as u8; 16],
            [fence_token as u8; 32],
            previous_hash,
            vec![sequence as u8, fence_token as u8],
        )
    }
}
