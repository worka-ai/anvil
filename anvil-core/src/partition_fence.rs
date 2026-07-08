use crate::{
    core_store::{
        CompareAndSwapRef, CoreMutationPrecondition, CoreObjectRef, CorePipelinePolicy,
        CoreRefValue, CoreStore, CoreTraceContext, GetBlob, WriteLogicalFileRequest,
    },
    error_codes::AnvilErrorCode,
    formats::{JournalFrame, hash32},
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::fmt;

type HmacSha256 = Hmac<Sha256>;

pub const OWNERSHIP_HELD: &str = "OwnershipHeld";
pub const OWNERSHIP_EXPIRED: &str = "OwnershipExpired";
pub const OWNERSHIP_NOT_FOUND: &str = "OwnershipNotFound";
pub const OWNERSHIP_OWNER_MISMATCH: &str = "OwnershipOwnerMismatch";
pub const OWNERSHIP_STALE_FENCE: &str = "StaleFence";
pub const OWNERSHIP_CAS_CONFLICT: &str = "OwnershipCasConflict";
pub const MAX_OWNERSHIP_LEASE_MS: u64 = 120_000;

const OWNERSHIP_LOCK_RETRY_ATTEMPTS: usize = 200;
const PARTITION_OWNER_REF_PREFIX: &str = "partition_owner:";
const OWNERSHIP_FENCE_REF_PREFIX: &str = "ownership_fence:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
    for _ in 0..OWNERSHIP_LOCK_RETRY_ATTEMPTS {
        let existing = match read_ownership_fence_state(
            storage,
            request.owner.tenant_id,
            &request.resource,
            signing_key,
        )
        .await
        {
            Ok(existing) => existing,
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        };
        let existing_record = existing.as_ref().map(|(_, record)| record);
        if let Some(existing) = existing_record {
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

        let fence = existing_record
            .map(|record| record.fence.saturating_add(1))
            .unwrap_or(1);
        let record = OwnershipFenceRecord {
            format_version: 1,
            resource: request.resource.clone(),
            owner: request.owner.clone(),
            fence,
            state: OwnershipFenceState::Active,
            lease_expires_at_nanos: request.now_nanos.saturating_add(request.ttl_nanos),
            last_heartbeat_at_nanos: request.now_nanos,
            generation: fence,
            last_operation: Some("acquire".to_string()),
            last_idempotency_key: nonempty_idempotency_key(request.idempotency_key.clone()),
            last_actor: Some(request.owner.clone()),
            ownership_hash: None,
            ownership_signature: None,
        }
        .seal(signing_key)?;
        match write_ownership_fence_state(
            storage,
            &record,
            existing.as_ref().map(|(ref_value, _)| ref_value),
        )
        .await
        {
            Ok(()) => {
                return Ok(OwnershipFenceOutcome {
                    record,
                    idempotent_replay: false,
                });
            }
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: ownership fence CAS retries exhausted"
    ))
}

pub async fn renew_ownership(
    storage: &Storage,
    request: RenewOwnership,
    signing_key: &[u8],
) -> Result<OwnershipFenceOutcome> {
    validate_renew_ownership(&request)?;
    for _ in 0..OWNERSHIP_LOCK_RETRY_ATTEMPTS {
        let Some((ref_value, mut record)) = read_ownership_fence_state(
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
        record.last_actor = Some(request.owner.clone());
        record = record.seal(signing_key)?;
        match write_ownership_fence_state(storage, &record, Some(&ref_value)).await {
            Ok(()) => {
                return Ok(OwnershipFenceOutcome {
                    record,
                    idempotent_replay: false,
                });
            }
            Err(err) if is_core_ref_cas_conflict(&err) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(anyhow!(
        "{OWNERSHIP_CAS_CONFLICT}: ownership fence renew CAS retries exhausted"
    ))
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
    let Some((ref_value, mut record)) = read_ownership_fence_state(
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
    write_ownership_fence_state(storage, &record, Some(&ref_value)).await?;
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
    let Some((ref_value, mut record)) = read_ownership_fence_state(
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
    write_ownership_fence_state(storage, &record, Some(&ref_value)).await?;
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
    let Some((ref_value, mut record)) = read_ownership_fence_state(
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
    write_ownership_fence_state(storage, &record, Some(&ref_value)).await?;
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
    Ok(
        read_ownership_fence_state(storage, tenant_id, resource, signing_key)
            .await?
            .map(|(_, record)| record),
    )
}

pub async fn list_partition_owners(
    storage: &Storage,
    signing_key: &[u8],
) -> Result<Vec<PartitionOwnerState>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut out = Vec::new();
    for ref_name in store.list_ref_names(PARTITION_OWNER_REF_PREFIX).await? {
        let Some((partition_family, partition_id)) = parse_partition_owner_ref_name(&ref_name)?
        else {
            continue;
        };
        let Some((_, owner)) =
            read_partition_owner_state(storage, &partition_family, &partition_id, signing_key)
                .await?
        else {
            continue;
        };
        out.push(owner);
    }
    out.sort_by(|left, right| {
        left.partition_family
            .cmp(&right.partition_family)
            .then(left.partition_id.cmp(&right.partition_id))
    });
    Ok(out)
}

pub async fn list_partition_owners_for_node(
    storage: &Storage,
    owner_node_id: &str,
    signing_key: &[u8],
) -> Result<Vec<PartitionOwnerState>> {
    Ok(list_partition_owners(storage, signing_key)
        .await?
        .into_iter()
        .filter(|owner| owner.owner_node_id == owner_node_id)
        .collect())
}

pub async fn force_expire_partition_owner_for_node(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<Option<PartitionOwnerState>> {
    let Some((ref_value, mut owner)) =
        read_partition_owner_state(storage, partition_family, partition_id, signing_key).await?
    else {
        return Ok(None);
    };
    if owner.owner_node_id != owner_node_id {
        return Ok(None);
    }
    owner.owner_node_id = format!("expired-{owner_node_id}");
    owner.fence_token = owner.fence_token.saturating_add(1);
    owner.recovery_epoch = owner.recovery_epoch.saturating_add(1);
    owner.status = PartitionOwnerStatus::Recovering;
    owner.updated_at_nanos = now_nanos;
    owner = owner.seal(signing_key)?;
    write_partition_owner_state(storage, &owner, Some(&ref_value)).await?;
    Ok(Some(owner))
}

pub async fn list_ownership_fences(
    storage: &Storage,
    signing_key: &[u8],
) -> Result<Vec<OwnershipFenceRecord>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut out = Vec::new();
    for ref_name in store.list_ref_names(OWNERSHIP_FENCE_REF_PREFIX).await? {
        let Some(record) = read_ownership_fence_ref(storage, &ref_name, signing_key).await? else {
            continue;
        };
        out.push(record);
    }
    out.sort_by(|left, right| {
        left.resource
            .resource_kind
            .as_str()
            .cmp(right.resource.resource_kind.as_str())
            .then(left.resource.resource_id.cmp(&right.resource.resource_id))
    });
    Ok(out)
}

pub async fn list_active_ownership_fences_for_node(
    storage: &Storage,
    owner_node_id: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<Vec<OwnershipFenceRecord>> {
    Ok(list_ownership_fences(storage, signing_key)
        .await?
        .into_iter()
        .filter(|record| {
            record.owner.principal_kind == "node"
                && record.owner.actor_instance_id == owner_node_id
                && record.is_active_unexpired(now_nanos)
        })
        .collect())
}

pub async fn acquire_partition_recovery(
    storage: &Storage,
    request: PartitionRecoveryAcquire,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    validate_recovery_acquire(&request)?;
    let existing = read_partition_owner_state(
        storage,
        &request.partition_family,
        &request.partition_id,
        signing_key,
    )
    .await?;
    let existing_state = existing.as_ref().map(|(_, state)| state);
    let fence_token = existing_state
        .map(|state| state.fence_token.saturating_add(1))
        .unwrap_or(1);
    let recovery_epoch = existing_state
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
    write_partition_owner_state(
        storage,
        &state,
        existing.as_ref().map(|(ref_value, _)| ref_value),
    )
    .await?;
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
    let Some((ref_value, mut state)) =
        read_partition_owner_state(storage, partition_family, partition_id, signing_key).await?
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
    write_partition_owner_state(storage, &state, Some(&ref_value)).await?;
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

pub async fn partition_write_ref_precondition(
    storage: &Storage,
    permit: &PartitionWritePermit,
    signing_key: &[u8],
) -> Result<CoreMutationPrecondition, FenceRejection> {
    let state = read_partition_owner_state(
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
    let Some((ref_value, owner)) = state else {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner state is absent",
        });
    };
    validate_write_permit_for_state(&owner, permit, true)?;
    Ok(CoreMutationPrecondition::Ref {
        ref_name: partition_owner_ref_name(&permit.partition_family, &permit.partition_id)
            .map_err(|_| FenceRejection {
                code: AnvilErrorCode::PartitionNotOwned,
                reason: "partition owner ref cannot be addressed",
            })?,
        expected_generation: Some(ref_value.generation),
        expected_target: Some(ref_value.target),
        require_absent: false,
        require_present: true,
        fence: None,
        authz_revision: None,
        source_watch_cursor: None,
    })
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
    Ok(
        read_partition_owner_state(storage, partition_family, partition_id, signing_key)
            .await?
            .map(|(_, owner)| owner),
    )
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

async fn read_ownership_fence_state(
    storage: &Storage,
    tenant_id: i64,
    resource: &OwnershipResource,
    signing_key: &[u8],
) -> Result<Option<(CoreRefValue, OwnershipFenceRecord)>> {
    validate_ownership_resource(resource)?;
    if tenant_id < 0 {
        return Err(anyhow!("ownership fence tenant id must be nonnegative"));
    }
    let ref_name = ownership_fence_ref_name(tenant_id, resource)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&ref_name).await? else {
        return Ok(None);
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    let record: OwnershipFenceRecord = serde_json::from_slice(&bytes)?;
    record.verify(signing_key)?;
    if record.owner.tenant_id != tenant_id || record.resource != *resource {
        return Err(anyhow!("ownership fence ref scope mismatch"));
    }
    Ok(Some((ref_value, record)))
}

async fn write_ownership_fence_state(
    storage: &Storage,
    record: &OwnershipFenceRecord,
    expected_ref: Option<&CoreRefValue>,
) -> Result<()> {
    let ref_name = ownership_fence_ref_name(record.owner.tenant_id, &record.resource)?;
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "ownership_fence".to_string(),
            generation: record.generation,
            logical_file_id: ref_name.clone(),
            source: serde_json::to_vec_pretty(record)?,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "ownership-fence:{}:{}:{}:{}:{}:{}",
                record.owner.tenant_id,
                record.resource.resource_kind.as_str(),
                record.generation,
                record.owner.principal_id,
                record.owner.actor_instance_id,
                record.ownership_hash.as_deref().unwrap_or("unsealed")
            ),
            region_id: "local".to_string(),
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

fn ownership_fence_ref_name(tenant_id: i64, resource: &OwnershipResource) -> Result<String> {
    validate_ownership_resource(resource)?;
    if tenant_id < 0 {
        return Err(anyhow!("ownership fence tenant id must be nonnegative"));
    }
    Ok(format!(
        "{OWNERSHIP_FENCE_REF_PREFIX}tenant:{tenant_id}:kind:{}:resource:{}",
        resource.resource_kind.as_str(),
        ownership_resource_hash(tenant_id, resource)?
    ))
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

async fn read_partition_owner_state(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    signing_key: &[u8],
) -> Result<Option<(CoreRefValue, PartitionOwnerState)>> {
    let ref_name = partition_owner_ref_name(partition_family, partition_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&ref_name).await? else {
        return Ok(None);
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    let owner: PartitionOwnerState = serde_json::from_slice(&bytes)?;
    owner.verify(signing_key)?;
    if owner.partition_family != partition_family || owner.partition_id != partition_id {
        return Err(anyhow!("partition owner ref scope mismatch"));
    }
    Ok(Some((ref_value, owner)))
}

async fn write_partition_owner_state(
    storage: &Storage,
    owner: &PartitionOwnerState,
    expected_ref: Option<&CoreRefValue>,
) -> Result<()> {
    let ref_name = partition_owner_ref_name(&owner.partition_family, &owner.partition_id)?;
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "partition_owner".to_string(),
            generation: owner.recovery_epoch,
            logical_file_id: ref_name.clone(),
            source: serde_json::to_vec_pretty(owner)?,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "partition-owner:{}:{}:{}:{}:{}",
                owner.partition_family,
                owner.partition_id,
                owner.recovery_epoch,
                owner.owner_node_id,
                owner.owner_hash.as_deref().unwrap_or("unsealed")
            ),
            region_id: "local".to_string(),
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

async fn read_ownership_fence_ref(
    storage: &Storage,
    ref_name: &str,
    signing_key: &[u8],
) -> Result<Option<OwnershipFenceRecord>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(ref_name).await? else {
        return Ok(None);
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    let record: OwnershipFenceRecord = serde_json::from_slice(&bytes)?;
    record.verify(signing_key)?;
    let expected_ref = ownership_fence_ref_name(record.owner.tenant_id, &record.resource)?;
    if expected_ref != ref_name {
        return Err(anyhow!("ownership fence ref scope mismatch"));
    }
    Ok(Some(record))
}

fn partition_owner_ref_name(partition_family: &str, partition_id: &str) -> Result<String> {
    require_nonempty(partition_family, "partition family")?;
    if partition_family.contains('\0')
        || partition_family.contains("..")
        || partition_family.contains(':')
        || partition_family.chars().any(char::is_control)
    {
        return Err(anyhow!("partition family contains an invalid component"));
    }
    validate_hex32(partition_id, "partition id")?;
    Ok(format!(
        "{PARTITION_OWNER_REF_PREFIX}family:{partition_family}:partition:{partition_id}"
    ))
}

fn parse_partition_owner_ref_name(ref_name: &str) -> Result<Option<(String, String)>> {
    let Some(rest) = ref_name.strip_prefix(PARTITION_OWNER_REF_PREFIX) else {
        return Ok(None);
    };
    let Some(rest) = rest.strip_prefix("family:") else {
        return Ok(None);
    };
    let Some((family, partition_id)) = rest.split_once(":partition:") else {
        return Ok(None);
    };
    validate_hex32(partition_id, "partition id")?;
    Ok(Some((family.to_string(), partition_id.to_string())))
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
    err.chain().any(|cause| {
        let message = cause.to_string();
        message.contains("generation mismatch")
            || message.contains("target mismatch")
            || message.contains("must be absent")
            || message.contains("must be present")
            || message.contains("CAS lock was not acquired")
            || message.contains("CoreStore stream idempotency conflict")
    })
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
mod tests;
