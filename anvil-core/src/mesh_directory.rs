use crate::mesh_control_stream::{
    self, ControlRecordDigest, ControlStreamFrame, ControlStreamSequence,
};
use crate::partition_fence::{self, PartitionWritePermit};
use crate::storage::Storage;
use crate::{routing, validation};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, btree_map::Entry};
use std::fmt;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

pub const MESH_DIRECTORY_ROOT: &str = "_anvil/control/v1/mesh";
pub const TENANT_NAME_SCHEMA: &str = "anvil.mesh.tenant_name.v1";
pub const TENANT_LOCATOR_SCHEMA: &str = "anvil.mesh.tenant_locator.v1";
pub const BUCKET_LOCATOR_SCHEMA: &str = "anvil.mesh.bucket_locator.v1";
pub const CONTROL_MUTATION_SCHEMA: &str = "anvil.mesh.control_mutation.v1";
pub const CONTROL_PARTITION_FAMILY: &str = "control_partition";

const TENANT_NAME_PARTITION_DOMAIN: &str = "tenant-name";
const TENANT_LOCATOR_PARTITION_DOMAIN: &str = "tenant-locator";
const BUCKET_LOCATOR_PARTITION_DOMAIN: &str = "bucket-locator";
const HOST_ALIAS_PARTITION_DOMAIN: &str = "host-alias";

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum RoutingRecordFamily {
    TenantName,
    TenantLocator,
    BucketLocator,
    HostAlias,
}

impl RoutingRecordFamily {
    pub fn all() -> [Self; 4] {
        [
            Self::TenantName,
            Self::TenantLocator,
            Self::BucketLocator,
            Self::HostAlias,
        ]
    }

    pub fn stream_family(self) -> &'static str {
        match self {
            Self::TenantName => "tenant_name",
            Self::TenantLocator => "tenant_locator",
            Self::BucketLocator => "bucket_locator",
            Self::HostAlias => "host_alias",
        }
    }

    pub fn from_stream_family(value: &str) -> Option<Self> {
        match value {
            "tenant_name" => Some(Self::TenantName),
            "tenant_locator" => Some(Self::TenantLocator),
            "bucket_locator" => Some(Self::BucketLocator),
            "host_alias" => Some(Self::HostAlias),
            _ => None,
        }
    }

    pub fn directory_segment(self) -> &'static str {
        match self {
            Self::TenantName => "tenant-names",
            Self::TenantLocator => "tenants",
            Self::BucketLocator => "buckets",
            Self::HostAlias => "host-aliases",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RoutingRecordDescriptor {
    pub family: RoutingRecordFamily,
    pub record_key: String,
    pub partition: String,
    pub descriptor_key: String,
    pub generation: u64,
    pub payload_json: String,
}

#[derive(Debug, Clone, Copy)]
pub struct MeshControlWriteAuthority<'a> {
    pub permit: &'a PartitionWritePermit,
    pub signing_key: &'a [u8],
}

pub fn control_partition_id(stream_family: &str, partition: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(stream_family.as_bytes());
    hasher.update(b"/");
    hasher.update(partition.as_bytes());
    hasher.finalize().to_hex().to_string()
}

#[derive(Debug, Error)]
pub enum MeshDirectoryError {
    #[error("invalid tenant name: {0}")]
    InvalidTenantName(String),
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),
    #[error("invalid {field}: {value}")]
    InvalidIdentifier { field: &'static str, value: String },
    #[error("bucket locator already exists for tenant {tenant_id} bucket {bucket_name}")]
    DuplicateBucketLocator {
        tenant_id: String,
        bucket_name: String,
    },
    #[error("tenant name already exists: {tenant_name}")]
    TenantNameAlreadyExists { tenant_name: String },
    #[error(
        "mesh directory generation conflict for {descriptor_key}: expected {expected}, actual {actual}"
    )]
    GenerationConflict {
        descriptor_key: String,
        expected: u64,
        actual: u64,
    },
    #[error("invalid mesh directory state for {descriptor_key}: {state}")]
    InvalidState {
        descriptor_key: String,
        state: String,
    },
    #[error("invalid RFC3339 timestamp in {field}: {value}")]
    InvalidTimestamp { field: &'static str, value: String },
    #[error("mesh directory record not found: {0}")]
    NotFound(String),
    #[error("invalid mesh control write permit for {stream_family}/{partition}: {reason}")]
    InvalidControlWritePermit {
        stream_family: String,
        partition: String,
        reason: String,
    },
    #[error("mesh control write fence rejected for {stream_family}/{partition}: {code}: {reason}")]
    ControlFenceRejected {
        stream_family: String,
        partition: String,
        code: &'static str,
        reason: &'static str,
    },
    #[error("mesh control stream write failed for {stream_family}/{partition}: {message}")]
    ControlStreamWrite {
        stream_family: String,
        partition: String,
        message: String,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

pub type MeshDirectoryResult<T> = Result<T, MeshDirectoryError>;

impl PartialEq for MeshDirectoryError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::InvalidTenantName(a), Self::InvalidTenantName(b)) => a == b,
            (Self::InvalidBucketName(a), Self::InvalidBucketName(b)) => a == b,
            (
                Self::InvalidIdentifier {
                    field: field_a,
                    value: value_a,
                },
                Self::InvalidIdentifier {
                    field: field_b,
                    value: value_b,
                },
            ) => field_a == field_b && value_a == value_b,
            (
                Self::DuplicateBucketLocator {
                    tenant_id: tenant_a,
                    bucket_name: bucket_a,
                },
                Self::DuplicateBucketLocator {
                    tenant_id: tenant_b,
                    bucket_name: bucket_b,
                },
            ) => tenant_a == tenant_b && bucket_a == bucket_b,
            (
                Self::TenantNameAlreadyExists {
                    tenant_name: name_a,
                },
                Self::TenantNameAlreadyExists {
                    tenant_name: name_b,
                },
            ) => name_a == name_b,
            (
                Self::GenerationConflict {
                    descriptor_key: key_a,
                    expected: expected_a,
                    actual: actual_a,
                },
                Self::GenerationConflict {
                    descriptor_key: key_b,
                    expected: expected_b,
                    actual: actual_b,
                },
            ) => key_a == key_b && expected_a == expected_b && actual_a == actual_b,
            (
                Self::InvalidState {
                    descriptor_key: key_a,
                    state: state_a,
                },
                Self::InvalidState {
                    descriptor_key: key_b,
                    state: state_b,
                },
            ) => key_a == key_b && state_a == state_b,
            (
                Self::InvalidTimestamp {
                    field: field_a,
                    value: value_a,
                },
                Self::InvalidTimestamp {
                    field: field_b,
                    value: value_b,
                },
            ) => field_a == field_b && value_a == value_b,
            (Self::NotFound(a), Self::NotFound(b)) => a == b,
            (
                Self::InvalidControlWritePermit {
                    stream_family: family_a,
                    partition: partition_a,
                    reason: reason_a,
                },
                Self::InvalidControlWritePermit {
                    stream_family: family_b,
                    partition: partition_b,
                    reason: reason_b,
                },
            ) => family_a == family_b && partition_a == partition_b && reason_a == reason_b,
            (
                Self::ControlFenceRejected {
                    stream_family: family_a,
                    partition: partition_a,
                    code: code_a,
                    reason: reason_a,
                },
                Self::ControlFenceRejected {
                    stream_family: family_b,
                    partition: partition_b,
                    code: code_b,
                    reason: reason_b,
                },
            ) => {
                family_a == family_b
                    && partition_a == partition_b
                    && code_a == code_b
                    && reason_a == reason_b
            }
            (
                Self::ControlStreamWrite {
                    stream_family: family_a,
                    partition: partition_a,
                    message: source_a,
                },
                Self::ControlStreamWrite {
                    stream_family: family_b,
                    partition: partition_b,
                    message: source_b,
                },
            ) => family_a == family_b && partition_a == partition_b && source_a == source_b,
            _ => false,
        }
    }
}

impl Eq for MeshDirectoryError {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MeshId(String);

impl MeshId {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        require_safe_component(&value, "mesh id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MeshId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        require_safe_component(&value, "tenant id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn partition_key(&self) -> Vec<u8> {
        partition_key_bytes(TENANT_LOCATOR_PARTITION_DOMAIN, &[self.as_str()])
    }

    pub fn partition(&self) -> String {
        stable_partition_prefix(&self.partition_key())
    }

    pub fn descriptor_key(&self) -> String {
        join_mesh_key(&[
            "tenants",
            &self.partition(),
            &format!("{}.json", self.as_str()),
        ])
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TenantName(String);

impl TenantName {
    pub fn canonicalize(value: impl AsRef<str>) -> MeshDirectoryResult<Self> {
        let raw = value.as_ref();
        if raw.contains('.') || !raw.is_ascii() {
            return Err(MeshDirectoryError::InvalidTenantName(raw.to_string()));
        }
        let canonical = raw.to_ascii_lowercase();
        validate_dns_label_name(&canonical)
            .map_err(|_| MeshDirectoryError::InvalidTenantName(raw.to_string()))?;
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn partition_key(&self) -> Vec<u8> {
        partition_key_bytes(TENANT_NAME_PARTITION_DOMAIN, &[self.as_str()])
    }

    pub fn partition(&self) -> String {
        stable_partition_prefix(&self.partition_key())
    }

    pub fn descriptor_key(&self) -> String {
        join_mesh_key(&[
            "tenant-names",
            &self.partition(),
            &format!("{}.json", self.as_str()),
        ])
    }
}

impl fmt::Display for TenantName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BucketName(String);

impl BucketName {
    pub fn canonicalize(value: impl AsRef<str>) -> MeshDirectoryResult<Self> {
        let raw = value.as_ref();
        if !raw.is_ascii() {
            return Err(MeshDirectoryError::InvalidBucketName(raw.to_string()));
        }
        let canonical = raw.to_ascii_lowercase();
        if !validation::is_valid_bucket_name(&canonical) {
            return Err(MeshDirectoryError::InvalidBucketName(raw.to_string()));
        }
        Ok(Self(canonical))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BucketId(String);

impl BucketId {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        require_safe_component(&value, "bucket id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BucketId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RegionName(String);

impl RegionName {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        if !validation::is_valid_region_name(&value) {
            return Err(MeshDirectoryError::InvalidIdentifier {
                field: "region",
                value,
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RegionName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CellId(String);

impl CellId {
    pub fn new(value: impl Into<String>) -> MeshDirectoryResult<Self> {
        let value = value.into();
        require_safe_component(&value, "cell id")?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CellId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TenantNameStatus {
    Reserved,
    Active,
    Tombstoned,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TenantLocatorStatus {
    Creating,
    Active,
    Suspended,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum BucketLocatorStatus {
    Creating,
    Active,
    ReadOnly,
    Moving,
    Draining,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantNameDescriptor {
    pub schema: String,
    pub mesh_id: MeshId,
    pub tenant_name: TenantName,
    pub tenant_id: TenantId,
    pub status: TenantNameStatus,
    pub idempotency_key: Option<String>,
    pub reservation_expires_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

impl TenantNameDescriptor {
    pub fn reserved(
        mesh_id: MeshId,
        tenant_name: TenantName,
        tenant_id: TenantId,
        idempotency_key: impl Into<String>,
        reservation_expires_at: impl Into<String>,
        now: impl Into<String>,
    ) -> MeshDirectoryResult<Self> {
        let idempotency_key = idempotency_key.into();
        let reservation_expires_at = reservation_expires_at.into();
        let now = now.into();
        require_nonempty(&idempotency_key, "idempotency key")?;
        require_nonempty(&reservation_expires_at, "reservation expiry")?;
        require_nonempty(&now, "timestamp")?;
        Ok(Self {
            schema: TENANT_NAME_SCHEMA.to_string(),
            mesh_id,
            tenant_name,
            tenant_id,
            status: TenantNameStatus::Reserved,
            idempotency_key: Some(idempotency_key),
            reservation_expires_at: Some(reservation_expires_at),
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }

    pub fn active(
        mesh_id: MeshId,
        tenant_name: TenantName,
        tenant_id: TenantId,
        now: impl Into<String>,
    ) -> MeshDirectoryResult<Self> {
        let now = now.into();
        require_nonempty(&now, "timestamp")?;
        Ok(Self {
            schema: TENANT_NAME_SCHEMA.to_string(),
            mesh_id,
            tenant_name,
            tenant_id,
            status: TenantNameStatus::Active,
            idempotency_key: None,
            reservation_expires_at: None,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }

    pub fn activate(&self, now: impl Into<String>) -> MeshDirectoryResult<Self> {
        let now = now.into();
        require_nonempty(&now, "timestamp")?;
        if self.status != TenantNameStatus::Reserved {
            return Err(MeshDirectoryError::InvalidState {
                descriptor_key: self.descriptor_key(),
                state: format!("{:?}", self.status),
            });
        }
        let mut active = self.clone();
        active.status = TenantNameStatus::Active;
        active.reservation_expires_at = None;
        active.updated_at = now;
        active.generation += 1;
        Ok(active)
    }

    pub fn tombstone(&self, now: impl Into<String>) -> MeshDirectoryResult<Self> {
        let now = now.into();
        require_nonempty(&now, "timestamp")?;
        let mut tombstone = self.clone();
        tombstone.status = TenantNameStatus::Tombstoned;
        tombstone.updated_at = now;
        tombstone.generation += 1;
        Ok(tombstone)
    }

    pub fn descriptor_key(&self) -> String {
        self.tenant_name.descriptor_key()
    }

    pub fn partition_key(&self) -> Vec<u8> {
        self.tenant_name.partition_key()
    }

    pub fn partition(&self) -> String {
        self.tenant_name.partition()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantLocatorDescriptor {
    pub schema: String,
    pub mesh_id: MeshId,
    pub tenant_id: TenantId,
    pub tenant_name: TenantName,
    pub home_region: RegionName,
    pub status: TenantLocatorStatus,
    pub profile_revision: u64,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

impl TenantLocatorDescriptor {
    pub fn active(
        mesh_id: MeshId,
        tenant_id: TenantId,
        tenant_name: TenantName,
        home_region: RegionName,
        now: impl Into<String>,
    ) -> MeshDirectoryResult<Self> {
        let now = now.into();
        require_nonempty(&now, "timestamp")?;
        Ok(Self {
            schema: TENANT_LOCATOR_SCHEMA.to_string(),
            mesh_id,
            tenant_id,
            tenant_name,
            home_region,
            status: TenantLocatorStatus::Active,
            profile_revision: 1,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }

    pub fn descriptor_key(&self) -> String {
        self.tenant_id.descriptor_key()
    }

    pub fn partition_key(&self) -> Vec<u8> {
        self.tenant_id.partition_key()
    }

    pub fn partition(&self) -> String {
        self.tenant_id.partition()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BucketLocatorKey {
    pub tenant_id: TenantId,
    pub bucket_name: BucketName,
}

impl BucketLocatorKey {
    pub fn new(tenant_id: TenantId, bucket_name: BucketName) -> Self {
        Self {
            tenant_id,
            bucket_name,
        }
    }

    pub fn partition_key(&self) -> Vec<u8> {
        partition_key_bytes(
            BUCKET_LOCATOR_PARTITION_DOMAIN,
            &[self.tenant_id.as_str(), self.bucket_name.as_str()],
        )
    }

    pub fn partition(&self) -> String {
        stable_partition_prefix(&self.partition_key())
    }

    pub fn descriptor_key(&self) -> String {
        join_mesh_key(&[
            "buckets",
            &self.partition(),
            self.tenant_id.as_str(),
            &format!("{}.json", self.bucket_name.as_str()),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketLocatorDescriptor {
    pub schema: String,
    pub mesh_id: MeshId,
    pub tenant_id: TenantId,
    pub bucket_name: BucketName,
    pub bucket_id: BucketId,
    pub home_region: RegionName,
    pub home_cell: CellId,
    pub status: BucketLocatorStatus,
    pub placement_policy: String,
    pub object_prefix: String,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

impl BucketLocatorDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn active(
        mesh_id: MeshId,
        tenant_id: TenantId,
        bucket_name: BucketName,
        bucket_id: BucketId,
        home_region: RegionName,
        home_cell: CellId,
        placement_policy: impl Into<String>,
        object_prefix: impl Into<String>,
        now: impl Into<String>,
    ) -> MeshDirectoryResult<Self> {
        let placement_policy = placement_policy.into();
        require_nonempty(&placement_policy, "placement policy")?;
        let object_prefix = object_prefix.into();
        require_control_path_fragment(&object_prefix, "object prefix")?;
        let now = now.into();
        require_nonempty(&now, "timestamp")?;

        Ok(Self {
            schema: BUCKET_LOCATOR_SCHEMA.to_string(),
            mesh_id,
            tenant_id,
            bucket_name,
            bucket_id,
            home_region,
            home_cell,
            status: BucketLocatorStatus::Active,
            placement_policy,
            object_prefix,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }

    pub fn key(&self) -> BucketLocatorKey {
        BucketLocatorKey::new(self.tenant_id.clone(), self.bucket_name.clone())
    }

    pub fn descriptor_key(&self) -> String {
        self.key().descriptor_key()
    }

    pub fn partition_key(&self) -> Vec<u8> {
        self.key().partition_key()
    }

    pub fn partition(&self) -> String {
        self.key().partition()
    }
}

pub fn host_alias_partition_key(hostname: &str) -> MeshDirectoryResult<Vec<u8>> {
    let hostname = routing::normalize_alias_hostname(hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: hostname.to_string(),
        }
    })?;
    Ok(partition_key_bytes(
        HOST_ALIAS_PARTITION_DOMAIN,
        &[&hostname],
    ))
}

pub fn host_alias_partition(hostname: &str) -> MeshDirectoryResult<String> {
    Ok(stable_partition_prefix(&host_alias_partition_key(
        hostname,
    )?))
}

pub fn host_alias_descriptor_key(hostname: &str) -> MeshDirectoryResult<String> {
    let hostname = routing::normalize_alias_hostname(hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: hostname.to_string(),
        }
    })?;
    let partition = host_alias_partition(&hostname)?;
    Ok(join_mesh_key(&[
        "host-aliases",
        &partition,
        &format!("{hostname}.json"),
    ]))
}

pub async fn write_host_alias_descriptor(
    storage: &Storage,
    descriptor: &routing::HostAliasDescriptor,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<()> {
    let hostname = routing::normalize_alias_hostname(&descriptor.hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: descriptor.hostname.clone(),
        }
    })?;
    let partition = host_alias_partition(&hostname)?;
    if let Some(existing) = read_host_alias_descriptor(storage, &hostname).await?
        && existing == *descriptor
    {
        return Ok(());
    }
    append_control_mutation(
        storage,
        RoutingRecordFamily::HostAlias,
        &partition,
        &hostname,
        "upsert",
        descriptor
            .generation
            .checked_sub(1)
            .filter(|generation| *generation > 0),
        descriptor.generation,
        None,
        descriptor,
        authority,
    )
    .await?;
    write_descriptor(storage, &host_alias_descriptor_key(&hostname)?, descriptor).await
}

pub async fn read_host_alias_descriptor(
    storage: &Storage,
    hostname: &str,
) -> MeshDirectoryResult<Option<routing::HostAliasDescriptor>> {
    let hostname = routing::normalize_alias_hostname(hostname).map_err(|_| {
        MeshDirectoryError::InvalidIdentifier {
            field: "hostname",
            value: hostname.to_string(),
        }
    })?;
    read_typed_routing_descriptor(storage, RoutingRecordFamily::HostAlias, &hostname).await
}

#[derive(Debug, Clone, Default)]
pub struct BucketLocatorDirectory {
    locators: BTreeMap<BucketLocatorKey, BucketLocatorDescriptor>,
}

impl BucketLocatorDirectory {
    pub fn insert(&mut self, locator: BucketLocatorDescriptor) -> MeshDirectoryResult<()> {
        let key = locator.key();
        match self.locators.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(locator);
                Ok(())
            }
            Entry::Occupied(entry) => Err(MeshDirectoryError::DuplicateBucketLocator {
                tenant_id: entry.key().tenant_id.to_string(),
                bucket_name: entry.key().bucket_name.to_string(),
            }),
        }
    }

    pub fn len(&self) -> usize {
        self.locators.len()
    }

    pub fn is_empty(&self) -> bool {
        self.locators.is_empty()
    }
}

pub fn stable_partition_prefix(canonical_key: &[u8]) -> String {
    let digest = blake3::hash(canonical_key);
    let bytes = digest.as_bytes();
    format!("{:02x}{:02x}", bytes[0], bytes[1])
}

fn mesh_id_from_payload_json(payload_json: &[u8]) -> MeshDirectoryResult<String> {
    let value: serde_json::Value = serde_json::from_slice(payload_json)?;
    Ok(value
        .get("mesh_id")
        .and_then(|mesh_id| mesh_id.as_str())
        .unwrap_or("default")
        .to_string())
}

async fn append_control_mutation<T: Serialize>(
    storage: &Storage,
    family: RoutingRecordFamily,
    partition: &str,
    record_key: &str,
    operation: &str,
    expected_generation: Option<u64>,
    new_generation: u64,
    idempotency_key: Option<&str>,
    payload: &T,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<()> {
    let stream_family = family.stream_family();
    let expected_partition_id = control_partition_id(stream_family, partition);
    if authority.permit.partition_family != CONTROL_PARTITION_FAMILY {
        return Err(MeshDirectoryError::InvalidControlWritePermit {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            reason: format!(
                "expected partition family {CONTROL_PARTITION_FAMILY}, got {}",
                authority.permit.partition_family
            ),
        });
    }
    if authority.permit.partition_id != expected_partition_id {
        return Err(MeshDirectoryError::InvalidControlWritePermit {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            reason: "permit partition id does not match control stream partition".to_string(),
        });
    }
    partition_fence::validate_partition_write(storage, authority.permit, authority.signing_key)
        .await
        .map_err(|rejection| MeshDirectoryError::ControlFenceRejected {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            code: rejection.code.as_str(),
            reason: rejection.reason,
        })?;

    let stream_path = storage
        .mesh_control_stream_path(stream_family, partition)
        .map_err(|err| MeshDirectoryError::ControlStreamWrite {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            message: err.to_string(),
        })?;
    let existing_log = mesh_control_stream::read_control_stream_log(&stream_path)
        .await
        .map_err(|err| MeshDirectoryError::ControlStreamWrite {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            message: err.to_string(),
        })?;
    let sequence = existing_log
        .records
        .last()
        .map(|record| record.metadata.sequence.get().saturating_add(1))
        .unwrap_or(1);
    let payload_json = serde_json::to_vec(payload).map_err(MeshDirectoryError::Json)?;
    let digest = ControlRecordDigest::blake3(&payload_json);
    let header_json = serde_json::to_vec(&serde_json::json!({
        "schema": CONTROL_MUTATION_SCHEMA,
        "mesh_id": mesh_id_from_payload_json(&payload_json)?,
        "stream_family": stream_family,
        "partition": partition,
        "sequence": ControlStreamSequence::new(sequence)
            .map_err(|err| MeshDirectoryError::ControlStreamWrite {
                stream_family: stream_family.to_string(),
                partition: partition.to_string(),
                message: err.to_string(),
            })?,
        "record_key": record_key,
        "operation": operation,
        "expected_generation": expected_generation,
        "new_generation": new_generation,
        "writer_node_id": authority.permit.owner_node_id.as_str(),
        "writer_fence": authority.permit.fence_token,
        "idempotency_key": idempotency_key,
        "record_digest": digest.as_str(),
        "created_at": Utc::now().to_rfc3339(),
    }))?;
    let frame = ControlStreamFrame::new(header_json, payload_json);
    mesh_control_stream::append_control_stream_frame(stream_path, &frame)
        .await
        .map_err(|err| MeshDirectoryError::ControlStreamWrite {
            stream_family: stream_family.to_string(),
            partition: partition.to_string(),
            message: err.to_string(),
        })?;
    Ok(())
}

pub async fn reserve_tenant_name(
    storage: &Storage,
    descriptor: &TenantNameDescriptor,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<TenantNameDescriptor> {
    if descriptor.status != TenantNameStatus::Reserved {
        return Err(MeshDirectoryError::InvalidState {
            descriptor_key: descriptor.descriptor_key(),
            state: format!("{:?}", descriptor.status),
        });
    }
    if descriptor
        .idempotency_key
        .as_deref()
        .unwrap_or_default()
        .is_empty()
        || descriptor
            .reservation_expires_at
            .as_deref()
            .unwrap_or_default()
            .is_empty()
    {
        return Err(MeshDirectoryError::InvalidState {
            descriptor_key: descriptor.descriptor_key(),
            state: "reserved tenant-name requires idempotency_key and reservation_expires_at"
                .to_string(),
        });
    }

    if let Some(existing) = read_tenant_name_descriptor(storage, &descriptor.tenant_name).await? {
        if existing.tenant_id == descriptor.tenant_id
            && (existing.status == TenantNameStatus::Active
                || existing.idempotency_key == descriptor.idempotency_key)
        {
            return Ok(existing);
        }
        return Err(MeshDirectoryError::TenantNameAlreadyExists {
            tenant_name: descriptor.tenant_name.as_str().to_string(),
        });
    }

    append_control_mutation(
        storage,
        RoutingRecordFamily::TenantName,
        &descriptor.partition(),
        descriptor.tenant_name.as_str(),
        "create",
        None,
        descriptor.generation,
        descriptor.idempotency_key.as_deref(),
        descriptor,
        authority,
    )
    .await?;
    create_descriptor(storage, &descriptor.descriptor_key(), descriptor).await?;
    Ok(descriptor.clone())
}

pub async fn create_tenant_locator(
    storage: &Storage,
    locator: &TenantLocatorDescriptor,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<TenantLocatorDescriptor> {
    if let Some(existing) = read_tenant_locator_descriptor(storage, &locator.tenant_id).await? {
        if existing.tenant_id == locator.tenant_id
            && existing.tenant_name == locator.tenant_name
            && existing.home_region == locator.home_region
        {
            return Ok(existing);
        }
        return Err(MeshDirectoryError::GenerationConflict {
            descriptor_key: locator.descriptor_key(),
            expected: 0,
            actual: existing.generation,
        });
    }

    append_control_mutation(
        storage,
        RoutingRecordFamily::TenantLocator,
        &locator.partition(),
        locator.tenant_id.as_str(),
        "create",
        None,
        locator.generation,
        None,
        locator,
        authority,
    )
    .await?;
    create_descriptor(storage, &locator.descriptor_key(), locator).await?;
    Ok(locator.clone())
}

pub async fn activate_tenant_name(
    storage: &Storage,
    tenant_name: &TenantName,
    tenant_id: &TenantId,
    expected_generation: u64,
    now: impl Into<String>,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<TenantNameDescriptor> {
    let now = now.into();
    let existing = read_tenant_name_descriptor(storage, tenant_name)
        .await?
        .ok_or_else(|| MeshDirectoryError::NotFound(tenant_name.descriptor_key()))?;
    if existing.tenant_id != *tenant_id {
        return Err(MeshDirectoryError::TenantNameAlreadyExists {
            tenant_name: tenant_name.as_str().to_string(),
        });
    }
    if existing.status == TenantNameStatus::Active {
        return Ok(existing);
    }
    if existing.status != TenantNameStatus::Reserved {
        return Err(MeshDirectoryError::InvalidState {
            descriptor_key: existing.descriptor_key(),
            state: format!("{:?}", existing.status),
        });
    }
    if existing.generation != expected_generation {
        return Err(MeshDirectoryError::GenerationConflict {
            descriptor_key: existing.descriptor_key(),
            expected: expected_generation,
            actual: existing.generation,
        });
    }
    let active = existing.activate(now)?;
    append_control_mutation(
        storage,
        RoutingRecordFamily::TenantName,
        &active.partition(),
        active.tenant_name.as_str(),
        "upsert",
        Some(expected_generation),
        active.generation,
        active.idempotency_key.as_deref(),
        &active,
        authority,
    )
    .await?;
    write_descriptor(storage, &active.descriptor_key(), &active).await?;
    Ok(active)
}

pub async fn tombstone_tenant_name(
    storage: &Storage,
    tenant_name: &TenantName,
    expected_generation: u64,
    now: impl Into<String>,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<TenantNameDescriptor> {
    let existing = read_tenant_name_descriptor(storage, tenant_name)
        .await?
        .ok_or_else(|| MeshDirectoryError::NotFound(tenant_name.descriptor_key()))?;
    if existing.generation != expected_generation {
        return Err(MeshDirectoryError::GenerationConflict {
            descriptor_key: existing.descriptor_key(),
            expected: expected_generation,
            actual: existing.generation,
        });
    }
    let tombstone = existing.tombstone(now)?;
    append_control_mutation(
        storage,
        RoutingRecordFamily::TenantName,
        &tombstone.partition(),
        tombstone.tenant_name.as_str(),
        "tombstone",
        Some(expected_generation),
        tombstone.generation,
        tombstone.idempotency_key.as_deref(),
        &tombstone,
        authority,
    )
    .await?;
    write_descriptor(storage, &tombstone.descriptor_key(), &tombstone).await?;
    Ok(tombstone)
}

pub async fn recover_tenant_name_reservation(
    storage: &Storage,
    tenant_name: &TenantName,
    now: impl Into<String>,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<Option<TenantNameDescriptor>> {
    let now = now.into();
    let Some(existing) = read_tenant_name_descriptor(storage, tenant_name).await? else {
        return Ok(None);
    };
    if existing.status != TenantNameStatus::Reserved {
        return Ok(Some(existing));
    }

    if let Some(locator) = read_tenant_locator_descriptor(storage, &existing.tenant_id).await?
        && locator.tenant_id == existing.tenant_id
        && locator.tenant_name == existing.tenant_name
    {
        return activate_tenant_name(
            storage,
            tenant_name,
            &existing.tenant_id,
            existing.generation,
            now,
            authority,
        )
        .await
        .map(Some);
    }

    let expires_at = existing.reservation_expires_at.as_deref().ok_or_else(|| {
        MeshDirectoryError::InvalidState {
            descriptor_key: existing.descriptor_key(),
            state: "reserved tenant-name missing reservation_expires_at".to_string(),
        }
    })?;
    let expires_at = parse_rfc3339(expires_at, "reservation_expires_at")?;
    let now_dt = parse_rfc3339(&now, "now")?;
    if expires_at <= now_dt {
        return tombstone_tenant_name(storage, tenant_name, existing.generation, now, authority)
            .await
            .map(Some);
    }

    Ok(Some(existing))
}

pub async fn write_bucket_locator(
    storage: &Storage,
    locator: &BucketLocatorDescriptor,
    authority: MeshControlWriteAuthority<'_>,
) -> MeshDirectoryResult<()> {
    if let Some(existing) = read_bucket_locator(storage, &locator.key()).await? {
        if existing == *locator {
            return Ok(());
        }
        if existing.bucket_id != locator.bucket_id
            && existing.status != BucketLocatorStatus::Deleted
        {
            return Err(MeshDirectoryError::DuplicateBucketLocator {
                tenant_id: locator.tenant_id.to_string(),
                bucket_name: locator.bucket_name.to_string(),
            });
        }
    }
    append_control_mutation(
        storage,
        RoutingRecordFamily::BucketLocator,
        &locator.partition(),
        &format!(
            "{}/{}",
            locator.tenant_id.as_str(),
            locator.bucket_name.as_str()
        ),
        "upsert",
        locator
            .generation
            .checked_sub(1)
            .filter(|generation| *generation > 0),
        locator.generation,
        None,
        locator,
        authority,
    )
    .await?;
    write_descriptor(storage, &locator.descriptor_key(), locator).await
}

pub async fn read_tenant_name_descriptor(
    storage: &Storage,
    tenant_name: &TenantName,
) -> MeshDirectoryResult<Option<TenantNameDescriptor>> {
    read_typed_routing_descriptor(
        storage,
        RoutingRecordFamily::TenantName,
        tenant_name.as_str(),
    )
    .await
}

pub async fn read_tenant_locator_descriptor(
    storage: &Storage,
    tenant_id: &TenantId,
) -> MeshDirectoryResult<Option<TenantLocatorDescriptor>> {
    read_typed_routing_descriptor(
        storage,
        RoutingRecordFamily::TenantLocator,
        tenant_id.as_str(),
    )
    .await
}

pub async fn read_bucket_locator(
    storage: &Storage,
    key: &BucketLocatorKey,
) -> MeshDirectoryResult<Option<BucketLocatorDescriptor>> {
    let record_key = format!("{}/{}", key.tenant_id.as_str(), key.bucket_name.as_str());
    read_typed_routing_descriptor(storage, RoutingRecordFamily::BucketLocator, &record_key).await
}

pub async fn list_routing_records(
    storage: &Storage,
    family_filter: Option<RoutingRecordFamily>,
) -> MeshDirectoryResult<Vec<RoutingRecordDescriptor>> {
    let mut records = BTreeMap::new();
    let families: Vec<_> = family_filter
        .map(|family| vec![family])
        .unwrap_or_else(|| RoutingRecordFamily::all().into_iter().collect());

    for family in families {
        for record in list_projected_routing_records(storage, family).await? {
            records.insert((record.family, record.record_key.clone()), record);
        }
        overlay_control_stream_routing_records(storage, family, &mut records).await?;
    }

    Ok(records.into_values().collect())
}

pub async fn list_projected_routing_records(
    storage: &Storage,
    family: RoutingRecordFamily,
) -> MeshDirectoryResult<Vec<RoutingRecordDescriptor>> {
    let mut records = Vec::new();
    {
        let family_root = storage
            .mesh_directory_root_path()
            .join(family.directory_segment());
        let mut files = json_files_under(&family_root).await?;
        files.sort();
        for path in files {
            let payload_json = tokio::fs::read_to_string(&path).await?;
            let payload: serde_json::Value = serde_json::from_str(&payload_json)?;
            let relative = path
                .strip_prefix(storage.mesh_directory_root_path())
                .map_err(|_| MeshDirectoryError::InvalidIdentifier {
                    field: "routing record path",
                    value: path.display().to_string(),
                })?;
            let descriptor_key =
                relative
                    .iter()
                    .fold(String::from(MESH_DIRECTORY_ROOT), |mut out, segment| {
                        out.push('/');
                        out.push_str(&segment.to_string_lossy());
                        out
                    });
            records.push(RoutingRecordDescriptor {
                family,
                record_key: routing_record_key(family, relative)?,
                partition: routing_record_partition(relative)?,
                descriptor_key,
                generation: payload
                    .get("generation")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0),
                payload_json,
            });
        }
    }
    records.sort_by(|left, right| {
        (left.family, left.record_key.as_str()).cmp(&(right.family, right.record_key.as_str()))
    });
    Ok(records)
}

async fn overlay_control_stream_routing_records(
    storage: &Storage,
    family: RoutingRecordFamily,
    records: &mut BTreeMap<(RoutingRecordFamily, String), RoutingRecordDescriptor>,
) -> MeshDirectoryResult<()> {
    let stream_family = family.stream_family();
    let family_path = storage
        .mesh_control_stream_family_path(stream_family)
        .map_err(|err| MeshDirectoryError::ControlStreamWrite {
            stream_family: stream_family.to_string(),
            partition: String::new(),
            message: err.to_string(),
        })?;
    let mut entries = match tokio::fs::read_dir(&family_path).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("anlog") {
            continue;
        }
        let partition = path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| MeshDirectoryError::ControlStreamWrite {
                stream_family: stream_family.to_string(),
                partition: String::new(),
                message: format!("invalid control stream file name {}", path.display()),
            })?
            .to_string();
        let log = mesh_control_stream::read_control_stream_log(&path)
            .await
            .map_err(|err| MeshDirectoryError::ControlStreamWrite {
                stream_family: stream_family.to_string(),
                partition: partition.clone(),
                message: err.to_string(),
            })?;
        if log.partial_final_frame.is_some() {
            return Err(MeshDirectoryError::ControlStreamWrite {
                stream_family: stream_family.to_string(),
                partition,
                message: "control stream has a partial final frame".to_string(),
            });
        }
        for record in log.records {
            let header: serde_json::Value = serde_json::from_slice(&record.frame.header_json)
                .map_err(|err| MeshDirectoryError::ControlStreamWrite {
                    stream_family: stream_family.to_string(),
                    partition: partition.clone(),
                    message: err.to_string(),
                })?;
            if header
                .get("stream_family")
                .and_then(serde_json::Value::as_str)
                != Some(stream_family)
                || header.get("partition").and_then(serde_json::Value::as_str)
                    != Some(partition.as_str())
            {
                return Err(MeshDirectoryError::ControlStreamWrite {
                    stream_family: stream_family.to_string(),
                    partition: partition.clone(),
                    message: "control stream header scope does not match path".to_string(),
                });
            }
            let record_key = header
                .get("record_key")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| MeshDirectoryError::ControlStreamWrite {
                    stream_family: stream_family.to_string(),
                    partition: partition.clone(),
                    message: "control stream header missing record_key".to_string(),
                })?;
            let operation = header
                .get("operation")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            if matches!(operation, "delete" | "deleted") {
                records.remove(&(family, record_key.to_string()));
                continue;
            }
            let descriptor = routing_record_descriptor_from_payload(
                family,
                record_key,
                record.frame.payload_json,
            )?;
            records.insert((family, record_key.to_string()), descriptor);
        }
    }
    Ok(())
}

pub fn routing_record_partition_for_key(
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<String> {
    match family {
        RoutingRecordFamily::TenantName => Ok(TenantName::canonicalize(record_key)?.partition()),
        RoutingRecordFamily::TenantLocator => Ok(TenantId::new(record_key)?.partition()),
        RoutingRecordFamily::BucketLocator => {
            let (tenant_id, bucket_name) = bucket_record_key(record_key)?;
            Ok(BucketLocatorKey::new(tenant_id, bucket_name).partition())
        }
        RoutingRecordFamily::HostAlias => host_alias_partition(record_key),
    }
}

pub fn routing_record_descriptor_key_for_key(
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<String> {
    match family {
        RoutingRecordFamily::TenantName => {
            Ok(TenantName::canonicalize(record_key)?.descriptor_key())
        }
        RoutingRecordFamily::TenantLocator => Ok(TenantId::new(record_key)?.descriptor_key()),
        RoutingRecordFamily::BucketLocator => {
            let (tenant_id, bucket_name) = bucket_record_key(record_key)?;
            Ok(BucketLocatorKey::new(tenant_id, bucket_name).descriptor_key())
        }
        RoutingRecordFamily::HostAlias => host_alias_descriptor_key(record_key),
    }
}

pub async fn read_routing_record_descriptor(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<RoutingRecordDescriptor> {
    read_routing_record_from_source_of_truth(storage, family, record_key)
        .await?
        .ok_or_else(|| MeshDirectoryError::NotFound(record_key.to_string()))
}

async fn read_routing_record_from_source_of_truth(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Option<RoutingRecordDescriptor>> {
    let projected = read_projected_routing_record_descriptor(storage, family, record_key).await?;
    let streamed = latest_routing_record_from_control_stream(storage, family, record_key).await?;
    let Some(streamed) = streamed else {
        return Ok(projected);
    };
    if projected.as_ref().is_none_or(|projected| {
        projected.generation != streamed.generation
            || serde_json::from_str::<serde_json::Value>(&projected.payload_json).ok()
                != serde_json::from_str::<serde_json::Value>(&streamed.payload_json).ok()
    }) {
        rebuild_routing_record_projection_from_payload(
            storage,
            family,
            record_key,
            streamed.payload_json.as_bytes(),
        )
        .await?;
    }
    Ok(Some(streamed))
}

async fn latest_routing_record_from_control_stream(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Option<RoutingRecordDescriptor>> {
    let partition = routing_record_partition_for_key(family, record_key)?;
    let stream_family = family.stream_family();
    let stream_path = storage
        .mesh_control_stream_path(stream_family, &partition)
        .map_err(|err| MeshDirectoryError::ControlStreamWrite {
            stream_family: stream_family.to_string(),
            partition: partition.clone(),
            message: err.to_string(),
        })?;
    let Some(record) = mesh_control_stream::latest_projected_record_from_control_stream(
        stream_path,
        stream_family,
        &partition,
        record_key,
    )
    .await
    .map_err(|err| MeshDirectoryError::ControlStreamWrite {
        stream_family: stream_family.to_string(),
        partition,
        message: err.to_string(),
    })?
    else {
        return Ok(None);
    };
    Ok(Some(routing_record_descriptor_from_payload(
        family,
        &record.record_key,
        record.payload_json,
    )?))
}

async fn read_projected_routing_record_descriptor(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Option<RoutingRecordDescriptor>> {
    let descriptor_key = routing_record_descriptor_key_for_key(family, record_key)?;
    let path = descriptor_path(storage, &descriptor_key)?;
    match tokio::fs::read(&path).await {
        Ok(payload_json) => Ok(Some(routing_record_descriptor_from_payload(
            family,
            record_key,
            payload_json,
        )?)),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

async fn read_typed_routing_descriptor<T: for<'de> Deserialize<'de>>(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
) -> MeshDirectoryResult<Option<T>> {
    let Some(record) =
        read_routing_record_from_source_of_truth(storage, family, record_key).await?
    else {
        return Ok(None);
    };
    Ok(Some(serde_json::from_str(&record.payload_json)?))
}

fn routing_record_descriptor_from_payload(
    family: RoutingRecordFamily,
    record_key: &str,
    payload_json: Vec<u8>,
) -> MeshDirectoryResult<RoutingRecordDescriptor> {
    let payload: serde_json::Value = serde_json::from_slice(&payload_json)?;
    Ok(RoutingRecordDescriptor {
        family,
        record_key: record_key.to_string(),
        partition: routing_record_partition_for_key(family, record_key)?,
        descriptor_key: routing_record_descriptor_key_for_key(family, record_key)?,
        generation: payload
            .get("generation")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        payload_json: String::from_utf8(payload_json).map_err(|err| {
            MeshDirectoryError::InvalidIdentifier {
                field: "routing record payload",
                value: err.to_string(),
            }
        })?,
    })
}

pub async fn rebuild_routing_record_projection_from_payload(
    storage: &Storage,
    family: RoutingRecordFamily,
    record_key: &str,
    payload_json: &[u8],
) -> MeshDirectoryResult<RoutingRecordDescriptor> {
    let expected_descriptor_key = routing_record_descriptor_key_for_key(family, record_key)?;
    match family {
        RoutingRecordFamily::TenantName => {
            let descriptor: TenantNameDescriptor = serde_json::from_slice(payload_json)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor(storage, &expected_descriptor_key, &descriptor).await?;
        }
        RoutingRecordFamily::TenantLocator => {
            let descriptor: TenantLocatorDescriptor = serde_json::from_slice(payload_json)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor(storage, &expected_descriptor_key, &descriptor).await?;
        }
        RoutingRecordFamily::BucketLocator => {
            let descriptor: BucketLocatorDescriptor = serde_json::from_slice(payload_json)?;
            ensure_descriptor_key_matches(&descriptor.descriptor_key(), &expected_descriptor_key)?;
            write_descriptor(storage, &expected_descriptor_key, &descriptor).await?;
        }
        RoutingRecordFamily::HostAlias => {
            let descriptor: routing::HostAliasDescriptor = serde_json::from_slice(payload_json)?;
            ensure_descriptor_key_matches(
                &host_alias_descriptor_key(&descriptor.hostname)?,
                &expected_descriptor_key,
            )?;
            write_descriptor(storage, &expected_descriptor_key, &descriptor).await?;
        }
    }
    routing_record_descriptor_from_payload(family, record_key, payload_json.to_vec())
}

async fn write_descriptor<T: Serialize>(
    storage: &Storage,
    descriptor_key: &str,
    descriptor: &T,
) -> MeshDirectoryResult<()> {
    let path = descriptor_path(storage, descriptor_key)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp_path = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::File::create(&tmp_path).await?;
    let bytes = serde_json::to_vec_pretty(descriptor)?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(tmp_path, path).await?;
    Ok(())
}

fn bucket_record_key(record_key: &str) -> MeshDirectoryResult<(TenantId, BucketName)> {
    let (tenant_id, bucket_name) =
        record_key
            .split_once('/')
            .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
                field: "bucket routing record key",
                value: record_key.to_string(),
            })?;
    Ok((
        TenantId::new(tenant_id)?,
        BucketName::canonicalize(bucket_name)?,
    ))
}

fn ensure_descriptor_key_matches(actual: &str, expected: &str) -> MeshDirectoryResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(MeshDirectoryError::InvalidIdentifier {
            field: "routing record payload descriptor key",
            value: format!("expected {expected}, got {actual}"),
        })
    }
}

async fn create_descriptor<T: Serialize>(
    storage: &Storage,
    descriptor_key: &str,
    descriptor: &T,
) -> MeshDirectoryResult<()> {
    let path = descriptor_path(storage, descriptor_key)?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    let bytes = serde_json::to_vec_pretty(descriptor)?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    Ok(())
}

fn descriptor_path(storage: &Storage, descriptor_key: &str) -> MeshDirectoryResult<PathBuf> {
    let relative = descriptor_key
        .strip_prefix(MESH_DIRECTORY_ROOT)
        .and_then(|value| value.strip_prefix('/'))
        .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
            field: "descriptor key",
            value: descriptor_key.to_string(),
        })?;
    if relative
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "descriptor key",
            value: descriptor_key.to_string(),
        });
    }
    Ok(relative
        .split('/')
        .fold(storage.mesh_directory_root_path(), |path, segment| {
            path.join(Path::new(segment))
        }))
}

async fn json_files_under(root: &Path) -> MeshDirectoryResult<Vec<PathBuf>> {
    match tokio::fs::metadata(root).await {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            return Err(MeshDirectoryError::InvalidIdentifier {
                field: "routing record directory",
                value: root.display().to_string(),
            });
        }
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(err.into()),
    }

    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let metadata = entry.metadata().await?;
            if metadata.is_dir() {
                stack.push(path);
            } else if metadata.is_file()
                && path
                    .extension()
                    .is_some_and(|extension| extension == "json")
            {
                out.push(path);
            }
        }
    }
    Ok(out)
}

fn routing_record_partition(relative: &Path) -> MeshDirectoryResult<String> {
    relative
        .components()
        .nth(1)
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .filter(|partition| partition.len() == 4)
        .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
            field: "routing record partition",
            value: relative.display().to_string(),
        })
}

fn routing_record_key(family: RoutingRecordFamily, relative: &Path) -> MeshDirectoryResult<String> {
    let segments = relative
        .iter()
        .map(|segment| segment.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    match family {
        RoutingRecordFamily::TenantName
        | RoutingRecordFamily::TenantLocator
        | RoutingRecordFamily::HostAlias => segments
            .get(2)
            .and_then(|file| file.strip_suffix(".json"))
            .map(str::to_string)
            .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
                field: "routing record key",
                value: relative.display().to_string(),
            }),
        RoutingRecordFamily::BucketLocator => {
            let tenant_id = segments.get(2);
            let bucket_file = segments.get(3);
            match (
                tenant_id,
                bucket_file.and_then(|file| file.strip_suffix(".json")),
            ) {
                (Some(tenant_id), Some(bucket_name)) => Ok(format!("{tenant_id}/{bucket_name}")),
                _ => Err(MeshDirectoryError::InvalidIdentifier {
                    field: "routing record key",
                    value: relative.display().to_string(),
                }),
            }
        }
    }
}

fn partition_key_bytes(domain: &str, components: &[&str]) -> Vec<u8> {
    let mut key = domain.as_bytes().to_vec();
    for component in components {
        key.push(0);
        key.extend_from_slice(component.as_bytes());
    }
    key
}

fn join_mesh_key(segments: &[&str]) -> String {
    let mut out = String::from(MESH_DIRECTORY_ROOT);
    for segment in segments {
        out.push('/');
        out.push_str(segment);
    }
    out
}

fn validate_dns_label_name(value: &str) -> Result<(), ()> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return Err(());
    }
    if !bytes[0].is_ascii_lowercase() {
        return Err(());
    }
    if !bytes[bytes.len() - 1].is_ascii_lowercase() && !bytes[bytes.len() - 1].is_ascii_digit() {
        return Err(());
    }
    if bytes
        .iter()
        .any(|byte| !byte.is_ascii_lowercase() && !byte.is_ascii_digit() && *byte != b'-')
    {
        return Err(());
    }
    Ok(())
}

fn require_safe_component(value: &str, field: &'static str) -> MeshDirectoryResult<()> {
    require_nonempty(value, field)?;
    if value.len() > 128
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && byte != b'_' && byte != b'-')
    {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn require_control_path_fragment(value: &str, field: &'static str) -> MeshDirectoryResult<()> {
    require_nonempty(value, field)?;
    if value.starts_with('/')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
        || value
            .split('/')
            .any(|segment| segment == "." || segment == "..")
    {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> MeshDirectoryResult<()> {
    if value.is_empty() {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field,
            value: value.to_string(),
        });
    }
    Ok(())
}

fn parse_rfc3339(value: &str, field: &'static str) -> MeshDirectoryResult<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| MeshDirectoryError::InvalidTimestamp {
            field,
            value: value.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use crate::storage::Storage;
    use tempfile::tempdir;

    const NOW: &str = "2026-07-02T00:00:00Z";
    const TEST_SIGNING_KEY: &[u8] = b"mesh-directory-control-stream-test-key";

    async fn mesh_permit(
        storage: &Storage,
        family: RoutingRecordFamily,
        partition: &str,
    ) -> PartitionWritePermit {
        let partition_id = control_partition_id(family.stream_family(), partition);
        let recovering = acquire_partition_recovery(
            storage,
            PartitionRecoveryAcquire {
                partition_family: CONTROL_PARTITION_FAMILY.to_string(),
                partition_id: partition_id.clone(),
                owner_node_id: "node-test".to_string(),
                recovered_through_sequence: 0,
                recovered_manifest_hash: hex::encode([0; 32]),
                now_nanos: Utc::now().timestamp_nanos_opt().unwrap(),
            },
            TEST_SIGNING_KEY,
        )
        .await
        .unwrap();
        let ready = publish_partition_ready(
            storage,
            CONTROL_PARTITION_FAMILY,
            &partition_id,
            "node-test",
            recovering.fence_token,
            0,
            &hex::encode([0; 32]),
            Utc::now().timestamp_nanos_opt().unwrap(),
            TEST_SIGNING_KEY,
        )
        .await
        .unwrap();
        ready.write_permit().unwrap()
    }

    fn authority(permit: &PartitionWritePermit) -> MeshControlWriteAuthority<'_> {
        MeshControlWriteAuthority {
            permit,
            signing_key: TEST_SIGNING_KEY,
        }
    }

    #[test]
    fn tenant_name_partition_path_is_stable() {
        let tenant_name = TenantName::canonicalize("Acme").unwrap();

        assert_eq!(tenant_name.as_str(), "acme");
        assert_eq!(tenant_name.partition_key().as_slice(), b"tenant-name\0acme");
        assert_eq!(tenant_name.partition(), "c1ae");
        assert_eq!(
            tenant_name.descriptor_key(),
            "_anvil/control/v1/mesh/tenant-names/c1ae/acme.json"
        );
    }

    #[test]
    fn bucket_locator_partition_path_is_stable() {
        let key = BucketLocatorKey::new(
            TenantId::new("tenant_acme").unwrap(),
            BucketName::canonicalize("releases").unwrap(),
        );

        assert_eq!(
            key.partition_key().as_slice(),
            b"bucket-locator\0tenant_acme\0releases"
        );
        assert_eq!(key.partition(), "b41d");
        assert_eq!(
            key.descriptor_key(),
            "_anvil/control/v1/mesh/buckets/b41d/tenant_acme/releases.json"
        );
    }

    #[test]
    fn duplicate_bucket_names_are_allowed_for_different_tenant_ids() {
        let mut directory = BucketLocatorDirectory::default();

        directory
            .insert(locator("tenant_acme", "bucket_01HYA"))
            .unwrap();
        directory
            .insert(locator("tenant_beta", "bucket_01HYB"))
            .unwrap();

        assert_eq!(directory.len(), 2);
        assert_ne!(
            BucketLocatorKey::new(
                TenantId::new("tenant_acme").unwrap(),
                BucketName::canonicalize("releases").unwrap(),
            )
            .descriptor_key(),
            BucketLocatorKey::new(
                TenantId::new("tenant_beta").unwrap(),
                BucketName::canonicalize("releases").unwrap(),
            )
            .descriptor_key()
        );
    }

    #[test]
    fn duplicate_bucket_names_in_same_tenant_are_rejected_at_locator_layer() {
        let mut directory = BucketLocatorDirectory::default();

        directory
            .insert(locator("tenant_acme", "bucket_01HYA"))
            .unwrap();
        let err = directory
            .insert(locator("tenant_acme", "bucket_01HYZ"))
            .unwrap_err();

        assert_eq!(
            err,
            MeshDirectoryError::DuplicateBucketLocator {
                tenant_id: "tenant_acme".to_string(),
                bucket_name: "releases".to_string(),
            }
        );
        assert_eq!(directory.len(), 1);
    }

    #[test]
    fn tenant_name_canonicalization_rejects_dotted_names() {
        assert!(matches!(
            TenantName::canonicalize("acme.prod"),
            Err(MeshDirectoryError::InvalidTenantName(_))
        ));
        assert!(matches!(
            TenantName::canonicalize("prod.acme."),
            Err(MeshDirectoryError::InvalidTenantName(_))
        ));
    }

    #[tokio::test]
    async fn tenant_name_reservation_is_create_once_and_promoted_by_generation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let tenant_name = TenantName::canonicalize("Acme").unwrap();
        let tenant_id = TenantId::new("tenant_01").unwrap();
        let reserved = TenantNameDescriptor::reserved(
            MeshId::new("mesh_01").unwrap(),
            tenant_name.clone(),
            tenant_id.clone(),
            "req-1",
            "2026-07-02T00:05:00Z",
            NOW,
        )
        .unwrap();

        let name_permit = mesh_permit(
            &storage,
            RoutingRecordFamily::TenantName,
            &reserved.partition(),
        )
        .await;
        let name_authority = authority(&name_permit);

        let written = reserve_tenant_name(&storage, &reserved, name_authority)
            .await
            .unwrap();
        assert_eq!(written.status, TenantNameStatus::Reserved);
        assert_eq!(written.generation, 1);

        let retry = reserve_tenant_name(&storage, &reserved, name_authority)
            .await
            .unwrap();
        assert_eq!(retry, written);

        let active =
            activate_tenant_name(&storage, &tenant_name, &tenant_id, 1, NOW, name_authority)
                .await
                .unwrap();
        assert_eq!(active.status, TenantNameStatus::Active);
        assert_eq!(active.generation, 2);
        assert_eq!(active.idempotency_key.as_deref(), Some("req-1"));
        assert_eq!(active.reservation_expires_at, None);

        let active_retry = reserve_tenant_name(&storage, &reserved, name_authority)
            .await
            .unwrap();
        assert_eq!(active_retry.status, TenantNameStatus::Active);
        assert_eq!(active_retry.generation, 2);

        let stream_path = storage
            .mesh_control_stream_path(
                RoutingRecordFamily::TenantName.stream_family(),
                &reserved.partition(),
            )
            .unwrap();
        let stream = mesh_control_stream::read_control_stream_log(stream_path)
            .await
            .unwrap();
        assert_eq!(stream.records.len(), 2);
        let first_header: serde_json::Value =
            serde_json::from_slice(&stream.records[0].frame.header_json).unwrap();
        let second_header: serde_json::Value =
            serde_json::from_slice(&stream.records[1].frame.header_json).unwrap();
        assert_eq!(first_header["operation"], "create");
        assert_eq!(first_header["sequence"], 1);
        assert_eq!(first_header["writer_node_id"], "node-test");
        assert_eq!(first_header["writer_fence"], name_permit.fence_token);
        assert_eq!(second_header["operation"], "upsert");
        assert_eq!(second_header["sequence"], 2);
    }

    #[tokio::test]
    async fn routing_reads_and_lists_use_control_stream_when_projection_is_stale_or_missing() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let tenant_name = TenantName::canonicalize("Acme").unwrap();
        let tenant_id = TenantId::new("tenant_01").unwrap();
        let reserved = TenantNameDescriptor::reserved(
            MeshId::new("mesh_01").unwrap(),
            tenant_name.clone(),
            tenant_id.clone(),
            "req-1",
            "2026-07-02T00:05:00Z",
            NOW,
        )
        .unwrap();
        let name_permit = mesh_permit(
            &storage,
            RoutingRecordFamily::TenantName,
            &reserved.partition(),
        )
        .await;
        let name_authority = authority(&name_permit);
        reserve_tenant_name(&storage, &reserved, name_authority)
            .await
            .unwrap();
        let active =
            activate_tenant_name(&storage, &tenant_name, &tenant_id, 1, NOW, name_authority)
                .await
                .unwrap();
        let path = descriptor_path(&storage, &active.descriptor_key()).unwrap();
        let mut stale_projection: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        stale_projection["tenant_id"] = serde_json::json!("tenant_wrong");
        stale_projection["generation"] = serde_json::json!(99);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&stale_projection).unwrap())
            .await
            .unwrap();

        let read = read_tenant_name_descriptor(&storage, &tenant_name)
            .await
            .unwrap()
            .expect("tenant-name from stream");
        assert_eq!(read.tenant_id.as_str(), "tenant_01");
        assert_eq!(read.generation, 2);
        let repaired_projection: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        assert_eq!(repaired_projection["tenant_id"], "tenant_01");
        assert_eq!(repaired_projection["generation"], 2);

        tokio::fs::remove_file(&path).await.unwrap();
        let recovered = read_tenant_name_descriptor(&storage, &tenant_name)
            .await
            .unwrap()
            .expect("tenant-name rebuilt from stream");
        assert_eq!(recovered.tenant_id.as_str(), "tenant_01");
        assert!(path.exists());

        let listed = list_routing_records(&storage, Some(RoutingRecordFamily::TenantName))
            .await
            .unwrap();
        let listed_acme = listed
            .iter()
            .find(|record| record.record_key == "acme")
            .expect("acme listed from stream");
        let listed_payload: serde_json::Value =
            serde_json::from_str(&listed_acme.payload_json).unwrap();
        assert_eq!(listed_payload["tenant_id"], "tenant_01");
        assert_eq!(listed_acme.generation, 2);
    }

    #[tokio::test]
    async fn tenant_name_reservation_rejects_competing_tenant_ids_and_stale_generations() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let tenant_name = TenantName::canonicalize("Acme").unwrap();
        let tenant_id = TenantId::new("tenant_01").unwrap();
        let reserved = TenantNameDescriptor::reserved(
            MeshId::new("mesh_01").unwrap(),
            tenant_name.clone(),
            tenant_id.clone(),
            "req-1",
            "2026-07-02T00:05:00Z",
            NOW,
        )
        .unwrap();
        let name_permit = mesh_permit(
            &storage,
            RoutingRecordFamily::TenantName,
            &reserved.partition(),
        )
        .await;
        let name_authority = authority(&name_permit);
        reserve_tenant_name(&storage, &reserved, name_authority)
            .await
            .unwrap();

        let competing = TenantNameDescriptor::reserved(
            MeshId::new("mesh_01").unwrap(),
            tenant_name.clone(),
            TenantId::new("tenant_02").unwrap(),
            "req-2",
            "2026-07-02T00:05:00Z",
            NOW,
        )
        .unwrap();
        assert!(matches!(
            reserve_tenant_name(&storage, &competing, name_authority).await,
            Err(MeshDirectoryError::TenantNameAlreadyExists { tenant_name })
                if tenant_name == "acme"
        ));

        assert!(matches!(
            activate_tenant_name(&storage, &tenant_name, &tenant_id, 99, NOW, name_authority).await,
            Err(MeshDirectoryError::GenerationConflict {
                expected: 99,
                actual: 1,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn tenant_name_recovery_completes_reserved_name_when_locator_exists() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let mesh_id = MeshId::new("mesh_01").unwrap();
        let tenant_name = TenantName::canonicalize("Acme").unwrap();
        let tenant_id = TenantId::new("tenant_01").unwrap();
        let reserved = TenantNameDescriptor::reserved(
            mesh_id.clone(),
            tenant_name.clone(),
            tenant_id.clone(),
            "req-1",
            "2026-07-02T00:05:00Z",
            NOW,
        )
        .unwrap();
        let name_permit = mesh_permit(
            &storage,
            RoutingRecordFamily::TenantName,
            &reserved.partition(),
        )
        .await;
        let name_authority = authority(&name_permit);
        reserve_tenant_name(&storage, &reserved, name_authority)
            .await
            .unwrap();
        let locator_descriptor = TenantLocatorDescriptor::active(
            mesh_id,
            tenant_id,
            tenant_name.clone(),
            RegionName::new("eu-west-1").unwrap(),
            NOW,
        )
        .unwrap();
        let locator_permit = mesh_permit(
            &storage,
            RoutingRecordFamily::TenantLocator,
            &locator_descriptor.partition(),
        )
        .await;
        create_tenant_locator(&storage, &locator_descriptor, authority(&locator_permit))
            .await
            .unwrap();

        let recovered = recover_tenant_name_reservation(
            &storage,
            &tenant_name,
            "2026-07-02T00:01:00Z",
            name_authority,
        )
        .await
        .unwrap()
        .expect("recovered tenant-name");

        assert_eq!(recovered.status, TenantNameStatus::Active);
        assert_eq!(recovered.generation, 2);
    }

    #[tokio::test]
    async fn tenant_name_recovery_tombstones_expired_reserved_name_without_locator() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let tenant_name = TenantName::canonicalize("Acme").unwrap();
        let reserved = TenantNameDescriptor::reserved(
            MeshId::new("mesh_01").unwrap(),
            tenant_name.clone(),
            TenantId::new("tenant_01").unwrap(),
            "req-1",
            "2026-07-02T00:05:00Z",
            NOW,
        )
        .unwrap();
        let name_permit = mesh_permit(
            &storage,
            RoutingRecordFamily::TenantName,
            &reserved.partition(),
        )
        .await;
        let name_authority = authority(&name_permit);
        reserve_tenant_name(&storage, &reserved, name_authority)
            .await
            .unwrap();

        let recovered = recover_tenant_name_reservation(
            &storage,
            &tenant_name,
            "2026-07-02T00:06:00Z",
            name_authority,
        )
        .await
        .unwrap()
        .expect("recovered tenant-name");

        assert_eq!(recovered.status, TenantNameStatus::Tombstoned);
        assert_eq!(recovered.generation, 2);

        let listed = list_routing_records(&storage, Some(RoutingRecordFamily::TenantName))
            .await
            .unwrap();
        assert!(listed.iter().any(|record| {
            record.record_key == tenant_name.as_str()
                && record.payload_json.contains("\"status\":\"tombstoned\"")
        }));
    }

    fn locator(tenant_id: &str, bucket_id: &str) -> BucketLocatorDescriptor {
        let tenant_id = TenantId::new(tenant_id).unwrap();
        BucketLocatorDescriptor::active(
            MeshId::new("mesh_01").unwrap(),
            tenant_id.clone(),
            BucketName::canonicalize("releases").unwrap(),
            BucketId::new(bucket_id).unwrap(),
            RegionName::new("eu-west-1").unwrap(),
            CellId::new("cell_a").unwrap(),
            "regional-primary",
            format!("objects/{tenant_id}/releases/"),
            NOW,
        )
        .unwrap()
    }
}
