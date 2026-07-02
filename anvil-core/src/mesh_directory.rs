use crate::validation;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, btree_map::Entry};
use std::fmt;
use thiserror::Error;

pub const MESH_DIRECTORY_ROOT: &str = "_anvil/control/v1/mesh";
pub const TENANT_NAME_SCHEMA: &str = "anvil.mesh.tenant_name.v1";
pub const TENANT_LOCATOR_SCHEMA: &str = "anvil.mesh.tenant_locator.v1";
pub const BUCKET_LOCATOR_SCHEMA: &str = "anvil.mesh.bucket_locator.v1";

const TENANT_NAME_PARTITION_DOMAIN: &str = "tenant-name";
const TENANT_LOCATOR_PARTITION_DOMAIN: &str = "tenant-locator";
const BUCKET_LOCATOR_PARTITION_DOMAIN: &str = "bucket-locator";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
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
}

pub type MeshDirectoryResult<T> = Result<T, MeshDirectoryError>;

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

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: &str = "2026-07-02T00:00:00Z";

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
