use super::*;
use crate::core_store::{
    core_meta_committed_row_common, core_meta_root_key_hash, decode_deterministic_proto,
    encode_deterministic_proto,
};
use prost::Message;
use serde::Serialize;

pub(super) const DESCRIPTOR_FILE_EXTENSION: &str = ".pb";
pub(super) const ROUTING_PROJECTION_ROW_SCHEMA: &str =
    "anvil.coremeta.mesh_directory_projection.v1";
pub(super) const ROUTING_PROJECTION_ROW_PREFIX: &str = "mesh-directory-projection";

pub(super) trait StoredRoutingRecord: Serialize {
    fn routing_family(&self) -> RoutingRecordFamily;
    fn routing_record_key(&self) -> String;
    fn routing_descriptor_key(&self) -> String;
    fn routing_generation(&self) -> u64;
    fn routing_mesh_id(&self) -> String;
    fn encode_routing_payload_proto(&self) -> MeshDirectoryResult<Vec<u8>>;

    fn operator_payload_json(&self) -> MeshDirectoryResult<String> {
        serde_json::to_string(self).map_err(MeshDirectoryError::Json)
    }
}

pub(super) trait DecodeRoutingRecord: Sized + Serialize {
    fn decode_routing_payload_proto(bytes: &[u8]) -> MeshDirectoryResult<Self>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RoutingProjectionRow {
    pub descriptor: RoutingRecordDescriptor,
    pub payload_proto: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct RoutingProjectionRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    family: String,
    #[prost(string, tag = "4")]
    record_key: String,
    #[prost(string, tag = "5")]
    partition: String,
    #[prost(string, tag = "6")]
    descriptor_key: String,
    #[prost(uint64, tag = "7")]
    generation: u64,
    #[prost(bytes, tag = "8")]
    descriptor_payload_proto: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct TenantNameDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(string, tag = "3")]
    tenant_name: String,
    #[prost(string, tag = "4")]
    tenant_id: String,
    #[prost(uint32, tag = "5")]
    status: u32,
    #[prost(string, optional, tag = "6")]
    idempotency_key: Option<String>,
    #[prost(string, optional, tag = "7")]
    reservation_expires_at: Option<String>,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(string, tag = "9")]
    updated_at: String,
    #[prost(uint64, tag = "10")]
    generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct TenantLocatorDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(string, tag = "3")]
    tenant_id: String,
    #[prost(string, tag = "4")]
    tenant_name: String,
    #[prost(string, tag = "5")]
    home_region: String,
    #[prost(uint32, tag = "6")]
    status: u32,
    #[prost(uint64, tag = "7")]
    profile_revision: u64,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(string, tag = "9")]
    updated_at: String,
    #[prost(uint64, tag = "10")]
    generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct BucketLocatorDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    mesh_id: String,
    #[prost(string, tag = "3")]
    tenant_id: String,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(string, tag = "5")]
    bucket_id: String,
    #[prost(string, tag = "6")]
    home_region: String,
    #[prost(string, tag = "7")]
    home_cell: String,
    #[prost(uint32, tag = "8")]
    status: u32,
    #[prost(string, tag = "9")]
    placement_policy: String,
    #[prost(string, tag = "10")]
    object_prefix: String,
    #[prost(string, tag = "11")]
    created_at: String,
    #[prost(string, tag = "12")]
    updated_at: String,
    #[prost(uint64, tag = "13")]
    generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct HostAliasDescriptorProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    hostname: String,
    #[prost(string, tag = "3")]
    tenant_id: String,
    #[prost(string, tag = "4")]
    bucket_name: String,
    #[prost(string, tag = "5")]
    region: String,
    #[prost(string, tag = "6")]
    prefix: String,
    #[prost(uint32, tag = "7")]
    state: u32,
    #[prost(string, tag = "8")]
    created_at: String,
    #[prost(string, tag = "9")]
    updated_at: String,
    #[prost(uint64, tag = "10")]
    generation: u64,
}

pub(super) fn routing_record_descriptor_from_record<T>(
    record: &T,
) -> MeshDirectoryResult<RoutingRecordDescriptor>
where
    T: StoredRoutingRecord,
{
    Ok(RoutingRecordDescriptor {
        family: record.routing_family(),
        record_key: record.routing_record_key(),
        partition: routing_record_partition_for_key(
            record.routing_family(),
            &record.routing_record_key(),
        )?,
        descriptor_key: record.routing_descriptor_key(),
        generation: record.routing_generation(),
        payload_json: record.operator_payload_json()?,
    })
}

pub(super) fn routing_record_descriptor_from_proto(
    family: RoutingRecordFamily,
    expected_record_key: &str,
    payload_proto: &[u8],
) -> MeshDirectoryResult<RoutingRecordDescriptor> {
    match family {
        RoutingRecordFamily::TenantName => descriptor_from_decoded(
            expected_record_key,
            &TenantNameDescriptor::decode_routing_payload_proto(payload_proto)?,
        ),
        RoutingRecordFamily::TenantLocator => descriptor_from_decoded(
            expected_record_key,
            &TenantLocatorDescriptor::decode_routing_payload_proto(payload_proto)?,
        ),
        RoutingRecordFamily::BucketLocator => descriptor_from_decoded(
            expected_record_key,
            &BucketLocatorDescriptor::decode_routing_payload_proto(payload_proto)?,
        ),
        RoutingRecordFamily::HostAlias => descriptor_from_decoded(
            expected_record_key,
            &routing::HostAliasDescriptor::decode_routing_payload_proto(payload_proto)?,
        ),
    }
}

pub(super) fn encode_routing_projection_row<T>(
    descriptor_key: &str,
    record: &T,
) -> MeshDirectoryResult<Vec<u8>>
where
    T: StoredRoutingRecord,
{
    let payload_proto = record.encode_routing_payload_proto()?;
    let descriptor = routing_record_descriptor_from_record(record)?;
    ensure_descriptor_key_matches(&descriptor.descriptor_key, descriptor_key)?;
    let row = RoutingProjectionRowProto {
        common: Some(core_meta_committed_row_common(
            "mesh",
            core_meta_root_key_hash(&format!(
                "mesh/directory/{}/{}",
                record.routing_family().stream_family(),
                record.routing_record_key()
            )),
            record.routing_generation(),
            format!(
                "mesh-directory:{}:{}",
                record.routing_family().stream_family(),
                record.routing_generation()
            ),
            0,
        )),
        schema: ROUTING_PROJECTION_ROW_SCHEMA.to_string(),
        family: record.routing_family().stream_family().to_string(),
        record_key: record.routing_record_key(),
        partition: descriptor.partition.clone(),
        descriptor_key: descriptor.descriptor_key.clone(),
        generation: record.routing_generation(),
        descriptor_payload_proto: payload_proto.clone(),
    };
    Ok(encode_deterministic_proto(&row))
}

pub(super) fn decode_routing_projection_row(
    payload: &[u8],
) -> MeshDirectoryResult<RoutingProjectionRow> {
    let row: RoutingProjectionRowProto =
        decode_deterministic(payload, "mesh directory projection row")?;
    row.common
        .as_ref()
        .ok_or_else(|| MeshDirectoryError::InvalidIdentifier {
            field: "mesh directory projection row",
            value: "missing CoreMeta common".to_string(),
        })?;
    ensure_schema(
        &row.schema,
        ROUTING_PROJECTION_ROW_SCHEMA,
        "mesh directory projection row",
    )?;
    let family = RoutingRecordFamily::from_stream_family(&row.family).ok_or_else(|| {
        MeshDirectoryError::InvalidIdentifier {
            field: "routing projection family",
            value: row.family.clone(),
        }
    })?;
    let mut descriptor = routing_record_descriptor_from_proto(
        family,
        &row.record_key,
        &row.descriptor_payload_proto,
    )?;
    if descriptor.partition != row.partition
        || descriptor.descriptor_key != row.descriptor_key
        || descriptor.generation != row.generation
    {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "mesh directory projection row scope",
            value: format!(
                "expected {}/{}/{}, got {}/{}/{}",
                descriptor.partition,
                descriptor.descriptor_key,
                descriptor.generation,
                row.partition,
                row.descriptor_key,
                row.generation
            ),
        });
    }
    descriptor.partition = row.partition;
    descriptor.descriptor_key = row.descriptor_key;
    Ok(RoutingProjectionRow {
        descriptor,
        payload_proto: row.descriptor_payload_proto,
    })
}

pub(super) fn decode_typed_routing_descriptor<T: DecodeRoutingRecord>(
    payload_proto: &[u8],
) -> MeshDirectoryResult<T> {
    T::decode_routing_payload_proto(payload_proto)
}

pub(crate) fn control_payload_operator_json(
    family: RoutingRecordFamily,
    expected_record_key: &str,
    payload_proto: &[u8],
) -> MeshDirectoryResult<Vec<u8>> {
    Ok(
        routing_record_descriptor_from_proto(family, expected_record_key, payload_proto)?
            .payload_json
            .into_bytes(),
    )
}

pub(crate) fn encode_control_payload_from_operator_json(
    family: RoutingRecordFamily,
    payload_json: &[u8],
) -> MeshDirectoryResult<Vec<u8>> {
    match family {
        RoutingRecordFamily::TenantName => {
            let descriptor: TenantNameDescriptor = serde_json::from_slice(payload_json)?;
            descriptor.encode_routing_payload_proto()
        }
        RoutingRecordFamily::TenantLocator => {
            let descriptor: TenantLocatorDescriptor = serde_json::from_slice(payload_json)?;
            descriptor.encode_routing_payload_proto()
        }
        RoutingRecordFamily::BucketLocator => {
            let descriptor: BucketLocatorDescriptor = serde_json::from_slice(payload_json)?;
            descriptor.encode_routing_payload_proto()
        }
        RoutingRecordFamily::HostAlias => {
            let descriptor: routing::HostAliasDescriptor = serde_json::from_slice(payload_json)?;
            descriptor.encode_routing_payload_proto()
        }
    }
}

fn descriptor_from_decoded<T>(
    expected_record_key: &str,
    record: &T,
) -> MeshDirectoryResult<RoutingRecordDescriptor>
where
    T: StoredRoutingRecord,
{
    let actual_record_key = record.routing_record_key();
    if actual_record_key != expected_record_key {
        return Err(MeshDirectoryError::InvalidIdentifier {
            field: "routing record protobuf record key",
            value: format!("expected {expected_record_key}, got {actual_record_key}"),
        });
    }
    routing_record_descriptor_from_record(record)
}

impl StoredRoutingRecord for TenantNameDescriptor {
    fn routing_family(&self) -> RoutingRecordFamily {
        RoutingRecordFamily::TenantName
    }

    fn routing_record_key(&self) -> String {
        self.tenant_name.as_str().to_string()
    }

    fn routing_descriptor_key(&self) -> String {
        self.descriptor_key()
    }

    fn routing_generation(&self) -> u64 {
        self.generation
    }

    fn routing_mesh_id(&self) -> String {
        self.mesh_id.as_str().to_string()
    }

    fn encode_routing_payload_proto(&self) -> MeshDirectoryResult<Vec<u8>> {
        Ok(encode_deterministic_proto(&TenantNameDescriptorProto {
            schema: self.schema.clone(),
            mesh_id: self.mesh_id.as_str().to_string(),
            tenant_name: self.tenant_name.as_str().to_string(),
            tenant_id: self.tenant_id.as_str().to_string(),
            status: tenant_name_status_to_proto(self.status),
            idempotency_key: self.idempotency_key.clone(),
            reservation_expires_at: self.reservation_expires_at.clone(),
            created_at: self.created_at.clone(),
            updated_at: self.updated_at.clone(),
            generation: self.generation,
        }))
    }
}

impl DecodeRoutingRecord for TenantNameDescriptor {
    fn decode_routing_payload_proto(bytes: &[u8]) -> MeshDirectoryResult<Self> {
        let proto: TenantNameDescriptorProto =
            decode_deterministic(bytes, "tenant-name descriptor")?;
        let descriptor = Self {
            schema: proto.schema,
            mesh_id: MeshId::new(proto.mesh_id)?,
            tenant_name: TenantName::canonicalize(proto.tenant_name)?,
            tenant_id: TenantId::new(proto.tenant_id)?,
            status: tenant_name_status_from_proto(proto.status)?,
            idempotency_key: proto.idempotency_key,
            reservation_expires_at: proto.reservation_expires_at,
            created_at: proto.created_at,
            updated_at: proto.updated_at,
            generation: proto.generation,
        };
        ensure_schema(
            &descriptor.schema,
            TENANT_NAME_SCHEMA,
            "tenant-name descriptor",
        )?;
        Ok(descriptor)
    }
}

impl StoredRoutingRecord for TenantLocatorDescriptor {
    fn routing_family(&self) -> RoutingRecordFamily {
        RoutingRecordFamily::TenantLocator
    }

    fn routing_record_key(&self) -> String {
        self.tenant_id.as_str().to_string()
    }

    fn routing_descriptor_key(&self) -> String {
        self.descriptor_key()
    }

    fn routing_generation(&self) -> u64 {
        self.generation
    }

    fn routing_mesh_id(&self) -> String {
        self.mesh_id.as_str().to_string()
    }

    fn encode_routing_payload_proto(&self) -> MeshDirectoryResult<Vec<u8>> {
        Ok(encode_deterministic_proto(&TenantLocatorDescriptorProto {
            schema: self.schema.clone(),
            mesh_id: self.mesh_id.as_str().to_string(),
            tenant_id: self.tenant_id.as_str().to_string(),
            tenant_name: self.tenant_name.as_str().to_string(),
            home_region: self.home_region.as_str().to_string(),
            status: tenant_locator_status_to_proto(self.status),
            profile_revision: self.profile_revision,
            created_at: self.created_at.clone(),
            updated_at: self.updated_at.clone(),
            generation: self.generation,
        }))
    }
}

impl DecodeRoutingRecord for TenantLocatorDescriptor {
    fn decode_routing_payload_proto(bytes: &[u8]) -> MeshDirectoryResult<Self> {
        let proto: TenantLocatorDescriptorProto =
            decode_deterministic(bytes, "tenant locator descriptor")?;
        let descriptor = Self {
            schema: proto.schema,
            mesh_id: MeshId::new(proto.mesh_id)?,
            tenant_id: TenantId::new(proto.tenant_id)?,
            tenant_name: TenantName::canonicalize(proto.tenant_name)?,
            home_region: RegionName::new(proto.home_region)?,
            status: tenant_locator_status_from_proto(proto.status)?,
            profile_revision: proto.profile_revision,
            created_at: proto.created_at,
            updated_at: proto.updated_at,
            generation: proto.generation,
        };
        ensure_schema(
            &descriptor.schema,
            TENANT_LOCATOR_SCHEMA,
            "tenant locator descriptor",
        )?;
        Ok(descriptor)
    }
}

impl StoredRoutingRecord for BucketLocatorDescriptor {
    fn routing_family(&self) -> RoutingRecordFamily {
        RoutingRecordFamily::BucketLocator
    }

    fn routing_record_key(&self) -> String {
        format!("{}/{}", self.tenant_id.as_str(), self.bucket_name.as_str())
    }

    fn routing_descriptor_key(&self) -> String {
        self.descriptor_key()
    }

    fn routing_generation(&self) -> u64 {
        self.generation
    }

    fn routing_mesh_id(&self) -> String {
        self.mesh_id.as_str().to_string()
    }

    fn encode_routing_payload_proto(&self) -> MeshDirectoryResult<Vec<u8>> {
        Ok(encode_deterministic_proto(&BucketLocatorDescriptorProto {
            schema: self.schema.clone(),
            mesh_id: self.mesh_id.as_str().to_string(),
            tenant_id: self.tenant_id.as_str().to_string(),
            bucket_name: self.bucket_name.as_str().to_string(),
            bucket_id: self.bucket_id.as_str().to_string(),
            home_region: self.home_region.as_str().to_string(),
            home_cell: self.home_cell.as_str().to_string(),
            status: bucket_locator_status_to_proto(self.status),
            placement_policy: self.placement_policy.clone(),
            object_prefix: self.object_prefix.clone(),
            created_at: self.created_at.clone(),
            updated_at: self.updated_at.clone(),
            generation: self.generation,
        }))
    }
}

impl DecodeRoutingRecord for BucketLocatorDescriptor {
    fn decode_routing_payload_proto(bytes: &[u8]) -> MeshDirectoryResult<Self> {
        let proto: BucketLocatorDescriptorProto =
            decode_deterministic(bytes, "bucket locator descriptor")?;
        let descriptor = Self {
            schema: proto.schema,
            mesh_id: MeshId::new(proto.mesh_id)?,
            tenant_id: TenantId::new(proto.tenant_id)?,
            bucket_name: BucketName::canonicalize(proto.bucket_name)?,
            bucket_id: BucketId::new(proto.bucket_id)?,
            home_region: RegionName::new(proto.home_region)?,
            home_cell: CellId::new(proto.home_cell)?,
            status: bucket_locator_status_from_proto(proto.status)?,
            placement_policy: proto.placement_policy,
            object_prefix: proto.object_prefix,
            created_at: proto.created_at,
            updated_at: proto.updated_at,
            generation: proto.generation,
        };
        ensure_schema(
            &descriptor.schema,
            BUCKET_LOCATOR_SCHEMA,
            "bucket locator descriptor",
        )?;
        require_control_path_fragment(&descriptor.object_prefix, "object prefix")?;
        Ok(descriptor)
    }
}

impl StoredRoutingRecord for routing::HostAliasDescriptor {
    fn routing_family(&self) -> RoutingRecordFamily {
        RoutingRecordFamily::HostAlias
    }

    fn routing_record_key(&self) -> String {
        self.hostname.clone()
    }

    fn routing_descriptor_key(&self) -> String {
        host_alias_descriptor_key(&self.hostname).unwrap_or_else(|_| String::new())
    }

    fn routing_generation(&self) -> u64 {
        self.generation
    }

    fn routing_mesh_id(&self) -> String {
        "default".to_string()
    }

    fn encode_routing_payload_proto(&self) -> MeshDirectoryResult<Vec<u8>> {
        Ok(encode_deterministic_proto(&HostAliasDescriptorProto {
            schema: self.schema.clone(),
            hostname: routing::normalize_alias_hostname(&self.hostname).map_err(|_| {
                MeshDirectoryError::InvalidIdentifier {
                    field: "hostname",
                    value: self.hostname.clone(),
                }
            })?,
            tenant_id: self.tenant_id.clone(),
            bucket_name: self.bucket_name.clone(),
            region: self.region.clone(),
            prefix: self.prefix.clone(),
            state: host_alias_state_to_proto(self.state),
            created_at: self.created_at.clone(),
            updated_at: self.updated_at.clone(),
            generation: self.generation,
        }))
    }
}

impl DecodeRoutingRecord for routing::HostAliasDescriptor {
    fn decode_routing_payload_proto(bytes: &[u8]) -> MeshDirectoryResult<Self> {
        let proto: HostAliasDescriptorProto = decode_deterministic(bytes, "host alias descriptor")?;
        let descriptor = Self {
            schema: proto.schema,
            hostname: routing::normalize_alias_hostname(&proto.hostname).map_err(|_| {
                MeshDirectoryError::InvalidIdentifier {
                    field: "hostname",
                    value: proto.hostname.clone(),
                }
            })?,
            tenant_id: proto.tenant_id,
            bucket_name: proto.bucket_name,
            region: proto.region,
            prefix: proto.prefix,
            state: host_alias_state_from_proto(proto.state)?,
            created_at: proto.created_at,
            updated_at: proto.updated_at,
            generation: proto.generation,
        };
        ensure_schema(
            &descriptor.schema,
            routing::HOST_ALIAS_DESCRIPTOR_SCHEMA,
            "host alias descriptor",
        )?;
        Ok(descriptor)
    }
}

fn decode_deterministic<M>(bytes: &[u8], context: &'static str) -> MeshDirectoryResult<M>
where
    M: Message + Default,
{
    decode_deterministic_proto(bytes, context).map_err(|err| {
        MeshDirectoryError::InvalidIdentifier {
            field: context,
            value: err.to_string(),
        }
    })
}

fn ensure_schema(
    actual: &str,
    expected: &'static str,
    context: &'static str,
) -> MeshDirectoryResult<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(MeshDirectoryError::InvalidIdentifier {
            field: context,
            value: format!("expected schema {expected}, got {actual}"),
        })
    }
}

fn tenant_name_status_to_proto(status: TenantNameStatus) -> u32 {
    match status {
        TenantNameStatus::Reserved => 1,
        TenantNameStatus::Active => 2,
        TenantNameStatus::Tombstoned => 3,
    }
}

fn tenant_name_status_from_proto(value: u32) -> MeshDirectoryResult<TenantNameStatus> {
    match value {
        1 => Ok(TenantNameStatus::Reserved),
        2 => Ok(TenantNameStatus::Active),
        3 => Ok(TenantNameStatus::Tombstoned),
        _ => Err(invalid_enum("tenant-name status", value)),
    }
}

fn tenant_locator_status_to_proto(status: TenantLocatorStatus) -> u32 {
    match status {
        TenantLocatorStatus::Creating => 1,
        TenantLocatorStatus::Active => 2,
        TenantLocatorStatus::Suspended => 3,
        TenantLocatorStatus::Deleting => 4,
        TenantLocatorStatus::Deleted => 5,
    }
}

fn tenant_locator_status_from_proto(value: u32) -> MeshDirectoryResult<TenantLocatorStatus> {
    match value {
        1 => Ok(TenantLocatorStatus::Creating),
        2 => Ok(TenantLocatorStatus::Active),
        3 => Ok(TenantLocatorStatus::Suspended),
        4 => Ok(TenantLocatorStatus::Deleting),
        5 => Ok(TenantLocatorStatus::Deleted),
        _ => Err(invalid_enum("tenant locator status", value)),
    }
}

fn bucket_locator_status_to_proto(status: BucketLocatorStatus) -> u32 {
    match status {
        BucketLocatorStatus::Creating => 1,
        BucketLocatorStatus::Active => 2,
        BucketLocatorStatus::ReadOnly => 3,
        BucketLocatorStatus::Moving => 4,
        BucketLocatorStatus::Draining => 5,
        BucketLocatorStatus::Deleted => 6,
    }
}

fn bucket_locator_status_from_proto(value: u32) -> MeshDirectoryResult<BucketLocatorStatus> {
    match value {
        1 => Ok(BucketLocatorStatus::Creating),
        2 => Ok(BucketLocatorStatus::Active),
        3 => Ok(BucketLocatorStatus::ReadOnly),
        4 => Ok(BucketLocatorStatus::Moving),
        5 => Ok(BucketLocatorStatus::Draining),
        6 => Ok(BucketLocatorStatus::Deleted),
        _ => Err(invalid_enum("bucket locator status", value)),
    }
}

fn host_alias_state_to_proto(state: routing::HostAliasState) -> u32 {
    match state {
        routing::HostAliasState::PendingVerification => 1,
        routing::HostAliasState::Active => 2,
        routing::HostAliasState::Suspended => 3,
        routing::HostAliasState::Deleted => 4,
    }
}

fn host_alias_state_from_proto(value: u32) -> MeshDirectoryResult<routing::HostAliasState> {
    match value {
        1 => Ok(routing::HostAliasState::PendingVerification),
        2 => Ok(routing::HostAliasState::Active),
        3 => Ok(routing::HostAliasState::Suspended),
        4 => Ok(routing::HostAliasState::Deleted),
        _ => Err(invalid_enum("host alias state", value)),
    }
}

fn invalid_enum(field: &'static str, value: u32) -> MeshDirectoryError {
    MeshDirectoryError::InvalidIdentifier {
        field,
        value: value.to_string(),
    }
}
