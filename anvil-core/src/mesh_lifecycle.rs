use crate::core_store::{
    CompareAndSwapRef, CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob,
    WriteLogicalFileRequest, core_object_ref_from_logical_file_manifest,
};
use crate::mesh_control_stream::{
    ControlRecordDigest, ControlStreamFrame, ControlStreamSequence, read_control_checkpoint,
    read_control_stream_log,
};
use crate::mesh_directory::{self, BucketLocatorDescriptor, BucketLocatorStatus};
use crate::partition_fence::{self, PartitionWritePermit};
use crate::routing::{self, HostAliasDescriptor, HostAliasState, RoutingConfig};
use crate::storage::Storage;
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

pub const REGION_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.region.v1";
pub const CELL_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.cell.v1";
pub const NODE_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.node.v1";
pub const ACTIVATION_CHECKPOINT_SCHEMA: &str = "anvil.mesh.activation_checkpoint.v1";
pub const BUCKET_DRAIN_EXCEPTION_SCHEMA: &str = "anvil.mesh.bucket_drain_exception.v1";
pub const REGION_DESCRIPTOR_STREAM_FAMILY: &str = "region_descriptor";
pub const CELL_DESCRIPTOR_STREAM_FAMILY: &str = "cell_descriptor";
pub const NODE_DESCRIPTOR_STREAM_FAMILY: &str = "node_descriptor";
const CONTROL_MUTATION_SCHEMA: &str = "anvil.mesh.control_mutation.v1";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";
const MESH_LIFECYCLE_STATE_REF: &str = "mesh_lifecycle_state:global";

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("{resource_kind} {resource_id} already exists")]
    AlreadyExists {
        resource_kind: &'static str,
        resource_id: String,
    },
    #[error("{resource_kind} {resource_id} not found")]
    NotFound {
        resource_kind: &'static str,
        resource_id: String,
    },
    #[error(
        "{resource_kind} {resource_id} generation conflict: expected {expected}, current {current}"
    )]
    GenerationConflict {
        resource_kind: &'static str,
        resource_id: String,
        expected: u64,
        current: u64,
    },
    #[error("lifecycle transition denied for {resource_kind} {resource_id}: {from:?} -> {to:?}")]
    LifecycleTransitionDenied {
        resource_kind: &'static str,
        resource_id: String,
        from: LifecycleState,
        to: LifecycleState,
    },
    #[error(
        "ActivationCheckpointNotReached: control stream {stream_family}/{partition} has not reached sequence {sequence} with digest {expected_digest}: {reason}"
    )]
    ActivationCheckpointNotReached {
        stream_family: String,
        partition: String,
        sequence: u64,
        expected_digest: String,
        reason: String,
    },
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type LifecycleResult<T> = Result<T, LifecycleError>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleState {
    Joining,
    Active,
    ReadOnly,
    Draining,
    Drained,
    DrainedWithExceptions,
    Offline,
    Removed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeCapability {
    Object,
    Index,
    PersonalDb,
    Gateway,
    Admin,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum BucketDrainDisposition {
    BlockUntilEmpty,
    RemainProxyOnly,
    ReadOnlyUntilRemoved,
    DeleteAfterRetention,
}

impl BucketDrainDisposition {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BlockUntilEmpty => "block_until_empty",
            Self::RemainProxyOnly => "remain_proxy_only",
            Self::ReadOnlyUntilRemoved => "read_only_until_removed",
            Self::DeleteAfterRetention => "delete_after_retention",
        }
    }

    pub fn allows_drained_exception(self) -> bool {
        matches!(self, Self::RemainProxyOnly | Self::ReadOnlyUntilRemoved)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeDrainDescriptor {
    pub started_at: String,
    pub graceful_timeout_ms: u64,
    pub force_after_timeout: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeDescriptor {
    pub schema: String,
    pub mesh_id: String,
    pub node_id: String,
    pub region: String,
    pub cell_id: String,
    pub libp2p_peer_id: String,
    pub public_api_addr: String,
    pub public_cluster_addrs: Vec<String>,
    pub capabilities: Vec<NodeCapability>,
    pub state: LifecycleState,
    pub drain: Option<NodeDrainDescriptor>,
    pub last_heartbeat_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegionDescriptor {
    pub schema: String,
    pub mesh_id: String,
    pub region: String,
    pub state: LifecycleState,
    pub public_base_url: String,
    pub virtual_host_suffix: String,
    pub placement_weight: u32,
    pub default_cell: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CellDescriptor {
    pub schema: String,
    pub mesh_id: String,
    pub region: String,
    pub cell_id: String,
    pub state: LifecycleState,
    pub placement_weight: u32,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRegionDescriptor {
    pub mesh_id: String,
    pub region: String,
    pub public_base_url: String,
    pub virtual_host_suffix: String,
    pub placement_weight: u32,
    pub default_cell: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterCellDescriptor {
    pub mesh_id: String,
    pub region: String,
    pub cell_id: String,
    pub placement_weight: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterNodeDescriptor {
    pub mesh_id: String,
    pub node_id: String,
    pub region: String,
    pub cell_id: String,
    pub libp2p_peer_id: String,
    pub public_api_addr: String,
    pub public_cluster_addrs: Vec<String>,
    pub capabilities: Vec<NodeCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateHostAliasDescriptor {
    pub hostname: String,
    pub tenant_id: String,
    pub bucket_name: String,
    pub region: String,
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivationCheckpoint {
    pub schema: String,
    pub mesh_id: String,
    pub region: String,
    pub created_at: String,
    #[serde(default)]
    pub required_streams: Vec<ActivationCheckpointStream>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActivationCheckpointStream {
    pub stream_family: String,
    pub partition: String,
    pub sequence: ControlStreamSequence,
    pub digest: ControlRecordDigest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BucketDrainExceptionDescriptor {
    pub schema: String,
    pub tenant_id: String,
    pub bucket_name: String,
    pub region: String,
    pub disposition: BucketDrainDisposition,
    pub reason: String,
    pub expires_at: Option<String>,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketDrainExceptionInput {
    pub tenant_id: String,
    pub bucket_name: String,
    pub region: String,
    pub disposition: BucketDrainDisposition,
    pub reason: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct LifecycleControlWriteAuthority<'a> {
    pub permit: &'a PartitionWritePermit,
    pub signing_key: &'a [u8],
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeshLifecycleState {
    pub regions: BTreeMap<String, RegionDescriptor>,
    pub cells: BTreeMap<String, CellDescriptor>,
    pub nodes: BTreeMap<String, NodeDescriptor>,
    #[serde(default)]
    pub host_aliases: BTreeMap<String, HostAliasDescriptor>,
    #[serde(default)]
    pub bucket_drain_exceptions: BTreeMap<String, BucketDrainExceptionDescriptor>,
}

pub async fn read_state(storage: &Storage) -> LifecycleResult<MeshLifecycleState> {
    let mut state = read_lifecycle_state_projection(storage).await?;
    overlay_lifecycle_control_streams(storage, &mut state).await?;
    Ok(state)
}

async fn write_state(storage: &Storage, state: &MeshLifecycleState) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let current = store.read_ref(MESH_LIFECYCLE_STATE_REF).await?;
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "mesh_control".to_string(),
            generation: current
                .as_ref()
                .map(|value| value.generation + 1)
                .unwrap_or(1),
            logical_file_id: MESH_LIFECYCLE_STATE_REF.to_string(),
            source: serde_json::to_vec_pretty(state)?,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!("mesh-lifecycle-state:{}", uuid::Uuid::new_v4()),
            region_id: "local".to_string(),
        })
        .await?;
    let object_ref = core_object_ref_from_logical_file_manifest(&manifest);
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: MESH_LIFECYCLE_STATE_REF.to_string(),
            expected_generation: current.as_ref().map(|value| value.generation),
            expected_target: current.as_ref().map(|value| value.target.clone()),
            require_absent: current.is_none(),
            require_present: current.is_some(),
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(())
}

async fn read_lifecycle_state_projection(storage: &Storage) -> LifecycleResult<MeshLifecycleState> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(MESH_LIFECYCLE_STATE_REF).await? else {
        return Ok(MeshLifecycleState::default());
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
async fn delete_lifecycle_state_projection(storage: &Storage) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    store
        .delete_ref(MESH_LIFECYCLE_STATE_REF, None, None, false)
        .await?;
    Ok(())
}

async fn overlay_lifecycle_control_streams(
    storage: &Storage,
    state: &mut MeshLifecycleState,
) -> LifecycleResult<()> {
    for stream_family in lifecycle_control_stream_families() {
        for partition in
            crate::mesh_control_stream::list_control_stream_partitions(storage, stream_family)
                .await
                .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?
        {
            let log = read_control_stream_log(storage, stream_family, &partition).await.map_err(|err| {
                LifecycleError::InvalidArgument(format!(
                    "could not replay lifecycle control stream {stream_family}/{partition}: {err}"
                ))
            })?;
            for record in log.records {
                apply_lifecycle_control_frame(
                    state,
                    stream_family,
                    &partition,
                    &record.frame.header_json,
                    &record.frame.payload_json,
                )?;
            }
        }
    }
    Ok(())
}

fn apply_lifecycle_control_frame(
    state: &mut MeshLifecycleState,
    expected_stream_family: &str,
    expected_partition: &str,
    header_json: &[u8],
    payload_json: &[u8],
) -> LifecycleResult<()> {
    let header: serde_json::Value = serde_json::from_slice(header_json)?;
    let stream_family = header
        .get("stream_family")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            LifecycleError::InvalidArgument(
                "lifecycle control frame missing stream_family".to_string(),
            )
        })?;
    let partition = header
        .get("partition")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            LifecycleError::InvalidArgument("lifecycle control frame missing partition".to_string())
        })?;
    if stream_family != expected_stream_family || partition != expected_partition {
        return Err(LifecycleError::InvalidArgument(format!(
            "lifecycle control frame scope {stream_family}/{partition} does not match path {expected_stream_family}/{expected_partition}"
        )));
    }
    let operation = header
        .get("operation")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let record_key = header
        .get("record_key")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            LifecycleError::InvalidArgument(
                "lifecycle control frame missing record_key".to_string(),
            )
        })?;
    if matches!(operation, "delete" | "tombstone") {
        remove_lifecycle_projection(state, stream_family, record_key)?;
        return Ok(());
    }
    match stream_family {
        REGION_DESCRIPTOR_STREAM_FAMILY => {
            let descriptor: RegionDescriptor = serde_json::from_slice(payload_json)?;
            if descriptor.region != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "region descriptor key mismatch: expected {record_key}, got {}",
                    descriptor.region
                )));
            }
            state.regions.insert(descriptor.region.clone(), descriptor);
        }
        CELL_DESCRIPTOR_STREAM_FAMILY => {
            let descriptor: CellDescriptor = serde_json::from_slice(payload_json)?;
            let key = cell_record_key(&descriptor.region, &descriptor.cell_id)?;
            if key != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "cell descriptor key mismatch: expected {record_key}, got {key}"
                )));
            }
            state.cells.insert(key, descriptor);
        }
        NODE_DESCRIPTOR_STREAM_FAMILY => {
            let descriptor: NodeDescriptor = serde_json::from_slice(payload_json)?;
            let key =
                node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
            if key != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "node descriptor key mismatch: expected {record_key}, got {key}"
                )));
            }
            state.nodes.insert(descriptor.node_id.clone(), descriptor);
        }
        _ => {
            return Err(LifecycleError::InvalidArgument(format!(
                "unknown lifecycle control stream family {stream_family}"
            )));
        }
    }
    Ok(())
}

fn remove_lifecycle_projection(
    state: &mut MeshLifecycleState,
    stream_family: &str,
    record_key: &str,
) -> LifecycleResult<()> {
    match stream_family {
        REGION_DESCRIPTOR_STREAM_FAMILY => {
            state.regions.remove(record_key);
        }
        CELL_DESCRIPTOR_STREAM_FAMILY => {
            state.cells.remove(record_key);
        }
        NODE_DESCRIPTOR_STREAM_FAMILY => {
            let (_, _, node_id) = parse_node_record_key(record_key)?;
            state.nodes.remove(node_id);
        }
        _ => {
            return Err(LifecycleError::InvalidArgument(format!(
                "unknown lifecycle control stream family {stream_family}"
            )));
        }
    }
    Ok(())
}

pub async fn create_region(
    storage: &Storage,
    input: CreateRegionDescriptor,
) -> LifecycleResult<RegionDescriptor> {
    create_region_inner(storage, input, None).await
}

pub async fn create_region_with_control(
    storage: &Storage,
    input: CreateRegionDescriptor,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<RegionDescriptor> {
    create_region_inner(storage, input, Some(authority)).await
}

async fn create_region_inner(
    storage: &Storage,
    input: CreateRegionDescriptor,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.region, "region")?;
    require_nonempty(&input.public_base_url, "public base url")?;
    require_nonempty(&input.virtual_host_suffix, "virtual host suffix")?;
    if let Some(default_cell) = &input.default_cell {
        require_identifier(default_cell, "default cell")?;
    }

    let mut state = read_state(storage).await?;
    if state.regions.contains_key(&input.region) {
        return Err(LifecycleError::AlreadyExists {
            resource_kind: "region",
            resource_id: input.region,
        });
    }

    let now = timestamp_now();
    let descriptor = RegionDescriptor {
        schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: input.mesh_id,
        region: input.region.clone(),
        state: LifecycleState::Joining,
        public_base_url: input.public_base_url,
        virtual_host_suffix: input.virtual_host_suffix,
        placement_weight: input.placement_weight,
        default_cell: input.default_cell,
        created_at: now.clone(),
        updated_at: now,
        generation: 1,
    };
    state
        .regions
        .insert(descriptor.region.clone(), descriptor.clone());
    if let Some(authority) = authority {
        append_lifecycle_control_mutation(
            storage,
            REGION_DESCRIPTOR_STREAM_FAMILY,
            &lifecycle_control_partition(REGION_DESCRIPTOR_STREAM_FAMILY, &descriptor.region),
            &descriptor.region,
            "create",
            None,
            descriptor.generation,
            &descriptor.mesh_id,
            &descriptor,
            authority,
        )
        .await?;
    }
    write_state(storage, &state).await?;
    Ok(descriptor)
}

pub async fn transition_region(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    target: LifecycleState,
) -> LifecycleResult<RegionDescriptor> {
    transition_region_inner(storage, region, expected_generation, target, None).await
}

pub async fn transition_region_with_control(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    target: LifecycleState,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<RegionDescriptor> {
    transition_region_inner(
        storage,
        region,
        expected_generation,
        target,
        Some(authority),
    )
    .await
}

async fn transition_region_inner(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    target: LifecycleState,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(region, "region")?;
    let mut state = read_state(storage).await?;
    {
        let descriptor = state
            .regions
            .get(region)
            .ok_or_else(|| LifecycleError::NotFound {
                resource_kind: "region",
                resource_id: region.to_string(),
            })?;
        ensure_generation("region", region, descriptor.generation, expected_generation)?;
        validate_region_transition(descriptor.state, target).map_err(|_| {
            LifecycleError::LifecycleTransitionDenied {
                resource_kind: "region",
                resource_id: region.to_string(),
                from: descriptor.state,
                to: target,
            }
        })?;
    }
    ensure_region_drain_completion_is_supported(storage, region, target).await?;
    let descriptor = state
        .regions
        .get_mut(region)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        })?;
    descriptor.state = target;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    if let Some(authority) = authority {
        append_lifecycle_control_mutation(
            storage,
            REGION_DESCRIPTOR_STREAM_FAMILY,
            &lifecycle_control_partition(REGION_DESCRIPTOR_STREAM_FAMILY, &out.region),
            &out.region,
            "upsert",
            Some(expected_generation),
            out.generation,
            &out.mesh_id,
            &out,
            authority,
        )
        .await?;
    }
    write_state(storage, &state).await?;
    Ok(out)
}

pub fn parse_activation_checkpoint_json(input: &str) -> LifecycleResult<ActivationCheckpoint> {
    require_nonempty(input, "activation checkpoint")?;
    serde_json::from_str(input).map_err(|err| {
        LifecycleError::InvalidArgument(format!("activation checkpoint JSON is invalid: {err}"))
    })
}

pub async fn activate_region(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    checkpoint: &ActivationCheckpoint,
) -> LifecycleResult<RegionDescriptor> {
    activate_region_inner(storage, region, expected_generation, checkpoint, None).await
}

pub async fn activate_region_with_control(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    checkpoint: &ActivationCheckpoint,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<RegionDescriptor> {
    activate_region_inner(
        storage,
        region,
        expected_generation,
        checkpoint,
        Some(authority),
    )
    .await
}

async fn activate_region_inner(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    checkpoint: &ActivationCheckpoint,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(region, "region")?;

    let mut state = read_state(storage).await?;
    let current = state
        .regions
        .get(region)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        })?;
    ensure_generation("region", region, current.generation, expected_generation)?;
    validate_region_transition(current.state, LifecycleState::Active).map_err(|_| {
        LifecycleError::LifecycleTransitionDenied {
            resource_kind: "region",
            resource_id: region.to_string(),
            from: current.state,
            to: LifecycleState::Active,
        }
    })?;
    validate_activation_checkpoint_header(checkpoint, &current.mesh_id, region)?;
    validate_activation_checkpoint_streams(storage, checkpoint).await?;
    ensure_region_activation_dependencies(&state, region)?;

    let descriptor = state
        .regions
        .get_mut(region)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        })?;
    descriptor.state = LifecycleState::Active;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    if let Some(authority) = authority {
        append_lifecycle_control_mutation(
            storage,
            REGION_DESCRIPTOR_STREAM_FAMILY,
            &lifecycle_control_partition(REGION_DESCRIPTOR_STREAM_FAMILY, &out.region),
            &out.region,
            "upsert",
            Some(expected_generation),
            out.generation,
            &out.mesh_id,
            &out,
            authority,
        )
        .await?;
    }
    write_state(storage, &state).await?;
    Ok(out)
}

pub async fn list_regions(storage: &Storage) -> LifecycleResult<Vec<RegionDescriptor>> {
    Ok(read_state(storage).await?.regions.into_values().collect())
}

pub async fn ensure_region_accepts_new_writes(
    storage: &Storage,
    region: &str,
) -> LifecycleResult<()> {
    require_identifier(region, "region")?;
    let state = read_state(storage).await?;
    ensure_region_accepts_new_writes_in_state(&state, region)
}

pub async fn ensure_new_writable_placement(
    storage: &Storage,
    region: &str,
    cell_id: &str,
    node_id: &str,
) -> LifecycleResult<()> {
    require_identifier(region, "region")?;
    require_identifier(cell_id, "cell id")?;
    require_identifier(node_id, "node id")?;

    let state = read_state(storage).await?;
    ensure_region_accepts_new_writes_in_state(&state, region)?;
    ensure_cell_accepts_new_writes_in_state(&state, region, cell_id)?;
    ensure_node_accepts_new_writes_in_state(&state, region, cell_id, node_id)?;
    Ok(())
}

pub async fn register_cell(
    storage: &Storage,
    input: RegisterCellDescriptor,
) -> LifecycleResult<CellDescriptor> {
    register_cell_inner(storage, input, None).await
}

pub async fn register_cell_with_control(
    storage: &Storage,
    input: RegisterCellDescriptor,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<CellDescriptor> {
    register_cell_inner(storage, input, Some(authority)).await
}

async fn register_cell_inner(
    storage: &Storage,
    input: RegisterCellDescriptor,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<CellDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.region, "region")?;
    require_identifier(&input.cell_id, "cell id")?;

    let mut state = read_state(storage).await?;
    if !state.regions.contains_key(&input.region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: input.region,
        });
    }
    let key = cell_key(&input.region, &input.cell_id)?;
    if state.cells.contains_key(&key) {
        return Err(LifecycleError::AlreadyExists {
            resource_kind: "cell",
            resource_id: input.cell_id,
        });
    }

    let now = timestamp_now();
    let descriptor = CellDescriptor {
        schema: CELL_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: input.mesh_id,
        region: input.region,
        cell_id: input.cell_id,
        state: LifecycleState::Joining,
        placement_weight: input.placement_weight,
        created_at: now.clone(),
        updated_at: now,
        generation: 1,
    };
    state.cells.insert(key, descriptor.clone());
    if let Some(authority) = authority {
        let record_key = cell_record_key(&descriptor.region, &descriptor.cell_id)?;
        append_lifecycle_control_mutation(
            storage,
            CELL_DESCRIPTOR_STREAM_FAMILY,
            &lifecycle_control_partition(CELL_DESCRIPTOR_STREAM_FAMILY, &record_key),
            &record_key,
            "create",
            None,
            descriptor.generation,
            &descriptor.mesh_id,
            &descriptor,
            authority,
        )
        .await?;
    }
    write_state(storage, &state).await?;
    Ok(descriptor)
}

pub async fn transition_cell(
    storage: &Storage,
    region: &str,
    cell_id: &str,
    expected_generation: u64,
    target: LifecycleState,
) -> LifecycleResult<CellDescriptor> {
    transition_cell_inner(storage, region, cell_id, expected_generation, target, None).await
}

pub async fn transition_cell_with_control(
    storage: &Storage,
    region: &str,
    cell_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<CellDescriptor> {
    transition_cell_inner(
        storage,
        region,
        cell_id,
        expected_generation,
        target,
        Some(authority),
    )
    .await
}

async fn transition_cell_inner(
    storage: &Storage,
    region: &str,
    cell_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<CellDescriptor> {
    let key = cell_key(region, cell_id)?;
    let mut state = read_state(storage).await?;
    let descriptor = state
        .cells
        .get_mut(&key)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: cell_id.to_string(),
        })?;
    ensure_generation("cell", cell_id, descriptor.generation, expected_generation)?;
    validate_region_transition(descriptor.state, target).map_err(|_| {
        LifecycleError::LifecycleTransitionDenied {
            resource_kind: "cell",
            resource_id: cell_id.to_string(),
            from: descriptor.state,
            to: target,
        }
    })?;
    descriptor.state = target;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    if let Some(authority) = authority {
        let record_key = cell_record_key(&out.region, &out.cell_id)?;
        append_lifecycle_control_mutation(
            storage,
            CELL_DESCRIPTOR_STREAM_FAMILY,
            &lifecycle_control_partition(CELL_DESCRIPTOR_STREAM_FAMILY, &record_key),
            &record_key,
            "upsert",
            Some(expected_generation),
            out.generation,
            &out.mesh_id,
            &out,
            authority,
        )
        .await?;
    }
    write_state(storage, &state).await?;
    Ok(out)
}

pub async fn list_cells(
    storage: &Storage,
    region_filter: Option<&str>,
) -> LifecycleResult<Vec<CellDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    let cells = read_state(storage)
        .await?
        .cells
        .into_values()
        .filter(|cell| {
            region_filter.is_none_or(|region| region.is_empty() || cell.region == region)
        })
        .collect();
    Ok(cells)
}

pub async fn register_node(
    storage: &Storage,
    input: RegisterNodeDescriptor,
) -> LifecycleResult<NodeDescriptor> {
    register_node_inner(storage, input, None).await
}

pub async fn register_node_with_control(
    storage: &Storage,
    input: RegisterNodeDescriptor,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<NodeDescriptor> {
    register_node_inner(storage, input, Some(authority)).await
}

async fn register_node_inner(
    storage: &Storage,
    input: RegisterNodeDescriptor,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<NodeDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.node_id, "node id")?;
    require_identifier(&input.region, "region")?;
    require_identifier(&input.cell_id, "cell id")?;
    require_nonempty(&input.libp2p_peer_id, "libp2p peer id")?;
    require_nonempty(&input.public_api_addr, "public api addr")?;
    if input.capabilities.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "node capabilities must not be empty".to_string(),
        ));
    }

    let mut state = read_state(storage).await?;
    if !state.regions.contains_key(&input.region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: input.region,
        });
    }
    let cell_key = cell_key(&input.region, &input.cell_id)?;
    if !state.cells.contains_key(&cell_key) {
        return Err(LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: input.cell_id,
        });
    }
    if state.nodes.contains_key(&input.node_id) {
        return Err(LifecycleError::AlreadyExists {
            resource_kind: "node",
            resource_id: input.node_id,
        });
    }

    let now = timestamp_now();
    let descriptor = NodeDescriptor {
        schema: NODE_DESCRIPTOR_SCHEMA.to_string(),
        mesh_id: input.mesh_id,
        node_id: input.node_id.clone(),
        region: input.region,
        cell_id: input.cell_id,
        libp2p_peer_id: input.libp2p_peer_id,
        public_api_addr: input.public_api_addr,
        public_cluster_addrs: input.public_cluster_addrs,
        capabilities: input.capabilities,
        state: LifecycleState::Joining,
        drain: None,
        last_heartbeat_at: None,
        created_at: now.clone(),
        updated_at: now,
        generation: 1,
    };
    state
        .nodes
        .insert(descriptor.node_id.clone(), descriptor.clone());
    if let Some(authority) = authority {
        let record_key =
            node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
        append_lifecycle_control_mutation(
            storage,
            NODE_DESCRIPTOR_STREAM_FAMILY,
            &lifecycle_control_partition(NODE_DESCRIPTOR_STREAM_FAMILY, &record_key),
            &record_key,
            "create",
            None,
            descriptor.generation,
            &descriptor.mesh_id,
            &descriptor,
            authority,
        )
        .await?;
    }
    write_state(storage, &state).await?;
    Ok(descriptor)
}

pub async fn transition_node(
    storage: &Storage,
    node_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    drain: Option<NodeDrainDescriptor>,
) -> LifecycleResult<NodeDescriptor> {
    transition_node_inner(storage, node_id, expected_generation, target, drain, None).await
}

pub async fn transition_node_with_control(
    storage: &Storage,
    node_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    drain: Option<NodeDrainDescriptor>,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<NodeDescriptor> {
    transition_node_inner(
        storage,
        node_id,
        expected_generation,
        target,
        drain,
        Some(authority),
    )
    .await
}

async fn transition_node_inner(
    storage: &Storage,
    node_id: &str,
    expected_generation: u64,
    target: LifecycleState,
    drain: Option<NodeDrainDescriptor>,
    authority: Option<LifecycleControlWriteAuthority<'_>>,
) -> LifecycleResult<NodeDescriptor> {
    require_identifier(node_id, "node id")?;
    let mut state = read_state(storage).await?;
    let current = state
        .nodes
        .get(node_id)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "node",
            resource_id: node_id.to_string(),
        })?;
    ensure_generation("node", node_id, current.generation, expected_generation)?;
    if target == LifecycleState::Active {
        ensure_node_placement_is_active(&state, current)?;
    }
    validate_node_transition(current.state, target).map_err(|_| {
        LifecycleError::LifecycleTransitionDenied {
            resource_kind: "node",
            resource_id: node_id.to_string(),
            from: current.state,
            to: target,
        }
    })?;

    let descriptor = state
        .nodes
        .get_mut(node_id)
        .ok_or_else(|| LifecycleError::NotFound {
            resource_kind: "node",
            resource_id: node_id.to_string(),
        })?;
    descriptor.state = target;
    descriptor.drain = if target == LifecycleState::Draining {
        drain
    } else {
        None
    };
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    if let Some(authority) = authority {
        let record_key = node_record_key(&out.region, &out.cell_id, &out.node_id)?;
        append_lifecycle_control_mutation(
            storage,
            NODE_DESCRIPTOR_STREAM_FAMILY,
            &lifecycle_control_partition(NODE_DESCRIPTOR_STREAM_FAMILY, &record_key),
            &record_key,
            "upsert",
            Some(expected_generation),
            out.generation,
            &out.mesh_id,
            &out,
            authority,
        )
        .await?;
    }
    write_state(storage, &state).await?;
    Ok(out)
}

pub async fn list_nodes(
    storage: &Storage,
    region_filter: Option<&str>,
    cell_filter: Option<&str>,
) -> LifecycleResult<Vec<NodeDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    if let Some(cell_id) = cell_filter.filter(|cell_id| !cell_id.is_empty()) {
        require_identifier(cell_id, "cell id")?;
    }
    let nodes = read_state(storage)
        .await?
        .nodes
        .into_values()
        .filter(|node| {
            region_filter.is_none_or(|region| region.is_empty() || node.region == region)
        })
        .filter(|node| cell_filter.is_none_or(|cell| cell.is_empty() || node.cell_id == cell))
        .collect();
    Ok(nodes)
}

pub async fn create_host_alias(
    storage: &Storage,
    config: &RoutingConfig,
    input: CreateHostAliasDescriptor,
) -> LifecycleResult<HostAliasDescriptor> {
    require_identifier(&input.tenant_id, "tenant id")?;
    require_identifier(&input.bucket_name, "bucket name")?;
    require_identifier(&input.region, "region")?;
    let hostname = routing::normalize_alias_hostname(&input.hostname)
        .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;

    let mut state = read_state(storage).await?;
    match state.regions.get(&input.region) {
        Some(region) if region.state == LifecycleState::Active => {}
        Some(_) => {
            return Err(LifecycleError::InvalidArgument(
                "host alias region must be active".to_string(),
            ));
        }
        None => {
            return Err(LifecycleError::NotFound {
                resource_kind: "region",
                resource_id: input.region,
            });
        }
    }
    if state.host_aliases.contains_key(&hostname) {
        return Err(LifecycleError::AlreadyExists {
            resource_kind: "host alias",
            resource_id: hostname,
        });
    }

    let mut descriptor = HostAliasDescriptor::active(
        hostname,
        input.tenant_id,
        input.bucket_name,
        input.region,
        input.prefix,
        config,
    )
    .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
    descriptor.state = HostAliasState::PendingVerification;
    let out = descriptor.clone();
    state.host_aliases.insert(out.hostname.clone(), descriptor);
    write_state(storage, &state).await?;
    Ok(out)
}

pub async fn transition_host_alias(
    storage: &Storage,
    hostname: &str,
    expected_generation: u64,
    target: HostAliasState,
) -> LifecycleResult<HostAliasDescriptor> {
    let hostname = routing::normalize_alias_hostname(hostname)
        .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
    let mut state = read_state(storage).await?;
    let descriptor =
        state
            .host_aliases
            .get_mut(&hostname)
            .ok_or_else(|| LifecycleError::NotFound {
                resource_kind: "host alias",
                resource_id: hostname.clone(),
            })?;
    ensure_generation(
        "host alias",
        &hostname,
        descriptor.generation,
        expected_generation,
    )?;
    validate_host_alias_transition(descriptor.state, target).map_err(|_| {
        LifecycleError::LifecycleTransitionDenied {
            resource_kind: "host alias",
            resource_id: hostname.clone(),
            from: lifecycle_state_for_host_alias(descriptor.state),
            to: lifecycle_state_for_host_alias(target),
        }
    })?;
    descriptor.state = target;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    write_state(storage, &state).await?;
    Ok(out)
}

pub async fn list_host_aliases(
    storage: &Storage,
    region_filter: Option<&str>,
) -> LifecycleResult<Vec<HostAliasDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    Ok(read_state(storage)
        .await?
        .host_aliases
        .into_values()
        .filter(|alias| {
            region_filter.is_none_or(|region| region.is_empty() || alias.region == region)
        })
        .collect())
}

pub async fn upsert_bucket_drain_exception(
    storage: &Storage,
    input: BucketDrainExceptionInput,
) -> LifecycleResult<BucketDrainExceptionDescriptor> {
    require_identifier(&input.tenant_id, "bucket drain exception tenant id")?;
    require_identifier(&input.bucket_name, "bucket drain exception bucket name")?;
    require_identifier(&input.region, "bucket drain exception region")?;
    require_nonempty(&input.reason, "bucket drain exception reason")?;
    if !input.disposition.allows_drained_exception() {
        return Err(LifecycleError::InvalidArgument(format!(
            "bucket drain exception disposition must be remain_proxy_only or read_only_until_removed, got {}",
            input.disposition.as_str()
        )));
    }
    if let Some(expires_at) = &input.expires_at {
        require_nonempty(expires_at, "bucket drain exception expires_at")?;
    }

    let mut state = read_state(storage).await?;
    let key = bucket_drain_exception_key(&input.region, &input.tenant_id, &input.bucket_name);
    let generation = state
        .bucket_drain_exceptions
        .get(&key)
        .map_or(1, |existing| existing.generation.saturating_add(1));
    let descriptor = BucketDrainExceptionDescriptor {
        schema: BUCKET_DRAIN_EXCEPTION_SCHEMA.to_string(),
        tenant_id: input.tenant_id,
        bucket_name: input.bucket_name,
        region: input.region,
        disposition: input.disposition,
        reason: input.reason,
        expires_at: input.expires_at,
        generation,
    };
    state
        .bucket_drain_exceptions
        .insert(key, descriptor.clone());
    write_state(storage, &state).await?;
    Ok(descriptor)
}

pub async fn list_bucket_drain_exceptions(
    storage: &Storage,
    region_filter: Option<&str>,
) -> LifecycleResult<Vec<BucketDrainExceptionDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "bucket drain exception region")?;
    }
    Ok(read_state(storage)
        .await?
        .bucket_drain_exceptions
        .into_values()
        .filter(|exception| {
            region_filter.is_none_or(|region| region.is_empty() || exception.region == region)
        })
        .collect())
}

pub fn validate_host_alias_transition(
    from: HostAliasState,
    to: HostAliasState,
) -> LifecycleResult<()> {
    use HostAliasState::*;
    if matches!(
        (from, to),
        (PendingVerification, Active)
            | (PendingVerification, Deleted)
            | (Active, Suspended)
            | (Active, Deleted)
            | (Suspended, Active)
            | (Suspended, Deleted)
    ) {
        Ok(())
    } else {
        Err(LifecycleError::LifecycleTransitionDenied {
            resource_kind: "host alias",
            resource_id: String::new(),
            from: lifecycle_state_for_host_alias(from),
            to: lifecycle_state_for_host_alias(to),
        })
    }
}

fn lifecycle_state_for_host_alias(state: HostAliasState) -> LifecycleState {
    match state {
        HostAliasState::PendingVerification => LifecycleState::Joining,
        HostAliasState::Active => LifecycleState::Active,
        HostAliasState::Suspended => LifecycleState::ReadOnly,
        HostAliasState::Deleted => LifecycleState::Removed,
    }
}

pub fn validate_node_transition(from: LifecycleState, to: LifecycleState) -> LifecycleResult<()> {
    use LifecycleState::*;
    if matches!(
        (from, to),
        (Joining, Active)
            | (Joining, Removed)
            | (Active, Draining)
            | (Active, Offline)
            | (Draining, Drained)
            | (Draining, Offline)
            | (Drained, Active)
            | (Drained, Removed)
            | (Offline, Active)
            | (Offline, Draining)
            | (Offline, Removed)
    ) {
        Ok(())
    } else {
        Err(LifecycleError::LifecycleTransitionDenied {
            resource_kind: "node",
            resource_id: String::new(),
            from,
            to,
        })
    }
}

pub fn validate_region_transition(from: LifecycleState, to: LifecycleState) -> LifecycleResult<()> {
    use LifecycleState::*;
    if matches!(
        (from, to),
        (Joining, Active)
            | (Joining, Removed)
            | (Active, ReadOnly)
            | (Active, Draining)
            | (Active, Offline)
            | (ReadOnly, Active)
            | (ReadOnly, Draining)
            | (Draining, Drained)
            | (Draining, DrainedWithExceptions)
            | (Draining, Offline)
            | (Drained, Active)
            | (DrainedWithExceptions, Active)
            | (DrainedWithExceptions, Draining)
            | (Drained, Removed)
            | (Offline, Active)
            | (Offline, Draining)
            | (Offline, Removed)
    ) {
        Ok(())
    } else {
        Err(LifecycleError::LifecycleTransitionDenied {
            resource_kind: "region",
            resource_id: String::new(),
            from,
            to,
        })
    }
}

fn ensure_node_placement_is_active(
    state: &MeshLifecycleState,
    descriptor: &NodeDescriptor,
) -> LifecycleResult<()> {
    let Some(region) = state.regions.get(&descriptor.region) else {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: descriptor.region.clone(),
        });
    };
    let cell_key = cell_key(&descriptor.region, &descriptor.cell_id)?;
    let Some(cell) = state.cells.get(&cell_key) else {
        return Err(LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: descriptor.cell_id.clone(),
        });
    };
    if !matches!(
        region.state,
        LifecycleState::Joining | LifecycleState::Active
    ) || cell.state != LifecycleState::Active
    {
        return Err(LifecycleError::InvalidArgument(
            "node activation requires a joining or active region and an active cell".to_string(),
        ));
    }
    Ok(())
}

fn ensure_region_activation_dependencies(
    state: &MeshLifecycleState,
    region: &str,
) -> LifecycleResult<()> {
    let active_cell_ids = state
        .cells
        .values()
        .filter(|cell| cell.region == region && cell.state == LifecycleState::Active)
        .map(|cell| cell.cell_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    if active_cell_ids.is_empty() {
        return Err(LifecycleError::InvalidArgument(format!(
            "region {region} activation requires at least one active cell"
        )));
    }
    let has_active_node = state.nodes.values().any(|node| {
        node.region == region
            && active_cell_ids.contains(node.cell_id.as_str())
            && node.state == LifecycleState::Active
    });
    if !has_active_node {
        return Err(LifecycleError::InvalidArgument(format!(
            "region {region} activation requires at least one active node in an active cell"
        )));
    }
    Ok(())
}

fn ensure_region_accepts_new_writes_in_state(
    state: &MeshLifecycleState,
    region: &str,
) -> LifecycleResult<()> {
    let Some(descriptor) = state.regions.get(region) else {
        if state.regions.is_empty() {
            return Ok(());
        }
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: region.to_string(),
        });
    };
    if descriptor.state == LifecycleState::Active {
        return Ok(());
    }
    Err(LifecycleError::InvalidArgument(format!(
        "region {region} is {:?} and cannot accept new writable placement",
        descriptor.state
    )))
}

fn ensure_cell_accepts_new_writes_in_state(
    state: &MeshLifecycleState,
    region: &str,
    cell_id: &str,
) -> LifecycleResult<()> {
    let key = cell_key(region, cell_id)?;
    let Some(descriptor) = state.cells.get(&key) else {
        if state.cells.is_empty() {
            return Ok(());
        }
        return Err(LifecycleError::NotFound {
            resource_kind: "cell",
            resource_id: format!("{region}/{cell_id}"),
        });
    };
    if descriptor.state == LifecycleState::Active {
        return Ok(());
    }
    Err(LifecycleError::InvalidArgument(format!(
        "cell {region}/{cell_id} is {:?} and cannot accept new writable placement",
        descriptor.state
    )))
}

fn ensure_node_accepts_new_writes_in_state(
    state: &MeshLifecycleState,
    region: &str,
    cell_id: &str,
    node_id: &str,
) -> LifecycleResult<()> {
    let Some(descriptor) = state.nodes.get(node_id) else {
        if state.nodes.is_empty() {
            return Ok(());
        }
        return Err(LifecycleError::NotFound {
            resource_kind: "node",
            resource_id: node_id.to_string(),
        });
    };
    if descriptor.region != region || descriptor.cell_id != cell_id {
        return Err(LifecycleError::InvalidArgument(format!(
            "node {node_id} belongs to {}/{} and cannot accept placement for {region}/{cell_id}",
            descriptor.region, descriptor.cell_id
        )));
    }
    if descriptor.state == LifecycleState::Active {
        return Ok(());
    }
    Err(LifecycleError::InvalidArgument(format!(
        "node {node_id} is {:?} and cannot accept new writable placement",
        descriptor.state
    )))
}

async fn ensure_region_drain_completion_is_supported(
    storage: &Storage,
    region: &str,
    target: LifecycleState,
) -> LifecycleResult<()> {
    match target {
        LifecycleState::Drained => {
            let blockers = bucket_locators_blocking_region_drain(storage, region).await?;
            if blockers.is_empty() {
                Ok(())
            } else {
                Err(LifecycleError::InvalidArgument(format!(
                    "region {region} drain cannot complete with block_until_empty: {} bucket locator(s) still name the region as primary: {}",
                    blockers.len(),
                    blockers.join(", ")
                )))
            }
        }
        LifecycleState::DrainedWithExceptions => {
            let blockers = bucket_locators_without_valid_drain_exception(storage, region).await?;
            if blockers.is_empty() {
                Ok(())
            } else {
                Err(LifecycleError::InvalidArgument(format!(
                    "region {region} drain cannot complete with exceptions: {} bucket locator(s) do not have a valid read-only drain exception: {}",
                    blockers.len(),
                    blockers.join(", ")
                )))
            }
        }
        _ => Ok(()),
    }
}

async fn bucket_locators_blocking_region_drain(
    storage: &Storage,
    region: &str,
) -> LifecycleResult<Vec<String>> {
    let records = mesh_directory::list_routing_records(
        storage,
        Some(mesh_directory::RoutingRecordFamily::BucketLocator),
    )
    .await
    .map_err(|err| {
        LifecycleError::InvalidArgument(format!(
            "could not inspect bucket locators for region drain: {err}"
        ))
    })?;
    let mut blockers = Vec::new();
    for record in records {
        let locator: BucketLocatorDescriptor =
            serde_json::from_str(&record.payload_json).map_err(|err| {
                LifecycleError::InvalidArgument(format!(
                    "bucket locator {} is invalid: {err}",
                    record.record_key
                ))
            })?;
        if locator.home_region.as_str() == region
            && bucket_locator_blocks_region_drain(locator.status)
        {
            blockers.push(format!("{}:{:?}", record.record_key, locator.status));
        }
    }
    Ok(blockers)
}

fn bucket_locator_blocks_region_drain(status: BucketLocatorStatus) -> bool {
    !matches!(status, BucketLocatorStatus::Deleted)
}

async fn bucket_locators_without_valid_drain_exception(
    storage: &Storage,
    region: &str,
) -> LifecycleResult<Vec<String>> {
    let state = read_state(storage).await?;
    let records = mesh_directory::list_routing_records(
        storage,
        Some(mesh_directory::RoutingRecordFamily::BucketLocator),
    )
    .await
    .map_err(|err| {
        LifecycleError::InvalidArgument(format!(
            "could not inspect bucket locators for region drain: {err}"
        ))
    })?;
    let mut blockers = Vec::new();
    for record in records {
        let locator: BucketLocatorDescriptor =
            serde_json::from_str(&record.payload_json).map_err(|err| {
                LifecycleError::InvalidArgument(format!(
                    "bucket locator {} is invalid: {err}",
                    record.record_key
                ))
            })?;
        if locator.home_region.as_str() != region
            || !bucket_locator_blocks_region_drain(locator.status)
        {
            continue;
        }
        let exception_key = bucket_drain_exception_key(
            region,
            locator.tenant_id.as_str(),
            locator.bucket_name.as_str(),
        );
        let Some(exception) = state.bucket_drain_exceptions.get(&exception_key) else {
            blockers.push(format!(
                "{}:{:?}:missing_exception",
                record.record_key, locator.status
            ));
            continue;
        };
        if locator.status != BucketLocatorStatus::ReadOnly {
            blockers.push(format!(
                "{}:{:?}:exception_requires_read_only_locator",
                record.record_key, locator.status
            ));
            continue;
        }
        if !exception.disposition.allows_drained_exception() {
            blockers.push(format!(
                "{}:{:?}:invalid_exception_disposition:{}",
                record.record_key,
                locator.status,
                exception.disposition.as_str()
            ));
            continue;
        }
    }
    Ok(blockers)
}

pub fn bucket_drain_exception_key(region: &str, tenant_id: &str, bucket_name: &str) -> String {
    format!("{region}/{tenant_id}/{bucket_name}")
}

pub fn lifecycle_control_partition(stream_family: &str, record_key: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(stream_family.as_bytes());
    hasher.update(b":");
    hasher.update(record_key.as_bytes());
    let digest = hasher.finalize();
    let bytes = digest.as_bytes();
    format!("{:02x}{:02x}", bytes[0], bytes[1])
}

pub fn lifecycle_control_stream_families() -> [&'static str; 3] {
    [
        REGION_DESCRIPTOR_STREAM_FAMILY,
        CELL_DESCRIPTOR_STREAM_FAMILY,
        NODE_DESCRIPTOR_STREAM_FAMILY,
    ]
}

async fn append_lifecycle_control_mutation<T: Serialize>(
    storage: &Storage,
    stream_family: &str,
    partition: &str,
    record_key: &str,
    operation: &str,
    expected_generation: Option<u64>,
    new_generation: u64,
    mesh_id: &str,
    payload: &T,
    authority: LifecycleControlWriteAuthority<'_>,
) -> LifecycleResult<()> {
    require_identifier(stream_family, "control stream family")?;
    require_identifier(partition, "control stream partition")?;
    require_control_record_key(record_key)?;
    let expected_partition_id = mesh_directory::control_partition_id(stream_family, partition);
    if authority.permit.partition_family != mesh_directory::CONTROL_PARTITION_FAMILY {
        return Err(LifecycleError::InvalidArgument(format!(
            "invalid lifecycle control write permit: expected partition family {}, got {}",
            mesh_directory::CONTROL_PARTITION_FAMILY,
            authority.permit.partition_family
        )));
    }
    if authority.permit.partition_id != expected_partition_id {
        return Err(LifecycleError::InvalidArgument(
            "invalid lifecycle control write permit: partition id does not match stream"
                .to_string(),
        ));
    }
    let partition_precondition = partition_fence::partition_write_ref_precondition(
        storage,
        authority.permit,
        authority.signing_key,
    )
    .await
    .map_err(|rejection| {
        LifecycleError::InvalidArgument(format!(
            "lifecycle control write fence rejected for {stream_family}/{partition}: {}: {}",
            rejection.code.as_str(),
            rejection.reason
        ))
    })?;

    let existing_log = read_control_stream_log(storage, stream_family, partition)
        .await
        .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
    let sequence = existing_log
        .records
        .last()
        .map(|record| record.metadata.sequence.get().saturating_add(1))
        .unwrap_or(1);
    let payload_json = serde_json::to_vec(payload)?;
    let digest = ControlRecordDigest::blake3(&payload_json);
    let header_json = serde_json::to_vec(&serde_json::json!({
        "schema": CONTROL_MUTATION_SCHEMA,
        "mesh_id": mesh_id,
        "stream_family": stream_family,
        "partition": partition,
        "sequence": ControlStreamSequence::new(sequence)
            .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?,
        "record_key": record_key,
        "operation": operation,
        "expected_generation": expected_generation,
        "new_generation": new_generation,
        "writer_node_id": authority.permit.owner_node_id.as_str(),
        "writer_fence": authority.permit.fence_token,
        "idempotency_key": null,
        "record_digest": digest.as_str(),
        "created_at": Utc::now().to_rfc3339(),
    }))?;
    let frame = ControlStreamFrame::new(header_json, payload_json);
    crate::mesh_control_stream::append_control_stream_frame(
        storage,
        stream_family,
        partition,
        &frame,
        Some(partition_precondition),
    )
    .await
    .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
    Ok(())
}

fn cell_record_key(region: &str, cell_id: &str) -> LifecycleResult<String> {
    require_identifier(region, "cell record region")?;
    require_identifier(cell_id, "cell record cell id")?;
    Ok(format!("{region}/{cell_id}"))
}

fn node_record_key(region: &str, cell_id: &str, node_id: &str) -> LifecycleResult<String> {
    require_identifier(region, "node record region")?;
    require_identifier(cell_id, "node record cell id")?;
    require_identifier(node_id, "node record node id")?;
    Ok(format!("{region}/{cell_id}/{node_id}"))
}

fn parse_node_record_key(record_key: &str) -> LifecycleResult<(&str, &str, &str)> {
    let mut parts = record_key.split('/');
    let region = parts.next().unwrap_or_default();
    let cell_id = parts.next().unwrap_or_default();
    let node_id = parts.next().unwrap_or_default();
    if parts.next().is_some() || region.is_empty() || cell_id.is_empty() || node_id.is_empty() {
        return Err(LifecycleError::InvalidArgument(format!(
            "invalid node record key {record_key}"
        )));
    }
    Ok((region, cell_id, node_id))
}

fn require_control_record_key(value: &str) -> LifecycleResult<()> {
    require_nonempty(value, "control record key")?;
    if value.contains("//") || value.chars().any(|ch| ch == '\0' || ch.is_control()) {
        return Err(LifecycleError::InvalidArgument(
            "control record key contains an invalid character".to_string(),
        ));
    }
    Ok(())
}

fn ensure_generation(
    resource_kind: &'static str,
    resource_id: &str,
    current: u64,
    expected: u64,
) -> LifecycleResult<()> {
    if current == expected {
        return Ok(());
    }
    Err(LifecycleError::GenerationConflict {
        resource_kind,
        resource_id: resource_id.to_string(),
        expected,
        current,
    })
}

fn validate_activation_checkpoint_header(
    checkpoint: &ActivationCheckpoint,
    mesh_id: &str,
    region: &str,
) -> LifecycleResult<()> {
    if checkpoint.schema != ACTIVATION_CHECKPOINT_SCHEMA {
        return Err(LifecycleError::InvalidArgument(format!(
            "activation checkpoint schema must be {ACTIVATION_CHECKPOINT_SCHEMA}"
        )));
    }
    require_identifier(&checkpoint.mesh_id, "activation checkpoint mesh id")?;
    require_identifier(&checkpoint.region, "activation checkpoint region")?;
    require_nonempty(&checkpoint.created_at, "activation checkpoint created_at")?;
    if checkpoint.mesh_id != mesh_id {
        return Err(LifecycleError::InvalidArgument(format!(
            "activation checkpoint mesh_id {} does not match region mesh_id {mesh_id}",
            checkpoint.mesh_id
        )));
    }
    if checkpoint.region != region {
        return Err(LifecycleError::InvalidArgument(format!(
            "activation checkpoint region {} does not match requested region {region}",
            checkpoint.region
        )));
    }
    for stream in &checkpoint.required_streams {
        require_identifier(&stream.stream_family, "activation checkpoint stream family")?;
        require_identifier(&stream.partition, "activation checkpoint partition")?;
    }
    Ok(())
}

async fn validate_activation_checkpoint_streams(
    storage: &Storage,
    checkpoint: &ActivationCheckpoint,
) -> LifecycleResult<()> {
    let supplied_streams = checkpoint
        .required_streams
        .iter()
        .map(|stream| (stream.stream_family.as_str(), stream.partition.as_str()))
        .collect::<BTreeSet<_>>();
    for (stream_family, partition) in existing_control_stream_partitions(storage).await? {
        if !supplied_streams.contains(&(stream_family.as_str(), partition.as_str())) {
            return Err(LifecycleError::ActivationCheckpointNotReached {
                stream_family,
                partition,
                sequence: 1,
                expected_digest: "checkpoint-required".to_string(),
                reason: "activation checkpoint omits an existing control stream partition"
                    .to_string(),
            });
        }
    }

    for required in &checkpoint.required_streams {
        let Some(region_checkpoint) = read_control_checkpoint(
            storage,
            &checkpoint.region,
            &required.stream_family,
            &required.partition,
        )
        .await
        .map_err(|err| {
            LifecycleError::InvalidArgument(format!(
                "activation checkpoint could not read regional checkpoint {}/{} for {}: {err}",
                required.stream_family, required.partition, checkpoint.region
            ))
        })?
        else {
            return Err(activation_checkpoint_not_reached(
                required,
                "regional control checkpoint is absent".to_string(),
            ));
        };
        if region_checkpoint.mesh_id != checkpoint.mesh_id {
            return Err(activation_checkpoint_not_reached(
                required,
                format!(
                    "regional checkpoint mesh_id {} does not match activation checkpoint mesh_id {}",
                    region_checkpoint.mesh_id, checkpoint.mesh_id
                ),
            ));
        }
        if region_checkpoint.last_sequence < required.sequence {
            return Err(activation_checkpoint_not_reached(
                required,
                format!(
                    "regional checkpoint latest sequence is {}",
                    region_checkpoint.last_sequence.get()
                ),
            ));
        }
        if region_checkpoint.last_sequence == required.sequence {
            if region_checkpoint.last_digest.as_str() != required.digest.as_str() {
                return Err(activation_checkpoint_not_reached(
                    required,
                    format!(
                        "regional checkpoint digest mismatch at sequence {}",
                        required.sequence.get()
                    ),
                ));
            }
            continue;
        }

        let log = read_control_stream_log(storage, &required.stream_family, &required.partition)
            .await
            .map_err(|err| {
                LifecycleError::InvalidArgument(format!(
                    "activation checkpoint could not read control stream {}/{}: {err}",
                    required.stream_family, required.partition
                ))
            })?;
        let mut found_sequence = false;
        for record in log.records {
            if record.metadata.sequence == required.sequence {
                found_sequence = true;
                if record.metadata.record_digest.as_str() != required.digest.as_str() {
                    return Err(activation_checkpoint_not_reached(
                        required,
                        format!("digest mismatch at sequence {}", required.sequence.get()),
                    ));
                }
            }
        }
        if !found_sequence {
            return Err(activation_checkpoint_not_reached(
                required,
                format!(
                    "regional checkpoint is beyond sequence {}, but the required stream position is not available for digest validation",
                    region_checkpoint.last_sequence.get()
                ),
            ));
        }
    }
    Ok(())
}

async fn existing_control_stream_partitions(
    storage: &Storage,
) -> LifecycleResult<Vec<(String, String)>> {
    let mut streams = Vec::new();
    let stream_families = mesh_directory::RoutingRecordFamily::all()
        .into_iter()
        .map(|family| family.stream_family())
        .chain(lifecycle_control_stream_families().into_iter());
    for stream_family in stream_families {
        for partition in
            crate::mesh_control_stream::list_control_stream_partitions(storage, stream_family)
                .await
                .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?
        {
            streams.push((stream_family.to_string(), partition));
        }
    }
    Ok(streams)
}

fn activation_checkpoint_not_reached(
    required: &ActivationCheckpointStream,
    reason: String,
) -> LifecycleError {
    LifecycleError::ActivationCheckpointNotReached {
        stream_family: required.stream_family.clone(),
        partition: required.partition.clone(),
        sequence: required.sequence.get(),
        expected_digest: required.digest.to_string(),
        reason,
    }
}

fn cell_key(region: &str, cell_id: &str) -> LifecycleResult<String> {
    require_identifier(region, "region")?;
    require_identifier(cell_id, "cell id")?;
    Ok(format!("{region}/{cell_id}"))
}

fn require_identifier(value: &str, field: &str) -> LifecycleResult<()> {
    require_nonempty(value, field)?;
    if value
        .chars()
        .any(|ch| ch == '/' || ch == '\0' || ch.is_control())
    {
        return Err(LifecycleError::InvalidArgument(format!(
            "{field} contains an invalid character"
        )));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &str) -> LifecycleResult<()> {
    if value.trim().is_empty() {
        return Err(LifecycleError::InvalidArgument(format!(
            "{field} must not be empty"
        )));
    }
    Ok(())
}

fn timestamp_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> LifecycleResult<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> LifecycleResult<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| {
            LifecycleError::InvalidArgument(
                "CoreStore ref target is not a CoreObjectRef".to_string(),
            )
        })?;
    Ok(serde_json::from_slice(
        &URL_SAFE_NO_PAD.decode(encoded).map_err(|err| {
            LifecycleError::InvalidArgument(format!("CoreStore ref target is invalid: {err}"))
        })?,
    )?)
}

#[cfg(test)]
mod tests;
