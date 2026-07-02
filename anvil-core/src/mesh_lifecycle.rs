use crate::routing::{self, HostAliasDescriptor, HostAliasState, RoutingConfig};
use crate::storage::Storage;
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use thiserror::Error;
use tokio::io::AsyncWriteExt;

pub const REGION_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.region.v1";
pub const CELL_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.cell.v1";
pub const NODE_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.node.v1";

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
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MeshLifecycleState {
    pub regions: BTreeMap<String, RegionDescriptor>,
    pub cells: BTreeMap<String, CellDescriptor>,
    pub nodes: BTreeMap<String, NodeDescriptor>,
    #[serde(default)]
    pub host_aliases: BTreeMap<String, HostAliasDescriptor>,
}

pub async fn read_state(storage: &Storage) -> LifecycleResult<MeshLifecycleState> {
    match tokio::fs::read(storage.mesh_lifecycle_state_path()).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(MeshLifecycleState::default()),
        Err(err) => Err(err.into()),
    }
}

async fn write_state(storage: &Storage, state: &MeshLifecycleState) -> LifecycleResult<()> {
    let path = storage.mesh_lifecycle_state_path();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let tmp_path = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::File::create(&tmp_path).await?;
    let bytes = serde_json::to_vec_pretty(state)?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(tmp_path, path).await?;
    Ok(())
}

pub async fn create_region(
    storage: &Storage,
    input: CreateRegionDescriptor,
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
    write_state(storage, &state).await?;
    Ok(descriptor)
}

pub async fn transition_region(
    storage: &Storage,
    region: &str,
    expected_generation: u64,
    target: LifecycleState,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(region, "region")?;
    let mut state = read_state(storage).await?;
    let descriptor = state
        .regions
        .get_mut(region)
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
    descriptor.state = target;
    descriptor.updated_at = timestamp_now();
    descriptor.generation = descriptor.generation.saturating_add(1);
    let out = descriptor.clone();
    write_state(storage, &state).await?;
    Ok(out)
}

pub async fn list_regions(storage: &Storage) -> LifecycleResult<Vec<RegionDescriptor>> {
    Ok(read_state(storage).await?.regions.into_values().collect())
}

pub async fn register_cell(
    storage: &Storage,
    input: RegisterCellDescriptor,
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
    if region.state != LifecycleState::Active || cell.state != LifecycleState::Active {
        return Err(LifecycleError::InvalidArgument(
            "node activation requires active region and cell".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn node_state_machine_rejects_invalid_transitions() {
        validate_node_transition(LifecycleState::Joining, LifecycleState::Active).unwrap();
        assert!(
            validate_node_transition(LifecycleState::Joining, LifecycleState::Draining).is_err()
        );
        assert!(validate_node_transition(LifecycleState::Active, LifecycleState::Removed).is_err());
        assert!(validate_node_transition(LifecycleState::Removed, LifecycleState::Active).is_err());
    }

    #[test]
    fn region_state_machine_rejects_invalid_transitions() {
        validate_region_transition(LifecycleState::Joining, LifecycleState::Active).unwrap();
        validate_region_transition(LifecycleState::Active, LifecycleState::ReadOnly).unwrap();
        assert!(
            validate_region_transition(LifecycleState::Joining, LifecycleState::Draining).is_err()
        );
        assert!(
            validate_region_transition(LifecycleState::Active, LifecycleState::Removed).is_err()
        );
    }

    #[tokio::test]
    async fn lifecycle_store_persists_descriptors_and_enforces_transitions() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        let region = create_region(
            &storage,
            CreateRegionDescriptor {
                mesh_id: "mesh-a".to_string(),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: Some("cell-a".to_string()),
            },
        )
        .await
        .unwrap();
        assert_eq!(region.state, LifecycleState::Joining);

        let cell = register_cell(
            &storage,
            RegisterCellDescriptor {
                mesh_id: "mesh-a".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                placement_weight: 100,
            },
        )
        .await
        .unwrap();
        let cell = transition_cell(
            &storage,
            "eu-west-1",
            "cell-a",
            cell.generation,
            LifecycleState::Active,
        )
        .await
        .unwrap();
        assert_eq!(cell.state, LifecycleState::Active);

        let region = transition_region(
            &storage,
            "eu-west-1",
            region.generation,
            LifecycleState::Active,
        )
        .await
        .unwrap();
        assert_eq!(region.state, LifecycleState::Active);

        let node = register_node(
            &storage,
            RegisterNodeDescriptor {
                mesh_id: "mesh-a".to_string(),
                node_id: "node-a".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                libp2p_peer_id: "peer-a".to_string(),
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
                capabilities: vec![NodeCapability::Object, NodeCapability::Admin],
            },
        )
        .await
        .unwrap();
        assert!(
            transition_node(
                &storage,
                "node-a",
                node.generation,
                LifecycleState::Draining,
                None,
            )
            .await
            .is_err()
        );

        let node = transition_node(
            &storage,
            "node-a",
            node.generation,
            LifecycleState::Active,
            None,
        )
        .await
        .unwrap();
        assert!(
            transition_node(
                &storage,
                "node-a",
                node.generation,
                LifecycleState::Removed,
                None,
            )
            .await
            .is_err()
        );
        let node = transition_node(
            &storage,
            "node-a",
            node.generation,
            LifecycleState::Draining,
            Some(NodeDrainDescriptor {
                started_at: timestamp_now(),
                graceful_timeout_ms: 1000,
                force_after_timeout: false,
            }),
        )
        .await
        .unwrap();
        assert_eq!(node.state, LifecycleState::Draining);

        let replayed = read_state(&storage).await.unwrap();
        assert_eq!(replayed.nodes["node-a"].state, LifecycleState::Draining);
    }

    #[tokio::test]
    async fn host_aliases_are_generation_checked_and_region_bound() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let routing_config = RoutingConfig::new("anvil-storage.com").unwrap();

        let region = create_region(
            &storage,
            CreateRegionDescriptor {
                mesh_id: "mesh-a".to_string(),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.com".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.com".to_string(),
                placement_weight: 100,
                default_cell: None,
            },
        )
        .await
        .unwrap();
        transition_region(
            &storage,
            "eu-west-1",
            region.generation,
            LifecycleState::Active,
        )
        .await
        .unwrap();

        let alias = create_host_alias(
            &storage,
            &routing_config,
            CreateHostAliasDescriptor {
                hostname: "CDN.Example.Com.".to_string(),
                tenant_id: "tenant-acme".to_string(),
                bucket_name: "releases".to_string(),
                region: "eu-west-1".to_string(),
                prefix: "public/".to_string(),
            },
        )
        .await
        .unwrap();

        assert_eq!(alias.hostname, "cdn.example.com");
        assert_eq!(alias.state, HostAliasState::PendingVerification);
        let stale = transition_host_alias(&storage, "cdn.example.com", 99, HostAliasState::Active)
            .await
            .unwrap_err();
        assert!(matches!(stale, LifecycleError::GenerationConflict { .. }));

        let active = transition_host_alias(
            &storage,
            "cdn.example.com",
            alias.generation,
            HostAliasState::Active,
        )
        .await
        .unwrap();
        assert_eq!(active.state, HostAliasState::Active);
        assert_eq!(active.generation, 2);

        let aliases = list_host_aliases(&storage, Some("eu-west-1"))
            .await
            .unwrap();
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].hostname, "cdn.example.com");
    }
}
