use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreStore, CoreTransaction, CoreTransactionUpdate, TABLE_MESH_NODE_ROW,
    TABLE_MESH_PARTITION_ROW, core_meta_payload_digest, core_meta_tuple_key,
};
use crate::mesh_control_stream::{
    ControlMutationHeaderInput, ControlRecordDigest, ControlStreamFrame, ControlStreamSequence,
    read_control_checkpoint, read_control_stream_log,
};
use crate::mesh_directory::{self, BucketLocatorStatus};
use crate::partition_fence::{self, PartitionWritePermit};
use crate::routing::{self, HostAliasDescriptor, HostAliasState, RoutingConfig};
use crate::storage::Storage;
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

mod bootstrap;
mod host_alias;
mod record_proto;
pub use bootstrap::{BootstrapMeshLifecycleProjection, install_bootstrap_lifecycle_projection};
pub use host_alias::{
    create_host_alias, create_host_alias_in_transaction, list_host_aliases, transition_host_alias,
    transition_host_alias_in_transaction,
};

pub const REGION_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.region.v1";
pub const CELL_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.cell.v1";
pub const NODE_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.node.v1";
pub const ACTIVATION_CHECKPOINT_SCHEMA: &str = "anvil.mesh.activation_checkpoint.v1";
pub const BUCKET_DRAIN_EXCEPTION_SCHEMA: &str = "anvil.mesh.bucket_drain_exception.v1";
pub const REGION_DESCRIPTOR_STREAM_FAMILY: &str = "region_descriptor";
pub const CELL_DESCRIPTOR_STREAM_FAMILY: &str = "cell_descriptor";
pub const NODE_DESCRIPTOR_STREAM_FAMILY: &str = "node_descriptor";
const CONTROL_MUTATION_SCHEMA: &str = "anvil.mesh.control_mutation.v1";
const MESH_LIFECYCLE_PROJECTION_PARTITION_ID: &str = "mesh-lifecycle-projection";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshLifecycleCommittedResource {
    Region {
        region: String,
    },
    Cell {
        region: String,
        cell_id: String,
    },
    Node {
        region: String,
        cell_id: String,
        node_id: String,
    },
}

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
    Metadata,
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
    pub receipt_signing_public_key_proto: Vec<u8>,
    pub public_api_addr: String,
    pub public_cluster_addrs: Vec<String>,
    pub capabilities: Vec<NodeCapability>,
    pub capacity_json_hash: String,
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
    pub failure_domain: String,
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
    pub failure_domain: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterNodeDescriptor {
    pub mesh_id: String,
    pub node_id: String,
    pub region: String,
    pub cell_id: String,
    pub libp2p_peer_id: String,
    pub receipt_signing_public_key_proto: Vec<u8>,
    pub public_api_addr: String,
    pub public_cluster_addrs: Vec<String>,
    pub capabilities: Vec<NodeCapability>,
    pub capacity_json: String,
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

pub async fn read_state_with_core_store(
    storage: &Storage,
    store: &CoreStore,
) -> LifecycleResult<MeshLifecycleState> {
    let mut state = read_lifecycle_state_projection_with_core_store(store)?;
    overlay_lifecycle_control_streams_with_store(storage, store, &mut state).await?;
    Ok(state)
}

async fn read_state_for_transaction(
    storage: &Storage,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<MeshLifecycleState> {
    let mut state = read_state(storage).await?;
    let store = CoreStore::new(storage.clone()).await?;
    let transaction = store
        .read_explicit_transaction_for_principal(transaction_id, principal)
        .await?;
    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::CoreMetaPut {
            cf,
            table_id,
            payload,
            ..
        } = update
        else {
            continue;
        };
        if cf == CF_MESH && is_lifecycle_projection_table(*table_id) {
            apply_lifecycle_projection_row(&mut state, *table_id, payload)?;
        }
    }
    Ok(state)
}

async fn write_state(storage: &Storage, state: &MeshLifecycleState) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let rows = encode_lifecycle_projection_rows(state)?;
    let mut desired_keys = BTreeSet::new();
    let mut preconditions = Vec::with_capacity(rows.len());
    let mut operations = Vec::with_capacity(rows.len());
    for row in rows {
        let table_id = lifecycle_projection_table_id(row.kind)?;
        let tuple_key = lifecycle_projection_row_key(row.kind, &row.record_key)?;
        desired_keys.insert((table_id, tuple_key.clone()));
        let current = store.read_coremeta_row(CF_MESH, table_id, &tuple_key)?;
        preconditions.push(CoreMutationPrecondition::CoreMetaRow {
            cf: CF_MESH.to_string(),
            table_id,
            tuple_key: tuple_key.clone(),
            expected_payload_hash: current
                .as_ref()
                .map(|payload| core_meta_payload_digest(table_id, payload)),
            require_absent: current.is_none(),
            require_present: current.is_some(),
        });
        operations.push(CoreMutationOperation::CoreMetaPut {
            partition_id: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
            cf: CF_MESH.to_string(),
            table_id,
            tuple_key,
            payload: row.payload,
        });
    }
    let prefix = lifecycle_projection_row_prefix()?;
    for table_id in [TABLE_MESH_PARTITION_ROW, TABLE_MESH_NODE_ROW] {
        for row in store.scan_coremeta_prefix(CF_MESH, table_id, &prefix)? {
            let projection = record_proto::decode_lifecycle_projection_row(&row.payload)?;
            let (kind, record_key) = lifecycle_projection_descriptor_key(&projection)?;
            let tuple_key = lifecycle_projection_row_key(kind, &record_key)?;
            if desired_keys.contains(&(table_id, tuple_key.clone())) {
                continue;
            }
            preconditions.push(CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash: Some(core_meta_payload_digest(table_id, &row.payload)),
                require_absent: false,
                require_present: true,
            });
            operations.push(CoreMutationOperation::CoreMetaDelete {
                partition_id: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
                cf: CF_MESH.to_string(),
                table_id,
                tuple_key,
            });
        }
    }
    if operations.is_empty() {
        return Ok(());
    }
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("mesh-lifecycle-projection:{}", uuid::Uuid::new_v4()),
            scope_partition: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
            committed_by_principal: "mesh-lifecycle".to_string(),
            preconditions,
            operations,
        })
        .await?;
    Ok(())
}

async fn read_lifecycle_state_projection(storage: &Storage) -> LifecycleResult<MeshLifecycleState> {
    let store = CoreStore::new(storage.clone()).await?;
    read_lifecycle_state_projection_with_core_store(&store)
}

fn read_lifecycle_state_projection_with_core_store(
    store: &CoreStore,
) -> LifecycleResult<MeshLifecycleState> {
    let mut state = MeshLifecycleState::default();
    for table_id in [TABLE_MESH_PARTITION_ROW, TABLE_MESH_NODE_ROW] {
        let prefix = lifecycle_projection_row_prefix()?;
        for row in store.scan_coremeta_prefix(CF_MESH, table_id, &prefix)? {
            apply_lifecycle_projection_row(&mut state, table_id, &row.payload)?;
        }
    }
    Ok(state)
}

#[cfg(test)]
async fn delete_lifecycle_state_projection(storage: &Storage) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let prefix = lifecycle_projection_row_prefix()?;
    let mut preconditions = Vec::new();
    let mut operations = Vec::new();
    for table_id in [TABLE_MESH_PARTITION_ROW, TABLE_MESH_NODE_ROW] {
        for row in store.scan_coremeta_prefix(CF_MESH, table_id, &prefix)? {
            let projection = record_proto::decode_lifecycle_projection_row(&row.payload)?;
            let (kind, record_key) = lifecycle_projection_descriptor_key(&projection)?;
            let tuple_key = lifecycle_projection_row_key(kind, &record_key)?;
            preconditions.push(CoreMutationPrecondition::CoreMetaRow {
                cf: CF_MESH.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash: Some(core_meta_payload_digest(table_id, &row.payload)),
                require_absent: false,
                require_present: true,
            });
            operations.push(CoreMutationOperation::CoreMetaDelete {
                partition_id: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
                cf: CF_MESH.to_string(),
                table_id,
                tuple_key,
            });
        }
    }
    if operations.is_empty() {
        return Ok(());
    }
    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("mesh-lifecycle-projection-delete:{}", uuid::Uuid::new_v4()),
            scope_partition: MESH_LIFECYCLE_PROJECTION_PARTITION_ID.to_string(),
            committed_by_principal: "mesh-lifecycle-test".to_string(),
            preconditions,
            operations,
        })
        .await?;
    Ok(())
}

fn encode_lifecycle_projection_rows(
    state: &MeshLifecycleState,
) -> LifecycleResult<Vec<record_proto::EncodedLifecycleProjectionRow>> {
    let mut rows = Vec::new();
    for descriptor in state.regions.values() {
        rows.push(record_proto::encode_region_projection_row(descriptor)?);
    }
    for descriptor in state.cells.values() {
        rows.push(record_proto::encode_cell_projection_row(descriptor)?);
    }
    for descriptor in state.nodes.values() {
        rows.push(record_proto::encode_node_projection_row(descriptor)?);
    }
    for descriptor in state.host_aliases.values() {
        rows.push(record_proto::encode_host_alias_projection_row(descriptor)?);
    }
    for descriptor in state.bucket_drain_exceptions.values() {
        rows.push(record_proto::encode_bucket_drain_exception_projection_row(
            descriptor,
        )?);
    }
    Ok(rows)
}

fn lifecycle_projection_table_id(kind: &str) -> LifecycleResult<u16> {
    match kind {
        record_proto::LIFECYCLE_PROJECTION_NODE_KIND => Ok(TABLE_MESH_NODE_ROW),
        record_proto::LIFECYCLE_PROJECTION_REGION_KIND
        | record_proto::LIFECYCLE_PROJECTION_CELL_KIND
        | record_proto::LIFECYCLE_PROJECTION_HOST_ALIAS_KIND
        | record_proto::LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND => {
            Ok(TABLE_MESH_PARTITION_ROW)
        }
        _ => Err(LifecycleError::InvalidArgument(format!(
            "unknown mesh lifecycle projection kind {kind}"
        ))),
    }
}

fn is_lifecycle_projection_table(table_id: u16) -> bool {
    matches!(table_id, TABLE_MESH_PARTITION_ROW | TABLE_MESH_NODE_ROW)
}

fn lifecycle_projection_row_prefix() -> LifecycleResult<Vec<u8>> {
    Ok(core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(
        record_proto::LIFECYCLE_PROJECTION_ROW_PREFIX,
    )])?)
}

fn lifecycle_projection_row_key(kind: &str, record_key: &str) -> LifecycleResult<Vec<u8>> {
    Ok(core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(record_proto::LIFECYCLE_PROJECTION_ROW_PREFIX),
        CoreMetaTuplePart::Utf8(kind),
        CoreMetaTuplePart::Utf8(record_key),
    ])?)
}

async fn stage_lifecycle_projection_row_in_transaction(
    storage: &Storage,
    row: record_proto::EncodedLifecycleProjectionRow,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<()> {
    let table_id = lifecycle_projection_table_id(row.kind)?;
    let tuple_key = lifecycle_projection_row_key(row.kind, &row.record_key)?;
    let store = CoreStore::new(storage.clone()).await?;
    let current = transaction_visible_lifecycle_payload(
        &store,
        transaction_id,
        principal,
        table_id,
        &tuple_key,
    )
    .await?;
    store
        .stage_coremeta_put_in_transaction(
            transaction_id,
            principal,
            CF_MESH,
            table_id,
            tuple_key,
            row.payload,
            current
                .as_ref()
                .map(|payload| core_meta_payload_digest(table_id, payload)),
            current.is_none(),
            current.is_some(),
        )
        .await?;
    Ok(())
}

async fn transaction_visible_lifecycle_payload(
    store: &CoreStore,
    transaction_id: &str,
    principal: &str,
    table_id: u16,
    tuple_key: &[u8],
) -> LifecycleResult<Option<Vec<u8>>> {
    let transaction = store
        .read_explicit_transaction_for_principal(transaction_id, principal)
        .await?;
    for update in transaction.visible_updates.iter().rev() {
        match update {
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id: update_table_id,
                tuple_key: update_tuple_key,
                payload,
                ..
            } if cf == CF_MESH && *update_table_id == table_id && update_tuple_key == tuple_key => {
                return Ok(Some(payload.clone()));
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id: update_table_id,
                tuple_key: update_tuple_key,
                ..
            } if cf == CF_MESH && *update_table_id == table_id && update_tuple_key == tuple_key => {
                return Ok(None);
            }
            _ => {}
        }
    }
    Ok(store.read_coremeta_row(CF_MESH, table_id, tuple_key)?)
}

fn apply_lifecycle_projection_row(
    state: &mut MeshLifecycleState,
    table_id: u16,
    payload: &[u8],
) -> LifecycleResult<()> {
    let projection = record_proto::decode_lifecycle_projection_row(payload)?;
    match projection {
        record_proto::LifecycleProjectionDescriptor::Region(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_REGION_KIND,
            )?;
            state.regions.insert(descriptor.region.clone(), descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::Cell(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_CELL_KIND,
            )?;
            let key = cell_key(&descriptor.region, &descriptor.cell_id)?;
            state.cells.insert(key, descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::Node(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_NODE_KIND,
            )?;
            state.nodes.insert(descriptor.node_id.clone(), descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::HostAlias(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_HOST_ALIAS_KIND,
            )?;
            state
                .host_aliases
                .insert(descriptor.hostname.clone(), descriptor);
        }
        record_proto::LifecycleProjectionDescriptor::BucketDrainException(descriptor) => {
            ensure_lifecycle_projection_table(
                table_id,
                record_proto::LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND,
            )?;
            let key = bucket_drain_exception_key(
                &descriptor.region,
                &descriptor.tenant_id,
                &descriptor.bucket_name,
            );
            state.bucket_drain_exceptions.insert(key, descriptor);
        }
    }
    Ok(())
}

pub fn committed_topology_resources_from_transaction(
    transaction: &CoreTransaction,
) -> LifecycleResult<Vec<MeshLifecycleCommittedResource>> {
    let mut regions = BTreeSet::new();
    let mut cells = BTreeSet::new();
    let mut nodes = BTreeSet::new();

    for update in &transaction.visible_updates {
        let CoreTransactionUpdate::CoreMetaPut {
            cf,
            table_id,
            payload,
            ..
        } = update
        else {
            continue;
        };
        if cf != CF_MESH || !is_lifecycle_projection_table(*table_id) {
            continue;
        }

        let projection = record_proto::decode_lifecycle_projection_row(payload)?;
        match projection {
            record_proto::LifecycleProjectionDescriptor::Region(descriptor) => {
                ensure_lifecycle_projection_table(
                    *table_id,
                    record_proto::LIFECYCLE_PROJECTION_REGION_KIND,
                )?;
                regions.insert(descriptor.region);
            }
            record_proto::LifecycleProjectionDescriptor::Cell(descriptor) => {
                ensure_lifecycle_projection_table(
                    *table_id,
                    record_proto::LIFECYCLE_PROJECTION_CELL_KIND,
                )?;
                cells.insert((descriptor.region, descriptor.cell_id));
            }
            record_proto::LifecycleProjectionDescriptor::Node(descriptor) => {
                ensure_lifecycle_projection_table(
                    *table_id,
                    record_proto::LIFECYCLE_PROJECTION_NODE_KIND,
                )?;
                nodes.insert((descriptor.region, descriptor.cell_id, descriptor.node_id));
            }
            record_proto::LifecycleProjectionDescriptor::HostAlias(_)
            | record_proto::LifecycleProjectionDescriptor::BucketDrainException(_) => {}
        }
    }

    let mut out = Vec::with_capacity(regions.len() + cells.len() + nodes.len());
    out.extend(
        regions
            .into_iter()
            .map(|region| MeshLifecycleCommittedResource::Region { region }),
    );
    out.extend(
        cells
            .into_iter()
            .map(|(region, cell_id)| MeshLifecycleCommittedResource::Cell { region, cell_id }),
    );
    out.extend(nodes.into_iter().map(|(region, cell_id, node_id)| {
        MeshLifecycleCommittedResource::Node {
            region,
            cell_id,
            node_id,
        }
    }));
    Ok(out)
}

fn ensure_lifecycle_projection_table(table_id: u16, kind: &str) -> LifecycleResult<()> {
    if lifecycle_projection_table_id(kind)? != table_id {
        return Err(LifecycleError::InvalidArgument(format!(
            "mesh lifecycle projection kind {kind} is stored in the wrong CoreMeta table"
        )));
    }
    Ok(())
}

fn lifecycle_projection_descriptor_key(
    projection: &record_proto::LifecycleProjectionDescriptor,
) -> LifecycleResult<(&'static str, String)> {
    match projection {
        record_proto::LifecycleProjectionDescriptor::Region(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_REGION_KIND,
            descriptor.region.clone(),
        )),
        record_proto::LifecycleProjectionDescriptor::Cell(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_CELL_KIND,
            cell_key(&descriptor.region, &descriptor.cell_id)?,
        )),
        record_proto::LifecycleProjectionDescriptor::Node(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_NODE_KIND,
            node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?,
        )),
        record_proto::LifecycleProjectionDescriptor::HostAlias(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_HOST_ALIAS_KIND,
            descriptor.hostname.clone(),
        )),
        record_proto::LifecycleProjectionDescriptor::BucketDrainException(descriptor) => Ok((
            record_proto::LIFECYCLE_PROJECTION_BUCKET_DRAIN_EXCEPTION_KIND,
            bucket_drain_exception_key(
                &descriptor.region,
                &descriptor.tenant_id,
                &descriptor.bucket_name,
            ),
        )),
    }
}

async fn overlay_lifecycle_control_streams(
    storage: &Storage,
    state: &mut MeshLifecycleState,
) -> LifecycleResult<()> {
    let store = CoreStore::new(storage.clone()).await?;
    overlay_lifecycle_control_streams_with_store(storage, &store, state).await
}

async fn overlay_lifecycle_control_streams_with_store(
    storage: &Storage,
    store: &CoreStore,
    state: &mut MeshLifecycleState,
) -> LifecycleResult<()> {
    for stream_family in lifecycle_control_stream_families() {
        for partition in crate::mesh_control_stream::list_control_stream_partitions_with_store(
            store,
            stream_family,
        )
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
                    &record.frame.header_proto,
                    &record.frame.payload_proto,
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
    header_proto: &[u8],
    payload_proto: &[u8],
) -> LifecycleResult<()> {
    let header = crate::mesh_control_stream::decode_control_mutation_header(header_proto)
        .map_err(|err| LifecycleError::InvalidArgument(err.to_string()))?;
    let stream_family = header.stream_family.as_str();
    let partition = header.partition.as_str();
    if stream_family != expected_stream_family || partition != expected_partition {
        return Err(LifecycleError::InvalidArgument(format!(
            "lifecycle control frame scope {stream_family}/{partition} does not match path {expected_stream_family}/{expected_partition}"
        )));
    }
    let operation = header.operation.as_str();
    let record_key = if header.record_key.trim().is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "lifecycle control frame missing record_key".to_string(),
        ));
    } else {
        header.record_key.as_str()
    };
    if matches!(operation, "delete" | "tombstone") {
        remove_lifecycle_projection(state, stream_family, record_key)?;
        return Ok(());
    }
    match record_proto::decode_lifecycle_control_payload(stream_family, payload_proto)? {
        record_proto::LifecycleControlDescriptor::Region(descriptor) => {
            if descriptor.region != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "region descriptor key mismatch: expected {record_key}, got {}",
                    descriptor.region
                )));
            }
            state.regions.insert(descriptor.region.clone(), descriptor);
        }
        record_proto::LifecycleControlDescriptor::Cell(descriptor) => {
            let key = cell_record_key(&descriptor.region, &descriptor.cell_id)?;
            if key != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "cell descriptor key mismatch: expected {record_key}, got {key}"
                )));
            }
            state.cells.insert(key, descriptor);
        }
        record_proto::LifecycleControlDescriptor::Node(descriptor) => {
            let key =
                node_record_key(&descriptor.region, &descriptor.cell_id, &descriptor.node_id)?;
            if key != record_key {
                return Err(LifecycleError::InvalidArgument(format!(
                    "node descriptor key mismatch: expected {record_key}, got {key}"
                )));
            }
            state.nodes.insert(descriptor.node_id.clone(), descriptor);
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

pub async fn put_region_in_transaction(
    storage: &Storage,
    input: CreateRegionDescriptor,
    target: Option<LifecycleState>,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<RegionDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.region, "region")?;
    require_nonempty(&input.virtual_host_suffix, "virtual host suffix")?;
    if let Some(default_cell) = &input.default_cell {
        require_identifier(default_cell, "default cell")?;
    }

    let mut state = read_state_for_transaction(storage, transaction_id, principal).await?;
    let mut descriptor = if let Some(existing) = state.regions.get(&input.region).cloned() {
        if !input.public_base_url.is_empty() && existing.public_base_url != input.public_base_url {
            return Err(LifecycleError::InvalidArgument(format!(
                "region {} already exists with endpoint {}",
                existing.region, existing.public_base_url
            )));
        }
        existing
    } else {
        require_nonempty(&input.public_base_url, "public base url")?;
        let now = timestamp_now();
        RegionDescriptor {
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
        }
    };

    if let Some(target) = target
        && descriptor.state != target
    {
        validate_region_transition(descriptor.state, target).map_err(|_| {
            LifecycleError::LifecycleTransitionDenied {
                resource_kind: "region",
                resource_id: descriptor.region.clone(),
                from: descriptor.state,
                to: target,
            }
        })?;
        ensure_region_drain_completion_is_supported(storage, &descriptor.region, target).await?;
        descriptor.state = target;
        descriptor.updated_at = timestamp_now();
        descriptor.generation = descriptor.generation.saturating_add(1);
    }

    state
        .regions
        .insert(descriptor.region.clone(), descriptor.clone());
    stage_lifecycle_projection_row_in_transaction(
        storage,
        record_proto::encode_region_projection_row(&descriptor)?,
        transaction_id,
        principal,
    )
    .await?;
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
    require_identifier(&input.failure_domain, "cell failure domain")?;

    let mut state = read_state(storage).await?;
    if !state.regions.contains_key(&input.region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: input.region.clone(),
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
        failure_domain: input.failure_domain,
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

pub async fn put_cell_in_transaction(
    storage: &Storage,
    input: RegisterCellDescriptor,
    target: Option<LifecycleState>,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<CellDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.region, "region")?;
    require_identifier(&input.cell_id, "cell id")?;
    require_identifier(&input.failure_domain, "cell failure domain")?;

    let mut state = read_state_for_transaction(storage, transaction_id, principal).await?;
    if !state.regions.contains_key(&input.region) {
        return Err(LifecycleError::NotFound {
            resource_kind: "region",
            resource_id: input.region.clone(),
        });
    }
    let key = cell_key(&input.region, &input.cell_id)?;
    let mut descriptor = if let Some(existing) = state.cells.get(&key).cloned() {
        if existing.failure_domain != input.failure_domain {
            return Err(LifecycleError::InvalidArgument(format!(
                "cell {}/{} already exists with failure domain {}",
                existing.region, existing.cell_id, existing.failure_domain
            )));
        }
        existing
    } else {
        let now = timestamp_now();
        CellDescriptor {
            schema: CELL_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            region: input.region,
            cell_id: input.cell_id,
            state: LifecycleState::Joining,
            placement_weight: input.placement_weight,
            failure_domain: input.failure_domain,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        }
    };

    if let Some(target) = target
        && descriptor.state != target
    {
        validate_region_transition(descriptor.state, target).map_err(|_| {
            LifecycleError::LifecycleTransitionDenied {
                resource_kind: "cell",
                resource_id: descriptor.cell_id.clone(),
                from: descriptor.state,
                to: target,
            }
        })?;
        descriptor.state = target;
        descriptor.updated_at = timestamp_now();
        descriptor.generation = descriptor.generation.saturating_add(1);
    }

    let key = cell_key(&descriptor.region, &descriptor.cell_id)?;
    state.cells.insert(key, descriptor.clone());
    stage_lifecycle_projection_row_in_transaction(
        storage,
        record_proto::encode_cell_projection_row(&descriptor)?,
        transaction_id,
        principal,
    )
    .await?;
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
    if input.receipt_signing_public_key_proto.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "receipt signing public key protobuf must not be empty".to_string(),
        ));
    }
    libp2p::identity::PublicKey::try_decode_protobuf(&input.receipt_signing_public_key_proto)
        .map_err(|err| {
            LifecycleError::InvalidArgument(format!(
                "receipt signing public key protobuf is invalid: {err}"
            ))
        })?;
    require_nonempty(&input.public_api_addr, "public api addr")?;
    if input.capabilities.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "node capabilities must not be empty".to_string(),
        ));
    }
    let capacity_json_hash = capacity_json_hash(&input.capacity_json)?;

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
            resource_id: input.cell_id.clone(),
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
        receipt_signing_public_key_proto: input.receipt_signing_public_key_proto,
        public_api_addr: input.public_api_addr,
        public_cluster_addrs: input.public_cluster_addrs,
        capabilities: input.capabilities,
        capacity_json_hash,
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

pub async fn put_node_in_transaction(
    storage: &Storage,
    input: RegisterNodeDescriptor,
    target: Option<LifecycleState>,
    transaction_id: &str,
    principal: &str,
) -> LifecycleResult<NodeDescriptor> {
    require_identifier(&input.mesh_id, "mesh id")?;
    require_identifier(&input.node_id, "node id")?;
    require_identifier(&input.region, "region")?;
    require_identifier(&input.cell_id, "cell id")?;
    require_nonempty(&input.libp2p_peer_id, "libp2p peer id")?;
    if input.receipt_signing_public_key_proto.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "receipt signing public key protobuf must not be empty".to_string(),
        ));
    }
    libp2p::identity::PublicKey::try_decode_protobuf(&input.receipt_signing_public_key_proto)
        .map_err(|err| {
            LifecycleError::InvalidArgument(format!(
                "receipt signing public key protobuf is invalid: {err}"
            ))
        })?;
    require_nonempty(&input.public_api_addr, "public api addr")?;
    if input.capabilities.is_empty() {
        return Err(LifecycleError::InvalidArgument(
            "node capabilities must not be empty".to_string(),
        ));
    }
    let capacity_json_hash = capacity_json_hash(&input.capacity_json)?;

    let mut state = read_state_for_transaction(storage, transaction_id, principal).await?;
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
    let mut descriptor = if let Some(existing) = state.nodes.get(&input.node_id).cloned() {
        if existing.region != input.region
            || existing.cell_id != input.cell_id
            || existing.libp2p_peer_id != input.libp2p_peer_id
            || existing.receipt_signing_public_key_proto != input.receipt_signing_public_key_proto
            || existing.public_api_addr != input.public_api_addr
            || existing.public_cluster_addrs != input.public_cluster_addrs
            || existing.capabilities != input.capabilities
            || existing.capacity_json_hash != capacity_json_hash
        {
            return Err(LifecycleError::InvalidArgument(format!(
                "node {} already exists with different immutable descriptor fields",
                existing.node_id
            )));
        }
        existing
    } else {
        let now = timestamp_now();
        NodeDescriptor {
            schema: NODE_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            node_id: input.node_id.clone(),
            region: input.region,
            cell_id: input.cell_id,
            libp2p_peer_id: input.libp2p_peer_id,
            receipt_signing_public_key_proto: input.receipt_signing_public_key_proto,
            public_api_addr: input.public_api_addr,
            public_cluster_addrs: input.public_cluster_addrs,
            capabilities: input.capabilities,
            capacity_json_hash,
            state: LifecycleState::Joining,
            drain: None,
            last_heartbeat_at: None,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        }
    };

    if let Some(target) = target
        && descriptor.state != target
    {
        if target == LifecycleState::Active {
            ensure_node_placement_is_active(&state, &descriptor)?;
        }
        validate_node_transition(descriptor.state, target).map_err(|_| {
            LifecycleError::LifecycleTransitionDenied {
                resource_kind: "node",
                resource_id: descriptor.node_id.clone(),
                from: descriptor.state,
                to: target,
            }
        })?;
        descriptor.state = target;
        descriptor.drain = None;
        descriptor.updated_at = timestamp_now();
        descriptor.generation = descriptor.generation.saturating_add(1);
    }

    state
        .nodes
        .insert(descriptor.node_id.clone(), descriptor.clone());
    stage_lifecycle_projection_row_in_transaction(
        storage,
        record_proto::encode_node_projection_row(&descriptor)?,
        transaction_id,
        principal,
    )
    .await?;
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
    let store = CoreStore::new(storage.clone()).await?;
    list_nodes_with_core_store(storage, &store, region_filter, cell_filter).await
}

pub async fn list_nodes_with_core_store(
    storage: &Storage,
    store: &CoreStore,
    region_filter: Option<&str>,
    cell_filter: Option<&str>,
) -> LifecycleResult<Vec<NodeDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    if let Some(cell_id) = cell_filter.filter(|cell_id| !cell_id.is_empty()) {
        require_identifier(cell_id, "cell id")?;
    }
    let nodes = read_state_with_core_store(storage, store)
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

pub fn list_node_projections_with_core_store(
    store: &CoreStore,
    region_filter: Option<&str>,
    cell_filter: Option<&str>,
) -> LifecycleResult<Vec<NodeDescriptor>> {
    if let Some(region) = region_filter.filter(|region| !region.is_empty()) {
        require_identifier(region, "region")?;
    }
    if let Some(cell_id) = cell_filter.filter(|cell| !cell.is_empty()) {
        require_identifier(cell_id, "cell id")?;
    }
    let nodes = read_lifecycle_state_projection_with_core_store(store)?
        .nodes
        .into_values()
        .filter(|node| {
            region_filter.is_none_or(|region| region.is_empty() || node.region == region)
        })
        .filter(|node| cell_filter.is_none_or(|cell| cell.is_empty() || node.cell_id == cell))
        .collect();
    Ok(nodes)
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

mod helpers;
pub use helpers::*;

pub(crate) fn control_payload_operator_json(
    stream_family: &str,
    record_key: &str,
    payload_proto: &[u8],
) -> LifecycleResult<Vec<u8>> {
    record_proto::control_payload_operator_json(stream_family, record_key, payload_proto)
}

#[cfg(test)]
mod tests;
