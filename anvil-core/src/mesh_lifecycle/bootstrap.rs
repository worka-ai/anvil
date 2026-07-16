use super::record_proto;
use super::*;
use crate::core_store::{
    CF_MESH, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, TABLE_MESH_NODE_ROW,
    TABLE_MESH_PARTITION_ROW,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BootstrapMeshLifecycleProjection {
    pub regions: Vec<CreateRegionDescriptor>,
    pub cells: Vec<RegisterCellDescriptor>,
    pub nodes: Vec<RegisterNodeDescriptor>,
}

pub fn install_bootstrap_lifecycle_projection(
    storage: &Storage,
    store: &CoreStore,
    input: BootstrapMeshLifecycleProjection,
) -> LifecycleResult<MeshLifecycleState> {
    let state = bootstrap_lifecycle_state(input)?;
    install_bootstrap_lifecycle_projection_state(storage, store, &state)?;
    Ok(state)
}

fn bootstrap_lifecycle_state(
    input: BootstrapMeshLifecycleProjection,
) -> LifecycleResult<MeshLifecycleState> {
    let now = timestamp_now();
    let mut state = MeshLifecycleState::default();

    for input in input.regions {
        require_identifier(&input.mesh_id, "bootstrap mesh id")?;
        require_identifier(&input.region, "bootstrap region")?;
        require_nonempty(&input.virtual_host_suffix, "bootstrap virtual host suffix")?;
        if let Some(default_cell) = &input.default_cell {
            require_identifier(default_cell, "bootstrap default cell")?;
        }
        if state.regions.contains_key(&input.region) {
            return Err(LifecycleError::AlreadyExists {
                resource_kind: "region",
                resource_id: input.region,
            });
        }
        let descriptor = RegionDescriptor {
            schema: REGION_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            region: input.region.clone(),
            state: LifecycleState::Active,
            public_base_url: input.public_base_url,
            virtual_host_suffix: input.virtual_host_suffix,
            placement_weight: input.placement_weight,
            default_cell: input.default_cell,
            created_at: now.clone(),
            updated_at: now.clone(),
            generation: 1,
        };
        state.regions.insert(descriptor.region.clone(), descriptor);
    }

    for input in input.cells {
        require_identifier(&input.mesh_id, "bootstrap mesh id")?;
        require_identifier(&input.region, "bootstrap cell region")?;
        require_identifier(&input.cell_id, "bootstrap cell id")?;
        require_identifier(&input.failure_domain, "bootstrap cell failure domain")?;
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
                resource_id: key,
            });
        }
        let descriptor = CellDescriptor {
            schema: CELL_DESCRIPTOR_SCHEMA.to_string(),
            mesh_id: input.mesh_id,
            region: input.region,
            cell_id: input.cell_id,
            state: LifecycleState::Active,
            placement_weight: input.placement_weight,
            failure_domain: input.failure_domain,
            created_at: now.clone(),
            updated_at: now.clone(),
            generation: 1,
        };
        state.cells.insert(key, descriptor);
    }

    for input in input.nodes {
        require_identifier(&input.mesh_id, "bootstrap mesh id")?;
        require_identifier(&input.node_id, "bootstrap node id")?;
        require_identifier(&input.region, "bootstrap node region")?;
        require_identifier(&input.cell_id, "bootstrap node cell id")?;
        require_nonempty(&input.libp2p_peer_id, "bootstrap node libp2p peer id")?;
        if input.receipt_signing_public_key_proto.is_empty() {
            return Err(LifecycleError::InvalidArgument(
                "bootstrap node receipt signing public key protobuf must not be empty".to_string(),
            ));
        }
        libp2p::identity::PublicKey::try_decode_protobuf(&input.receipt_signing_public_key_proto)
            .map_err(|err| {
            LifecycleError::InvalidArgument(format!(
                "bootstrap node receipt signing public key protobuf is invalid: {err}"
            ))
        })?;
        require_nonempty(&input.public_api_addr, "bootstrap node public api addr")?;
        if input.capabilities.is_empty() {
            return Err(LifecycleError::InvalidArgument(
                "bootstrap node capabilities must not be empty".to_string(),
            ));
        }
        if !state.regions.contains_key(&input.region) {
            return Err(LifecycleError::NotFound {
                resource_kind: "region",
                resource_id: input.region,
            });
        }
        let key = cell_key(&input.region, &input.cell_id)?;
        if !state.cells.contains_key(&key) {
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
            capacity_json_hash: capacity_json_hash(&input.capacity_json)?,
            state: LifecycleState::Active,
            drain: None,
            last_heartbeat_at: None,
            created_at: now.clone(),
            updated_at: now.clone(),
            generation: 1,
        };
        state.nodes.insert(descriptor.node_id.clone(), descriptor);
    }

    Ok(state)
}

fn install_bootstrap_lifecycle_projection_state(
    storage: &Storage,
    store: &CoreStore,
    state: &MeshLifecycleState,
) -> LifecycleResult<()> {
    let rows = encode_lifecycle_projection_rows(state)?;
    let mut desired_keys = BTreeSet::new();
    let mut tuple_keys = Vec::with_capacity(rows.len());
    let mut table_ids = Vec::with_capacity(rows.len());
    let mut payloads = Vec::with_capacity(rows.len());

    for row in rows {
        let table_id = lifecycle_projection_table_id(row.kind)?;
        let tuple_key = lifecycle_projection_row_key(row.kind, &row.record_key)?;
        desired_keys.insert((table_id, tuple_key.clone()));
        table_ids.push(table_id);
        tuple_keys.push(tuple_key);
        payloads.push(row.payload);
    }

    let prefix = lifecycle_projection_row_prefix()?;
    let mut delete_rows = Vec::new();
    for table_id in [TABLE_MESH_PARTITION_ROW, TABLE_MESH_NODE_ROW] {
        for row in store.scan_coremeta_prefix(CF_MESH, table_id, &prefix)? {
            let projection = record_proto::decode_lifecycle_projection_row(&row.payload)?;
            let (kind, record_key) = lifecycle_projection_descriptor_key(&projection)?;
            let tuple_key = lifecycle_projection_row_key(kind, &record_key)?;
            if !desired_keys.contains(&(table_id, tuple_key.clone())) {
                delete_rows.push((table_id, tuple_key));
            }
        }
    }

    let mut ops = Vec::with_capacity(payloads.len() + delete_rows.len());
    for index in 0..payloads.len() {
        ops.push(CoreMetaBatchOp {
            cf: CF_MESH,
            table_id: table_ids[index],
            tuple_key: &tuple_keys[index],
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payloads[index]),
        });
    }
    for (table_id, tuple_key) in &delete_rows {
        ops.push(CoreMetaBatchOp {
            cf: CF_MESH,
            table_id: *table_id,
            tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Delete,
        });
    }
    if ops.is_empty() {
        return Ok(());
    }
    CoreMetaStore::open(storage.core_store_meta_path())?.write_local_committed_batch(&ops)?;
    Ok(())
}
