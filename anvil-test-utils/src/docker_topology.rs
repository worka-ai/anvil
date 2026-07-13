use super::*;

async fn local_node_descriptors(
    admin_addrs: &[String],
    admin_token: &str,
) -> Vec<anvil::anvil_api::NodeDescriptor> {
    let mut descriptors = Vec::with_capacity(admin_addrs.len());
    for admin_addr in admin_addrs {
        let mut client = AdminServiceClient::connect(admin_addr.clone())
            .await
            .expect("connect Docker admin endpoint for local node descriptor");
        let mut request = tonic::Request::new(anvil::anvil_api::GetLocalNodeDescriptorRequest {});
        add_docker_admin_bearer(&mut request, admin_token);
        descriptors.push(
            client
                .get_local_node_descriptor(request)
                .await
                .expect("Docker admin GetLocalNodeDescriptor")
                .into_inner()
                .node
                .expect("local node descriptor response includes node"),
        );
    }
    descriptors
}

fn put_node_request(
    descriptor: &anvil::anvil_api::NodeDescriptor,
) -> anvil::anvil_api::PutNodeRequest {
    anvil::anvil_api::PutNodeRequest {
        node_id: descriptor.node_id.clone(),
        region_id: descriptor.region.clone(),
        cell_id: descriptor.cell_id.clone(),
        advertise_addr: descriptor.public_api_addr.clone(),
        state: "active".to_string(),
        capacity_json: "{}".to_string(),
        options: None,
        libp2p_peer_id: descriptor.libp2p_peer_id.clone(),
        receipt_signing_public_key_proto: descriptor.receipt_signing_public_key_proto.clone(),
        cluster_addrs: descriptor.public_cluster_addrs.clone(),
        capabilities: vec![
            "object".to_string(),
            "index".to_string(),
            "personaldb".to_string(),
            "metadata".to_string(),
            "gateway".to_string(),
            "admin".to_string(),
        ],
    }
}

fn bootstrap_topology_request(
    region: &str,
    descriptors: &[anvil::anvil_api::NodeDescriptor],
) -> anvil::anvil_api::BootstrapMeshTopologyRequest {
    let mut cells = BTreeSet::new();
    let cells = descriptors
        .iter()
        .filter_map(|descriptor| {
            cells
                .insert(descriptor.cell_id.clone())
                .then(|| anvil::anvil_api::PutCellRequest {
                    region_id: region.to_string(),
                    cell_id: descriptor.cell_id.clone(),
                    failure_domain: descriptor.cell_id.clone(),
                    state: "active".to_string(),
                    options: None,
                })
        })
        .collect();

    anvil::anvil_api::BootstrapMeshTopologyRequest {
        regions: vec![anvil::anvil_api::PutRegionRequest {
            region_id: region.to_string(),
            endpoint: format!("http://{region}.anvil-storage.test"),
            state: "active".to_string(),
            options: None,
        }],
        cells,
        nodes: descriptors.iter().map(put_node_request).collect(),
        canonical_coremeta_rows: Vec::new(),
    }
}

pub(super) async fn ensure_docker_topology(
    admin_addrs: &[String],
    admin_token: &str,
    region: &str,
) {
    let descriptors = local_node_descriptors(admin_addrs, admin_token).await;
    let topology = bootstrap_topology_request(region, &descriptors);

    // Genesis is installed directly into each node's local CoreMeta before any
    // node participates in quorum. Every node therefore starts normal writes
    // with the same active topology instead of independently replaying the
    // lifecycle API and producing divergent root histories.
    let mut seed_client = MeshControlServiceClient::connect(admin_addrs[0].clone())
        .await
        .expect("connect Docker seed admin endpoint for topology bootstrap");
    let mut seed_request = tonic::Request::new(topology.clone());
    add_docker_admin_bearer(&mut seed_request, admin_token);
    let seed = seed_client
        .bootstrap_mesh_topology(seed_request)
        .await
        .expect("Docker seed BootstrapMeshTopology")
        .into_inner();
    if seed.already_initialised {
        return;
    }
    assert!(
        !seed.canonical_coremeta_rows.is_empty(),
        "Docker seed topology bootstrap must return a canonical CoreMeta snapshot"
    );

    for admin_addr in admin_addrs.iter().skip(1) {
        let mut client = MeshControlServiceClient::connect(admin_addr.clone())
            .await
            .expect("connect Docker joining admin endpoint for topology bootstrap");
        let mut join = topology.clone();
        join.canonical_coremeta_rows = seed.canonical_coremeta_rows.clone();
        let mut request = tonic::Request::new(join);
        add_docker_admin_bearer(&mut request, admin_token);
        let response = client
            .bootstrap_mesh_topology(request)
            .await
            .expect("Docker joining BootstrapMeshTopology")
            .into_inner();
        assert!(
            !response.already_initialised,
            "fresh Docker joining node unexpectedly had an existing mesh topology"
        );
    }
}
