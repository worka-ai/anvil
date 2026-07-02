use anvil::anvil_api::admin_service_client::AdminServiceClient;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::*;
use anvil_test_utils::wait_for_port;
use std::time::Duration;
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tonic::Code;

struct AdminNode {
    public_url: String,
    admin_url: String,
    state: anvil::AppState,
    handle: JoinHandle<()>,
    _temp: TempDir,
}

impl Drop for AdminNode {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn spawn_admin_node() -> AdminNode {
    let temp = tempfile::tempdir().unwrap();
    let storage_path = temp.path().join("node-a");
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let public_addr = public_listener.local_addr().unwrap();
    let admin_addr = admin_listener.local_addr().unwrap();

    let config = anvil::config::Config {
        cluster_secret: Some("test-cluster-secret".to_string()),
        jwt_secret: "test-secret".to_string(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        cluster_listen_addr: "/ip4/127.0.0.1/udp/0/quic-v1".to_string(),
        public_cluster_addrs: vec![],
        metadata_cache_ttl_secs: 1,
        public_api_addr: format!("http://{public_addr}"),
        api_listen_addr: public_addr.to_string(),
        admin_listen_addr: admin_addr.to_string(),
        mesh_id: "mesh-test".to_string(),
        region: "eu-west-1".to_string(),
        cell_id: "cell-a".to_string(),
        public_region_base_domain: "eu-west-1.anvil-storage.test".to_string(),
        bootstrap_addrs: vec![],
        init_cluster: false,
        enable_mdns: false,
        storage_path: storage_path.to_string_lossy().into_owned(),
        node_id_path: storage_path.join("node-id").to_string_lossy().into_owned(),
        cluster_keypair_path: storage_path
            .join("cluster-keypair.pb")
            .to_string_lossy()
            .into_owned(),
        personaldb_snapshot_entry_threshold: 1024,
        personaldb_snapshot_payload_bytes_threshold: 64 * 1024 * 1024,
        ..anvil::config::Config::default()
    };

    let state = anvil::AppState::new(config, None).await.unwrap();
    let swarm = anvil::cluster::create_swarm(state.config.clone())
        .await
        .unwrap();
    let state_for_handle = state.clone();
    let handle = tokio::spawn(async move {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        anvil::start_node_with_admin_listener(
            public_listener,
            Some(admin_listener),
            state_for_handle,
            swarm,
            rx,
        )
        .await
        .unwrap();
    });

    assert!(wait_for_port(public_addr, Duration::from_secs(5)).await);
    assert!(wait_for_port(admin_addr, Duration::from_secs(5)).await);

    AdminNode {
        public_url: format!("http://{public_addr}"),
        admin_url: format!("http://{admin_addr}"),
        state,
        handle,
        _temp: temp,
    }
}

fn admin_token(node: &AdminNode) -> String {
    node.state
        .jwt_manager
        .mint_token(
            "admin-principal".to_string(),
            vec!["anvil_admin:*|anvil_admin:cluster:mesh-test".to_string()],
            0,
        )
        .unwrap()
}

fn non_admin_token(node: &AdminNode) -> String {
    node.state
        .jwt_manager
        .mint_token(
            "object-principal".to_string(),
            vec!["bucket:read|*".to_string()],
            0,
        )
        .unwrap()
}

fn with_auth<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn context(label: &str, expected_generation: u64) -> AdminRequestContext {
    AdminRequestContext {
        request_id: format!("req-{label}"),
        idempotency_key: format!("idem-{label}"),
        audit_reason: format!("test {label}"),
        expected_generation,
    }
}

fn empty_activation_checkpoint_json(mesh_id: &str, region: &str) -> String {
    serde_json::json!({
        "schema": anvil::mesh_lifecycle::ACTIVATION_CHECKPOINT_SCHEMA,
        "mesh_id": mesh_id,
        "region": region,
        "created_at": "2026-07-02T00:00:00Z",
        "required_streams": []
    })
    .to_string()
}

async fn activation_checkpoint_json_from_existing_streams(
    node: &AdminNode,
    region: &str,
) -> String {
    let mut required_streams = Vec::new();
    for family in anvil::mesh_directory::RoutingRecordFamily::all() {
        let stream_family = family.stream_family();
        let family_path = node
            .state
            .storage
            .mesh_control_stream_family_path(stream_family)
            .unwrap();
        let mut entries = match tokio::fs::read_dir(&family_path).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => panic!("read control stream directory {family_path:?}: {err}"),
        };
        while let Some(entry) = entries.next_entry().await.unwrap() {
            let path = entry.path();
            if path.extension().and_then(|value| value.to_str()) != Some("anlog") {
                continue;
            }
            let Some(partition) = path
                .file_stem()
                .and_then(|value| value.to_str())
                .map(str::to_string)
            else {
                continue;
            };
            if node
                .state
                .storage
                .mesh_control_stream_path(stream_family, &partition)
                .is_err()
            {
                continue;
            }
            let log = anvil::mesh_control_stream::read_control_stream_log(&path)
                .await
                .unwrap();
            let Some(record) = log.records.last() else {
                continue;
            };
            anvil::mesh_control_stream::write_control_checkpoint(
                &node.state.storage,
                &anvil::mesh_control_stream::ControlCheckpointRecord::new(
                    "mesh-test",
                    region,
                    stream_family,
                    &partition,
                    record.metadata.sequence,
                    record.metadata.record_digest.clone(),
                    "2026-07-02T00:00:00Z",
                ),
            )
            .await
            .unwrap();
            required_streams.push(serde_json::json!({
                "stream_family": stream_family,
                "partition": partition,
                "sequence": record.metadata.sequence.get(),
                "digest": record.metadata.record_digest.as_str(),
            }));
        }
    }
    serde_json::json!({
        "schema": anvil::mesh_lifecycle::ACTIVATION_CHECKPOINT_SCHEMA,
        "mesh_id": "mesh-test",
        "region": region,
        "created_at": "2026-07-02T00:00:00Z",
        "required_streams": required_streams,
    })
    .to_string()
}

fn missing_activation_checkpoint_json(mesh_id: &str, region: &str) -> String {
    serde_json::json!({
        "schema": anvil::mesh_lifecycle::ACTIVATION_CHECKPOINT_SCHEMA,
        "mesh_id": mesh_id,
        "region": region,
        "created_at": "2026-07-02T00:00:00Z",
        "required_streams": [
            {
                "stream_family": "bucket_locator",
                "partition": "0a7f",
                "sequence": 1,
                "digest": "blake3:0000000000000000000000000000000000000000000000000000000000000000"
            }
        ]
    })
    .to_string()
}

async fn prepare_active_region_dependencies(
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
    label: &str,
    region: &str,
    cell_id: &str,
    node_id: &str,
) {
    let cell = client
        .register_cell(with_auth(
            tonic::Request::new(RegisterCellRequest {
                context: Some(context(&format!("{label}-register-cell"), 0)),
                region: region.to_string(),
                cell_id: cell_id.to_string(),
                placement_weight: 100,
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();
    let cell = client
        .activate_cell(with_auth(
            tonic::Request::new(ActivateCellRequest {
                context: Some(context(&format!("{label}-activate-cell"), cell.generation)),
                region: region.to_string(),
                cell_id: cell_id.to_string(),
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();
    assert_eq!(cell.state, 2);

    let node = client
        .register_node(with_auth(
            tonic::Request::new(RegisterNodeRequest {
                context: Some(context(&format!("{label}-register-node"), 0)),
                node_id: node_id.to_string(),
                region: region.to_string(),
                cell_id: cell_id.to_string(),
                libp2p_peer_id: format!("peer-{label}"),
                public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                capabilities: vec![1, 5],
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    let node = client
        .activate_node(with_auth(
            tonic::Request::new(ActivateNodeRequest {
                context: Some(context(&format!("{label}-activate-node"), node.generation)),
                node_id: node_id.to_string(),
            }),
            token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(node.state, 2);
}

#[tokio::test]
async fn admin_service_is_absent_public_present_admin_and_requires_auth() {
    let node = spawn_admin_node().await;
    let admin_token = admin_token(&node);
    let non_admin_token = non_admin_token(&node);

    let mut public_client = AdminServiceClient::connect(node.public_url.clone())
        .await
        .unwrap();
    let public_err = public_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &admin_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(public_err.code(), Code::Unimplemented);

    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();
    let unauthenticated = admin_client
        .list_regions(tonic::Request::new(ListRegionsRequest { page: None }))
        .await
        .unwrap_err();
    assert_eq!(unauthenticated.code(), Code::Unauthenticated);

    let denied = admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &non_admin_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let response = admin_client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &admin_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(response.regions.is_empty());
}

#[tokio::test]
async fn admin_lifecycle_rejects_invalid_region_cell_and_node_transitions() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let region = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context("create-region", 0)),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(region.state, 1);

    let drain_joining_region = client
        .drain_region(with_auth(
            tonic::Request::new(DrainRegionRequest {
                context: Some(context("drain-joining-region", region.generation)),
                region: "eu-west-1".to_string(),
                default_disposition: 1,
                bucket_overrides: vec![],
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(drain_joining_region.code(), Code::FailedPrecondition);

    let cell = client
        .register_cell(with_auth(
            tonic::Request::new(RegisterCellRequest {
                context: Some(context("register-cell", 0)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                placement_weight: 100,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();

    let drain_joining_cell = client
        .drain_cell(with_auth(
            tonic::Request::new(DrainCellRequest {
                context: Some(context("drain-joining-cell", cell.generation)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(drain_joining_cell.code(), Code::FailedPrecondition);

    let cell = client
        .activate_cell(with_auth(
            tonic::Request::new(ActivateCellRequest {
                context: Some(context("activate-cell", cell.generation)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();
    assert_eq!(cell.state, 2);

    let registered_node = client
        .register_node(with_auth(
            tonic::Request::new(RegisterNodeRequest {
                context: Some(context("register-node", 0)),
                node_id: "node-a".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                libp2p_peer_id: "peer-a".to_string(),
                public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                capabilities: vec![1, 5],
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(registered_node.state, 1);

    let drain_joining_node = client
        .drain_node(with_auth(
            tonic::Request::new(DrainNodeRequest {
                context: Some(context("drain-joining-node", registered_node.generation)),
                node_id: "node-a".to_string(),
                graceful_timeout_ms: 1000,
                force_after_timeout: false,
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(drain_joining_node.code(), Code::FailedPrecondition);

    let active_node = client
        .activate_node(with_auth(
            tonic::Request::new(ActivateNodeRequest {
                context: Some(context("activate-node", registered_node.generation)),
                node_id: "node-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(active_node.state, 2);

    let not_reached = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context(
                    "activate-region-missing-checkpoint",
                    region.generation,
                )),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: missing_activation_checkpoint_json(
                    "mesh-test",
                    "eu-west-1",
                ),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(not_reached.code(), Code::FailedPrecondition);
    assert!(
        not_reached
            .message()
            .contains("ActivationCheckpointNotReached")
    );

    let region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("activate-region", region.generation)),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: empty_activation_checkpoint_json(
                    "mesh-test",
                    "eu-west-1",
                ),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(region.state, 2);

    let read_only_region = client
        .set_region_read_only(with_auth(
            tonic::Request::new(SetRegionReadOnlyRequest {
                context: Some(context("set-region-read-only", region.generation)),
                region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(read_only_region.state, 3);
    assert_eq!(read_only_region.generation, region.generation + 1);

    let listed_regions = client
        .list_regions(with_auth(
            tonic::Request::new(ListRegionsRequest { page: None }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed_regions.regions.len(), 1);
    assert_eq!(listed_regions.regions[0].state, 3);
    assert_eq!(
        listed_regions.regions[0].generation,
        read_only_region.generation
    );

    let region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context(
                    "reactivate-read-only-region",
                    read_only_region.generation,
                )),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: empty_activation_checkpoint_json(
                    "mesh-test",
                    "eu-west-1",
                ),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    assert_eq!(region.state, 2);

    let remove_active_region = client
        .remove_region(with_auth(
            tonic::Request::new(RemoveRegionRequest {
                context: Some(context("remove-active-region", region.generation)),
                region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(remove_active_region.code(), Code::FailedPrecondition);

    let remove_active_node = client
        .remove_node(with_auth(
            tonic::Request::new(RemoveNodeRequest {
                context: Some(context("remove-active-node", active_node.generation)),
                node_id: "node-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(remove_active_node.code(), Code::FailedPrecondition);

    let offline_node = client
        .force_offline_node(with_auth(
            tonic::Request::new(ForceOfflineNodeRequest {
                context: Some(context("force-offline-node", active_node.generation)),
                node_id: "node-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(offline_node.state, 7);
    assert_eq!(offline_node.generation, active_node.generation + 1);

    let listed_offline = client
        .list_nodes(with_auth(
            tonic::Request::new(ListNodesRequest {
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed_offline.nodes.len(), 1);
    assert_eq!(listed_offline.nodes[0].state, 7);
    assert_eq!(listed_offline.nodes[0].generation, offline_node.generation);

    let active_node = client
        .activate_node(with_auth(
            tonic::Request::new(ActivateNodeRequest {
                context: Some(context("reactivate-offline-node", offline_node.generation)),
                node_id: "node-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    assert_eq!(active_node.state, 2);

    let drained = client
        .drain_node(with_auth(
            tonic::Request::new(DrainNodeRequest {
                context: Some(context("drain-node", active_node.generation)),
                node_id: "node-a".to_string(),
                graceful_timeout_ms: 1000,
                force_after_timeout: false,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(drained.state, 4);

    let listed = client
        .list_nodes(with_auth(
            tonic::Request::new(ListNodesRequest {
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.nodes.len(), 1);
    assert_eq!(listed.nodes[0].state, 4);
}

#[tokio::test]
async fn admin_region_drain_applies_bucket_dispositions_and_exceptions() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let region = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context("create-drain-region", 0)),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    let cell = client
        .register_cell(with_auth(
            tonic::Request::new(RegisterCellRequest {
                context: Some(context("register-drain-cell", 0)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                placement_weight: 100,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .cell
        .unwrap();
    client
        .activate_cell(with_auth(
            tonic::Request::new(ActivateCellRequest {
                context: Some(context("activate-drain-cell", cell.generation)),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap();
    let local_node_id = node.state.config.node_id.clone();
    let node_descriptor = client
        .register_node(with_auth(
            tonic::Request::new(RegisterNodeRequest {
                context: Some(context("register-drain-node", 0)),
                node_id: local_node_id.clone(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                libp2p_peer_id: "peer-a".to_string(),
                public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                capabilities: vec![1, 5],
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .node
        .unwrap();
    client
        .activate_node(with_auth(
            tonic::Request::new(ActivateNodeRequest {
                context: Some(context("activate-drain-node", node_descriptor.generation)),
                node_id: local_node_id,
            }),
            &token,
        ))
        .await
        .unwrap();
    let region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("activate-drain-region", region.generation)),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: empty_activation_checkpoint_json(
                    "mesh-test",
                    "eu-west-1",
                ),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();

    let tenant = node
        .state
        .persistence
        .create_tenant("tenant-a", "tenant-a-idempotency")
        .await
        .unwrap();
    node.state
        .persistence
        .create_bucket(tenant.id, "docs", "eu-west-1")
        .await
        .unwrap();

    let drained = client
        .drain_region(with_auth(
            tonic::Request::new(DrainRegionRequest {
                context: Some(context("drain-region-with-exception", region.generation)),
                region: "eu-west-1".to_string(),
                default_disposition: 1,
                bucket_overrides: vec![BucketDrainOverride {
                    tenant_id: tenant.id.to_string(),
                    bucket_name: "docs".to_string(),
                    disposition: 2,
                    reason: "customer-approved delayed migration".to_string(),
                    expires_at: "2026-08-02T00:00:00Z".to_string(),
                }],
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(drained.state, 4);

    let locator = node
        .state
        .persistence
        .get_mesh_bucket_locator(tenant.id, "docs")
        .await
        .unwrap()
        .expect("bucket locator");
    assert_eq!(format!("{:?}", locator.status), "ReadOnly");
    let exceptions =
        anvil::mesh_lifecycle::list_bucket_drain_exceptions(&node.state.storage, Some("eu-west-1"))
            .await
            .unwrap();
    assert_eq!(exceptions.len(), 1);
    assert_eq!(exceptions[0].bucket_name, "docs");

    let audit = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "list-drain-audit".to_string(),
                principal_id: String::new(),
                resource_id: "region:eu-west-1".to_string(),
                action: "admin.region.drain".to_string(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(audit.events.len(), 1);
    assert!(
        audit.events[0]
            .details_json
            .contains("bucket_disposition_decisions")
    );
    let disposition_audit = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "list-bucket-disposition-audit".to_string(),
                principal_id: String::new(),
                resource_id: format!("tenant:{}:bucket:docs:region:eu-west-1", tenant.id),
                action: "admin.region.bucket_disposition".to_string(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(disposition_audit.events.len(), 1);
    assert!(
        disposition_audit.events[0]
            .details_json
            .contains("remain_proxy_only")
    );
}

#[tokio::test]
async fn admin_tenant_app_and_bucket_workflow_issues_usable_credentials() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut admin_client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let tenant = admin_client
        .create_tenant(with_auth(
            tonic::Request::new(CreateTenantRequest {
                context: Some(context("admin-create-tenant", 0)),
                name: "operator-tenant".to_string(),
                home_region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .tenant
        .unwrap();
    assert_eq!(tenant.name, "operator-tenant");

    let app_secret = admin_client
        .create_application(with_auth(
            tonic::Request::new(CreateApplicationRequest {
                context: Some(context("admin-create-app", 0)),
                tenant_id: tenant.tenant_id.clone(),
                app_name: "publisher".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(app_secret.tenant_id, tenant.tenant_id);
    assert_eq!(app_secret.app_name, "publisher");
    assert!(app_secret.client_id.starts_with("app_"));
    assert!(app_secret.client_secret.starts_with("secret_"));

    let app_details = node
        .state
        .persistence
        .get_app_by_client_id(&app_secret.client_id)
        .await
        .unwrap()
        .unwrap();
    node.state
        .persistence
        .grant_policy(app_details.id, "*", "*")
        .await
        .unwrap();

    let mut auth_client = AuthServiceClient::connect(node.public_url.clone())
        .await
        .unwrap();
    let token_response = auth_client
        .get_access_token(tonic::Request::new(GetAccessTokenRequest {
            client_id: app_secret.client_id.clone(),
            client_secret: app_secret.client_secret.clone(),
            scopes: vec!["*".to_string()],
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!token_response.access_token.is_empty());

    let bucket = admin_client
        .create_bucket_admin(with_auth(
            tonic::Request::new(CreateBucketAdminRequest {
                context: Some(context("admin-create-bucket", 0)),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: "release-assets".to_string(),
                region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket
        .unwrap();
    assert_eq!(bucket.name, "release-assets");
    assert!(!bucket.is_public_read);

    let public_bucket = admin_client
        .set_bucket_public_access_admin(with_auth(
            tonic::Request::new(SetBucketPublicAccessAdminRequest {
                context: Some(context("admin-public-bucket", 1)),
                tenant_id: tenant.tenant_id,
                bucket_name: "release-assets".to_string(),
                allow_public_read: true,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket
        .unwrap();
    assert!(public_bucket.is_public_read);
}

#[tokio::test]
async fn admin_routing_records_list_and_repair_mesh_locators() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let tenant = node
        .state
        .persistence
        .create_tenant("route-tenant", "unused")
        .await
        .unwrap();
    node.state
        .persistence
        .create_bucket(tenant.id, "route-bucket", "eu-west-1")
        .await
        .unwrap();

    let records = client
        .list_routing_records(with_auth(
            tonic::Request::new(ListRoutingRecordsRequest {
                family: 0,
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    assert!(records.iter().any(|record| record.family == 1
        && record.record_key == "route-tenant"
        && record.payload_json.contains("\"tenant_name\"")));
    let bucket_record = records
        .iter()
        .find(|record| {
            record.family == 3 && record.record_key == format!("{}/route-bucket", tenant.id)
        })
        .cloned()
        .expect("bucket locator record should be listed");

    let descriptor_relative = bucket_record
        .descriptor_key
        .strip_prefix("_anvil/control/v1/mesh/")
        .unwrap();
    let descriptor_path = std::path::Path::new(&node.state.config.storage_path)
        .join("_anvil/control/v1/mesh")
        .join(descriptor_relative);
    tokio::fs::remove_file(&descriptor_path).await.unwrap();

    let missing_after_delete = client
        .list_routing_records(with_auth(
            tonic::Request::new(ListRoutingRecordsRequest {
                family: 3,
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    assert!(
        !missing_after_delete
            .iter()
            .any(|record| record.record_key == format!("{}/route-bucket", tenant.id))
    );
    let diagnostics_after_delete = client
        .list_diagnostics(with_auth(
            tonic::Request::new(ListDiagnosticsRequest {
                request_id: "req-route-diagnostics-after-delete".to_string(),
                source: "mesh_routing_projection".to_string(),
                tenant_id: String::new(),
                bucket_name: String::new(),
                index_name: String::new(),
                severity: String::new(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;
    assert!(diagnostics_after_delete.iter().any(|diagnostic| {
        diagnostic.code == "mesh_control_projection_missing_record"
            && diagnostic
                .details_json
                .contains(&format!("{}/route-bucket", tenant.id))
    }));

    let repaired = client
        .repair_routing_record(with_auth(
            tonic::Request::new(RepairRoutingRecordRequest {
                context: Some(context("repair-bucket-routing-record", 1)),
                family: 3,
                record_key: format!("{}/route-bucket", tenant.id),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(repaired.generation, 1);
    assert_eq!(repaired.resource_id, bucket_record.descriptor_key);

    let listed_after_repair = client
        .list_routing_records(with_auth(
            tonic::Request::new(ListRoutingRecordsRequest {
                family: 3,
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    assert!(
        listed_after_repair
            .iter()
            .any(|record| record.record_key == format!("{}/route-bucket", tenant.id))
    );
    tokio::fs::remove_file(&descriptor_path).await.unwrap();
    let projection_repair = client
        .run_repair(with_auth(
            tonic::Request::new(RunRepairRequest {
                context: Some(context("repair-routing-projection", 0)),
                repair_kind: 5,
                tenant_id: String::new(),
                bucket_name: String::new(),
                index_name: String::new(),
                derived_index_id: String::new(),
                database_id: String::new(),
                rebuild: false,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(projection_repair.status, "completed");
    assert!(
        projection_repair
            .details_json
            .contains("\"repair_kind\":\"mesh_routing_projection\"")
    );
    assert!(
        projection_repair
            .details_json
            .contains("\"repaired_count\":1")
    );
    let diagnostics_after_repair = client
        .list_diagnostics(with_auth(
            tonic::Request::new(ListDiagnosticsRequest {
                request_id: "req-route-diagnostics-after-repair".to_string(),
                source: "mesh_routing_projection".to_string(),
                tenant_id: String::new(),
                bucket_name: String::new(),
                index_name: String::new(),
                severity: String::new(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;
    assert!(
        !diagnostics_after_repair.iter().any(|diagnostic| {
            diagnostic
                .details_json
                .contains(&format!("{}/route-bucket", tenant.id))
        }),
        "routing projection diagnostics should clear after stream-backed repair"
    );
}

#[tokio::test]
async fn admin_host_aliases_are_generation_checked_and_lifecycle_managed() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let denied_token = non_admin_token(&node);

    let mut public_client = AdminServiceClient::connect(node.public_url.clone())
        .await
        .unwrap();
    let public_err = public_client
        .list_host_aliases(with_auth(
            tonic::Request::new(ListHostAliasesRequest {
                region: String::new(),
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(public_err.code(), Code::Unimplemented);

    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let denied = client
        .list_host_aliases(with_auth(
            tonic::Request::new(ListHostAliasesRequest {
                region: String::new(),
                page: None,
            }),
            &denied_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let region = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context("alias-create-region", 0)),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: String::new(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();
    prepare_active_region_dependencies(
        &mut client,
        &token,
        "alias",
        "eu-west-1",
        "cell-a",
        "node-a",
    )
    .await;
    let _region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("alias-activate-region", region.generation)),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: empty_activation_checkpoint_json(
                    "mesh-test",
                    "eu-west-1",
                ),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .region
        .unwrap();

    let native_hostname = client
        .create_host_alias(with_auth(
            tonic::Request::new(CreateHostAliasRequest {
                context: Some(context("alias-native-hostname", 0)),
                hostname: "releases.tenant-alias.eu-west-1.anvil-storage.test".to_string(),
                tenant_id: "tenant-alias".to_string(),
                bucket_name: "releases".to_string(),
                region: "eu-west-1".to_string(),
                prefix: "public/".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(native_hostname.code(), Code::InvalidArgument);

    let created = client
        .create_host_alias(with_auth(
            tonic::Request::new(CreateHostAliasRequest {
                context: Some(context("alias-create", 0)),
                hostname: "CDN.Example.Com.".to_string(),
                tenant_id: "tenant-alias".to_string(),
                bucket_name: "releases".to_string(),
                region: "eu-west-1".to_string(),
                prefix: "public/".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .unwrap();
    assert_eq!(created.hostname, "cdn.example.com");
    assert_eq!(created.state, 1);
    assert_eq!(created.generation, 1);

    let stale = client
        .activate_host_alias(with_auth(
            tonic::Request::new(ActivateHostAliasRequest {
                context: Some(context("alias-activate-stale", created.generation + 1)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::Aborted);

    let missing_generation = client
        .activate_host_alias(with_auth(
            tonic::Request::new(ActivateHostAliasRequest {
                context: Some(context("alias-activate-missing-generation", 0)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(missing_generation.code(), Code::InvalidArgument);

    let active = client
        .activate_host_alias(with_auth(
            tonic::Request::new(ActivateHostAliasRequest {
                context: Some(context("alias-activate", created.generation)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .unwrap();
    assert_eq!(active.state, 2);
    assert_eq!(active.generation, created.generation + 1);

    let read = client
        .read_host_alias(with_auth(
            tonic::Request::new(ReadHostAliasRequest {
                request_id: "req-alias-read".to_string(),
                hostname: "CDN.EXAMPLE.COM.".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .unwrap();
    assert_eq!(read.hostname, "cdn.example.com");
    assert_eq!(read.state, 2);
    assert_eq!(read.generation, active.generation);

    let routing_records = client
        .list_routing_records(with_auth(
            tonic::Request::new(ListRoutingRecordsRequest {
                family: 4,
                page: None,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .records;
    let host_alias_record = routing_records
        .iter()
        .find(|record| record.record_key == "cdn.example.com")
        .expect("host alias routing record should be materialised");
    assert_eq!(host_alias_record.family, 4);
    assert_eq!(host_alias_record.generation, active.generation);
    assert!(
        host_alias_record
            .payload_json
            .contains("\"cdn.example.com\"")
    );

    let listed = client
        .list_host_aliases(with_auth(
            tonic::Request::new(ListHostAliasesRequest {
                region: "eu-west-1".to_string(),
                page: Some(PageRequest {
                    cursor: String::new(),
                    limit: 10,
                }),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.host_aliases.len(), 1);
    assert_eq!(listed.host_aliases[0].hostname, "cdn.example.com");
    assert_eq!(listed.host_aliases[0].state, 2);

    let suspended = client
        .suspend_host_alias(with_auth(
            tonic::Request::new(SuspendHostAliasRequest {
                context: Some(context("alias-suspend", active.generation)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .host_alias
        .unwrap();
    assert_eq!(suspended.state, 3);

    let deleted = client
        .delete_host_alias(with_auth(
            tonic::Request::new(DeleteHostAliasRequest {
                context: Some(context("alias-delete", suspended.generation)),
                hostname: "cdn.example.com".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.resource_id, "cdn.example.com");
    assert_eq!(deleted.generation, suspended.generation + 1);
}

#[tokio::test]
async fn admin_object_links_are_cas_checked_metadata_entries() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let denied_token = non_admin_token(&node);
    let tenant = node
        .state
        .persistence
        .create_tenant("tenant-links", "unused")
        .await
        .unwrap();
    let bucket = node
        .state
        .persistence
        .create_bucket(tenant.id, "releases", "eu-west-1")
        .await
        .unwrap();
    let target_v1 = node
        .state
        .persistence
        .create_object(
            tenant.id,
            bucket.id,
            "versions/app-v1.bin",
            "hash-v1",
            8,
            "etag-v1",
            Some("application/octet-stream"),
            None,
            None,
            Some(b"v1-bytes".to_vec()),
        )
        .await
        .unwrap();
    let target_v2 = node
        .state
        .persistence
        .create_object(
            tenant.id,
            bucket.id,
            "versions/app-v2.bin",
            "hash-v2",
            8,
            "etag-v2",
            Some("application/octet-stream"),
            None,
            None,
            Some(b"v2-bytes".to_vec()),
        )
        .await
        .unwrap();

    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let created = client
        .create_object_link(with_auth(
            tonic::Request::new(CreateObjectLinkRequest {
                context: Some(context("create-link", 0)),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
                target_key: "versions/app-v1.bin".to_string(),
                target_version: String::new(),
                resolution: 1,
                allow_dangling: false,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .link
        .unwrap();
    assert_eq!(created.generation, 1);
    assert_eq!(created.target_key, "versions/app-v1.bin");
    assert_eq!(created.created_by, "principal:admin-principal");

    let duplicate_create = client
        .create_object_link(with_auth(
            tonic::Request::new(CreateObjectLinkRequest {
                context: Some(context("create-link-stale", 0)),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
                target_key: "versions/app-v1.bin".to_string(),
                target_version: String::new(),
                resolution: 1,
                allow_dangling: false,
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(duplicate_create.code(), Code::AlreadyExists);

    let denied_read = client
        .read_object_link(with_auth(
            tonic::Request::new(ReadObjectLinkRequest {
                request_id: "req-denied-read-link".to_string(),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
            }),
            &denied_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied_read.code(), Code::PermissionDenied);

    let read = client
        .read_object_link(with_auth(
            tonic::Request::new(ReadObjectLinkRequest {
                request_id: "req-read-link".to_string(),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .link
        .unwrap();
    assert_eq!(read.generation, created.generation);
    assert_eq!(read.target_key, "versions/app-v1.bin");

    let listed = client
        .list_object_links(with_auth(
            tonic::Request::new(ListObjectLinksRequest {
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                prefix: "latest".to_string(),
                page: Some(PageRequest {
                    cursor: String::new(),
                    limit: 10,
                }),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(listed.links.len(), 1);
    assert_eq!(listed.links[0].link_key, "latest.bin");

    let link_entry = node
        .state
        .persistence
        .get_object(bucket.id, "latest.bin")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(link_entry.size, 0);
    assert!(link_entry.inline_payload.is_none());

    let stale_update = client
        .update_object_link(with_auth(
            tonic::Request::new(UpdateObjectLinkRequest {
                context: Some(context("update-link-stale", created.generation + 1)),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
                target_key: "versions/app-v2.bin".to_string(),
                target_version: String::new(),
                resolution: 1,
                allow_dangling: false,
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale_update.code(), Code::Aborted);

    let updated = client
        .update_object_link(with_auth(
            tonic::Request::new(UpdateObjectLinkRequest {
                context: Some(context("update-link", created.generation)),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
                target_key: "versions/app-v2.bin".to_string(),
                target_version: String::new(),
                resolution: 1,
                allow_dangling: false,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .link
        .unwrap();
    assert_eq!(updated.generation, created.generation + 1);
    assert_eq!(updated.target_key, "versions/app-v2.bin");

    let updated_entry = node
        .state
        .persistence
        .get_object(bucket.id, "latest.bin")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(updated_entry.size, 0);
    assert!(updated_entry.inline_payload.is_none());

    let stale_delete = client
        .delete_object_link(with_auth(
            tonic::Request::new(DeleteObjectLinkRequest {
                context: Some(context("delete-link-stale", created.generation)),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale_delete.code(), Code::Aborted);

    let deleted = client
        .delete_object_link(with_auth(
            tonic::Request::new(DeleteObjectLinkRequest {
                context: Some(context("delete-link", updated.generation)),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.generation, updated.generation + 1);

    let deleted_read = client
        .read_object_link(with_auth(
            tonic::Request::new(ReadObjectLinkRequest {
                request_id: "req-read-deleted-link".to_string(),
                tenant_id: "tenant-links".to_string(),
                bucket_name: "releases".to_string(),
                link_key: "latest.bin".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(deleted_read.code(), Code::NotFound);

    assert!(
        node.state
            .persistence
            .get_object(bucket.id, "latest.bin")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        node.state
            .persistence
            .get_object(bucket.id, "versions/app-v1.bin")
            .await
            .unwrap()
            .unwrap()
            .inline_payload,
        target_v1.inline_payload
    );
    assert_eq!(
        node.state
            .persistence
            .get_object(bucket.id, "versions/app-v2.bin")
            .await
            .unwrap()
            .unwrap()
            .inline_payload,
        target_v2.inline_payload
    );
}

#[tokio::test]
async fn admin_mutations_are_returned_by_durable_audit_listing() {
    let node = spawn_admin_node().await;
    let token = admin_token(&node);
    let mut client = AdminServiceClient::connect(node.admin_url.clone())
        .await
        .unwrap();

    let tenant_response = client
        .create_tenant(with_auth(
            tonic::Request::new(CreateTenantRequest {
                context: Some(context("audit-create-tenant", 0)),
                name: "audit-tenant".to_string(),
                home_region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let tenant = tenant_response.tenant.clone().unwrap();
    let tenant_id = tenant.tenant_id.parse::<i64>().unwrap();

    let app_response = client
        .create_application(with_auth(
            tonic::Request::new(CreateApplicationRequest {
                context: Some(context("audit-create-app", 0)),
                tenant_id: tenant.tenant_id.clone(),
                app_name: "publisher".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let bucket_response = client
        .create_bucket_admin(with_auth(
            tonic::Request::new(CreateBucketAdminRequest {
                context: Some(context("audit-create-bucket", 0)),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: "release-assets".to_string(),
                region: "eu-west-1".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let bucket = bucket_response.bucket.clone().unwrap();

    let public_bucket_response = client
        .set_bucket_public_access_admin(with_auth(
            tonic::Request::new(SetBucketPublicAccessAdminRequest {
                context: Some(context("audit-public-bucket", 1)),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: bucket.name.clone(),
                allow_public_read: true,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    node.state
        .persistence
        .create_object(
            tenant_id,
            bucket.bucket_id,
            "versions/app-v1.bin",
            "hash-v1",
            8,
            "etag-v1",
            Some("application/octet-stream"),
            None,
            None,
            Some(b"v1-bytes".to_vec()),
        )
        .await
        .unwrap();

    let link_response = client
        .create_object_link(with_auth(
            tonic::Request::new(CreateObjectLinkRequest {
                context: Some(context("audit-create-link", 0)),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: bucket.name.clone(),
                link_key: "latest.bin".to_string(),
                target_key: "versions/app-v1.bin".to_string(),
                target_version: String::new(),
                resolution: 1,
                allow_dangling: false,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let region_response = client
        .create_region(with_auth(
            tonic::Request::new(CreateRegionRequest {
                context: Some(context("audit-create-region", 0)),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: String::new(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let region = region_response.region.clone().unwrap();
    prepare_active_region_dependencies(
        &mut client,
        &token,
        "audit",
        "eu-west-1",
        "cell-a",
        "node-a",
    )
    .await;
    let active_region_response = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("audit-activate-region", region.generation)),
                region: "eu-west-1".to_string(),
                activation_checkpoint_json: activation_checkpoint_json_from_existing_streams(
                    &node,
                    "eu-west-1",
                )
                .await,
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let host_alias_response = client
        .create_host_alias(with_auth(
            tonic::Request::new(CreateHostAliasRequest {
                context: Some(context("audit-create-host-alias", 0)),
                hostname: "Audit.Example.Com.".to_string(),
                tenant_id: tenant.tenant_id.clone(),
                bucket_name: bucket.name.clone(),
                region: "eu-west-1".to_string(),
                prefix: "public/".to_string(),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let audit = client
        .list_audit_events(with_auth(
            tonic::Request::new(ListAuditEventsRequest {
                request_id: "req-list-admin-audit".to_string(),
                principal_id: "admin-principal".to_string(),
                resource_id: String::new(),
                action: String::new(),
                page: Some(PageRequest {
                    cursor: String::new(),
                    limit: 100,
                }),
            }),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let find_event = |action: &str| {
        audit
            .events
            .iter()
            .find(|event| event.action == action)
            .unwrap_or_else(|| panic!("missing audit event for {action}"))
    };
    let details = |event: &AuditEventRecord| -> serde_json::Value {
        serde_json::from_str(&event.details_json).unwrap()
    };

    let tenant_event = find_event("admin.tenant.create");
    assert_eq!(tenant_event.audit_event_id, tenant_response.audit_event_id);
    assert_eq!(tenant_event.resource_id, format!("tenant:{tenant_id}"));
    assert_eq!(tenant_event.principal_id, "admin-principal");
    let tenant_details = details(tenant_event);
    assert_eq!(tenant_details["tenant_name"], "audit-tenant");
    assert_eq!(
        tenant_details["idempotency_key"],
        "idem-audit-create-tenant"
    );

    let app_event = find_event("admin.app.create");
    assert_eq!(app_event.audit_event_id, app_response.audit_event_id);
    let app_details = details(app_event);
    assert_eq!(app_details["tenant_id"], tenant_id);
    assert_eq!(app_details["app_name"], "publisher");

    let bucket_event = find_event("admin.bucket.create");
    assert_eq!(bucket_event.audit_event_id, bucket_response.audit_event_id);
    assert_eq!(
        bucket_event.resource_id,
        format!("tenant:{tenant_id}:bucket:release-assets")
    );
    let bucket_details = details(bucket_event);
    assert_eq!(bucket_details["bucket_id"], bucket.bucket_id);
    assert_eq!(bucket_details["region"], "eu-west-1");

    let public_bucket_event = find_event("admin.bucket.public_access.set");
    assert_eq!(
        public_bucket_event.audit_event_id,
        public_bucket_response.audit_event_id
    );
    let public_bucket_details = details(public_bucket_event);
    assert_eq!(public_bucket_details["allow_public_read"], true);
    assert_eq!(public_bucket_details["expected_generation"], 1);

    let link_event = find_event("admin.object_link.create");
    assert_eq!(link_event.audit_event_id, link_response.audit_event_id);
    assert_eq!(
        link_event.resource_id,
        format!("tenant:{tenant_id}:bucket:release-assets:link:latest.bin")
    );
    let link_details = details(link_event);
    assert_eq!(link_details["target_key"], "versions/app-v1.bin");
    assert_eq!(link_details["resolution"], "follow");

    let region_event = find_event("admin.region.create");
    assert_eq!(region_event.audit_event_id, region_response.audit_event_id);
    assert_eq!(region_event.resource_id, "region:eu-west-1");
    let region_details = details(region_event);
    assert_eq!(region_details["state"], "joining");
    assert_eq!(region_details["placement_weight"], 100);

    let active_region_event = find_event("admin.region.activate");
    assert_eq!(
        active_region_event.audit_event_id,
        active_region_response.audit_event_id
    );
    let active_region_details = details(active_region_event);
    assert_eq!(active_region_details["state"], "active");
    assert!(active_region_details["activation_checkpoint"].is_object());

    let host_alias_event = find_event("admin.host_alias.create");
    assert_eq!(
        host_alias_event.audit_event_id,
        host_alias_response.audit_event_id
    );
    assert_eq!(host_alias_event.resource_id, "host_alias:audit.example.com");
    let host_alias_details = details(host_alias_event);
    assert_eq!(host_alias_details["hostname"], "audit.example.com");
    assert_eq!(host_alias_details["prefix"], "public/");
}
