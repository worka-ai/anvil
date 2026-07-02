use anvil::anvil_api::admin_service_client::AdminServiceClient;
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

    let region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("activate-region", region.generation)),
                region: "eu-west-1".to_string(),
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
    let _region = client
        .activate_region(with_auth(
            tonic::Request::new(ActivateRegionRequest {
                context: Some(context("alias-activate-region", region.generation)),
                region: "eu-west-1".to_string(),
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
