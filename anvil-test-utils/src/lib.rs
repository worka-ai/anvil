use std::collections::{BTreeSet, HashSet};
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Once, OnceLock};
use std::time::{Duration, Instant};

use anvil::anvil_api::GetAccessTokenRequest;
use anvil::anvil_api::admin_service_client::AdminServiceClient;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil_core::{AppState, access_control};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Credentials;
use futures_util::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tracing_subscriber::{self, EnvFilter};

static INIT_LOGGER: Once = Once::new();
static TEST_CLUSTER_SEMAPHORE: OnceLock<Arc<Semaphore>> = OnceLock::new();

fn test_cluster_limit() -> usize {
    std::env::var("ANVIL_TEST_CLUSTER_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(2)
}

async fn acquire_test_cluster_permit() -> OwnedSemaphorePermit {
    TEST_CLUSTER_SEMAPHORE
        .get_or_init(|| Arc::new(Semaphore::new(test_cluster_limit())))
        .clone()
        .acquire_owned()
        .await
        .expect("test cluster semaphore is not closed")
}

pub fn test_timing_enabled() -> bool {
    std::env::var_os("ANVIL_TEST_TIMINGS").is_some()
}

pub fn emit_test_timing(label: impl AsRef<str>, elapsed: Duration) {
    let label = label.as_ref();
    if test_timing_enabled() {
        eprintln!("[timing] {label}={elapsed:?}");
    }
    if anvil_core::perf::enabled() {
        anvil_core::perf::record_duration("anvil_test_span", &[("span", label)], elapsed);
    }
}

#[allow(dead_code)]
pub async fn get_auth_token(_admin_state_path: &str, grpc_addr: &str) -> String {
    let grpc_url = if grpc_addr.ends_with("/grpc") {
        grpc_addr.to_string()
    } else {
        format!("{}/grpc", grpc_addr.trim_end_matches('/'))
    };
    let mut auth_client = AuthServiceClient::connect(grpc_url).await.unwrap();
    let token_res = auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: "test-app".to_string(),
            client_secret: "test-secret".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    token_res.access_token
}

#[allow(dead_code)]
#[allow(unused)]
pub struct TestCluster {
    pub nodes: Vec<JoinHandle<()>>,
    pub states: Vec<AppState>,
    pub grpc_addrs: Vec<String>,
    pub admin_addrs: Vec<String>,
    pub token: String,
    pub admin_state_path: String,
    pub config: Arc<anvil_core::config::Config>,
    pub storage_path: PathBuf,
    cleanup_path: PathBuf,
    _cluster_permit: OwnedSemaphorePermit,
}

impl TestCluster {
    pub async fn create_bucket(&self, bucket_name: &str, region: &str) {
        let mut bucket_client =
            anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
                self.grpc_addrs[0].clone(),
            )
            .await
            .unwrap();
        let mut create_req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
            bucket_name: bucket_name.to_string(),
            region: region.to_string(),

            options: None,
        });
        create_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.token).parse().unwrap(),
        );
        bucket_client.create_bucket(create_req).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn new(regions: &[&str]) -> Self {
        Self::new_with_config(regions, |_| {}).await
    }

    #[allow(dead_code)]
    pub async fn new_with_config(
        regions: &[&str],
        configure: impl FnOnce(&mut anvil_core::config::Config),
    ) -> Self {
        let cluster_permit = acquire_test_cluster_permit().await;

        INIT_LOGGER.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::new(
                    "warn,anvil=debug,anvil_core=debug,anvil_core::cluster=warn",
                ))
                .try_init();
        });

        let cluster_storage_root =
            std::env::temp_dir().join(format!("anvil-test-storage-{}", uuid::Uuid::new_v4()));
        let mut config = anvil_core::config::Config {
            cluster_secret: Some("test-cluster-secret".to_string()),
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            cluster_listen_addr: "/ip4/127.0.0.1/udp/0/quic-v1".to_string(),
            public_cluster_addrs: vec![],
            corestore_internal_bearer_token: "test-corestore-internal-token".to_string(),
            metadata_cache_ttl_secs: 1,
            public_api_addr: "127.0.0.1:0".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            mesh_id: "test-mesh".to_string(),
            region: "".to_string(),
            cell_id: "test-cell-1".to_string(),
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "admin-principal".to_string(),
            bootstrap_addrs: vec![],
            init_cluster: false,
            enable_mdns: false,
            storage_path: cluster_storage_root
                .join("template")
                .to_string_lossy()
                .into_owned(),
            personaldb_snapshot_entry_threshold: 1024,
            personaldb_snapshot_payload_bytes_threshold: 64 * 1024 * 1024,
            allow_test_only_embedding_provider: true,
            run_background_worker: false,
            ..anvil_core::config::Config::default()
        };
        configure(&mut config);
        let config = Arc::new(config);

        let unique_regions: HashSet<String> = regions.iter().map(|s| s.to_string()).collect();
        let first_region = regions.first().copied().unwrap_or("default");
        let admin_state_path = cluster_storage_root.join(format!("node-000-{first_region}"));
        let mut states = Vec::new();
        for (node_index, region_name) in regions.iter().enumerate() {
            let mut node_config = config.deref().clone();
            node_config.region = (*region_name).to_string();
            node_config.cell_id = format!("test-cell-{}", node_index + 1);
            node_config.metadata_cache_ttl_secs = 1;
            node_config.storage_path = cluster_storage_root
                .join(format!("node-{node_index:03}-{region_name}"))
                .to_string_lossy()
                .into_owned();
            let state = AppState::new(node_config, None).await.unwrap();
            state.persistence.create_region(region_name).await.unwrap();
            let tenant = if let Some(existing) = state
                .persistence
                .get_tenant_by_name("default")
                .await
                .unwrap()
            {
                existing
            } else {
                state
                    .persistence
                    .create_tenant("default", "default-key")
                    .await
                    .unwrap()
            };
            if state
                .persistence
                .get_app_by_client_id("test-app")
                .await
                .unwrap()
                .is_none()
            {
                let encrypted_secret = state.secret_keyring.encrypt(b"test-secret").unwrap();
                let app = state
                    .persistence
                    .create_app(tenant.id, "test-app", "test-app", &encrypted_secret)
                    .await
                    .unwrap();
                access_control::grant_storage_tenant_owner(
                    &state.persistence,
                    tenant.id,
                    &app.id.to_string(),
                    "test-cluster",
                    "grant test app ownership of its storage tenant",
                )
                .await
                .unwrap();
            }
            states.push(state);
        }
        for region in unique_regions {
            for state in &states {
                state.persistence.create_region(&region).await.unwrap();
            }
        }
        install_canonical_coremeta_bootstrap_snapshot(&states);

        Self {
            nodes: Vec::new(),
            states,
            grpc_addrs: Vec::new(),
            admin_addrs: Vec::new(),
            token: String::new(),
            admin_state_path: admin_state_path.to_string_lossy().into_owned(),
            config,
            storage_path: admin_state_path,
            cleanup_path: cluster_storage_root,
            _cluster_permit: cluster_permit,
        }
    }

    pub async fn start_and_converge(&mut self, timeout: Duration) {
        self.start_and_converge_no_new_token(timeout, true).await
    }

    pub async fn start_and_converge_no_new_token(
        &mut self,
        timeout: Duration,
        get_new_token: bool,
    ) {
        let total_start = Instant::now();
        let node_count = self.states.len();

        let swarms_start = Instant::now();
        let mut swarms = Vec::new();
        for state in &self.states {
            swarms.push(
                anvil_core::cluster::create_swarm(state.config.clone())
                    .await
                    .unwrap(),
            );
        }
        emit_test_timing(
            format!("start_and_converge swarm_create nodes={node_count}"),
            swarms_start.elapsed(),
        );

        let swarm_listen_start = Instant::now();
        let mut listen_addrs = Vec::new();
        for swarm in &mut swarms {
            let address = loop {
                if let Some(event) = swarm.next().await {
                    if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } = event {
                        break address;
                    }
                } else {
                    panic!("Swarm stream ended before a listener was established");
                }
            };
            listen_addrs.push(address);
        }
        emit_test_timing(
            format!("start_and_converge swarm_listen nodes={node_count}"),
            swarm_listen_start.elapsed(),
        );

        let swarm_dial_start = Instant::now();
        for i in 0..swarms.len() {
            for (j, addr) in listen_addrs.iter().enumerate() {
                if i != j {
                    swarms[i].dial(addr.clone()).unwrap();
                }
            }
        }
        emit_test_timing(
            format!("start_and_converge swarm_dial nodes={node_count}"),
            swarm_dial_start.elapsed(),
        );

        let node_spawn_start = Instant::now();
        let peer_ids = swarms
            .iter()
            .map(|swarm| swarm.local_peer_id().to_string())
            .collect::<Vec<_>>();
        let mut listeners = Vec::new();
        let mut admin_listeners = Vec::new();
        for _ in 0..self.states.len() {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let admin_addr = admin_listener.local_addr().unwrap();
            self.grpc_addrs.push(format!("http://{}", addr));
            self.admin_addrs.push(format!("http://{}", admin_addr));
            listeners.push(listener);
            admin_listeners.push(admin_listener);
        }

        for i in 0..self.states.len() {
            let mut cfg = anvil_core::config::Config::from_ref(self.states[i].config.deref());
            cfg.public_api_addr = self.grpc_addrs[i].clone();
            cfg.corestore_internal_bearer_token = self.states[i]
                .jwt_manager
                .mint_token(cfg.node_id.clone(), 0)
                .unwrap();
            self.states[i] = AppState::new(cfg, None).await.unwrap();
        }

        for i in 0..self.states.len() {
            let state = self.states[i].clone();
            let swarm = swarms.remove(0);
            let listener = listeners.remove(0);
            let admin_listener = admin_listeners.remove(0);

            let handle = tokio::spawn(async move {
                let (_tx, rx) = tokio::sync::mpsc::channel(1);
                anvil::start_node_with_admin_listener(
                    listener,
                    Some(admin_listener),
                    state,
                    swarm,
                    rx,
                )
                .await
                .unwrap();
            });
            self.nodes.push(handle);
        }
        emit_test_timing(
            format!("start_and_converge node_spawn nodes={node_count}"),
            node_spawn_start.elapsed(),
        );

        let token_start = Instant::now();
        if get_new_token {
            let test_app = self.states[0]
                .persistence
                .get_app_by_client_id("test-app")
                .await
                .unwrap()
                .expect("test-app is seeded before cluster start");
            self.token = self.states[0]
                .jwt_manager
                .mint_token(test_app.id.to_string(), test_app.tenant_id)
                .unwrap();
        }
        emit_test_timing(
            format!("start_and_converge token nodes={node_count}"),
            token_start.elapsed(),
        );

        let start = Instant::now();
        loop {
            let mut all_ports_ready = true;
            for addr_str in self.grpc_addrs.iter().chain(self.admin_addrs.iter()) {
                let addr: SocketAddr = addr_str.replace("http://", "").parse().unwrap();
                if !is_port_open(addr).await {
                    all_ports_ready = false;
                    break;
                }
            }
            if all_ports_ready {
                emit_test_timing(
                    format!("start_and_converge port_ready nodes={node_count}"),
                    start.elapsed(),
                );
                let lifecycle_seed_start = Instant::now();
                self.seed_corestore_mesh_lifecycle(&peer_ids, &listen_addrs)
                    .await;
                emit_test_timing(
                    format!("start_and_converge mesh_lifecycle_seed nodes={node_count}"),
                    lifecycle_seed_start.elapsed(),
                );
                let stabilization_start = Instant::now();
                tokio::time::sleep(Duration::from_secs(3)).await;
                emit_test_timing(
                    format!("start_and_converge stabilization_sleep nodes={node_count}"),
                    stabilization_start.elapsed(),
                );
                emit_test_timing(
                    format!("start_and_converge total nodes={node_count}"),
                    total_start.elapsed(),
                );
                return;
            }
            if start.elapsed() >= timeout {
                let mut cluster_sizes = Vec::with_capacity(self.states.len());
                for state in &self.states {
                    cluster_sizes.push(state.cluster.read().await.len());
                }
                panic!(
                    "Cluster ports did not become ready in time; observed gossip membership sizes: {:?}",
                    cluster_sizes
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn seed_corestore_mesh_lifecycle(
        &self,
        peer_ids: &[String],
        listen_addrs: &[libp2p::Multiaddr],
    ) {
        let mut seen_regions = BTreeSet::new();
        let mut regions = Vec::new();
        let mut seen_cells = BTreeSet::new();
        let mut cells = Vec::new();
        let mut nodes = Vec::new();

        for (index, source) in self.states.iter().enumerate() {
            if seen_regions.insert(source.config.region.clone()) {
                regions.push(anvil_core::mesh_lifecycle::CreateRegionDescriptor {
                    mesh_id: source.config.mesh_id.clone(),
                    region: source.config.region.clone(),
                    public_base_url: source.config.public_api_addr.clone(),
                    virtual_host_suffix: format!("{}.test.anvil.local", source.config.region),
                    placement_weight: 100,
                    default_cell: Some(source.config.cell_id.clone()),
                });
            }

            let cell_key = format!("{}/{}", source.config.region, source.config.cell_id);
            if seen_cells.insert(cell_key) {
                cells.push(anvil_core::mesh_lifecycle::RegisterCellDescriptor {
                    mesh_id: source.config.mesh_id.clone(),
                    region: source.config.region.clone(),
                    cell_id: source.config.cell_id.clone(),
                    placement_weight: 100,
                    failure_domain: source.config.cell_id.clone(),
                });
            }

            nodes.push(anvil_core::mesh_lifecycle::RegisterNodeDescriptor {
                mesh_id: source.config.mesh_id.clone(),
                node_id: source.config.node_id.clone(),
                region: source.config.region.clone(),
                cell_id: source.config.cell_id.clone(),
                libp2p_peer_id: peer_ids[index].clone(),
                receipt_signing_public_key_proto: source
                    .core_store
                    .local_receipt_signing_public_key_proto(),
                public_api_addr: source.config.public_api_addr.clone(),
                public_cluster_addrs: vec![listen_addrs[index].to_string()],
                capabilities: vec![
                    anvil_core::mesh_lifecycle::NodeCapability::Object,
                    anvil_core::mesh_lifecycle::NodeCapability::Index,
                    anvil_core::mesh_lifecycle::NodeCapability::PersonalDb,
                    anvil_core::mesh_lifecycle::NodeCapability::Metadata,
                    anvil_core::mesh_lifecycle::NodeCapability::Gateway,
                    anvil_core::mesh_lifecycle::NodeCapability::Admin,
                ],
                capacity_json: "{}".to_string(),
            });
        }

        let projection = anvil_core::mesh_lifecycle::BootstrapMeshLifecycleProjection {
            regions,
            cells,
            nodes,
        };

        let node_default_grants = self
            .states
            .iter()
            .map(|source| {
                (
                    source.config.region.clone(),
                    source.config.cell_id.clone(),
                    source.config.node_id.clone(),
                )
            })
            .collect::<Vec<_>>();

        let Some(canonical) = self.states.first() else {
            return;
        };
        for source in &self.states {
            canonical
                .core_store
                .register_node_receipt_signing_public_key(
                    &source.config.node_id,
                    &source.core_store.local_receipt_signing_public_key_proto(),
                )
                .unwrap();
        }
        anvil_core::access_control::grant_node_defaults_batch(
            &canonical.persistence,
            &node_default_grants,
            "test-cluster",
            "test cluster node bootstrap",
        )
        .await
        .unwrap_or_else(|err| {
            panic!(
                "grant_node_defaults_batch target={}: {err:?}",
                canonical.config.node_id
            )
        });
        anvil_core::mesh_lifecycle::install_bootstrap_lifecycle_projection(
            &canonical.storage,
            &canonical.core_store,
            projection,
        )
        .unwrap();
        install_canonical_coremeta_bootstrap_snapshot(&self.states);
    }

    #[allow(unused)]
    pub async fn get_s3_client(
        &self,
        region: &str,
        access_key: &str,
        secret_key: &str,
    ) -> S3Client {
        let credentials = Credentials::new(access_key, secret_key, None, None, "static");
        let config = aws_sdk_s3::Config::builder()
            .credentials_provider(credentials)
            .region(aws_sdk_s3::config::Region::new(region.to_string()))
            .endpoint_url(&self.grpc_addrs[0])
            .force_path_style(true)
            .behavior_version(BehaviorVersion::latest())
            .build();
        S3Client::from_conf(config)
    }

    #[allow(unused)]
    pub async fn restart(&mut self, timeout: Duration) {
        for node in self.nodes.drain(..) {
            node.abort();
        }
        self.grpc_addrs.clear();
        self.admin_addrs.clear();
        self.start_and_converge(timeout).await;
    }

    pub fn admin_token(&self) -> String {
        self.states[0]
            .jwt_manager
            .mint_token("admin-principal".to_string(), 0)
            .unwrap()
    }

    pub async fn create_application(&self, tenant_id: &str, app_name: &str) -> (String, String) {
        let (_app_id, client_id, client_secret) =
            self.create_application_with_id(tenant_id, app_name).await;
        (client_id, client_secret)
    }

    pub async fn create_application_with_id(
        &self,
        tenant_id: &str,
        app_name: &str,
    ) -> (String, String, String) {
        let mut client = AdminServiceClient::connect(self.admin_addrs[0].clone())
            .await
            .unwrap();
        let mut request = tonic::Request::new(anvil::anvil_api::CreateApplicationRequest {
            context: Some(test_admin_context(&format!("create-app-{app_name}"), 0)),
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.admin_token()).parse().unwrap(),
        );
        let response = client
            .create_application(request)
            .await
            .unwrap()
            .into_inner();
        (response.app_id, response.client_id, response.client_secret)
    }

    pub async fn grant_application_policy(
        &self,
        tenant_id: &str,
        app_name: &str,
        action: &str,
        resource: &str,
    ) {
        let mut client = AdminServiceClient::connect(self.admin_addrs[0].clone())
            .await
            .unwrap();
        let mut request = tonic::Request::new(anvil::anvil_api::GrantApplicationPolicyRequest {
            context: Some(test_admin_context(&format!("grant-{app_name}"), 0)),
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
            action: action.to_string(),
            resource: resource.to_string(),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.admin_token()).parse().unwrap(),
        );
        client.grant_application_policy(request).await.unwrap();
    }

    pub async fn rotate_application_secret(
        &self,
        tenant_id: &str,
        app_name: &str,
    ) -> (String, String) {
        let mut client = AdminServiceClient::connect(self.admin_addrs[0].clone())
            .await
            .unwrap();
        let mut request = tonic::Request::new(anvil::anvil_api::RotateApplicationSecretRequest {
            context: Some(test_admin_context(&format!("rotate-app-{app_name}"), 1)),
            tenant_id: tenant_id.to_string(),
            app_name: app_name.to_string(),
        });
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.admin_token()).parse().unwrap(),
        );
        let response = client
            .rotate_application_secret(request)
            .await
            .unwrap()
            .into_inner();
        (response.client_id, response.client_secret)
    }

    pub async fn create_application_with_policy(
        &self,
        tenant_id: &str,
        app_name: &str,
        action: &str,
        resource: &str,
    ) -> (String, String) {
        let credentials = self.create_application(tenant_id, app_name).await;
        self.grant_application_policy(tenant_id, app_name, action, resource)
            .await;
        credentials
    }

    pub async fn create_application_with_storage_tenant_owner(
        &self,
        tenant_ref: &str,
        app_name: &str,
    ) -> (String, String) {
        let (app_id, client_id, client_secret) =
            self.create_application_with_id(tenant_ref, app_name).await;
        let tenant_id = if let Ok(tenant_id) = tenant_ref.parse::<i64>() {
            tenant_id
        } else {
            self.states[0]
                .persistence
                .get_tenant_by_name(tenant_ref)
                .await
                .unwrap()
                .expect("tenant exists")
                .id
        };
        access_control::grant_storage_tenant_owner(
            &self.states[0].persistence,
            tenant_id,
            &app_id,
            "test-admin",
            "test grant storage tenant owner",
        )
        .await
        .unwrap();
        (client_id, client_secret)
    }
}

fn test_admin_context(
    label: &str,
    expected_generation: u64,
) -> anvil::anvil_api::AdminRequestContext {
    anvil::anvil_api::AdminRequestContext {
        request_id: format!("test-{label}-{}", uuid::Uuid::new_v4().simple()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        audit_reason: format!("test {label}"),
        expected_generation,
    }
}

fn install_canonical_coremeta_bootstrap_snapshot(states: &[AppState]) {
    let Some(canonical) = states.first() else {
        return;
    };
    let snapshot = canonical
        .core_store
        .export_coremeta_snapshot_rows()
        .expect("export canonical CoreMeta bootstrap snapshot")
        .into_iter()
        .filter(|row| !is_node_local_coremeta_row(row))
        .filter(|row| !is_local_derived_coremeta_row(row))
        .filter(|row| !contains_local_corestore_locator(row))
        .collect::<Vec<_>>();

    for target in states.iter().skip(1) {
        target
            .core_store
            .install_coremeta_snapshot_rows(&snapshot)
            .expect("install canonical CoreMeta bootstrap snapshot");
    }
}

fn is_node_local_coremeta_row(row: &anvil_core::core_store::CoreMetaEncodedOwnedRow) -> bool {
    if row.cf != anvil_core::core_store::CF_MESH
        || coremeta_table_id(row) != Some(anvil_core::core_store::TABLE_NODE_SIGNING_KEYPAIR_ROW)
    {
        return false;
    }
    let Ok(tuple_key) = anvil_core::core_store::core_meta_record_tuple_key(&row.core_meta_key)
    else {
        return false;
    };
    let local_tuples = [
        anvil_core::core_store::core_meta_tuple_key(&[
            anvil_core::core_store::CoreMetaTuplePart::Raw(b"node-signing-keypair"),
        ]),
        anvil_core::core_store::core_meta_tuple_key(&[
            anvil_core::core_store::CoreMetaTuplePart::Utf8("cluster-identity"),
            anvil_core::core_store::CoreMetaTuplePart::Utf8("local"),
        ]),
    ];
    local_tuples
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .any(|local_tuple| tuple_key == local_tuple.as_slice())
}

fn is_local_derived_coremeta_row(row: &anvil_core::core_store::CoreMetaEncodedOwnedRow) -> bool {
    matches!(
        (row.cf.as_str(), coremeta_table_id(row)),
        (
            anvil_core::core_store::CF_MATERIALISATION,
            Some(anvil_core::core_store::TABLE_MATERIALISATION_CURSOR_ROW)
        ) | (
            anvil_core::core_store::CF_MATERIALISATION,
            Some(anvil_core::core_store::TABLE_WRITER_SEGMENT_ROW)
        )
    )
}

fn contains_local_corestore_locator(row: &anvil_core::core_store::CoreMetaEncodedOwnedRow) -> bool {
    row.value_envelope
        .windows(b"local-node".len())
        .any(|window| window == b"local-node")
}

fn coremeta_table_id(row: &anvil_core::core_store::CoreMetaEncodedOwnedRow) -> Option<u16> {
    if row.core_meta_key.len() < 3 {
        return None;
    }
    Some(u16::from_le_bytes([
        row.core_meta_key[1],
        row.core_meta_key[2],
    ]))
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in self.nodes.drain(..) {
            node.abort();
        }
        if let Err(e) = std::fs::remove_dir_all(&self.cleanup_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "Failed to remove test storage {}: {}",
                    self.cleanup_path.display(),
                    e
                );
            }
        }
    }
}

#[allow(dead_code)]
pub async fn is_port_open(addr: SocketAddr) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_millis(100),
            tokio::net::TcpStream::connect(addr)
        )
        .await,
        Ok(Ok(_))
    )
}

pub async fn wait_for_port(addr: SocketAddr, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}
