use std::collections::HashSet;
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use anvil::anvil_api::GetAccessTokenRequest;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil_core::AppState;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::Credentials;
use futures_util::StreamExt;
use tokio::task::JoinHandle;
use tracing_subscriber::{self, EnvFilter};

static INIT_LOGGER: Once = Once::new();

#[allow(dead_code)]
pub fn extract_credential(output: &str, key: &str) -> String {
    output
        .lines()
        .find(|line| line.contains(key))
        .map(|line| line.split(':').nth(1).unwrap().trim().to_string())
        .unwrap()
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
            scopes: vec!["*|*".to_string()],
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
    pub token: String,
    pub admin_state_path: String,
    pub config: Arc<anvil_core::config::Config>,
    pub storage_path: PathBuf,
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
        });
        create_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", self.token).parse().unwrap(),
        );
        bucket_client.create_bucket(create_req).await.unwrap();
    }

    #[allow(dead_code)]
    pub async fn new(regions: &[&str]) -> Self {
        INIT_LOGGER.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_env_filter(EnvFilter::new(
                    "warn,anvil=debug,anvil_core=debug,anvil_core::cluster=warn",
                ))
                .try_init();
        });

        let storage_path =
            std::env::temp_dir().join(format!("anvil-test-storage-{}", uuid::Uuid::new_v4()));
        let config = Arc::new(anvil_core::config::Config {
            cluster_secret: Some("test-cluster-secret".to_string()),
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            cluster_listen_addr: "/ip4/127.0.0.1/udp/0/quic-v1".to_string(),
            public_cluster_addrs: vec![],
            metadata_cache_ttl_secs: 1,
            public_api_addr: "127.0.0.1:0".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "".to_string(),
            bootstrap_addrs: vec![],
            init_cluster: false,
            enable_mdns: false,
            storage_path: storage_path.to_string_lossy().into_owned(),
        });

        let unique_regions: HashSet<String> = regions.iter().map(|s| s.to_string()).collect();
        let first_region = regions.first().copied().unwrap_or("default");
        let admin_state_path = storage_path.join(format!("node-{first_region}"));
        let mut states = Vec::new();
        for region_name in regions {
            let mut node_config = config.deref().clone();
            node_config.region = region_name.to_string();
            node_config.metadata_cache_ttl_secs = 1;
            node_config.storage_path = storage_path
                .join(format!("node-{region_name}"))
                .to_string_lossy()
                .into_owned();
            let state = AppState::new(node_config, None).await.unwrap();
            state.db.create_region(region_name).await.unwrap();
            let tenant = state
                .db
                .create_tenant("default", "default-key")
                .await
                .unwrap();
            let encryption_key = hex::decode(&state.config.anvil_secret_encryption_key).unwrap();
            if state
                .db
                .get_app_by_client_id("test-app")
                .await
                .unwrap()
                .is_none()
            {
                let encrypted_secret =
                    anvil::crypto::encrypt(b"test-secret", &encryption_key).unwrap();
                let app = state
                    .db
                    .create_app(tenant.id, "test-app", "test-app", &encrypted_secret)
                    .await
                    .unwrap();
                state.db.grant_policy(app.id, "*", "*").await.unwrap();
            }
            states.push(state);
        }
        for region in unique_regions {
            for state in &states {
                state.db.create_region(&region).await.unwrap();
            }
        }

        Self {
            nodes: Vec::new(),
            states,
            grpc_addrs: Vec::new(),
            token: String::new(),
            admin_state_path: admin_state_path.to_string_lossy().into_owned(),
            config,
            storage_path,
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
        let mut swarms = Vec::new();
        for state in &self.states {
            swarms.push(
                anvil_core::cluster::create_swarm(state.config.clone())
                    .await
                    .unwrap(),
            );
        }

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

        for i in 0..swarms.len() {
            for (j, addr) in listen_addrs.iter().enumerate() {
                if i != j {
                    swarms[i].dial(addr.clone()).unwrap();
                }
            }
        }

        for i in 0..self.states.len() {
            let mut state = self.states[i].clone();
            let swarm = swarms.remove(0);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            self.grpc_addrs.push(format!("http://{}", addr));

            let cfg = &state.config.deref();
            let mut cfg = anvil_core::config::Config::from_ref(cfg);
            cfg.public_api_addr = format!("http://{}", addr);
            state.config = Arc::new(cfg);

            let handle = tokio::spawn(async move {
                let (_tx, rx) = tokio::sync::mpsc::channel(1);
                anvil::start_node(listener, state, swarm, rx).await.unwrap();
            });
            self.nodes.push(handle);
        }

        if get_new_token {
            self.token = self.states[0]
                .jwt_manager
                .mint_token("test-app".to_string(), vec!["*|*".to_string()], 1)
                .unwrap();
        }

        let start = Instant::now();
        while start.elapsed() < timeout {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let mut all_converged = true;
            for state in &self.states {
                let cluster_state = state.cluster.read().await;
                if cluster_state.len() < self.nodes.len() {
                    all_converged = false;
                    break;
                }
            }
            if all_converged {
                for addr_str in &self.grpc_addrs {
                    let addr: SocketAddr = addr_str.replace("http://", "").parse().unwrap();
                    if !wait_for_port(addr, Duration::from_secs(5)).await {
                        panic!("gRPC port {} did not open in time", addr);
                    }
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
                return;
            }
        }
        panic!("Cluster did not converge in time");
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
        self.start_and_converge(timeout).await;
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in self.nodes.drain(..) {
            node.abort();
        }
        if let Err(e) = std::fs::remove_dir_all(&self.storage_path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "Failed to remove test storage {}: {}",
                    self.storage_path.display(),
                    e
                );
            }
        }
    }
}

#[allow(dead_code)]
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
