use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::GetAccessTokenRequest;
use anvil::{run_migrations, AppState};
use anyhow::Result;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use futures_util::StreamExt;
use std::process::Command;
use std::str::FromStr;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;

use anvil::config::Config;

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub mod migrations {
    use refinery_macros::embed_migrations;
    embed_migrations!("./migrations_global");
}

pub mod regional_migrations {
    use refinery_macros::embed_migrations;
    embed_migrations!("./migrations_regional");
}

pub fn create_pool(db_url: &str) -> Result<Pool> {
    let pg_config = tokio_postgres::Config::from_str(db_url)?;
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr = deadpool_postgres::Manager::from_config(pg_config, NoTls, mgr_config);
    Pool::builder(mgr).build().map_err(Into::into)
}

#[allow(dead_code)]
pub fn extract_credential(output: &str, key: &str) -> String {
    output
        .lines()
        .find(|line| line.contains(key))
        .map(|line| line.split(':').nth(1).unwrap().trim().to_string())
        .unwrap()
}

#[allow(dead_code)]
pub async fn get_auth_token(global_db_url: &str, grpc_addr: &str) -> String {
    let admin_args = &["run", "--bin", "admin", "--"];

    let app_output = Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "--global-database-url",
            global_db_url,
            // Provide a dummy key since the admin tool now requires it.
            "--worka-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "apps",
            "create",
            "--tenant-name",
            "default",
            "--app-name",
            "test-app",
        ]))
        .output()
        .unwrap();
    assert!(app_output.status.success());
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");

    let policy_args = &[
        "--global-database-url",
        global_db_url,
        "--worka-secret-encryption-key",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "policies",
        "grant",
        "--app-name",
        "test-app",
        "--action",
        "*",
        "--resource",
        "*",
    ];
    let status = Command::new("cargo")
        .args(admin_args.iter().chain(policy_args.iter()))
        .status()
        .unwrap();
    assert!(status.success());

    // Wait a moment for the server to be ready before connecting.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Ensure auth client uses gRPC path under /grpc
    let grpc_url = if grpc_addr.ends_with("/grpc") { grpc_addr.to_string() } else { format!("{}/grpc", grpc_addr.trim_end_matches('/')) };
    let mut auth_client = AuthServiceClient::connect(grpc_url)
        .await
        .unwrap();
    let token_res = auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id,
            client_secret,
            scopes: vec![],
        })
        .await
        .unwrap()
        .into_inner();

    token_res.access_token
}

#[allow(dead_code)]
pub struct TestCluster {
    pub nodes: Vec<JoinHandle<()>>,
    pub states: Vec<AppState>,
    pub grpc_addrs: Vec<String>,
    pub token: String,
    pub global_db_url: String,
    pub regional_db_urls: Vec<String>,
    pub config: Arc<Config>,
}

impl TestCluster {
    #[allow(dead_code)]
    pub async fn new(regions: &[&str]) -> Self {
        // Programmatically create config for tests instead of parsing args
        let config = Arc::new(Config {
            global_database_url: "".to_string(), // Will be replaced by create_isolated_dbs
            regional_database_url: "".to_string(), // Will be replaced by create_isolated_dbs
            jwt_secret: "test-secret".to_string(),
            worka_secret_encryption_key: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            http_bind_addr: "127.0.0.1:0".to_string(),
            quic_bind_addr: "/ip4/127.0.0.1/udp/0/quic-v1".to_string(),
            public_addrs: vec![],
            public_grpc_addr: "".to_string(), // Will be set dynamically
            grpc_bind_addr: "127.0.0.1:0".to_string(),
            region: "".to_string(), // Will be set per-node
            bootstrap_addrs: vec![],
            init_cluster: false,
            enable_mdns: false, // Disable for hermetic tests
        });
        // 1. Determine unique regions needed
        let unique_regions: HashSet<String> = regions.iter().map(|s| s.to_string()).collect();

        // 2. Create one DB for global and one for each unique region
        let (global_db_url, regional_dbs, _maint_client) =
            create_isolated_dbs(unique_regions.len()).await;
        let regional_db_map = regional_dbs
            .into_iter()
            .enumerate()
            .map(|(i, db_url)| (unique_regions.iter().nth(i).unwrap().to_string(), db_url))
            .collect::<HashMap<String, String>>();
        // 3. Run migrations on all created databases
        run_migrations(
            &global_db_url,
            migrations::migrations::runner(),
            "refinery_schema_history_global",
        )
        .await
        .unwrap();
        for (_, db_url) in regional_db_map.iter() {
            run_migrations(
                db_url,
                regional_migrations::migrations::runner(),
                "refinery_schema_history_regional",
            )
            .await
            .unwrap();
        }

        // 4. Create one connection pool for each unique regional database
        let mut regional_pools = HashMap::new();
        for (region_name, db_url) in regional_db_map.iter() {
            regional_pools.insert(region_name.clone(), create_pool(db_url).unwrap());
        }

        // 5. Create AppState for each node, sharing pools based on region
        let global_pool = create_pool(&global_db_url).unwrap();
        for region in &unique_regions {
            create_default_tenant(&global_pool, region).await;
        }

        let mut states = Vec::new();
        for region_name in regions {
            let regional_pool = regional_pools.get(*region_name).unwrap().clone();
            let mut node_config = config.deref().clone();
            node_config.region = region_name.to_string();
            let state = AppState::new(global_pool.clone(), regional_pool, node_config)
                .await
                .unwrap();
            states.push(state);
        }

        // 6. Return the TestCluster, ready to be started
        Self {
            nodes: Vec::new(),
            states,
            grpc_addrs: Vec::new(),
            token: String::new(),
            global_db_url,
            regional_db_urls: regional_db_map.values().cloned().collect(),
            config,
        }
    }

    pub async fn start_and_converge(&mut self, timeout: Duration) {
        let mut swarms = Vec::new();
        for _ in 0..self.states.len() {
            swarms.push(anvil::cluster::create_swarm(self.config.clone()).await.unwrap());
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
            for j in 0..listen_addrs.len() {
                if i == j {
                    continue;
                }
                swarms[i].dial(listen_addrs[j].clone()).unwrap();
            }
        }

        for i in 0..self.states.len() {
            let mut state = self.states[i].clone();
            let swarm = swarms.remove(0);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            self.grpc_addrs.push(format!("http://{}", addr));

            let cfg = &state.config.deref();
            let mut cfg = anvil::config::Config::from_ref(cfg);
            cfg.public_grpc_addr = format!("http://{}", addr);
            state.config = Arc::new(cfg);

            let handle = tokio::spawn(async move {
                anvil::start_node(listener, state, swarm).await.unwrap();
            });
            self.nodes.push(handle);
        }

        self.token = get_auth_token(&self.global_db_url, &self.grpc_addrs[0]).await;

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
                println!("Cluster converged with {} nodes.", self.nodes.len());
                return;
            }
        }
        panic!("Cluster did not converge in time");
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in &self.nodes {
            node.abort();
        }
    }
}

async fn create_isolated_dbs(num_regional: usize) -> (String, Vec<String>, tokio_postgres::Client) {
    dotenvy::dotenv().ok();
    let maint_db_url =
        std::env::var("MAINTENANCE_DATABASE_URL").expect("MAINTENANCE_DATABASE_URL must be set");

    let mut attempt = 0;
    let (maint_client, connection) = loop {
        if attempt > 10 {
            panic!("Failed to connect to maintenance database after 10 attempts");
        }
        match tokio_postgres::connect(&maint_db_url, NoTls).await {
            Ok(conn) => break conn,
            Err(e) => {
                println!("Failed to connect to maintenance DB, retrying... ({})", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                attempt += 1;
            }
        }
    };
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("maintenance connection error: {}", e);
        }
    });

    let suffix = uuid::Uuid::new_v4().to_string().replace('-', "");
    let global_db_name = format!("test_global_{}", suffix);
    maint_client
        .execute(&format!("CREATE DATABASE \"{}\"", global_db_name), &[])
        .await
        .unwrap();

    let mut regional_db_urls = Vec::new();
    let base_db_url = "postgres://worka:worka@localhost:5432";

    for i in 0..num_regional {
        let regional_db_name = format!("test_regional_{}_{}", suffix, i);
        maint_client
            .execute(&format!("CREATE DATABASE \"{}\"", regional_db_name), &[])
            .await
            .unwrap();
        regional_db_urls.push(format!("{}/{}", base_db_url, regional_db_name));
    }

    let global_db_url = format!("{}/{}", base_db_url, global_db_name);

    (global_db_url, regional_db_urls, maint_client)
}

pub async fn create_default_tenant(global_pool: &Pool, region: &str) {
    let client = global_pool.get().await.unwrap();
    client
        .execute(
            "INSERT INTO tenants (id, name, api_key) VALUES (1, 'default', 'default-key') ON CONFLICT (id) DO NOTHING",
            &[],
        )
        .await.unwrap();
    client
        .execute(
            "INSERT INTO regions (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[&region],
        )
        .await
        .unwrap();
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
