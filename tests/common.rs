use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::GetAccessTokenRequest;
use anvil::{run_migrations, AppState};
use anyhow::Result;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use futures_util::StreamExt;
use libp2p::Multiaddr;
use std::process::Command;
use std::str::FromStr;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;

use std::collections::HashMap;
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
        .args(
            admin_args
                .iter()
                .chain(&["apps", "create", "--tenant-name", "default", "--app-name", "test-app"]),
        )
        .env("GLOBAL_DATABASE_URL", global_db_url)
        .output()
        .unwrap();
    assert!(app_output.status.success());
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");

    let policy_args = &[
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
        .env("GLOBAL_DATABASE_URL", global_db_url)
        .status()
        .unwrap();
    assert!(status.success());

    // Wait a moment for the server to be ready before connecting.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
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
    _global_db_url: String,
    _regional_db_urls: Vec<String>,
}

impl TestCluster {
    #[allow(dead_code)]
    pub async fn new(regions: Vec<&str>) -> Self {
        let num_nodes = regions.len();
        let (global_db_url, regional_db_urls, _maint_client) = create_isolated_dbs(num_nodes).await;

        run_migrations(
            &global_db_url,
            migrations::migrations::runner(),
            "refinery_schema_history_global",
        )
        .await
        .unwrap();
        for db_url in &regional_db_urls {
            run_migrations(
                db_url,
                regional_migrations::migrations::runner(),
                "refinery_schema_history_regional",
            )
            .await
            .unwrap();
        }
        
        let global_pool = create_pool(&global_db_url).unwrap();
        for region in &regions {
            create_default_tenant(&global_pool, region).await;
        }

        let mut states = Vec::new();
        for i in 0..num_nodes {
            let regional_pool = create_pool(&regional_db_urls[i]).unwrap();
            let global_pool = create_pool(&global_db_url).unwrap();
            let state = AppState::new(
                global_pool,
                regional_pool,
                regions[i].to_string(),
                "test-secret".to_string(),
            )
            .await
            .unwrap();
            states.push(state);
        }

        Self {
            nodes: Vec::new(),
            states,
            grpc_addrs: Vec::new(),
            token: String::new(),
            _global_db_url: global_db_url,
            _regional_db_urls: regional_db_urls,
        }
    }

    pub async fn start_and_converge(&mut self, timeout: Duration) {
        let mut swarms = Vec::new();
        for _ in 0..self.states.len() {
            swarms.push(anvil::cluster::create_swarm().await.unwrap());
        }

        let mut listen_addrs = Vec::new();
        for swarm in &mut swarms {
            swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();
            if let Some(event) = swarm.next().await {
                if let libp2p::swarm::SwarmEvent::NewListenAddr { address, .. } = event {
                    listen_addrs.push(address);
                } else { panic!("Expected NewListenAddr event"); }
            } else { panic!("Swarm stream ended unexpectedly"); }
        }

        for i in 0..swarms.len() {
            for j in 0..listen_addrs.len() {
                if i == j { continue; }
                swarms[i].dial(listen_addrs[j].clone()).unwrap();
            }
        }

        for i in 0..self.states.len() {
            let state = self.states[i].clone();
            let swarm = swarms.remove(0);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            self.grpc_addrs.push(format!("http://{}", listener.local_addr().unwrap()));
            
            let handle = tokio::spawn(async move {
                anvil::start_node(listener, state, swarm, None).await.unwrap();
            });
            self.nodes.push(handle);
        }

        self.token = get_auth_token(&self._global_db_url, &self.grpc_addrs[0]).await;

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
    let (maint_client, connection) = tokio_postgres::connect(&maint_db_url, NoTls)
        .await
        .expect("Failed to connect to maintenance database");
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
