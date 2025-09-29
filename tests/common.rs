use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::{CreateBucketRequest, GetAccessTokenRequest};
use anvil::auth::JwtManager;
use anyhow::Result;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use std::future::Future;
use std::net::SocketAddr;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::NoTls;

pub mod migrations {
    use refinery_macros::embed_migrations;
    embed_migrations!("migrations_global");
}

pub mod regional_migrations {
    use refinery_macros::embed_migrations;
    embed_migrations!("migrations_regional");
}

pub fn create_pool(db_url: &str) -> Result<Pool> {
    let pg_config = tokio_postgres::Config::from_str(db_url)?;
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr = deadpool_postgres::Manager::from_config(pg_config, NoTls, mgr_config);
    Pool::builder(mgr).build().map_err(Into::into)
}

/// A test fixture that creates unique, isolated databases for a single test run
/// and guarantees they are cleaned up afterwards.
#[allow(dead_code)]
pub fn extract_credential(output: &str, key: &str) -> String {
    output
        .lines()
        .find(|line| line.contains(key))
        .map(|line| line.split(':').nth(1).unwrap().trim().to_string())
        .unwrap()
}

pub async fn with_test_dbs<F, Fut>(test_body: F)
where
    F: FnOnce(String, String, String) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
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
    let east_db_name = format!("test_east_{}", suffix);
    let west_db_name = format!("test_west_{}", suffix);

    maint_client
        .execute(&format!("CREATE DATABASE \"{}\"", global_db_name), &[])
        .await
        .unwrap();
    maint_client
        .execute(&format!("CREATE DATABASE \"{}\"", east_db_name), &[])
        .await
        .unwrap();
    maint_client
        .execute(&format!("CREATE DATABASE \"{}\"", west_db_name), &[])
        .await
        .unwrap();

    let base_db_url = "postgres://worka:worka@localhost:5432";
    let global_db_url = format!("{}/{}", base_db_url, global_db_name);
    let east_db_url = format!("{}/{}", base_db_url, east_db_name);
    let west_db_url = format!("{}/{}", base_db_url, west_db_name);

    let test_future = test_body(
        global_db_url.clone(),
        east_db_url.clone(),
        west_db_url.clone(),
    );
    let result = tokio::spawn(test_future).await;

    // Cleanup
    maint_client
        .execute(
            &format!("DROP DATABASE \"{}\" WITH (FORCE)", global_db_name),
            &[],
        )
        .await
        .unwrap();
    maint_client
        .execute(
            &format!("DROP DATABASE \"{}\" WITH (FORCE)", east_db_name),
            &[],
        )
        .await
        .unwrap();
    maint_client
        .execute(
            &format!("DROP DATABASE \"{}\" WITH (FORCE)", west_db_name),
            &[],
        )
        .await
        .unwrap();

    if let Err(err) = result {
        if err.is_panic() {
            std::panic::resume_unwind(err.into_panic());
        }
    }
}

pub async fn wait_for_port(addr: SocketAddr, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    false
}

/// Connects to databases, runs migrations, and returns a fully configured AppState and Swarm.
pub async fn prepare_node_state(
    global_db_url: &str,
    regional_db_url: &str,
    region_name: &str,
) -> Result<(
    anvil::AppState,
    libp2p::Swarm<anvil::cluster::ClusterBehaviour>,
)> {
    let global_pool = create_pool(global_db_url)?;
    let regional_pool = create_pool(regional_db_url)?;

    // Run migrations
    let mut global_client = global_pool.get().await?;
    migrations::migrations::runner()
        .set_migration_table_name("refinery_schema_history_global")
        .run_async(&mut **global_client)
        .await?;

    let mut regional_client = regional_pool.get().await?;
    regional_migrations::migrations::runner()
        .set_migration_table_name(&format!(
            "refinery_schema_history_{}",
            region_name.to_lowercase()
        ))
        .run_async(&mut **regional_client)
        .await?;

    // Insert global metadata
    global_client
        .execute(
            "INSERT INTO tenants (id, name, api_key) VALUES (1, 'default', 'default-key') ON CONFLICT (id) DO NOTHING",
            &[],
        )
        .await?;
    global_client
        .execute(
            "INSERT INTO regions (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[&region_name],
        )
        .await?;

    // Create AppState
    let storage = anvil::storage::Storage::new().await?;
    let cluster_state =
        std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    let swarm = anvil::cluster::create_swarm().await?;

    let state = anvil::AppState {
        db: anvil::persistence::Persistence::new(global_pool, regional_pool),
        storage,
        cluster: cluster_state.clone(),
        sharder: anvil::sharding::ShardManager::new(),
        placer: anvil::placement::PlacementManager::default(),
        jwt_manager: Arc::new(JwtManager::new("secret".to_string())),
        region: region_name.to_string(),
    };

    Ok((state, swarm))
}

#[allow(dead_code)]
pub async fn get_auth_token(global_db_url: &str, grpc_addr: &str) -> String {
    let admin_args = &["run", "--bin", "admin", "--"];

    // Create app
    let app_output = Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "apps",
            "create",
            "--tenant-name",
            "default",
            "--app-name",
            "test-app",
        ]))
        .env("GLOBAL_DATABASE_URL", global_db_url)
        .output()
        .unwrap();
    assert!(app_output.status.success());
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");

    // Grant policy
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

    // Get token
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

use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::AppState;
use tokio::task::JoinHandle;
use tonic::Request;

// Starts a cluster of nodes for testing.
pub async fn start_cluster(
    global_db_url: &str,
    regional_db_url: &str,
    num_nodes: usize,
) -> (Vec<JoinHandle<()>>, Vec<String>, String) {
    let cluster_state = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    let mut nodes = Vec::new();
    let mut grpc_addrs = Vec::new();

    for _ in 0..num_nodes {
        let global_pool = create_pool(global_db_url).unwrap();
        let regional_pool = create_pool(regional_db_url).unwrap();

        let mut state = AppState::new(
            global_pool,
            regional_pool,
            "TEST_REGION".to_string(),
            "test-secret".to_string(),
        )
        .await
        .unwrap();
        state.cluster = cluster_state.clone();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let http_addr = format!("http://{}", addr);
        grpc_addrs.push(http_addr);

        let server_state = state.clone();
        let node_handle = tokio::spawn(async move {
            anvil::run(addr).await.unwrap();
        });
        nodes.push(node_handle);
    }

    // Wait for cluster to form
    loop {
        let state = cluster_state.read().await;
        if state.len() >= num_nodes {
            tokio::time::sleep(Duration::from_secs(2)).await; // allow for stabilization
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let token = get_auth_token(global_db_url, &grpc_addrs[0]).await;

    (nodes, grpc_addrs, token)
}
// Starts a single node server for testing and returns its state and address.
pub async fn start_test_server(global_db_url: &str, regional_db_url: &str) -> (AppState, String) {
    let global_pool = create_pool(global_db_url).unwrap();
    let regional_pool = create_pool(regional_db_url).unwrap();
    let state = AppState::new(
        global_pool,
        regional_pool,
        "TEST_REGION".to_string(),
        "test-secret".to_string(),
    )
    .await
    .unwrap();
    let grpc_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap(); // Use port 0 for random port

    let listener = tokio::net::TcpListener::bind(grpc_addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        anvil::run(actual_addr).await.unwrap();
    });

    (state, format!("http://{}", actual_addr))
}

pub async fn create_test_bucket(grpc_addr: &str, bucket_name: &str, token: &str) {
    let mut client = BucketServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    let mut req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.to_string(),
        region: "TEST_REGION".to_string(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    client.create_bucket(req).await.unwrap();
}

// Creates an app and returns its client_id and client_secret.
pub fn create_app(global_db_url: &str, app_name: &str) -> (String, String) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let app_output = Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "apps",
            "create",
            "--tenant-name",
            "default",
            "--app-name",
            app_name,
        ]))
        .env("GLOBAL_DATABASE_URL", global_db_url)
        .output()
        .unwrap();
    assert!(app_output.status.success());
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");
    (client_id, client_secret)
}

pub async fn get_auth_token_for_app(
    global_db_url: &str,
    grpc_addr: &str,
    app_name: &str,
    action: &str,
    resource: &str,
) -> String {
    let (client_id, client_secret) = create_app(global_db_url, app_name);
    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policies",
        "grant",
        "--app-name",
        app_name,
        "--action",
        action,
        "--resource",
        resource,
    ];
    let status = Command::new("cargo")
        .args(admin_args.iter().chain(policy_args.iter()))
        .env("GLOBAL_DATABASE_URL", global_db_url)
        .status()
        .unwrap();
    assert!(status.success());

    get_token_for_scopes(grpc_addr, &client_id, &client_secret, vec![]).await
}

pub async fn get_token_for_scopes(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
    scopes: Vec<String>,
) -> String {
    try_get_token_for_scopes(grpc_addr, client_id, client_secret, scopes)
        .await
        .unwrap()
}

pub async fn try_get_token_for_scopes(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
    scopes: Vec<String>,
) -> Result<String, tonic::Status> {
    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            scopes,
        })
        .await
        .map(|r| r.into_inner().access_token)
}
