use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::{CreateBucketRequest, GetAccessTokenRequest};
use anvil::auth::JwtManager;
use anyhow::Result;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use std::future::Future;
use std::process::Command;
use std::str::FromStr;
use std::sync::Arc;
use tokio_postgres::NoTls;

use std::{net::SocketAddr, time::{Duration, Instant}};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time,
};
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

    // Wait for the new databases to be ready
    let db_timeout = Duration::from_secs(5);
    wait_for_db(&global_db_url, db_timeout).await;
    wait_for_db(&east_db_url, db_timeout).await;
    wait_for_db(&west_db_url, db_timeout).await;

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

pub async fn wait_for_db(db_url: &str, timeout: Duration) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if tokio_postgres::connect(db_url, NoTls).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("Database {} did not become available in time", db_url);
}

/// Poll `addr` until an HTTP response is observed or `timeout` elapses.
/// Returns true if an HTTP response was read (status line or headers), false on timeout.
pub async fn wait_for_port(addr: SocketAddr, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let per_try_cap = Duration::from_millis(500); // cap any single step

    loop {
        // stop if we've run out of time
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let remaining = deadline.saturating_duration_since(now);
        let step_budget = remaining.min(per_try_cap);

        // 1) connect with a small timeout
        let stream = match time::timeout(step_budget, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => s,
            Ok(Err(_e)) => {
                // refused or other connect error — try again shortly
                time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(_elapsed) => {
                // connect attempt took too long — try again
                time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        // 2) send a minimal HTTP/1.1 request (ask server to close to avoid hanging)
        let mut stream = stream;
        let req = format!(
            "GET / HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
            addr
        );
        if let Err(_e) = time::timeout(step_budget, stream.write_all(req.as_bytes())).await {
            // write stalled — try again
            let _ = stream.shutdown().await;
            time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        // 3) read until we see the end of headers or we time out
        let mut buf = Vec::with_capacity(1024);
        let mut tmp = [0u8; 512];

        let read_deadline = Instant::now() + step_budget;
        let success = loop {
            // abort this inner loop if this attempt exceeds its budget
            if Instant::now() >= read_deadline {
                break false;
            }
            let left = read_deadline.saturating_duration_since(Instant::now());
            match time::timeout(left, stream.read(&mut tmp)).await {
                Ok(Ok(0)) => {
                    // EOF. Check whatever we got.
                    break looks_like_http(&buf);
                }
                Ok(Ok(n)) => {
                    buf.extend_from_slice(&tmp[..n]);
                    if looks_like_http(&buf) {
                        break true;
                    }
                    // keep reading a bit more until headers complete or budget ends
                    continue;
                }
                Ok(Err(_e)) => break false,  // read error on this attempt
                Err(_elapsed) => break false, // per-try read timeout
            }
        };

        let _ = stream.shutdown().await;

        if success {
            // Optionally: print what we saw (trim to avoid huge spam in tests)
            #[cfg(test)]
            {
                let s = String::from_utf8_lossy(&buf);
                println!("Got response from {}:\n{}", addr, s);
            }
            return true;
        }

        // Back off briefly before the next attempt.
        time::sleep(Duration::from_millis(100)).await;
    }
}

/// Heuristic: if the buffer starts with "HTTP/" or contains "\r\n\r\n",
/// we consider it an HTTP response (status line or completed headers).
fn looks_like_http(buf: &[u8]) -> bool {
    if buf.starts_with(b"HTTP/") {
        return true;
    }
    // Some servers might send a TLS alert / redirect; but for a plain TCP HTTP probe
    // we accept end-of-headers as a good enough signal.
    memchr::memmem::find(buf, b"\r\n\r\n").is_some()
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

// The new, preferred test architecture.

pub struct TestWorld {
    pub state: AppState,
    pub grpc_addr: String,
    pub global_db_url: String,
    pub regional_db_url: String,
}

impl TestWorld {
    pub async fn new() -> Self {
        let (global_db_url, regional_db_url) = create_isolated_dbs().await;

        wait_for_db(&global_db_url, Duration::from_secs(5)).await;
        wait_for_db(&regional_db_url, Duration::from_secs(5)).await;

        let (state, grpc_addr) = start_test_server(&global_db_url, &regional_db_url).await;

        Self {
            state,
            grpc_addr,
            global_db_url,
            regional_db_url,
        }
    }
}

async fn create_isolated_dbs() -> (String, String) {
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
    let regional_db_name = format!("test_regional_{}", suffix);

    maint_client
        .execute(&format!("CREATE DATABASE \"{}\"", global_db_name), &[])
        .await
        .unwrap();
    maint_client
        .execute(&format!("CREATE DATABASE \"{}\"", regional_db_name), &[])
        .await
        .unwrap();

    let base_db_url = "postgres://worka:worka@localhost:5432";
    let global_db_url = format!("{}/{}", base_db_url, global_db_name);
    let regional_db_url = format!("{}/{}", base_db_url, regional_db_name);

    // We don't cleanup here. The test runner will kill the process, and we can
    // manually clean up databases if needed. A more robust solution could involve
    // a separate cleanup script or a test runner that supports async drop.

    (global_db_url, regional_db_url)
}

pub struct TestCluster {
    pub nodes: Vec<JoinHandle<()>>,
    pub grpc_addrs: Vec<String>,
    pub token: String,
    pub global_db_url: String,
    pub regional_db_url: String,
}

impl TestCluster {
    pub async fn new(num_nodes: usize) -> Self {
        let (global_db_url, regional_db_url) = create_isolated_dbs().await;

        // Run migrations once before starting any nodes
        anvil::run_migrations(
            &global_db_url,
            migrations::migrations::runner(),
            "refinery_schema_history_global",
        )
        .await
        .unwrap();
        anvil::run_migrations(
            &regional_db_url,
            regional_migrations::migrations::runner(),
            "refinery_schema_history_regional",
        )
        .await
        .unwrap();

        let mut nodes = Vec::new();
        let mut grpc_addrs = Vec::new();

        for _ in 0..num_nodes {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let http_addr = format!("http://{}", addr);
            grpc_addrs.push(http_addr);

            let regional_db_url_clone = regional_db_url.to_string();
            let global_db_url_clone = global_db_url.to_string();

            let node_handle = tokio::spawn(async move {
                anvil::start_node(
                    listener,
                    "TEST_REGION".to_string(),
                    global_db_url_clone,
                    regional_db_url_clone,
                    "test-secret".to_string(),
                )
                .await
                .unwrap();
            });
            nodes.push(node_handle);
        }

        // Wait for all nodes to start
        for addr in &grpc_addrs {
            let socket_addr = addr.replace("http://", "").parse().unwrap();
            assert!(
                wait_for_port(socket_addr, Duration::from_secs(5)).await,
                "Server did not start in time"
            );
        }

        let token = get_auth_token(&global_db_url, &grpc_addrs[0]).await;

        Self {
            nodes,
            grpc_addrs,
            token,
            global_db_url,
            regional_db_url,
        }
    }
}
// Starts a single node server for testing and returns its state and address.
pub async fn start_test_server(global_db_url: &str, regional_db_url: &str) -> (AppState, String) {
    // Run migrations once before starting the server.
    anvil::run_migrations(
        global_db_url,
        migrations::migrations::runner(),
        "refinery_schema_history_global",
    )
    .await
    .unwrap();
    anvil::run_migrations(
        regional_db_url,
        regional_migrations::migrations::runner(),
        "refinery_schema_history_regional",
    )
    .await
    .unwrap();

    let global_pool = create_pool(global_db_url).unwrap();
    let regional_pool = create_pool(regional_db_url).unwrap();
    let region = "TEST_REGION".to_string();
    let jwt_secret = "test-secret".to_string();
    let state = AppState::new(
        global_pool,
        regional_pool,
        region.clone(),
        jwt_secret.clone(),
    )
    .await
    .unwrap();
    let grpc_addr = "127.0.0.1:0".parse::<SocketAddr>().unwrap();

    let listener = tokio::net::TcpListener::bind(grpc_addr).await.unwrap();
    let actual_addr = listener.local_addr().unwrap();

    let global_db_url_clone = global_db_url.to_string();
    let regional_db_url_clone = regional_db_url.to_string();

    let handle = tokio::spawn(async move {
        match anvil::start_node(
            listener,
            region,
            global_db_url_clone,
            regional_db_url_clone,
            jwt_secret,
        )
        .await
        {
            Ok(_) => {
                eprintln!("Test server exited unexpectedly with no error.")
            }
            Err(e) => {
                eprintln!("Test server exited unexpectedly. {:?}", e);
            }
        };
    });
    assert!(
        wait_for_port(actual_addr, Duration::from_secs(5)).await,
        "Server did not start in time"
    );
    if handle.is_finished() {
        // Awaiting here will not block because it’s already finished.
        match handle.await {
            Ok(val) => {
                // Task returned successfully (val is whatever your task returns).
                panic!("Task unexpectedly finished early: {:?}", val);
            }
            Err(e) => {
                // Task panicked or was cancelled.
                // Distinguish reasons:
                if e.is_panic() {
                    // You could inspect the panic payload with e.into_panic()
                    panic!("Task panicked: {:?}", e);
                } else if e.is_cancelled() {
                    panic!("Task was cancelled: {:?}", e);
                } else {
                    panic!("Task failed: {:?}", e);
                }
            }
        }
    } else {
        println!(
            "Test server port started accepted a connection so presuming it is ready for us to use...{}",
            actual_addr
        );
    }
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
