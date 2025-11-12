use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use anvil_test_utils::*;

static CLI_PATH: OnceLock<String> = OnceLock::new();

fn get_cli_path() -> &'static str {
    CLI_PATH.get_or_init(|| {
        let status = Command::new("cargo")
            .args(&["build", "--package", "anvil-cli"])
            .status()
            .expect("Failed to build anvil-cli");
        assert!(status.success());

        let metadata_output = Command::new("cargo")
            .arg("metadata")
            .arg("--format-version=1")
            .output()
            .expect("Failed to get cargo metadata");
        let metadata: serde_json::Value = serde_json::from_slice(&metadata_output.stdout).unwrap();
        let target_dir = metadata["target_directory"].as_str().unwrap();
        format!("{}/debug/anvil-cli", target_dir)
    })
}

async fn run_cli(args: &[&str], config_dir: &std::path::Path) -> std::process::Output {
    let cli_path = get_cli_path().to_string();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let config_dir = config_dir.to_path_buf();

    tokio::task::spawn_blocking(move || {
        println!(
            "Running CLI command: {} {} (HOME={})",
            cli_path,
            args.join(" "),
            config_dir.to_str().unwrap()
        );
        let output = Command::new(&cli_path)
            .args(&args)
            .env("HOME", &config_dir)
            .output()
            .expect("Failed to run anvil-cli");

        println!("CLI command finished: {:?}", args);
        println!("  Status: {}", output.status);
        println!("  Stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("  Stderr: {}", String::from_utf8_lossy(&output.stderr));

        if !output.status.success() {
            eprintln!("CLI command failed: {:?}", args);
            eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }

        output
    })
    .await
    .unwrap()
}

use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::ListBucketsRequest;
use tonic::Request;

async fn wait_for_bucket(bucket_name: &str, cluster: &TestCluster) {
    let start = Instant::now();
    let timeout = Duration::from_secs(30);

    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .expect("Failed to connect to bucket service");

    loop {
        if start.elapsed() > timeout {
            panic!("Timeout waiting for bucket {} to be created", bucket_name);
        }

        let mut request = Request::new(ListBucketsRequest {});
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", cluster.token).parse().unwrap(),
        );

        match bucket_client.list_buckets(request).await {
            Ok(response) => {
                let buckets = response.into_inner().buckets;
                if buckets.iter().any(|b| b.name == bucket_name) {
                    println!("Bucket {} found.", bucket_name);
                    return;
                }
            }
            Err(status) => {
                println!(
                    "Error listing buckets while waiting: {:?}. Retrying...",
                    status
                );
            }
        }

        println!("Waiting for bucket {} to appear...", bucket_name);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn setup_test_profile(cluster: &TestCluster, config_dir: &std::path::Path) -> (String, String) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let global_db_url = cluster.global_db_url.clone();
    let app_name = format!("cli-test-app-{}", uuid::Uuid::new_v4());

    // Create the app
    let create_args: Vec<String> = admin_args
        .iter()
        .map(|s| s.to_string())
        .chain([
            "--global-database-url".to_string(),
            global_db_url.clone(),
            "--anvil-secret-encryption-key".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "apps".to_string(),
            "create".to_string(),
            "--tenant-name".to_string(),
            "default".to_string(),
            "--app-name".to_string(),
            app_name.to_string(),
        ])
        .collect();

    let app_output = tokio::task::spawn_blocking(move || {
        Command::new("cargo").args(&create_args).output().unwrap()
    })
    .await
    .unwrap();

    assert!(app_output.status.success());
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");

    // Grant policies to the app
    let grant_args: Vec<String> = admin_args
        .iter()
        .map(|s| s.to_string())
        .chain([
            "--global-database-url".to_string(),
            global_db_url,
            "--anvil-secret-encryption-key".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "policies".to_string(),
            "grant".to_string(),
            "--app-name".to_string(),
            app_name.to_string(),
            "--action".to_string(),
            "*".to_string(),
            "--resource".to_string(),
            "*".to_string(),
        ])
        .collect();

    let grant_output = tokio::task::spawn_blocking(move || {
        Command::new("cargo").args(&grant_args).output().unwrap()
    })
    .await
    .unwrap();
    assert!(grant_output.status.success());


    // Configure the CLI profile
    let output = run_cli(
        &[
            "static-config",
            "--name",
            "default",
            "--host",
            &cluster.grpc_addrs[0],
            "--client-id",
            &client_id,
            "--client-secret",
            &client_secret,
            "--default",
        ],
        config_dir,
    )
    .await;
    assert!(output.status.success());
    (client_id, client_secret)
}

#[tokio::test]
async fn test_cli_auth_get_token() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let (client_id, client_secret) = setup_test_profile(&cluster, config_dir.path()).await;

    let output = run_cli(&["auth", "get-token", "--client-id", &client_id, "--client-secret", &client_secret], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.is_empty());
}


async fn create_app(cluster: &TestCluster, app_name: &str) -> (String, String) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let global_db_url = cluster.global_db_url.clone();

    // Create the app
    let create_args: Vec<String> = admin_args
        .iter()
        .map(|s| s.to_string())
        .chain([
            "--global-database-url".to_string(),
            global_db_url.clone(),
            "--anvil-secret-encryption-key".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "apps".to_string(),
            "create".to_string(),
            "--tenant-name".to_string(),
            "default".to_string(),
            "--app-name".to_string(),
            app_name.to_string(),
        ])
        .collect();

    let app_output = tokio::task::spawn_blocking(move || {
        Command::new("cargo").args(&create_args).output().unwrap()
    })
    .await
    .unwrap();

    assert!(app_output.status.success());
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");

    // Grant policies to the app
    let grant_args: Vec<String> = admin_args
        .iter()
        .map(|s| s.to_string())
        .chain([
            "--global-database-url".to_string(),
            global_db_url,
            "--anvil-secret-encryption-key".to_string(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            "policies".to_string(),
            "grant".to_string(),
            "--app-name".to_string(),
            app_name.to_string(),
            "--action".to_string(),
            "*".to_string(),
            "--resource".to_string(),
            "*".to_string(),
        ])
        .collect();

    let grant_output = tokio::task::spawn_blocking(move || {
        Command::new("cargo").args(&grant_args).output().unwrap()
    })
    .await
    .unwrap();
    assert!(grant_output.status.success());

    (client_id, client_secret)
}

#[tokio::test]
async fn test_cli_auth_grant() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let grantee_app_name = format!("grantee-app-{}", uuid::Uuid::new_v4());
    let (_grantee_client_id, _) = create_app(&cluster, &grantee_app_name).await;

    let output = run_cli(&["auth", "grant", &grantee_app_name, "read", "bucket:my-bucket"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Permission granted."));
}

#[tokio::test]
async fn test_cli_auth_revoke() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let grantee_app_name = format!("grantee-app-{}", uuid::Uuid::new_v4());
    let (_grantee_client_id, _) = create_app(&cluster, &grantee_app_name).await;

    let output = run_cli(&["auth", "grant", &grantee_app_name, "read", "bucket:my-bucket"], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["auth", "revoke", &grantee_app_name, "read", "bucket:my-bucket"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Permission revoked."));
}

#[tokio::test]
async fn test_cli_bucket_set_public() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-public-bucket-{}", uuid::Uuid::new_v4());
    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    wait_for_bucket(&bucket_name, &cluster).await;

    let output = run_cli(&["bucket", "set-public", &bucket_name, "--allow", "true"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!("Public access for bucket {} set to true", bucket_name)));

    let output = run_cli(&["bucket", "set-public", &bucket_name, "--allow", "false"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!("Public access for bucket {} set to false", bucket_name)));
}

#[tokio::test]
async fn test_cli_object_rm() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-object-rm-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "my-object-to-rm";
    let content = "hello from object rm test";

    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    wait_for_bucket(&bucket_name, &cluster).await;

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, content).unwrap();

    let dest = format!("s3://{}/{}", bucket_name, object_key);
    let output = run_cli(&["object", "put", file_path.to_str().unwrap(), &dest], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["object", "rm", &dest], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Removed"));
}

#[tokio::test]
async fn test_cli_object_ls() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-object-ls-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "my-object-to-ls";
    let content = "hello from object ls test";

    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    wait_for_bucket(&bucket_name, &cluster).await;

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, content).unwrap();

    let dest = format!("s3://{}/{}", bucket_name, object_key);
    let output = run_cli(&["object", "put", file_path.to_str().unwrap(), &dest], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["object", "ls", &format!("s3://{}/", bucket_name)], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(object_key));
}

#[tokio::test]
async fn test_cli_object_get_to_file() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-object-get-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "my-object-to-get";
    let content = "hello from object get to file test";

    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    wait_for_bucket(&bucket_name, &cluster).await;
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, content).unwrap();

    let dest_s3 = format!("s3://{}/{}", bucket_name, object_key);
    let output = run_cli(&["object", "put", file_path.to_str().unwrap(), &dest_s3], config_dir.path()).await;
    assert!(output.status.success());

    let download_path = temp_dir.path().join("downloaded.txt");
    let output = run_cli(&["object", "get", &dest_s3, download_path.to_str().unwrap()], config_dir.path()).await;
    assert!(output.status.success());

    let downloaded_content = std::fs::read_to_string(download_path).unwrap();
    assert_eq!(content, downloaded_content);
}

#[tokio::test]
async fn test_cli_hf_key_ls() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let output = run_cli(&["hf", "key", "add", "--name", "test-key", "--token", "test-token"], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["hf", "key", "ls"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("test-key"));
}

#[tokio::test]
async fn test_cli_hf_key_rm() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let output = run_cli(&["hf", "key", "add", "--name", "test-key", "--token", "test-token"], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["hf", "key", "rm", "--name", "test-key"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("deleted key: test-key"));
}

#[tokio::test]
async fn test_cli_hf_ingest_cancel() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let output = run_cli(&["hf", "key", "add", "--name", "test-key", "--token", "test-token"], config_dir.path()).await;
    assert!(output.status.success());

    let bucket_name = format!("my-hf-ingest-cancel-bucket-{}", uuid::Uuid::new_v4());
    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    wait_for_bucket(&bucket_name, &cluster).await;

    let output = run_cli(&[
        "hf", "ingest", "start",
        "--key", "test-key",
        "--repo", "openai/gpt-oss-20b",
        "--bucket", &bucket_name,
        "--target-region", "test-region-1",
    ], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let ingestion_id = stdout.split_whitespace().last().unwrap();

    let output = run_cli(&["hf", "ingest", "cancel", "--id", ingestion_id], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("canceled"));
}

#[tokio::test]
async fn test_cli_hf_ingest_start_with_options() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let output = run_cli(&["hf", "key", "add", "--name", "test-key", "--token", "test-token"], config_dir.path()).await;
    assert!(output.status.success());

    let bucket_name = format!("hf-ingest-opts-{}", uuid::Uuid::new_v4());
    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    wait_for_bucket(&bucket_name, &cluster).await;

    let output = run_cli(&[
        "hf", "ingest", "start",
        "--key", "test-key",
        "--repo", "openai/gpt-oss-20b",
        "--bucket", &bucket_name,
        "--target-region", "test-region-1",
        "--revision", "main",
        "--prefix", "my-prefix",
        "--exclude", "*.txt",
    ], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("ingestion id:"));
}

#[tokio::test]
#[ignore]
async fn test_cli_configure_interactive() {
    todo!()
}
