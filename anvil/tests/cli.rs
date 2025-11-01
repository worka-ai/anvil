use std::process::Command;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tempfile::tempdir;

mod common;

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

async fn setup_test_profile(cluster: &common::TestCluster, config_dir: &std::path::Path) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let global_db_url = cluster.global_db_url.clone();
    let app_name = "cli-test-app";

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
    let client_id = common::extract_credential(&creds, "Client ID");
    let client_secret = common::extract_credential(&creds, "Client Secret");

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
}

#[tokio::test]
async fn test_cli_configure_and_bucket_ls() {
    let mut cluster = common::TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-cli-bucket-{}", uuid::Uuid::new_v4());
    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["bucket", "ls"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&bucket_name));
}

#[tokio::test]
async fn test_cli_bucket_create_and_rm() {
    let mut cluster = common::TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-cli-bucket-{}", uuid::Uuid::new_v4());

    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["bucket", "ls"], config_dir.path()).await;
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&bucket_name));

    let output = run_cli(&["bucket", "rm", &bucket_name], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["bucket", "ls"], config_dir.path()).await;
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains(&bucket_name));
}

#[tokio::test]
async fn test_cli_object_put_and_get() {
    let mut cluster = common::TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-cli-object-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "my-cli-object";
    let content = "hello from cli object test";

    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, content).unwrap();

    let dest = format!("s3://{}/{}", bucket_name, object_key);
    let output = run_cli(&["object", "put", file_path.to_str().unwrap(), &dest], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&["object", "get", &dest], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout, content);
}

#[tokio::test]
async fn test_cli_hf_ingestion() {
    let mut cluster = common::TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    let config_dir = tempdir().unwrap();
    setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-cli-hf-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "config.json";

    let output = run_cli(&["bucket", "create", &bucket_name, "test-region-1"], config_dir.path()).await;
    assert!(output.status.success());

    let hf_token = "test-token";
    let output = run_cli(&["hf", "key", "add", "--name", "test-key", "--token", &hf_token], config_dir.path()).await;
    assert!(output.status.success());

    let output = run_cli(&[
        "hf", "ingest", "start",
        "--key", "test-key",
        "--repo", "openai/gpt-oss-20b",
        "--bucket", &bucket_name,
        "--target-region", "test-region-1",
        "--include", "config.json",
    ], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let ingestion_id = stdout.split_whitespace().last().unwrap();

    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(120) {
            panic!("Timeout waiting for HF ingestion to complete");
        }
        let output = run_cli(&["hf", "ingest", "status", "--id", ingestion_id], config_dir.path()).await;
        let stdout = String::from_utf8(output.stdout).unwrap();
        if stdout.contains("state=completed") {
            break;
        }
        if stdout.contains("state=failed") {
            panic!("Ingestion failed: {}", stdout);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    let dest = format!("s3://{}/{}", bucket_name, object_key);
    let output = run_cli(&["object", "head", &dest], config_dir.path()).await;
    assert!(output.status.success());
}