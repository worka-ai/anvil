#![recursion_limit = "512"]

use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::tempdir;

use anvil_test_utils::*;

async fn run_cli(args: &[&str], config_dir: &std::path::Path) -> std::process::Output {
    let cli_path = env!("CARGO_BIN_EXE_anvil").to_string();
    let config_path = config_dir.join("config.toml");
    let mut all_args = vec![
        "--config".to_string(),
        config_path.to_str().unwrap().to_string(),
    ];
    all_args.extend(args.iter().map(|s| s.to_string()));

    let config_dir_path = config_dir.to_path_buf();

    tokio::task::spawn_blocking(move || {
        println!("Running CLI command: {} {}", cli_path, all_args.join(" "),);
        let output = Command::new(&cli_path)
            .args(&all_args)
            .env("HOME", &config_dir_path)
            .output()
            .expect("Failed to run anvil");

        println!("CLI command finished: {:?}", all_args);
        println!("  Status: {}", output.status);
        println!("  Stdout: {}", String::from_utf8_lossy(&output.stdout));
        println!("  Stderr: {}", String::from_utf8_lossy(&output.stderr));

        if !output.status.success() {
            eprintln!("CLI command failed: {:?}", all_args);
            eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
        }

        output
    })
    .await
    .unwrap()
}

async fn setup_test_profile(cluster: &DockerTestCluster, config_dir: &std::path::Path) {
    let actor = create_docker_storage_test_actor(cluster, "cli-test-app").await;

    let output = run_cli(
        &[
            "static-config",
            "--name",
            "default",
            "--host",
            &actor.grpc_addr,
            "--client-id",
            &actor.client_id,
            "--client-secret",
            &actor.client_secret,
            "--default",
        ],
        config_dir,
    )
    .await;
    assert!(output.status.success());
}

#[tokio::test]
async fn test_cli_configure_and_bucket_ls() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-cli-bucket-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let output = run_cli(&["bucket", "ls"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&bucket_name));
}

#[tokio::test]
async fn test_cli_bucket_create_and_rm() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-cli-bucket-{}", uuid::Uuid::new_v4());

    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
        config_dir.path(),
    )
    .await;
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
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-cli-object-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "my-cli-object";
    let content = "hello from cli object test";

    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, content).unwrap();

    let dest = format!("s3://{}/{}", bucket_name, object_key);
    let output = run_cli(
        &["object", "put", file_path.to_str().unwrap(), &dest],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let output = run_cli(&["object", "get", &dest], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert_eq!(stdout, content);
}

#[tokio::test]
async fn test_cli_hf_ingestion() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-cli-hf-bucket-{}", uuid::Uuid::new_v4());
    let repo = "openai/gpt-oss-20b";
    let file = "config.json";
    let object_key = format!("{}/{}", repo, file);

    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let hf_token = "test-token";
    let output = run_cli(
        &[
            "hf", "key", "add", "--name", "test-key", "--token", &hf_token,
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let output = run_cli(
        &[
            "hf",
            "ingest",
            "start",
            "--key",
            "test-key",
            "--repo",
            repo,
            "--bucket",
            &bucket_name,
            "--target-region",
            &cluster.region,
            "--include",
            file,
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let ingestion_id = stdout.split_whitespace().last().unwrap();

    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(120) {
            panic!("Timeout waiting for HF ingestion to complete");
        }
        let output = run_cli(
            &["hf", "ingest", "status", "--id", ingestion_id],
            config_dir.path(),
        )
        .await;
        let stdout = String::from_utf8(output.stdout).unwrap();
        println!("HF Ingestion Status: {}", stdout);
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

    // Verify anvil-index.json
    let index_key = format!("{}/anvil-index.json", repo);
    let index_dest = format!("s3://{}/{}", bucket_name, index_key);
    let output = run_cli(&["object", "get", &index_dest], config_dir.path()).await;
    assert!(output.status.success(), "Failed to get anvil-index.json");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let index_json: serde_json::Value =
        serde_json::from_str(&stdout).expect("Failed to parse anvil-index.json");

    assert_eq!(index_json["meta"]["source_repo"], repo);
    assert_eq!(index_json["meta"]["total_files"], 1);
    assert!(index_json["files"][file].is_object());
}
