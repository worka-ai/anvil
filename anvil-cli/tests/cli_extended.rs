#![recursion_limit = "512"]

use std::process::Command;
use tempfile::tempdir;

use anvil_test_utils::*;

fn get_cli_path() -> &'static str {
    env!("CARGO_BIN_EXE_anvil")
}

async fn run_cli(args: &[&str], config_dir: &std::path::Path) -> std::process::Output {
    let cli_path = get_cli_path().to_string();
    let config_path = config_dir.join(".anvil").join("config.toml");
    let mut all_args = vec![
        "--config".to_string(),
        config_path.to_str().unwrap().to_string(),
    ];
    all_args.extend(args.iter().map(|s| s.to_string()));

    //let config_dir_path = config_dir.to_path_buf();

    tokio::task::spawn_blocking(move || {
        println!("Running CLI command: {} {}", cli_path, all_args.join(" "),);
        let output = Command::new(&cli_path)
            .args(&all_args)
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

async fn setup_test_profile(
    cluster: &DockerTestCluster,
    config_dir: &std::path::Path,
) -> DockerTestStorageActor {
    let actor = create_docker_storage_test_actor(cluster, "cli-test-app").await;
    let client_id = actor.client_id.clone();
    let client_secret = actor.client_secret.clone();

    let output = run_cli(
        &[
            "static-config",
            "--name",
            "default",
            "--host",
            &actor.grpc_addr,
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
    actor
}

#[tokio::test]
async fn test_cli_auth_get_token() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let actor = setup_test_profile(&cluster, config_dir.path()).await;

    let output = run_cli(
        &[
            "auth",
            "get-token",
            "--client-id",
            &actor.client_id,
            "--client-secret",
            &actor.client_secret,
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.is_empty());
}

async fn create_app(
    cluster: &DockerTestCluster,
    tenant_id: i64,
    app_name: &str,
) -> (String, String) {
    let (_app_id, client_id, client_secret) = cluster
        .create_application_with_id(tenant_id, app_name)
        .await;
    (client_id, client_secret)
}

#[tokio::test]
async fn test_cli_auth_grant() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let actor = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("grant-bucket-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let grantee_app_name = format!("grantee-app-{}", uuid::Uuid::new_v4());
    let (_grantee_client_id, _) = create_app(&cluster, actor.tenant_id, &grantee_app_name).await;

    let output = run_cli(
        &[
            "auth",
            "grant",
            &grantee_app_name,
            "bucket:read",
            &bucket_name,
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Permission granted."));
}

#[tokio::test]
async fn test_cli_auth_revoke() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let actor = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("revoke-bucket-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let grantee_app_name = format!("grantee-app-{}", uuid::Uuid::new_v4());
    let (_grantee_client_id, _) = create_app(&cluster, actor.tenant_id, &grantee_app_name).await;

    let output = run_cli(
        &[
            "auth",
            "grant",
            &grantee_app_name,
            "bucket:read",
            &bucket_name,
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let output = run_cli(
        &[
            "auth",
            "revoke",
            &grantee_app_name,
            "bucket:read",
            &bucket_name,
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Permission revoked."));
}

#[tokio::test]
async fn test_cli_bucket_set_public() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-public-bucket-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let output = run_cli(
        &["bucket", "set-public", &bucket_name, "--allow", "true"],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!(
        "Public access for bucket {} set to true",
        bucket_name
    )));

    let output = run_cli(
        &["bucket", "set-public", &bucket_name, "--allow", "false"],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!(
        "Public access for bucket {} set to false",
        bucket_name
    )));
}

#[tokio::test]
async fn test_cli_object_rm() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-object-rm-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "my-object-to-rm";
    let content = "hello from object rm test";

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

    let output = run_cli(&["object", "rm", &dest], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Removed"));
}

#[tokio::test]
async fn test_cli_object_ls() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-object-ls-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "my-object-to-ls";
    let content = "hello from object ls test";

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

    let output = run_cli(
        &["object", "ls", &format!("s3://{}/", bucket_name)],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(object_key));
}

#[tokio::test]
async fn test_cli_object_get_to_file() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let bucket_name = format!("my-object-get-bucket-{}", uuid::Uuid::new_v4());
    let object_key = "my-object-to-get";
    let content = "hello from object get to file test";

    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.txt");
    std::fs::write(&file_path, content).unwrap();

    let dest_s3 = format!("s3://{}/{}", bucket_name, object_key);
    let output = run_cli(
        &["object", "put", file_path.to_str().unwrap(), &dest_s3],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let download_path = temp_dir.path().join("downloaded.txt");
    let output = run_cli(
        &["object", "get", &dest_s3, download_path.to_str().unwrap()],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let downloaded_content = std::fs::read_to_string(download_path).unwrap();
    assert_eq!(content, downloaded_content);
}

#[tokio::test]
async fn test_cli_hf_key_ls() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let key_name = format!("test-key-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &[
            "hf",
            "key",
            "add",
            "--name",
            &key_name,
            "--token",
            "test-token",
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let output = run_cli(&["hf", "key", "ls"], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&key_name));
}

#[tokio::test]
async fn test_cli_hf_key_rm() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let key_name = format!("test-key-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &[
            "hf",
            "key",
            "add",
            "--name",
            &key_name,
            "--token",
            "test-token",
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let output = run_cli(&["hf", "key", "rm", "--name", &key_name], config_dir.path()).await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(&format!("deleted key: {}", key_name)));
}

#[tokio::test]
async fn test_cli_hf_ingest_cancel() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let key_name = format!("test-key-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &[
            "hf",
            "key",
            "add",
            "--name",
            &key_name,
            "--token",
            "test-token",
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let bucket_name = format!("my-hf-ingest-cancel-bucket-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
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
            &key_name,
            "--repo",
            "openai/gpt-oss-20b",
            "--bucket",
            &bucket_name,
            "--target-region",
            &cluster.region,
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    let ingestion_id = stdout.split_whitespace().last().unwrap();

    let output = run_cli(
        &["hf", "ingest", "cancel", "--id", ingestion_id],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("canceled"));
}

#[tokio::test]
async fn test_cli_hf_ingest_start_with_options() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();
    let _ = setup_test_profile(&cluster, config_dir.path()).await;

    let key_name = format!("test-key-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &[
            "hf",
            "key",
            "add",
            "--name",
            &key_name,
            "--token",
            "test-token",
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());

    let bucket_name = format!("hf-ingest-opts-{}", uuid::Uuid::new_v4());
    let output = run_cli(
        &["bucket", "create", &bucket_name, &cluster.region],
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
            &key_name,
            "--repo",
            "openai/gpt-oss-20b",
            "--bucket",
            &bucket_name,
            "--target-region",
            &cluster.region,
            "--revision",
            "main",
            "--prefix",
            "my-prefix",
            "--exclude",
            "*.txt",
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("ingestion id:"));
}

#[tokio::test]
async fn test_cli_configure_interactive() {
    let cluster = shared_docker_test_cluster().await;
    let config_dir = tempdir().unwrap();

    let actor = create_docker_storage_test_actor(&cluster, "cli-configure").await;

    let output = run_cli(
        &[
            "configure",
            "--name",
            "configured",
            "--host",
            &actor.grpc_addr,
            "--client-id",
            &actor.client_id,
            "--client-secret",
            &actor.client_secret,
            "--default",
        ],
        config_dir.path(),
    )
    .await;
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Profile 'configured' saved."));

    let output = run_cli(&["bucket", "ls"], config_dir.path()).await;
    assert!(output.status.success());
}
