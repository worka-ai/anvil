use anvil_test_utils::TestCluster;
use serde_json::Value;
use std::env;
use std::process::Command;
use std::sync::OnceLock;
use std::time::Duration;
use tempfile::tempdir;
use tokio::process::Command as TokioCommand;
use uuid::Uuid;

static ADMIN_PATH: OnceLock<String> = OnceLock::new();

fn cargo_path() -> String {
    if let Ok(p) = env::var("CARGO") {
        return p;
    }
    // Fallback to `which cargo`
    let output = Command::new("which")
        .arg("cargo")
        .output()
        .expect("Failed to locate cargo in PATH");
    assert!(output.status.success(), "cargo not found in PATH");
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn get_admin_path() -> &'static str {
    ADMIN_PATH.get_or_init(|| {
        let status = Command::new(cargo_path())
            .args(&["build", "--package", "anvil-storage", "--bin", "admin"])
            .status()
            .expect("Failed to build admin");
        assert!(status.success());

        let metadata_output = Command::new(cargo_path())
            .arg("metadata")
            .arg("--format-version=1")
            .output()
            .expect("Failed to get cargo metadata");
        let metadata: Value = serde_json::from_slice(&metadata_output.stdout).unwrap();
        let target_dir = metadata["target_directory"].as_str().unwrap();
        format!("{}/debug/admin", target_dir)
    })
}

// We will call cargo directly via absolute path

// This verifies that anvil-cli can obtain an access token using a configured
// profile and then use that token for an authenticated CLI operation. Subprocess
// calls use tokio::process so the in-process test server is not starved while
// the CLI waits for a gRPC response.
#[tokio::test]
async fn test_cli_auth_and_hf_key_add() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let config_dir = tempdir().unwrap();
    let config_path = config_dir.path().join("config.toml");
    let app_name = format!("test-app-{}", Uuid::new_v4());

    // 1. Create app
    let admin_bin = get_admin_path();
    let mut admin_cmd = TokioCommand::new(admin_bin);
    admin_cmd.args(&[
        "--anvil-secret-encryption-key",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--storage-path",
        &cluster.admin_state_path,
        "app",
        "create",
        "--tenant-name",
        "default",
        "--app-name",
        &app_name,
    ]);
    let admin_output = admin_cmd.output().await.unwrap();
    assert!(
        admin_output.status.success(),
        "admin apps create failed: {}",
        String::from_utf8_lossy(&admin_output.stderr)
    );
    let output_str = String::from_utf8(admin_output.stdout).unwrap();

    let client_id = output_str
        .lines()
        .find(|line| line.starts_with("Client ID:"))
        .map(|line| line.split_whitespace().last().unwrap())
        .unwrap();
    let client_secret = output_str
        .lines()
        .find(|line| line.starts_with("Client Secret:"))
        .map(|line| line.split_whitespace().last().unwrap())
        .unwrap();

    let grant_output = TokioCommand::new(admin_bin)
        .args([
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--storage-path",
            &cluster.admin_state_path,
            "policy",
            "grant",
            "--app-name",
            &app_name,
            "--action",
            "*",
            "--resource",
            "*",
        ])
        .output()
        .await
        .unwrap();
    assert!(
        grant_output.status.success(),
        "admin policy grant failed: {}",
        String::from_utf8_lossy(&grant_output.stderr)
    );

    // 2. Configure the CLI
    // 2. Configure the CLI using `cargo run` with absolute cargo path
    let mut cli_cmd = TokioCommand::new(cargo_path());
    cli_cmd.args(&[
        "run",
        "-p",
        "anvil-storage-cli",
        "--",
        "--config",
        config_path.to_str().unwrap(),
        "static-config",
        "--name",
        "test-profile",
        "--host",
        &grpc_addr,
        "--client-id",
        client_id,
        "--client-secret",
        client_secret,
        "--default",
    ]);
    let cli_output = cli_cmd.output().await.unwrap();
    if !cli_output.status.success() {
        eprintln!(
            "static-config failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&cli_output.stdout),
            String::from_utf8_lossy(&cli_output.stderr)
        );
    }
    assert!(cli_output.status.success());

    // 3. Get a token
    let mut cli_cmd = TokioCommand::new(cargo_path());
    cli_cmd.args(&[
        "run",
        "-p",
        "anvil-storage-cli",
        "--",
        "--config",
        config_path.to_str().unwrap(),
        "--profile",
        "test-profile",
        "auth",
        "get-token",
    ]);
    let cli_output = cli_cmd.output().await.unwrap();
    println!(
        "get-token stdout: {}",
        String::from_utf8_lossy(&cli_output.stdout)
    );
    println!(
        "get-token stderr: {}",
        String::from_utf8_lossy(&cli_output.stderr)
    );
    assert!(cli_output.status.success());
    let auth_token = String::from_utf8(cli_output.stdout)
        .unwrap()
        .trim()
        .to_string();

    // 4. Add an HF key
    let mut cli_cmd = TokioCommand::new(cargo_path());
    cli_cmd.args(&[
        "run",
        "-p",
        "anvil-storage-cli",
        "--",
        "--config",
        config_path.to_str().unwrap(),
        "--profile",
        "test-profile",
        "hf",
        "key",
        "add",
        "--name",
        "test-key",
        "--token",
        "dummy-hf-token",
    ]);
    cli_cmd.env("ANVIL_AUTH_TOKEN", auth_token);
    let cli_output = cli_cmd.output().await.unwrap();
    assert!(
        cli_output.status.success(),
        "anvil-cli hf key add failed: {}",
        String::from_utf8_lossy(&cli_output.stderr)
    );
}
