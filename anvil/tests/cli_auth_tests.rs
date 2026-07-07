use anvil_test_utils::TestCluster;
use std::env;
use std::process::Command;
use std::time::Duration;
use tempfile::tempdir;
use tokio::process::Command as TokioCommand;
use uuid::Uuid;

fn cargo_path() -> String {
    if let Ok(path) = env::var("CARGO") {
        return path;
    }
    let output = Command::new("which")
        .arg("cargo")
        .output()
        .expect("locate cargo in PATH");
    assert!(output.status.success(), "cargo not found in PATH");
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

// This verifies that anvil can obtain an access token using a configured
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

    // 1. Create an application through the network admin API.
    let (client_id, client_secret) = cluster
        .create_application_with_policy("default", &app_name, "*", "*")
        .await;

    // 2. Configure the CLI
    // 2. Configure the CLI using `cargo run` with absolute cargo path
    let mut cli_cmd = TokioCommand::new(cargo_path());
    cli_cmd.args(&[
        "run",
        "-p",
        "anvil-storage-cli",
        "--bin",
        "anvil",
        "--",
        "--config",
        config_path.to_str().unwrap(),
        "static-config",
        "--name",
        "test-profile",
        "--host",
        &grpc_addr,
        "--client-id",
        &client_id,
        "--client-secret",
        &client_secret,
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
        "--bin",
        "anvil",
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
        "--bin",
        "anvil",
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
        "anvil hf key add failed: {}",
        String::from_utf8_lossy(&cli_output.stderr)
    );
}
