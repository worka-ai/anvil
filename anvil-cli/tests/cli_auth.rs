#![recursion_limit = "512"]

use anvil_test_utils::{create_docker_storage_test_actor, shared_docker_test_cluster};
use tempfile::tempdir;
use tokio::process::Command as TokioCommand;

fn cli_path() -> &'static str {
    env!("CARGO_BIN_EXE_anvil")
}

// This verifies that anvil can obtain an access token using a configured
// profile and then use that token for an authenticated CLI operation. Subprocess
// calls use tokio::process so the in-process test server is not starved while
// the CLI waits for a gRPC response.
#[tokio::test]
async fn test_cli_auth_and_hf_key_add() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "cli-auth-hf").await;

    let grpc_addr = actor.grpc_addr.clone();
    let config_dir = tempdir().unwrap();
    let config_path = config_dir.path().join("config.toml");
    // 1. Use a Docker-backed tenant app created through the network admin API.
    let client_id = actor.client_id.clone();
    let client_secret = actor.client_secret.clone();

    // 2. Configure the CLI using the binary Cargo built for this test target.
    let mut cli_cmd = TokioCommand::new(cli_path());
    cli_cmd.args([
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
    let mut cli_cmd = TokioCommand::new(cli_path());
    cli_cmd.args([
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
    let mut cli_cmd = TokioCommand::new(cli_path());
    cli_cmd.args([
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
