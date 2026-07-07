use std::process::Command;

#[test]
fn public_and_admin_cli_binary_names_are_stable() {
    let public_cli = env!("CARGO_BIN_EXE_anvil");
    let admin_cli = env!("CARGO_BIN_EXE_anvil-admin");

    assert!(
        public_cli.ends_with("/anvil") || public_cli.ends_with("\\anvil.exe"),
        "public CLI must be produced as `anvil`, got {public_cli}"
    );
    assert!(
        admin_cli.ends_with("/anvil-admin") || admin_cli.ends_with("\\anvil-admin.exe"),
        "admin CLI must be produced as `anvil-admin`, got {admin_cli}"
    );
    assert!(
        option_env!("CARGO_BIN_EXE_admin").is_none(),
        "the legacy `admin` binary must not be produced"
    );
}

#[test]
fn produced_cli_binaries_start_and_show_help() {
    for binary in [
        env!("CARGO_BIN_EXE_anvil"),
        env!("CARGO_BIN_EXE_anvil-admin"),
    ] {
        let output = Command::new(binary)
            .arg("--help")
            .output()
            .unwrap_or_else(|err| panic!("failed to execute {binary}: {err}"));

        assert!(
            output.status.success(),
            "{binary} --help failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn admin_cli_requires_explicit_private_admin_endpoint() {
    let output = Command::new(env!("CARGO_BIN_EXE_anvil-admin"))
        .args(["node", "list"])
        .env_remove("ANVIL_ADMIN_ENDPOINT")
        .output()
        .expect("run anvil-admin without admin endpoint");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("anvil-admin requires --host or ANVIL_ADMIN_ENDPOINT"),
        "unexpected stderr: {stderr}"
    );
}
