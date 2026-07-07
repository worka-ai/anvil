use std::process::Command;

fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil package has workspace parent")
        .to_path_buf()
}

fn run_script(script: &str) {
    let output = Command::new(script)
        .current_dir(repo_root())
        .output()
        .unwrap_or_else(|err| panic!("failed to run {script}: {err}"));
    assert!(
        output.status.success(),
        "{script} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn docs_tenant_tutorials_do_not_use_admin_cli() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn docs_cli_commands_exist() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn docs_proto_fields_exist_for_snippets() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn docs_no_known_pseudo_json_fields() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn docs_index_examples_use_real_create_index_request_shape() {
    run_script("./scripts/check-docs-hardening.sh");
}

#[test]
fn release_blog_post_required_for_tag() {
    run_script("./scripts/test-release-notes.sh");
}

#[test]
fn release_blog_front_matter_version_matches_tag() {
    run_script("./scripts/test-release-notes.sh");
}

#[test]
fn release_notes_render_from_blog_post() {
    run_script("./scripts/test-release-notes.sh");
}

#[test]
fn release_notes_include_artifact_metadata() {
    run_script("./scripts/test-release-notes.sh");
}

#[test]
fn release_workflow_uses_shared_release_gates() {
    let root = repo_root();
    let release = std::fs::read_to_string(root.join(".github/workflows/release.yml")).unwrap();
    let ci = std::fs::read_to_string(root.join(".github/workflows/ci.yml")).unwrap();
    assert!(
        root.join("scripts/release-gates.sh").exists(),
        "shared release gate script must exist"
    );
    assert!(
        release.contains("./scripts/release-gates.sh"),
        "release workflow must call the shared release gate script"
    );
    assert!(
        release.contains("cargo publish -p anvil-storage"),
        "release workflow must publish the anvil-storage Rust client crate"
    );
    assert!(
        release.contains("scripts/crate-version-exists.py"),
        "release workflow must skip cargo publish only when the exact crate version already exists"
    );
    assert!(
        release.contains("docker buildx imagetools inspect"),
        "release workflow must resolve the actual pushed Docker image digest"
    );
    assert!(
        !release.contains("sha256:<published-by-ghcr>"),
        "release workflow must not use placeholder Docker digests"
    );
    assert!(
        ci.contains("./scripts/release-gates.sh"),
        "PR CI workflow must call the shared release gate script"
    );
}

#[test]
fn public_proto_messages_do_not_reuse_admin_context() {
    let proto = std::fs::read_to_string(repo_root().join("anvil-core/proto/anvil.proto")).unwrap();
    let object_service = proto
        .split("service ObjectService {")
        .nth(1)
        .and_then(|tail| tail.split("\n}").next())
        .expect("ObjectService block");
    assert!(
        !object_service.contains("AdminMutationResponse"),
        "public ObjectService must not return AdminMutationResponse"
    );
    for message in [
        "CreateObjectLinkRequest",
        "UpdateObjectLinkRequest",
        "DeleteObjectLinkRequest",
        "CreateHostAliasRequest",
        "VerifyHostAliasRequest",
        "DeleteHostAliasRequest",
    ] {
        let start = proto
            .find(&format!("message {message} "))
            .unwrap_or_else(|| panic!("missing proto message {message}"));
        let body = &proto[start..];
        let end = body
            .find("\n}")
            .unwrap_or_else(|| panic!("unterminated proto message {message}"));
        let body = &body[..end];
        assert!(
            !body.contains("AdminRequestContext"),
            "{message} is public-facing and must not carry AdminRequestContext"
        );
        assert!(
            body.contains("PublicMutationContext")
                || message.starts_with("Read")
                || message.starts_with("List"),
            "{message} must use PublicMutationContext for public mutations"
        );
    }
}
