use std::process::Command;
use std::time::{Duration, Instant};

#[allow(unused)]
fn run(cmd: &str, args: &[&str]) {
    let status = Command::new(cmd).args(args).status().expect("run");
    assert!(status.success(), "command failed: {} {:?}", cmd, args);
}

fn docker_admin(compose_file: &std::path::Path, args: &[&str]) {
    let mut command_args = vec![
        "compose",
        "-f",
        compose_file.to_str().unwrap(),
        "exec",
        "-T",
        "anvil1",
        "admin",
    ];
    command_args.extend_from_slice(args);
    run("docker", &command_args);
}

fn docker_admin_output(compose_file: &std::path::Path, args: &[&str]) -> std::process::Output {
    let mut command_args = vec![
        "compose",
        "-f",
        compose_file.to_str().unwrap(),
        "exec",
        "-T",
        "anvil1",
        "admin",
    ];
    command_args.extend_from_slice(args);
    Command::new("docker")
        .args(command_args)
        .output()
        .expect("failed to run docker admin command")
}

#[allow(dead_code)]
#[allow(unused)]
async fn wait_ready(url: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("timeout waiting for ready: {}", url);
        }
        match reqwest::get(url).await {
            Ok(r) if r.status().is_success() => return,
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

fn host_api_port(node: u8) -> String {
    let (name, default) = match node {
        1 => ("ANVIL_TEST_API1_PORT", "55051"),
        2 => ("ANVIL_TEST_API2_PORT", "55052"),
        3 => ("ANVIL_TEST_API3_PORT", "55053"),
        _ => panic!("unsupported Docker test node"),
    };
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn host_api_url(node: u8) -> String {
    format!("http://localhost:{}", host_api_port(node))
}

async fn get_public_text_with_retry(url: &str, timeout: Duration) -> String {
    let start = Instant::now();
    let mut last_error = "no attempts completed".to_string();
    loop {
        if start.elapsed() > timeout {
            panic!("timed out fetching {url}: {last_error}");
        }

        match reqwest::get(url).await {
            Ok(response) if response.status().is_success() => match response.text().await {
                Ok(text) => return text,
                Err(error) => {
                    last_error = format!("failed to read response body: {error}");
                }
            },
            Ok(response) => {
                last_error = format!("unexpected HTTP status {}", response.status());
            }
            Err(error) => {
                last_error = format!("request failed: {error}");
            }
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[allow(dead_code)]
#[allow(unused)]
struct ComposeGuard {
    compose_file: std::path::PathBuf,
}

impl Drop for ComposeGuard {
    fn drop(&mut self) {
        let mut command = Command::new("docker");
        command.env(
            "ANVIL_IMAGE",
            std::env::var("ANVIL_IMAGE").unwrap_or_else(|_| "anvil:test".to_string()),
        );
        let _ = command
            .args([
                "compose",
                "-f",
                self.compose_file.to_str().unwrap_or_default(),
                "down",
                "-v",
            ])
            .status();
    }
}

#[tokio::test]
async fn hf_ingestion_config_json() {
    if std::env::var("ANVIL_RUN_HF_E2E").as_deref() != Ok("1") {
        eprintln!("skipping release-gated test; set ANVIL_RUN_HF_E2E=1 to run");
        return;
    }

    // Bring up cluster via compose (reuse existing compose file and image tag).
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let compose_file_path =
        std::path::Path::new(&manifest_dir).join("tests/docker-compose.test.yml");
    run(
        "docker",
        &[
            "compose",
            "-f",
            compose_file_path.to_str().unwrap(),
            "up",
            "-d",
        ],
    );
    let _guard = ComposeGuard {
        compose_file: compose_file_path.clone(),
    };

    wait_ready(
        &format!("{}/ready", host_api_url(1)),
        Duration::from_secs(60),
    )
    .await;

    // Prepare tenant/app through the network admin API exposed inside the container.
    docker_admin(
        &compose_file_path,
        &[
            "tenant",
            "create",
            "--name",
            "default",
            "--home-region",
            "docker-test",
            "--audit-reason",
            "docker hf e2e tenant",
        ],
    );

    let app_out = docker_admin_output(
        &compose_file_path,
        &[
            "app",
            "create",
            "--tenant-id",
            "default",
            "--app-name",
            "hf-e2e-app",
            "--audit-reason",
            "docker hf e2e app",
        ],
    );
    assert!(
        app_out.status.success(),
        "admin apps create failed: {}",
        String::from_utf8_lossy(&app_out.stderr)
    );
    let out: serde_json::Value = serde_json::from_slice(&app_out.stdout).unwrap();
    let client_id = out["resource"]["client_id"].as_str().unwrap().to_string();
    let client_secret = out["resource"]["client_secret"]
        .as_str()
        .unwrap()
        .to_string();

    // Wildcard policy for simplicity in e2e.
    docker_admin(
        &compose_file_path,
        &[
            "policy",
            "grant",
            "--tenant-id",
            "default",
            "--app-name",
            "hf-e2e-app",
            "--action",
            "*",
            "--resource",
            "*",
            "--audit-reason",
            "docker hf e2e wildcard policy",
        ],
    );

    // Get access token
    let mut auth_client =
        anvil::anvil_api::auth_service_client::AuthServiceClient::connect(host_api_url(1))
            .await
            .unwrap();
    let token = auth_client
        .get_access_token(anvil::anvil_api::GetAccessTokenRequest {
            client_id: client_id.clone(),
            client_secret: client_secret.clone(),
            scopes: vec!["*".into()],
        })
        .await
        .unwrap()
        .into_inner()
        .access_token;

    // Create bucket
    let mut bucket_client =
        anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(host_api_url(1))
            .await
            .unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: "models".into(),
        region: "docker-test".into(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let _ = bucket_client.create_bucket(req).await;

    // The assertions below intentionally verify unauthenticated S3-compatible
    // HTTP reads, so the test bucket must opt into public read access.
    let mut public_req = tonic::Request::new(anvil::anvil_api::SetPublicAccessRequest {
        bucket: "models".into(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client
        .set_public_access(public_req)
        .await
        .expect("make HF test bucket public");

    // Create HF key via public API (empty token for public repo)
    let mut key_client =
        anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient::connect(
            host_api_url(1),
        )
        .await
        .unwrap();
    let mut kreq = tonic::Request::new(anvil::anvil_api::CreateHfKeyRequest {
        name: "test".into(),
        token: "".into(),
        note: "".into(),
    });
    kreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    key_client.create_key(kreq).await.expect("create hf key");

    // Start ingestion for config.json only
    let mut ing_client =
        anvil::anvil_api::hf_ingestion_service_client::HfIngestionServiceClient::connect(
            host_api_url(1),
        )
        .await
        .unwrap();
    let mut sreq = tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
        key_name: "test".into(),
        repo: "openai/gpt-oss-20b".into(),
        revision: "main".into(),
        target_bucket: "models".into(),
        target_prefix: "gpt-oss-20b".into(),
        include_globs: vec!["config.json".into()],
        exclude_globs: vec![],
        target_region: "docker-test".into(),
    });
    sreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let ing_id = ing_client
        .start_ingestion(sreq)
        .await
        .unwrap()
        .into_inner()
        .ingestion_id;

    // Poll status
    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(90) {
            panic!("timeout waiting for ingestion");
        }
        let mut streq = tonic::Request::new(anvil::anvil_api::GetHfIngestionStatusRequest {
            ingestion_id: ing_id.clone(),
        });
        streq.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let status = ing_client
            .get_ingestion_status(streq)
            .await
            .unwrap()
            .into_inner();
        if status.state == "completed" {
            break;
        }
        if status.state == "failed" {
            panic!("ingestion failed: {}", status.error);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify GET on the object returns 200 and valid JSON
    let url = format!("{}/models/gpt-oss-20b/config.json", host_api_url(1));
    let txt = get_public_text_with_retry(&url, Duration::from_secs(60)).await;
    let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
    assert!(v.is_object());

    // Verify anvil-index.json
    let index_url = format!("{}/models/gpt-oss-20b/anvil-index.json", host_api_url(1));
    let index_txt = get_public_text_with_retry(&index_url, Duration::from_secs(60)).await;
    let index_v: serde_json::Value = serde_json::from_str(&index_txt).unwrap();

    // Assert meta fields
    assert_eq!(index_v["meta"]["source_repo"], "openai/gpt-oss-20b");
    assert_eq!(index_v["meta"]["revision"], "main");
    assert_eq!(index_v["meta"]["total_files"], 1);
    assert!(index_v["meta"]["total_bytes"].is_number());
    assert!(index_v["meta"]["generated_at"].is_string());

    // Assert files entry
    let files_map = index_v["files"].as_object().unwrap();
    assert_eq!(files_map.len(), 1);
    assert!(files_map.contains_key("config.json"));

    let config_json_entry = files_map["config.json"].as_object().unwrap();
    assert!(config_json_entry.contains_key("size"));
    assert!(config_json_entry["size"].is_number());
    assert!(config_json_entry.contains_key("etag"));
    assert!(config_json_entry["etag"].is_string());
    assert!(config_json_entry.contains_key("last_modified"));
    assert!(config_json_entry["last_modified"].is_string());

    // The size in anvil-index.json should match the actual file size.
    let expected_config_size = txt.len() as i64;
    assert_eq!(
        config_json_entry["size"].as_i64().unwrap(),
        expected_config_size
    );

    // --- Second Ingestion (Merge Test) ---
    // Ingest README.md to the same location to verify index merging
    let mut sreq2 = tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
        key_name: "test".into(),
        repo: "openai/gpt-oss-20b".into(),
        revision: "main".into(),
        target_bucket: "models".into(),
        target_prefix: "gpt-oss-20b".into(),
        include_globs: vec!["README.md".into()],
        exclude_globs: vec![],
        target_region: "docker-test".into(),
    });
    sreq2.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let ing_id_2 = ing_client
        .start_ingestion(sreq2)
        .await
        .unwrap()
        .into_inner()
        .ingestion_id;

    // Poll status for job 2
    let start2 = Instant::now();
    loop {
        if start2.elapsed() > Duration::from_secs(90) {
            panic!("timeout waiting for second ingestion");
        }
        let mut streq = tonic::Request::new(anvil::anvil_api::GetHfIngestionStatusRequest {
            ingestion_id: ing_id_2.clone(),
        });
        streq.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let status = ing_client
            .get_ingestion_status(streq)
            .await
            .unwrap()
            .into_inner();
        if status.state == "completed" {
            break;
        }
        if status.state == "failed" {
            panic!("second ingestion failed: {}", status.error);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify merged anvil-index.json
    let index_txt_2 = get_public_text_with_retry(&index_url, Duration::from_secs(60)).await;
    let index_v_2: serde_json::Value = serde_json::from_str(&index_txt_2).unwrap();

    // Assert meta fields
    assert_eq!(
        index_v_2["meta"]["total_files"], 2,
        "Index should contain 2 files after merge (found: {:?})",
        index_v_2["files"]
    );

    let files_map_2 = index_v_2["files"].as_object().unwrap();
    assert!(
        files_map_2.contains_key("config.json"),
        "Index should still contain config.json"
    );
    assert!(
        files_map_2.contains_key("README.md"),
        "Index should now contain README.md"
    );
}
