use std::process::Command;
use std::time::{Duration, Instant};

use anvil_test_utils::emit_test_timing;

#[allow(dead_code)]
#[allow(unused)]
fn run(cmd: &str, args: &[&str]) {
    let status = Command::new(cmd)
        .args(args)
        .status()
        .expect("failed to run command");
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
        "anvil-admin",
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
        "anvil-admin",
    ];
    command_args.extend_from_slice(args);
    Command::new("docker")
        .args(command_args)
        .output()
        .expect("failed to run docker admin command")
}

#[allow(unused)]
async fn wait_ready(url: &str, timeout: Duration) {
    let start = Instant::now();
    let mut attempts = 0_u64;
    loop {
        attempts += 1;
        if start.elapsed() > timeout {
            emit_test_timing(
                format!("docker_wait_ready timeout url={url} attempts={attempts}"),
                start.elapsed(),
            );
            panic!("timeout waiting for ready: {}", url);
        }
        match reqwest::get(url).await {
            Ok(resp) if resp.status().is_success() => {
                emit_test_timing(
                    format!("docker_wait_ready url={url} attempts={attempts}"),
                    start.elapsed(),
                );
                return;
            }
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
async fn docker_cluster_end_to_end() {
    if std::env::var("ANVIL_RUN_DOCKER_E2E").as_deref() != Ok("1") {
        eprintln!("skipping release-gated test; set ANVIL_RUN_DOCKER_E2E=1 to run");
        return;
    }

    // This test now assumes that the Docker image has been pre-built by a previous CI step
    // and that the ANVIL_IMAGE environment variable is set to the correct image tag.
    // The CI workflow is responsible for this setup.

    // Construct an absolute path to the test compose file to avoid CWD issues.
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

    // Wait for nodes to be ready
    wait_ready(
        &format!("{}/ready", host_api_url(1)),
        Duration::from_secs(60),
    )
    .await;
    wait_ready(
        &format!("{}/ready", host_api_url(2)),
        Duration::from_secs(60),
    )
    .await;
    wait_ready(
        &format!("{}/ready", host_api_url(3)),
        Duration::from_secs(60),
    )
    .await;

    // Initialise the Docker node through its internal admin API.
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
            "docker e2e tenant",
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
            "docker-e2e-app",
            "--audit-reason",
            "docker e2e app",
        ],
    );
    if !app_out.status.success() {
        eprintln!(
            "admin create stdout: {}",
            String::from_utf8_lossy(&app_out.stdout)
        );
        eprintln!(
            "admin create stderr: {}",
            String::from_utf8_lossy(&app_out.stderr)
        );
        panic!("admin create failed");
    }
    let creds: serde_json::Value = serde_json::from_slice(&app_out.stdout).unwrap();
    let client_id = creds["resource"]["client_id"].as_str().unwrap().to_string();
    let client_secret = creds["resource"]["client_secret"]
        .as_str()
        .unwrap()
        .to_string();

    docker_admin(
        &compose_file_path,
        &[
            "policy",
            "grant",
            "--tenant-id",
            "default",
            "--app-name",
            "docker-e2e-app",
            "--action",
            "*",
            "--resource",
            "*",
            "--audit-reason",
            "docker e2e wildcard policy",
        ],
    );

    // Allow small delay
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get token via gRPC
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

    // Create buckets via gRPC
    let mut bucket_client =
        anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(host_api_url(1))
            .await
            .unwrap();
    let suffix = uuid::Uuid::new_v4().to_string();
    let private_bucket = format!("e2e-private-{}", suffix);
    let public_bucket = format!("e2e-public-{}", suffix);

    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: private_bucket.clone(),
        region: "docker-test".into(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    if let Err(e) = bucket_client.create_bucket(req).await {
        panic!("create private bucket failed: {:?}", e);
    }

    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: public_bucket.clone(),
        region: "docker-test".into(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    if let Err(e) = bucket_client.create_bucket(req).await {
        panic!("create public bucket failed: {:?}", e);
    }

    // Make public bucket public
    let mut auth_client =
        anvil::anvil_api::auth_service_client::AuthServiceClient::connect(host_api_url(1))
            .await
            .unwrap();
    let mut public_req = tonic::Request::new(anvil::anvil_api::SetPublicAccessRequest {
        bucket: public_bucket.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    if let Err(e) = auth_client.set_public_access(public_req).await {
        panic!("set public access failed: {:?}", e);
    }

    // Use S3 client against node1
    let credentials =
        aws_sdk_s3::config::Credentials::new(&client_id, &client_secret, None, None, "static");
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("test-region"))
        .endpoint_url(host_api_url(1))
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    let s3 = aws_sdk_s3::Client::from_conf(config);

    let private_key = "private.txt";
    let public_key = "public.txt";
    let private_content = b"docker private";
    let public_content = b"docker public";

    s3.put_object()
        .bucket(&private_bucket)
        .key(private_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            private_content.to_vec(),
        ))
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket(&public_bucket)
        .key(public_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(
            public_content.to_vec(),
        ))
        .send()
        .await
        .unwrap();

    // Read private via S3
    let resp = s3
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .unwrap();
    let data = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), private_content);

    // Read public via HTTP
    let public_url = format!("{}/{}/{}", host_api_url(1), public_bucket, public_key);
    let public_resp = reqwest::get(&public_url).await.unwrap();
    assert_eq!(public_resp.status(), 200);
    let public_data = public_resp.bytes().await.unwrap();
    assert_eq!(public_data.as_ref(), public_content);

    // Private anonymous should fail
    let private_url = format!("{}/{}/{}", host_api_url(1), private_bucket, private_key);
    let private_resp = reqwest::get(&private_url).await.unwrap();
    assert!(private_resp.status() == 403 || private_resp.status() == 404);

    // Anonymous List on public bucket should succeed (200) with XML
    let public_list_url = format!("{}/{}?list-type=2", host_api_url(1), public_bucket);
    let public_list_resp = reqwest::get(&public_list_url).await.unwrap();
    assert_eq!(public_list_resp.status(), 200);
    let public_list_body = public_list_resp.text().await.unwrap();
    assert!(public_list_body.contains("<ListBucketResult"));

    // Anonymous List on private bucket should be unauthorized (401/403)
    let private_list_url = format!("{}/{}?list-type=2", host_api_url(1), private_bucket);
    let private_list_resp = reqwest::get(&private_list_url).await.unwrap();
    assert!(private_list_resp.status() == 401 || private_list_resp.status() == 403);

    // Authenticated List on private bucket should succeed
    let list = s3
        .list_objects_v2()
        .bucket(&private_bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(list.key_count(), Some(1));
}
