use std::process::Command;
use std::time::{Duration, Instant};

fn run(cmd: &str, args: &[&str]) {
    let status = Command::new(cmd)
        .args(args)
        .status()
        .expect("failed to run command");
    assert!(status.success(), "command failed: {} {:?}", cmd, args);
}

fn output(cmd: &str, args: &[&str]) -> String {
    let out = Command::new(cmd)
        .args(args)
        .output()
        .expect("failed to run command");
    assert!(out.status.success(), "command failed: {} {:?}", cmd, args);
    String::from_utf8(out.stdout).expect("utf8")
}

async fn wait_ready(url: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("timeout waiting for ready: {}", url);
        }
        match reqwest::get(url).await {
            Ok(resp) if resp.status().is_success() => return,
            _ => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

struct ComposeGuard;
impl Drop for ComposeGuard {
    fn drop(&mut self) {
        // best-effort teardown
        let _ = Command::new("docker")
            .args(["compose", "down", "-v"])
            .status();
    }
}

#[tokio::test]
async fn docker_cluster_end_to_end() {
    // Bring up the cluster
    run("docker", &["compose", "build"]);
    run("docker", &["compose", "up", "-d"]);
    let _guard = ComposeGuard;

    // Wait for nodes to be ready
    wait_ready("http://localhost:50051/ready", Duration::from_secs(60)).await;
    wait_ready("http://localhost:50052/ready", Duration::from_secs(60)).await;
    wait_ready("http://localhost:50053/ready", Duration::from_secs(60)).await;

    // Ensure region exists, then create tenant and app
    let region_args: Vec<&str> = vec![
        "run",
        "--bin",
        "admin",
        "--",
        "--global-database-url",
        "postgres://worka:worka@localhost:5433/anvil_global",
        "--anvil-secret-encryption-key",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "regions",
        "create",
        "DOCKER_TEST",
    ];
    run("cargo", &region_args);

    // Create tenant and app
    let tenant_args: Vec<&str> = vec![
        "run",
        "--bin",
        "admin",
        "--",
        "--global-database-url",
        "postgres://worka:worka@localhost:5433/anvil_global",
        "--anvil-secret-encryption-key",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "tenants",
        "create",
        "default",
    ];
    run("cargo", &tenant_args);

    let mut create_args: Vec<String> =
        vec!["run".into(), "--bin".into(), "admin".into(), "--".into()];
    create_args.extend(
        [
            "--global-database-url",
            "postgres://worka:worka@localhost:5433/anvil_global",
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "apps",
            "create",
            "--tenant-name",
            "default",
            "--app-name",
            "docker-e2e-app",
        ]
        .into_iter()
        .map(|s| s.to_string()),
    );
    let app_out = Command::new("cargo")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(&create_args)
        .output()
        .expect("failed to create app");
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
    let creds = String::from_utf8(app_out.stdout).unwrap();

    fn extract_credential(output: &str, label: &str) -> String {
        output
            .lines()
            .find_map(|line| {
                line.split_once(": ").and_then(|(k, v)| {
                    if k.trim() == label {
                        Some(v.trim().to_string())
                    } else {
                        None
                    }
                })
            })
            .expect("credential not found")
    }

    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");

    // Grant wildcard policy
    let grant_args: Vec<&str> = vec![
        "run",
        "--bin",
        "admin",
        "--",
        "--global-database-url",
        "postgres://worka:worka@localhost:5433/anvil_global",
        "--anvil-secret-encryption-key",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "policies",
        "grant",
        "--app-name",
        "docker-e2e-app",
        "--action",
        "*",
        "--resource",
        "*",
    ];
    run("cargo", &grant_args);

    // Allow small delay
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Get token via gRPC
    let mut auth_client = anvil::anvil_api::auth_service_client::AuthServiceClient::connect(
        "http://localhost:50051".to_string(),
    )
    .await
    .unwrap();
    let token = auth_client
        .get_access_token(anvil::anvil_api::GetAccessTokenRequest {
            client_id: client_id.clone(),
            client_secret: client_secret.clone(),
            scopes: vec!["read:*".into(), "write:*".into(), "grant:*".into()],
        })
        .await
        .unwrap()
        .into_inner()
        .access_token;

    // Create buckets via gRPC
    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        "http://localhost:50051".to_string(),
    )
    .await
    .unwrap();
    let suffix = uuid::Uuid::new_v4().to_string();
    let private_bucket = format!("e2e-private-{}", suffix);
    let public_bucket = format!("e2e-public-{}", suffix);

    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: private_bucket.clone(),
        region: "DOCKER_TEST".into(),
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
        region: "DOCKER_TEST".into(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    if let Err(e) = bucket_client.create_bucket(req).await {
        panic!("create public bucket failed: {:?}", e);
    }

    // Make public bucket public
    let mut auth_client = anvil::anvil_api::auth_service_client::AuthServiceClient::connect(
        "http://localhost:50051".to_string(),
    )
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
        .endpoint_url("http://localhost:50051")
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
    let public_url = format!("http://localhost:50051/{}/{}", public_bucket, public_key);
    let public_resp = reqwest::get(&public_url).await.unwrap();
    assert_eq!(public_resp.status(), 200);
    let public_data = public_resp.bytes().await.unwrap();
    assert_eq!(public_data.as_ref(), public_content);

    // Private anonymous should fail
    let private_url = format!("http://localhost:50051/{}/{}", private_bucket, private_key);
    let private_resp = reqwest::get(&private_url).await.unwrap();
    assert!(private_resp.status() == 403 || private_resp.status() == 404);

    // Anonymous List on public bucket should succeed (200) with XML
    let public_list_url = format!("http://localhost:50051/{}?list-type=2", public_bucket);
    let public_list_resp = reqwest::get(&public_list_url).await.unwrap();
    assert_eq!(public_list_resp.status(), 200);
    let public_list_body = public_list_resp.text().await.unwrap();
    assert!(public_list_body.contains("<ListBucketResult"));

    // Anonymous List on private bucket should be unauthorized (401/403)
    let private_list_url = format!("http://localhost:50051/{}?list-type=2", private_bucket);
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
