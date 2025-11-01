use std::process::Command;
use std::time::{Duration, Instant};

#[allow(unused)]
fn run(cmd: &str, args: &[&str]) {
    let status = Command::new(cmd).args(args).status().expect("run");
    assert!(status.success(), "command failed: {} {:?}", cmd, args);
}

#[allow(dead_code)]
#[allow(unused)]
async fn wait_ready(url: &str, timeout: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout { panic!("timeout waiting for ready: {}", url); }
        match reqwest::get(url).await { Ok(r) if r.status().is_success() => return, _ => tokio::time::sleep(Duration::from_millis(500)).await }
    }
}

#[allow(dead_code)]
#[allow(unused)]
struct ComposeGuard;

impl Drop for ComposeGuard { fn drop(&mut self) { let _ = Command::new("docker").args(["compose","down","-v"]).status(); } }

#[tokio::test]
#[cfg(target_os = "linux")]
async fn hf_ingestion_config_json() {
    // Bring up cluster via compose (reuse existing compose file and image tag).
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let compose_file_path = std::path::Path::new(&manifest_dir).join("tests/docker-compose.test.yml");
    run("docker", &["compose","-f", compose_file_path.to_str().unwrap(), "up","-d"]);
    let _guard = ComposeGuard;

    wait_ready("http://localhost:50051/ready", Duration::from_secs(60)).await;

    // Prepare region/tenant/app via admin
    run("cargo", &["run","--bin","admin","--","--global-database-url","postgres://worka:worka@localhost:5433/anvil_global","--anvil-secret-encryption-key","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","regions","create","DOCKER_TEST"]);
    run("cargo", &["run","--bin","admin","--","--global-database-url","postgres://worka:worka@localhost:5433/anvil_global","--anvil-secret-encryption-key","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tenants","create","default"]);

    let app_out = Command::new("cargo")
        .args(["run","--bin","admin","--","--global-database-url","postgres://worka:worka@localhost:5433/anvil_global","--anvil-secret-encryption-key","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","apps","create","--tenant-name","default","--app-name","hf-e2e-app"]).output().expect("admin apps create");
    assert!(app_out.status.success(), "admin apps create failed: {}", String::from_utf8_lossy(&app_out.stderr));
    let out = String::from_utf8(app_out.stdout).unwrap();
    fn extract(s: &str, label: &str) -> String { s.lines().find_map(|l| l.split_once(": ").and_then(|(k,v)| if k.trim()==label { Some(v.trim().to_string()) } else { None })).unwrap() }
    let client_id = extract(&out, "Client ID");
    let client_secret = extract(&out, "Client Secret");

    // Wildcard policy for simplicity in e2e
    run("cargo", &["run","--bin","admin","--","--global-database-url","postgres://worka:worka@localhost:5433/anvil_global","--anvil-secret-encryption-key","aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","policies","grant","--app-name","hf-e2e-app","--action","*","--resource","*"]);

    // Get access token
    let mut auth_client = anvil::anvil_api::auth_service_client::AuthServiceClient::connect("http://localhost:50051".to_string()).await.unwrap();
    let token = auth_client.get_access_token(anvil::anvil_api::GetAccessTokenRequest{
        client_id: client_id.clone(), client_secret: client_secret.clone(), scopes: vec!["read:*".into(),"write:*".into(),"grant:*".into()] }).await.unwrap().into_inner().access_token;

    // Create bucket
    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect("http://localhost:50051".to_string()).await.unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest{ bucket_name: "models".into(), region: "DOCKER_TEST".into()});
    req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
    let _ = bucket_client.create_bucket(req).await;

    // Create HF key via public API (empty token for public repo)
    let mut key_client = anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient::connect("http://localhost:50051".to_string()).await.unwrap();
    let mut kreq = tonic::Request::new(anvil::anvil_api::CreateHfKeyRequest{ name: "test".into(), token: "".into(), note: "".into() });
    kreq.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
    key_client.create_key(kreq).await.expect("create hf key");

    // Start ingestion for config.json only
    let mut ing_client = anvil::anvil_api::hf_ingestion_service_client::HfIngestionServiceClient::connect("http://localhost:50051".to_string()).await.unwrap();
    let mut sreq = tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
        key_name: "test".into(),
        repo: "openai/gpt-oss-20b".into(),
        revision: "main".into(),
        target_bucket: "models".into(),
        target_prefix: "gpt-oss-20b".into(),
        include_globs: vec!["config.json".into()],
        exclude_globs: vec![],
        target_region: "DOCKER_TEST".into(),
    });
    sreq.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
    let ing_id = ing_client.start_ingestion(sreq).await.unwrap().into_inner().ingestion_id;

    // Poll status
    let start = Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(90) { panic!("timeout waiting for ingestion"); }
        let mut streq = tonic::Request::new(anvil::anvil_api::GetHfIngestionStatusRequest{ ingestion_id: ing_id.clone() });
        streq.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
        let status = ing_client.get_ingestion_status(streq).await.unwrap().into_inner();
        if status.state == "completed" { break; }
        if status.state == "failed" { panic!("ingestion failed: {}", status.error); }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Verify GET on the object returns 200 and valid JSON
    let url = "http://localhost:50051/models/gpt-oss-20b/config.json";
    let resp = reqwest::get(url).await.unwrap();
    assert_eq!(resp.status(), 200);
    let txt = resp.text().await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
    assert!(v.is_object());
}

