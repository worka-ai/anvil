use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::{GetAccessTokenRequest, SetPublicAccessRequest};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::time::Duration;

mod common;

// Helper function to create an app, since it's used in auth tests.
fn create_app(global_db_url: &str, app_name: &str) -> (String, String) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let app_output = std::process::Command::new("cargo")
        .args(
            admin_args
                .iter()
                .chain(&["--global-database-url", global_db_url, "--worka-secret-encryption-key", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "apps", "create", "--tenant-name", "default", "--app-name",app_name]),
        )
        .output()
        .unwrap();
    assert!(app_output.status.success());
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let client_id = common::extract_credential(&creds, "Client ID");
    let client_secret = common::extract_credential(&creds, "Client Secret");
    (client_id, client_secret)
}

// Helper to get a token for specific scopes.
async fn get_token_for_scopes(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
    scopes: Vec<String>,
) -> String {
    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            scopes,
        })
        .await
        .unwrap()
        .into_inner()
        .access_token
}


#[tokio::test]
async fn test_s3_public_and_private_access() {
    let mut cluster = common::TestCluster::new(&["TEST_REGION"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let (client_id, client_secret) = create_app(&cluster.global_db_url, "s3-test-app");

    // Grant wildcard policy to the app before getting a token
    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policies",
        "grant",
        "--app-name",
        "s3-test-app",
        "--action",
        "*",
        "--resource",
        "*",
    ];
    let status = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(&["--global-database-url", &cluster.global_db_url, "--worka-secret-encryption-key", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]).chain(policy_args.iter()))
        .status()
        .unwrap();
    assert!(status.success());

    // Allow a moment for the policy change to propagate or be read by the server.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &client_id,
        &client_secret,
        vec!["read:*".to_string(), "write:*".to_string(), "grant:*".to_string()],
    )
    .await;

    // 1. Create a private and a public bucket
    let private_bucket = "private-s3-bucket".to_string();
    let public_bucket = "public-s3-bucket".to_string();

    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(cluster.grpc_addrs[0].clone()).await.unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: private_bucket.clone(),
        region: "TEST_REGION".to_string(),
    });
    req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
    bucket_client.create_bucket(req).await.unwrap();

    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: public_bucket.clone(),
        region: "TEST_REGION".to_string(),
    });
    req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
    bucket_client.create_bucket(req).await.unwrap();

    // 2. Set the public bucket to be public
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone()).await.unwrap();
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: public_bucket.clone(),
        allow_public_read: true,
    });
    public_req
        .metadata_mut()
        .insert("authorization", format!("Bearer {}", token).parse().unwrap());
    auth_client.set_public_access(public_req).await.unwrap();

    // 3. Configure AWS S3 client to talk to our local server
    let credentials = aws_sdk_s3::config::Credentials::new(
        &client_id,
        &client_secret,
        None, // session token
        None, // expiry
        "static",
    );

    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("test-region"))
        .endpoint_url(&cluster.grpc_addrs[0])
        .behavior_version_latest()
        .build();
    let client = Client::from_conf(config);

    let private_key = "private.txt";
    let public_key = "public.txt";
    let private_content = b"this is private content";
    let public_content = b"this is public content";

    // 4. Put an object into each bucket using the S3 client (tests SigV4 auth)
    client
        .put_object()
        .bucket(&private_bucket)
        .key(private_key)
        .body(ByteStream::from(private_content.to_vec()))
        .send()
        .await
        .expect("Failed to put private object");

    client
        .put_object()
        .bucket(&public_bucket)
        .key(public_key)
        .body(ByteStream::from(public_content.to_vec()))
        .send()
        .await
        .expect("Failed to put public object");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Test Private Access (Success): Use S3 client to get from private bucket
    let resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .expect("Failed to get private object with S3 client");
    let data = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), private_content);

    // 6. Test Public Access (Success): Use reqwest (no auth) to get from public bucket
    let public_url = format!("{}/{}/{}", cluster.grpc_addrs[0], public_bucket, public_key);
    let public_resp = reqwest::get(&public_url)
        .await
        .expect("Failed to make public request");
    assert_eq!(public_resp.status(), 200);
    let public_data = public_resp.bytes().await.unwrap();
    assert_eq!(public_data.as_ref(), public_content);

    // 7. Test Private Access (Failure): Use reqwest (no auth) to get from private bucket
    let private_url = format!("{}/{}/{}", cluster.grpc_addrs[0], private_bucket, private_key);
    let private_resp = reqwest::get(&private_url).await.unwrap();
    assert!(
        private_resp.status() == 403 || private_resp.status() == 404,
        "Private bucket should be blocked for anonymous access"
    );
}
