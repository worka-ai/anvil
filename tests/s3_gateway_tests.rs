use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::SetPublicAccessRequest;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use std::time::Duration;

mod common;

#[tokio::test]
async fn test_s3_public_and_private_access() {
    let world = common::TestWorld::new().await;
    let (client_id, client_secret) = common::create_app(&world.global_db_url, "test-app");

    // Grant wildcard policy to the app before getting a token
    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policies",
        "grant",
        "--app-name",
        "test-app",
        "--action",
        "*",
        "--resource",
        "*",
    ];
    let status = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(policy_args.iter()))
        .env("GLOBAL_DATABASE_URL", &world.global_db_url)
        .env("WORKA_SECRET_ENCRYPTION_KEY", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        .status()
        .unwrap();
    assert!(status.success());

    let token = common::get_token_for_scopes(
        &world.grpc_addr,
        &client_id,
        &client_secret,
        vec!["read:*".to_string(), "write:*".to_string(), "grant:*".to_string()],
    )
    .await;

    // 1. Create a private and a public bucket
    let private_bucket = "private-s3-bucket".to_string();
    let public_bucket = "public-s3-bucket".to_string();
    common::create_test_bucket(&world.grpc_addr, &private_bucket, &token).await;
    common::create_test_bucket(&world.grpc_addr, &public_bucket, &token).await;

    // 2. Set the public bucket to be public
    let mut auth_client = AuthServiceClient::connect(world.grpc_addr.clone()).await.unwrap();
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
        .region(Region::new("test-region"))
        .endpoint_url(&world.grpc_addr)
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

    // Give server a moment to process
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
    let public_url = format!("{}/{}/{}", world.grpc_addr, public_bucket, public_key);
    let public_resp = reqwest::get(&public_url)
        .await
        .expect("Failed to make public request");
    assert_eq!(public_resp.status(), 200);
    let public_data = public_resp.bytes().await.unwrap();
    assert_eq!(public_data.as_ref(), public_content);

    // 7. Test Private Access (Failure): Use reqwest (no auth) to get from private bucket
    let private_url = format!("{}/{}/{}", world.grpc_addr, private_bucket, private_key);
    let private_resp = reqwest::get(&private_url)
        .await
        .expect("Failed to make private request");
    assert_eq!(private_resp.status(), 403, "Private bucket should be forbidden for anonymous access");
}
