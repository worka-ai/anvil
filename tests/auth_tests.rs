use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    CreateBucketRequest, GetAccessTokenRequest, GetObjectRequest, GrantAccessRequest,
    ObjectMetadata, PutObjectRequest, RevokeAccessRequest, SetPublicAccessRequest,
};
use std::time::Duration;
use tonic::Request;

mod common;

// Helper function to create an app, since it's used in auth tests.
fn create_app(global_db_url: &str, app_name: &str) -> (String, String) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let app_output = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "--global-database-url",
            global_db_url,
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "apps",
            "create",
            "--tenant-name",
            "default",
            "--app-name",
            app_name,
        ]))
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
    try_get_token_for_scopes(grpc_addr, client_id, client_secret, scopes)
        .await
        .unwrap()
}

async fn try_get_token_for_scopes(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
    scopes: Vec<String>,
) -> Result<String, tonic::Status> {
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
        .map(|r| r.into_inner().access_token)
}

#[tokio::test]
async fn test_grant_and_revoke_access() {
    let mut cluster = common::TestCluster::new(&["TEST_REGION"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let (granter_client_id, granter_client_secret) =
        create_app(&cluster.global_db_url, "granter-app");

    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policies",
        "grant",
        "--app-name",
        "granter-app",
        "--action",
        "*",
        "--resource",
        "*",
    ];
    let status = std::process::Command::new("cargo")
        .args(
            admin_args
                .iter()
                .chain(&[
                    "--global-database-url",
                    &cluster.global_db_url,
                    "--anvil-secret-encryption-key",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ])
                .chain(policy_args.iter()),
        )
        .status()
        .unwrap();
    assert!(status.success());

    tokio::time::sleep(Duration::from_secs(2)).await;

    let granter_token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &granter_client_id,
        &granter_client_secret,
        vec!["*".to_string()],
    )
    .await;

    let (grantee_client_id, grantee_client_secret) =
        create_app(&cluster.global_db_url, "grantee-app");

    let bucket_name = "grant-test-bucket".to_string();
    let resource = format!("bucket:{}", bucket_name);

    // 2. Grant access
    let mut grant_req = Request::new(GrantAccessRequest {
        grantee_app_id: "grantee-app".to_string(),
        resource: resource.clone(),
        action: "read".to_string(),
    });
    grant_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", granter_token).parse().unwrap(),
    );
    auth_client.grant_access(grant_req).await.unwrap();

    // 3. Verify grantee can now get a token and access the resource
    let grantee_token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &grantee_client_id,
        &grantee_client_secret,
        vec![format!("read:{}", resource)],
    )
    .await;
    assert!(!grantee_token.is_empty());

    // 4. Revoke access
    let mut revoke_req = Request::new(RevokeAccessRequest {
        grantee_app_id: "grantee-app".to_string(),
        resource: resource.clone(),
        action: "read".to_string(),
    });
    revoke_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", granter_token).parse().unwrap(),
    );
    auth_client.revoke_access(revoke_req).await.unwrap();

    // 5. Verify grantee can no longer get a token for that scope
    let res = try_get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &grantee_client_id,
        &grantee_client_secret,
        vec![format!("read:{}", resource)],
    )
    .await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_set_public_access_and_get() {
    let mut cluster = common::TestCluster::new(&["TEST_REGION"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let token = cluster.token.clone();
    let bucket_name = "public-access-bucket".to_string();
    let object_key = "public-object".to_string();

    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "TEST_REGION".to_string(),
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap();

    // Put an object
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"public data".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    // Set bucket to public
    let mut public_req = Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    // Get object without auth
    let get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    let _res = object_client.get_object(get_req).await.unwrap();

    // Set bucket to private
    let mut private_req = Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: false,
    });
    private_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(private_req).await.unwrap();

    // Get object without auth should now fail
    let get_req_2 = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    let res_2 = object_client.get_object(get_req_2).await;
    assert!(res_2.is_err());
}

#[tokio::test]
async fn test_reset_app_secret() {
    let mut cluster = common::TestCluster::new(&["eu1"]).await;
    cluster
        .start_and_converge_no_new_token(Duration::from_secs(5), false)
        .await;

    let app_name = "app-to-reset";

    // 1. Create an app and get original credentials
    let (client_id, original_secret) = create_app(&cluster.global_db_url, app_name);

    // Grant it permissions
    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policies",
        "grant",
        "--app-name",
        app_name,
        "--action",
        "*",
        "--resource",
        "*",
    ];
    let grant_status = std::process::Command::new("cargo")
        .args(
            admin_args
                .iter()
                .chain(&[
                    "--global-database-url",
                    &cluster.global_db_url,
                    "--anvil-secret-encryption-key",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ])
                .chain(policy_args.iter()),
        )
        .status()
        .unwrap();
    assert!(grant_status.success());

    // 2. Reset the secret using the new admin command
    let reset_output = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "--global-database-url",
            &cluster.global_db_url,
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "apps",
            "reset-secret",
            "--app-name",
            app_name,
        ]))
        .output()
        .unwrap();

    assert!(reset_output.status.success());
    let reset_creds = String::from_utf8(reset_output.stdout).unwrap();
    let new_secret = common::extract_credential(&reset_creds, "Client Secret");

    // 3. Verify the secret has changed
    assert_ne!(original_secret, new_secret);

    // 4. Restart the cluster to ensure it picks up the new secret, clearing any cache.
    cluster.restart(Duration::from_secs(10)).await;

    // 5. Verify the NEW secret works against the restarted node
    let s3_client_new = cluster.get_s3_client("eu1", &client_id, &new_secret).await;
    match s3_client_new.list_buckets().send().await {
        Ok(_list_bucket_output) => {}
        Err(e) => {
            panic!("List buckets failed with the new secret: {:?}", e);
        }
    }

    // 6. Verify the OLD secret fails
    let s3_client_old = cluster
        .get_s3_client("eu1", &client_id, &original_secret)
        .await;
    let list_buckets_old = s3_client_old.list_buckets().send().await;
    assert!(
        list_buckets_old.is_err(),
        "List buckets should fail with the old secret"
    );
}
