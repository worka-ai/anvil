use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::{CreateBucketRequest, GetAccessTokenRequest};
use std::time::Duration;

use anvil_test_utils::*;

#[tokio::test]
async fn test_auth_flow_with_wildcard_scopes() {
    let mut cluster = TestCluster::new(&["auth-test"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let (client_id, client_secret) = cluster
        .create_application_with_policy("default", "auth-app", "bucket:create", "auth-test-*")
        .await;

    // Get a token (requesting no specific scopes, should get all allowed)
    let mut auth_client = AuthServiceClient::connect(grpc_addr.clone()).await.unwrap();
    let token_res = auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id,
            client_secret,
            scopes: vec![], // Let the server return all allowed scopes
        })
        .await
        .unwrap()
        .into_inner();
    let token = token_res.access_token;

    // Use the token to create a bucket that MATCHES the wildcard policy
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut req_good = tonic::Request::new(CreateBucketRequest {
        bucket_name: "auth-test-bucket".to_string(),
        region: "auth-test".to_string(),
    });
    req_good.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let create_res_good = bucket_client.create_bucket(req_good).await;
    assert!(
        create_res_good.is_ok(),
        "Bucket creation should succeed with a matching wildcard scope"
    );

    // Use the SAME token to try creating a bucket that DOES NOT MATCH
    let mut req_bad = tonic::Request::new(CreateBucketRequest {
        bucket_name: "unauthorized-bucket".to_string(),
        region: "auth-test".to_string(),
    });
    req_bad.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let create_res_bad = bucket_client.create_bucket(req_bad).await;
    assert!(
        create_res_bad.is_err(),
        "Bucket creation should fail with a non-matching scope"
    );
    assert_eq!(
        create_res_bad.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
}
