use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::{CreateBucketRequest, GetAccessTokenRequest};
use anvil::auth::JwtManager;
use std::time::Duration;

use anvil_test_utils::*;

#[tokio::test]
async fn token_identifies_principal_and_zanzibar_grants_authorise_runtime_actions() {
    let mut cluster = TestCluster::new(&["auth-test"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let (client_id, client_secret) = cluster
        .create_application_with_policy("default", "auth-app", "bucket:create", "auth-test-*")
        .await;

    let mut auth_client = AuthServiceClient::connect(grpc_addr.clone()).await.unwrap();
    let token_res = auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id,
            client_secret,
        })
        .await
        .unwrap()
        .into_inner();
    let token = token_res.access_token;
    let claims = JwtManager::new(cluster.config.jwt_secret.clone())
        .verify_token(&token)
        .unwrap();
    assert!(claims.sub.parse::<i64>().is_ok());
    assert_eq!(claims.tenant_id, 1);

    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut req_good = tonic::Request::new(CreateBucketRequest {
        bucket_name: "auth-test-bucket".to_string(),
        region: "auth-test".to_string(),

        options: None,
    });
    req_good.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let create_res_good = bucket_client.create_bucket(req_good).await;
    assert!(
        create_res_good.is_ok(),
        "bucket creation should succeed through the Zanzibar grant written by the admin plane"
    );

    let (unauthorised_client_id, unauthorised_client_secret) =
        cluster.create_application("default", "ungranted-app").await;
    let unauthorised_token = auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: unauthorised_client_id,
            client_secret: unauthorised_client_secret,
        })
        .await
        .unwrap()
        .into_inner()
        .access_token;
    let mut req_bad = tonic::Request::new(CreateBucketRequest {
        bucket_name: "unauthorized-bucket".to_string(),
        region: "auth-test".to_string(),
        options: None,
    });
    req_bad.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", unauthorised_token).parse().unwrap(),
    );
    assert_eq!(
        bucket_client
            .create_bucket(req_bad)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}
