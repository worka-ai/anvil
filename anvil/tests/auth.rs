use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::{CreateBucketRequest, GetAccessTokenRequest};
use anvil::auth::JwtManager;
use anvil_test_utils::*;

#[tokio::test]
async fn token_identifies_principal_and_zanzibar_grants_authorise_runtime_actions() {
    let cluster = shared_docker_test_cluster().await;

    let grpc_addr = cluster.grpc_addr_for_test("auth-zanzibar");
    let app_name = unique_test_name("auth-app");
    let ungranted_app_name = unique_test_name("ungranted-app");
    let bucket_name = unique_test_name("auth-bucket");
    let tenant_id = cluster
        .create_tenant(&unique_test_name("auth-tenant"))
        .await;
    let (app_id, client_id, client_secret) = cluster
        .create_application_with_id(tenant_id, &app_name)
        .await;
    cluster
        .grant_application_policy(tenant_id, &app_name, "bucket:create", &bucket_name)
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
    let claims = JwtManager::new("docker-test-secret".to_string())
        .verify_token(&token)
        .unwrap();
    assert_eq!(claims.sub, app_id);
    assert_eq!(claims.tenant_id, tenant_id);

    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut req_good = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

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

    let (_, unauthorised_client_id, unauthorised_client_secret) = cluster
        .create_application_with_id(tenant_id, &ungranted_app_name)
        .await;
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
        bucket_name: unique_test_name("unauth-bucket"),
        region: "test-region-1".to_string(),
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
