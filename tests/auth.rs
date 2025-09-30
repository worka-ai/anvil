use std::net::SocketAddr;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::{CreateBucketRequest, GetAccessTokenRequest};
use std::process::Command;
use tonic::Request;
use crate::common::create_pool;

mod common;

#[tokio::test]
async fn test_auth_flow_with_wildcard_scopes() {
    common::with_test_dbs(|global_db_url, regional_db_url, _| async move {
        // 1. Set environment for the server process
        let server_env = vec![
            ("GLOBAL_DATABASE_URL", global_db_url.clone()),
            ("DATABASE_URL_REGION_AUTH_TEST", regional_db_url.clone()),
            ("REGION", "AUTH_TEST".to_string()),
            ("JWT_SECRET", "auth-test-secret".to_string()),
        ];
        let server_env2 = server_env.clone();
        // 2. Start the server
        let grpc_addr = "127.0.0.1:50099".parse::<SocketAddr>().unwrap();
        let listener = tokio::net::TcpListener::bind(grpc_addr).await.unwrap();
        tokio::spawn(async move {
            anvil::run(
                listener,
                server_env[2].1.clone(), // region
                server_env[0].1.clone(), // global_db_url
                server_env[1].1.clone(), // regional_db_url
                server_env[3].1.clone(), // jwt_secret
            )
            .await
            .unwrap();
        });
        assert!(common::wait_for_port(grpc_addr, std::time::Duration::from_secs(5)).await, "Server did not start in time");
        common::create_default_tenant(&create_pool(&global_db_url).unwrap(), "AUTH_TEST").await;

        // 3. Use the admin CLI to create app and grant policy (which now runs against a clean DB)
        let admin_args = &["run", "--bin", "admin", "--"];
        let app_output = Command::new("cargo")
            .args(admin_args.iter().chain(&["apps", "create", "--tenant-name", "default", "--app-name", "auth-app"]))
            .envs(server_env2.clone())
            .output().unwrap();
        assert!(app_output.status.success());
        let creds = String::from_utf8(app_output.stdout).unwrap();
        let client_id = common::extract_credential(&creds, "Client ID");
        let client_secret = common::extract_credential(&creds, "Client Secret");

        let policy_args = &["policies", "grant", "--app-name", "auth-app", "--action", "write", "--resource", "bucket:auth-test-*"];
        let status = Command::new("cargo")
            .args(admin_args.iter().chain(policy_args.iter()))
            .envs(server_env2.clone())
            .status().unwrap();
        assert!(status.success());

        // 3. Get a token (requesting no specific scopes, should get all allowed)
        let mut auth_client = AuthServiceClient::connect("http://127.0.0.1:50099").await.unwrap();
        let token_res = auth_client.get_access_token(GetAccessTokenRequest {
            client_id,
            client_secret,
            scopes: vec![], // Let the server return all allowed scopes
        }).await.unwrap().into_inner();
        let token = token_res.access_token;

        // 4. Use the token to create a bucket that MATCHES the wildcard policy
        let mut bucket_client = BucketServiceClient::connect("http://127.0.0.1:50099").await.unwrap();
        let mut req_good = tonic::Request::new(CreateBucketRequest { bucket_name: "auth-test-bucket".to_string(), region: "AUTH_TEST".to_string() });
        req_good.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
        let create_res_good = bucket_client.create_bucket(req_good).await;
        assert!(create_res_good.is_ok(), "Bucket creation should succeed with a matching wildcard scope");

        // 5. Use the SAME token to try creating a bucket that DOES NOT MATCH
        let mut req_bad = tonic::Request::new(CreateBucketRequest { bucket_name: "unauthorized-bucket".to_string(), region: "AUTH_TEST".to_string() });
        req_bad.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
        let create_res_bad = bucket_client.create_bucket(req_bad).await;
        assert!(create_res_bad.is_err(), "Bucket creation should fail with a non-matching scope");
        assert_eq!(create_res_bad.unwrap_err().code(), tonic::Code::PermissionDenied);

    }).await;
}
