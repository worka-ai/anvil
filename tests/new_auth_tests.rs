use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::{GrantAccessRequest, RevokeAccessRequest, SetPublicAccessRequest};
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{GetObjectRequest, PutObjectRequest, ObjectMetadata};
use tonic::Request;

mod common;

#[tokio::test]
async fn test_grant_and_revoke_access() {
    common::with_test_dbs(|global_db_url, regional_db_url, _|
        async move {
            let (_state, grpc_addr) = common::start_test_server(&global_db_url, &regional_db_url).await;
            let mut auth_client = AuthServiceClient::connect(grpc_addr.clone()).await.unwrap();
            let granter_token = common::get_auth_token_for_app(&global_db_url, &grpc_addr, "granter-app", "*", "*").await;
            let (grantee_client_id, grantee_client_secret) = common::create_app(&global_db_url, "grantee-app");

            let bucket_name = "grant-test-bucket".to_string();
            let resource = format!("bucket:{}", bucket_name);

            // 2. Grant access
            let mut grant_req = Request::new(GrantAccessRequest {
                grantee_app_id: "grantee-app".to_string(),
                resource: resource.clone(),
                action: "read".to_string(),
            });
            grant_req.metadata_mut().insert("authorization", format!("Bearer {}", granter_token).parse().unwrap());
            auth_client.grant_access(grant_req).await.unwrap();

            // 3. Verify grantee can now get a token and access the resource
            let grantee_token = common::get_token_for_scopes(&grpc_addr, &grantee_client_id, &grantee_client_secret, vec![format!("read:{}", resource)]).await;
            assert!(!grantee_token.is_empty());

            // 4. Revoke access
            let mut revoke_req = Request::new(RevokeAccessRequest {
                grantee_app_id: "grantee-app".to_string(),
                resource: resource.clone(),
                action: "read".to_string(),
            });
            revoke_req.metadata_mut().insert("authorization", format!("Bearer {}", granter_token).parse().unwrap());
            auth_client.revoke_access(revoke_req).await.unwrap();

            // 5. Verify grantee can no longer get a token for that scope
            let res = common::try_get_token_for_scopes(&grpc_addr, &grantee_client_id, &grantee_client_secret, vec![format!("read:{}", resource)]).await;
            assert!(res.is_err());
        }
    ).await;
}

#[tokio::test]
async fn test_set_public_access_and_get() {
    common::with_test_dbs(|global_db_url, regional_db_url, _|
        async move {
            let (_state, grpc_addr) = common::start_test_server(&global_db_url, &regional_db_url).await;
            let mut auth_client = AuthServiceClient::connect(grpc_addr.clone()).await.unwrap();
            let mut object_client = ObjectServiceClient::connect(grpc_addr.clone()).await.unwrap();

            let token = common::get_auth_token(&global_db_url, &grpc_addr).await;
            let bucket_name = "public-access-bucket".to_string();
            let object_key = "public-object".to_string();
            common::create_test_bucket(&grpc_addr, &bucket_name, &token).await;

            // Put an object
            let metadata = ObjectMetadata { bucket_name: bucket_name.clone(), object_key: object_key.clone() };
            let chunks = vec![
                PutObjectRequest { data: Some(anvil::anvil_api::put_object_request::Data::Metadata(metadata)) },
                PutObjectRequest { data: Some(anvil::anvil_api::put_object_request::Data::Chunk(b"public data".to_vec())) },
            ];
            let mut put_req = Request::new(tokio_stream::iter(chunks));
            put_req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
            object_client.put_object(put_req).await.unwrap();

            // Set bucket to public
            let mut public_req = Request::new(SetPublicAccessRequest { bucket: bucket_name.clone(), allow_public_read: true });
            public_req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
            auth_client.set_public_access(public_req).await.unwrap();

            // Get object without auth
            let get_req = Request::new(GetObjectRequest { bucket_name: bucket_name.clone(), object_key: object_key.clone(), version_id: None });
            let res = object_client.get_object(get_req).await;
            assert!(res.is_ok());

            // Set bucket to private
            let mut private_req = Request::new(SetPublicAccessRequest { bucket: bucket_name.clone(), allow_public_read: false });
            private_req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());
            auth_client.set_public_access(private_req).await.unwrap();

            // Get object without auth should now fail
            let get_req_2 = Request::new(GetObjectRequest { bucket_name: bucket_name.clone(), object_key: object_key.clone(), version_id: None });
            let res_2 = object_client.get_object(get_req_2).await;
            assert!(res_2.is_err());
        }
    ).await;
}
