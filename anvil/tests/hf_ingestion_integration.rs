use anvil_test_utils::*;
use std::time::Duration;

#[tokio::test]
async fn hf_ingestion_single_file_integration() {
    // Use the same harness patterns as other tests (TestCluster handles dotenv + DB)
    // Spin up a single-node cluster with isolated DBs
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;

    let token = cluster.token.clone();

    // Create a bucket via gRPC
    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: "models".into(),
        region: "test-region-1".into(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(req).await.unwrap();



    // Create HF key with empty token (public repo)
    let mut key_client = anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut kreq = tonic::Request::new(anvil::anvil_api::CreateHfKeyRequest {
        name: "test".into(),
        token: "test-token".into(),
        note: "".into(),
    });
    kreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    key_client.create_key(kreq).await.unwrap();

    // Start ingestion for public config.json
    let mut ing_client = anvil::anvil_api::hf_ingestion_service_client::HfIngestionServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut sreq = tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
        key_name: "test".into(),
        repo: "openai/gpt-oss-20b".into(),
        revision: "main".into(),
        target_bucket: "models".into(),
        target_region: "test-region-1".into(),
        target_prefix: "gpt-oss-20b".into(),
        include_globs: vec!["config.json".into()],
        exclude_globs: vec![],
    });
    sreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let ing_id = ing_client
        .start_ingestion(sreq)
        .await
        .unwrap()
        .into_inner()
        .ingestion_id;

    // Poll status to completion
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > Duration::from_secs(60) {
            panic!("timeout waiting for ingestion");
        }
        let mut streq = tonic::Request::new(anvil::anvil_api::GetHfIngestionStatusRequest {
            ingestion_id: ing_id.clone(),
        });
        streq.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let st = ing_client.get_ingestion_status(streq).await.unwrap().into_inner();
        if st.state == "completed" {
            break;
        }
        if st.state == "failed" {
            panic!("ingestion failed: {}", st.error);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    // Verify object is not public initially
    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let url = format!("{}/models/gpt-oss-20b/config.json", http_base);
    let resp_before = reqwest::get(&url).await.unwrap();
    assert_eq!(resp_before.status(), 403, "Object should be private initially");

    // Make the bucket public
    let mut auth_client = anvil::anvil_api::auth_service_client::AuthServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::SetPublicAccessRequest {
        bucket: "models".into(),
        allow_public_read: true,
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(req).await.unwrap();

    // Verify object is now public
    let resp_after = reqwest::get(&url).await.unwrap();
    assert_eq!(resp_after.status(), 200, "Object should be public after setting policy");
    let txt = resp_after.text().await.unwrap();
    let v: serde_json::Value = serde_json::from_str(&txt).unwrap();
    assert!(v.is_object());
}

#[tokio::test]
async fn hf_ingestion_permission_denied() {
    // Harness handles dotenv + DB
    // Spin up cluster
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;

    let limited_token = cluster
        .states[0]
        .jwt_manager
        .mint_token("test-app".into(), vec!["read:*".into()], 0)
        .unwrap();

    // Create bucket
    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: "models-denied".into(),
        region: "test-region-1".into(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", limited_token).parse().unwrap(),
    );
    let _ = bucket_client.create_bucket(req).await;

    // Create key with auth ok
    let mut key_client = anvil::anvil_api::hugging_face_key_service_client::HuggingFaceKeyServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut kreq = tonic::Request::new(anvil::anvil_api::CreateHfKeyRequest {
        name: "pd-test".into(),
        token: "test-token".into(),
        note: "".into(),
    });
    kreq.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", limited_token).parse().unwrap(),
    );
    key_client.create_key(kreq).await.unwrap();

    // Start ingestion with a token that lacks required scopes -> PermissionDenied
    let mut ing_client = anvil::anvil_api::hf_ingestion_service_client::HfIngestionServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut sreq = tonic::Request::new(anvil::anvil_api::StartHfIngestionRequest {
        key_name: "pd-test".into(),
        repo: "openai/gpt-oss-20b".into(),
        revision: "main".into(),
        target_bucket: "models-denied".into(),
        target_region: "test-region-1".into(),
        target_prefix: "gpt-oss-20b".into(),
        include_globs: vec!["config.json".into()],
        exclude_globs: vec![],
    });
    // Forge a very limited token: no hf:ingest:start scopes
    let limited_token = cluster
        .states[0]
        .jwt_manager
        .mint_token("test-app".into(), vec!["read:*".into()], 0)
        .unwrap();
    sreq
        .metadata_mut()
        .insert("authorization", format!("Bearer {}", limited_token).parse().unwrap());
    let err = ing_client.start_ingestion(sreq).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}