use super::*;

#[tokio::test]
async fn test_s3_regional_host_routing_reads_same_object_and_rejects_dotted_hosts() {
    let mut cluster = TestCluster::new_with_config(&["test-region-1"], |config| {
        configure_test_public_region(config);
    })
    .await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let s3 = s3_client(http_base, "test-app", "test-secret");
    let bucket = format!("host-route-{}", uuid::Uuid::new_v4());
    let key = "nested/host-routed.txt";
    let body = b"regional host routing resolves the same object";

    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("CreateBucket should succeed");
    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .expect("PutObject should succeed");

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: bucket.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    let http = reqwest::Client::new();
    let path_style = http
        .get(format!("{http_base}/default/{bucket}/{key}"))
        .header(reqwest::header::HOST, "test-region-1.anvil-storage.test")
        .send()
        .await
        .expect("path-style regional GET should send");
    assert_eq!(path_style.status(), reqwest::StatusCode::OK);
    assert_eq!(path_style.bytes().await.unwrap().as_ref(), body);

    let virtual_host = http
        .get(format!("{http_base}/{key}"))
        .header(
            reqwest::header::HOST,
            format!("{bucket}.default.test-region-1.anvil-storage.test"),
        )
        .send()
        .await
        .expect("virtual-host regional GET should send");
    assert_eq!(virtual_host.status(), reqwest::StatusCode::OK);
    assert_eq!(virtual_host.bytes().await.unwrap().as_ref(), body);

    let dotted_bucket = http
        .get(format!("{http_base}/{key}"))
        .header(
            reqwest::header::HOST,
            format!("assets.{bucket}.default.test-region-1.anvil-storage.test"),
        )
        .send()
        .await
        .expect("dotted bucket host form should send");
    assert_eq!(dotted_bucket.status(), reqwest::StatusCode::BAD_REQUEST);

    let dotted_tenant = http
        .get(format!("{http_base}/{key}"))
        .header(
            reqwest::header::HOST,
            format!("{bucket}.team.default.test-region-1.anvil-storage.test"),
        )
        .send()
        .await
        .expect("dotted tenant host form should send");
    assert_eq!(dotted_tenant.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_s3_public_get_returns_latest_overwritten_inline_object() {
    let mut cluster = TestCluster::new_with_config(&["test-region-1"], |config| {
        configure_test_public_region(config);
    })
    .await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-public-overwrite-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster, &app_name).await;
    grant_storage_tenant_owner_for_test(&cluster, &app_name).await;

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let s3 = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("public-overwrite-{}", uuid::Uuid::new_v4());
    let key = "models/gpt-oss-20b/anvil-index.json";
    let first = br#"{"files":[{"path":"config.json"}]}"#;
    let second = br#"{"files":[{"path":"config.json"},{"path":"README.md"}]}"#;

    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("CreateBucket should succeed");

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: bucket.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(first))
        .send()
        .await
        .expect("first inline PUT should succeed");
    assert_public_get_body(http_base, &bucket, key, first).await;

    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(second))
        .send()
        .await
        .expect("overwriting inline PUT should succeed");
    assert_public_get_body(http_base, &bucket, key, second).await;
}

async fn assert_public_get_body(http_base: &str, bucket: &str, key: &str, expected: &[u8]) {
    let response = reqwest::Client::new()
        .get(tenant_routed_public_url(http_base, "default", bucket, key))
        .header(reqwest::header::HOST, TEST_PUBLIC_REGION_HOST)
        .send()
        .await
        .expect("public GET should send");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let content_length = response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());
    assert_eq!(content_length, Some(expected.len()));
    let body = response.bytes().await.expect("public GET body should read");
    assert_eq!(body.as_ref(), expected);
}

#[tokio::test]
async fn test_s3_regional_routes_public_reads_to_tenant_scoped_duplicate_bucket() {
    let mut cluster = TestCluster::new_with_config(&["test-region-1"], |config| {
        configure_test_public_region(config);
    })
    .await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let s3 = s3_client(http_base, "test-app", "test-secret");
    let bucket = format!("tenant-scoped-{}", uuid::Uuid::new_v4());
    let key = "same/key.txt";
    let default_only_key = "default-only.txt";
    let routed_only_key = "routed-only.txt";
    let link_key = "latest.txt";
    let default_body = b"default tenant object";
    let routed_body = b"routed tenant object";
    let routed_only_body = b"routed tenant only";

    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("default tenant CreateBucket should succeed");
    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(default_body))
        .send()
        .await
        .expect("default tenant PutObject should succeed");
    s3.put_object()
        .bucket(&bucket)
        .key(default_only_key)
        .body(ByteStream::from_static(b"default tenant only"))
        .send()
        .await
        .expect("default tenant-only PutObject should succeed");

    let persistence = &cluster.states[0].persistence;
    let routed_tenant = persistence
        .create_tenant("tenant-b", "unused")
        .await
        .expect("routed tenant should be created");
    let routed_bucket = persistence
        .create_bucket(routed_tenant.id, &bucket, "test-region-1")
        .await
        .expect("same bucket name should be allowed in another tenant");
    let routed_claims = anvil::auth::Claims {
        sub: "routed-test-app".to_string(),
        exp: usize::MAX,
        tenant_id: routed_tenant.id,
        jti: None,
    };
    anvil::access_control::grant_storage_tenant_owner(
        persistence,
        routed_tenant.id,
        &routed_claims.sub,
        "test",
        "s3 routed tenant seed",
    )
    .await
    .unwrap();
    anvil::access_control::grant_bucket_defaults(
        persistence,
        &routed_bucket,
        &routed_claims.sub,
        "test",
        "s3 routed tenant seed",
    )
    .await
    .unwrap();
    cluster.states[0]
        .object_manager
        .put_object(
            &routed_claims,
            &routed_bucket.name,
            key,
            tokio_stream::iter(vec![Ok(routed_body.to_vec())]),
            anvil::object_manager::ObjectWriteOptions {
                content_type: Some("text/plain".to_string()),
                user_metadata: None,
                transaction_id: None,
                transaction_principal: None,
                storage_class_id: None,
            },
        )
        .await
        .expect("routed tenant object should be written");
    cluster.states[0]
        .object_manager
        .put_object(
            &routed_claims,
            &routed_bucket.name,
            routed_only_key,
            tokio_stream::iter(vec![Ok(routed_only_body.to_vec())]),
            anvil::object_manager::ObjectWriteOptions {
                content_type: Some("text/plain".to_string()),
                user_metadata: None,
                transaction_id: None,
                transaction_principal: None,
                storage_class_id: None,
            },
        )
        .await
        .expect("routed tenant-only object should be written");
    persistence
        .put_object_link(PutObjectLinkRequest {
            tenant_id: routed_tenant.id,
            bucket_id: routed_bucket.id,
            link_key: link_key.to_string(),
            target_key: key.to_string(),
            target_version: None,
            resolution: ObjectLinkResolution::Follow,
            expected_generation: None,
            create_only: true,
            allow_dangling: false,
            idempotency_key: "test-routed-link".to_string(),
            created_by: "test".to_string(),
            transaction_id: None,
            transaction_principal: None,
        })
        .await
        .expect("routed tenant object link should be written");
    persistence
        .set_bucket_public_access(routed_tenant.id, &bucket, true)
        .await
        .expect("routed tenant bucket should be public");
    anvil::access_control::write_bucket_public_read_tuple(
        persistence,
        &routed_bucket,
        true,
        "test",
        "s3 routed tenant public read seed",
    )
    .await
    .expect("routed tenant public read tuple should be written");

    let response = reqwest::Client::new()
        .get(format!("{http_base}/tenant-b/{bucket}/{key}"))
        .header(reqwest::header::HOST, "test-region-1.anvil-storage.test")
        .send()
        .await
        .expect("tenant-routed public GET should send");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), routed_body);

    let versions = reqwest::Client::new()
        .get(format!("{http_base}/tenant-b/{bucket}?versions"))
        .header(reqwest::header::HOST, "test-region-1.anvil-storage.test")
        .send()
        .await
        .expect("tenant-routed public version listing should send");
    assert_eq!(versions.status(), reqwest::StatusCode::OK);
    let versions_xml = versions.text().await.expect("version listing body");
    assert!(versions_xml.contains(routed_only_key));
    assert!(!versions_xml.contains(default_only_key));

    let link_metadata = reqwest::Client::new()
        .get(format!("{http_base}/tenant-b/{bucket}/{link_key}"))
        .header(reqwest::header::HOST, "test-region-1.anvil-storage.test")
        .header("x-anvil-link-mode", "metadata")
        .send()
        .await
        .expect("tenant-routed link metadata GET should send");
    assert_eq!(link_metadata.status(), reqwest::StatusCode::OK);
    let link_metadata = link_metadata
        .json::<serde_json::Value>()
        .await
        .expect("link metadata JSON");
    assert_eq!(link_metadata["tenant_id"], routed_tenant.id.to_string());
    assert_eq!(link_metadata["link_key"], link_key);
    assert_eq!(link_metadata["target_key"], key);
}

#[tokio::test]
async fn test_s3_custom_host_alias_routes_to_bucket_prefix() {
    let mut cluster = TestCluster::new_with_config(&["test-region-1"], |config| {
        configure_test_public_region(config);
    })
    .await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let s3 = s3_client(http_base, "test-app", "test-secret");
    let bucket = format!("host-alias-{}", uuid::Uuid::new_v4());
    let key = "public/latest.exe";
    let body = b"custom host alias resolves through the configured prefix";

    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("CreateBucket should succeed");
    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .expect("PutObject should succeed");

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: bucket.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    let persistence = &cluster.states[0].persistence;

    let hostname = format!("assets-{}.example.test", uuid::Uuid::new_v4().simple());
    let routing_config =
        RoutingConfig::new("anvil-storage.test").expect("valid routing base domain");
    let alias = persistence
        .create_host_alias_descriptor(
            &routing_config,
            CreateHostAliasDescriptor {
                hostname: hostname.clone(),
                tenant_id: "1".to_string(),
                bucket_name: bucket.clone(),
                region: "test-region-1".to_string(),
                prefix: "public/".to_string(),
            },
        )
        .await
        .expect("host alias should be created");
    let alias = persistence
        .transition_host_alias_descriptor(&hostname, alias.generation, HostAliasState::Active)
        .await
        .expect("host alias should activate");
    assert_eq!(alias.state, HostAliasState::Active);

    let response = reqwest::Client::new()
        .get(format!("{http_base}/latest.exe"))
        .header(reqwest::header::HOST, hostname)
        .send()
        .await
        .expect("custom host alias GET should send");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), body);
}
