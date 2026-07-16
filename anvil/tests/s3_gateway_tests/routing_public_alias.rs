use super::*;

#[tokio::test]
async fn test_s3_regional_host_routing_reads_same_object_and_rejects_dotted_hosts() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "host-route").await;

    let http_base = actor.grpc_addr.trim_end_matches('/');
    let s3 = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("host-route");
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

    set_bucket_public_for_docker_app(&actor, &bucket).await;

    let http = reqwest::Client::new();
    let path_style = http
        .get(format!(
            "{http_base}/{}/{bucket}/{key}",
            docker_actor_tenant_route(&actor)
        ))
        .header(reqwest::header::HOST, &cluster.public_region_host)
        .send()
        .await
        .expect("path-style regional GET should send");
    assert_eq!(path_style.status(), reqwest::StatusCode::OK);
    assert_eq!(path_style.bytes().await.unwrap().as_ref(), body);

    let virtual_host = http
        .get(format!("{http_base}/{key}"))
        .header(
            reqwest::header::HOST,
            format!(
                "{bucket}.{}.{}",
                docker_actor_tenant_route(&actor),
                cluster.public_region_host
            ),
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
            format!(
                "assets.{bucket}.{}.{}",
                docker_actor_tenant_route(&actor),
                cluster.public_region_host
            ),
        )
        .send()
        .await
        .expect("dotted bucket host form should send");
    assert_eq!(dotted_bucket.status(), reqwest::StatusCode::BAD_REQUEST);

    let dotted_tenant = http
        .get(format!("{http_base}/{key}"))
        .header(
            reqwest::header::HOST,
            format!(
                "{bucket}.team.{}.{}",
                docker_actor_tenant_route(&actor),
                cluster.public_region_host
            ),
        )
        .send()
        .await
        .expect("dotted tenant host form should send");
    assert_eq!(dotted_tenant.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_s3_public_get_returns_latest_overwritten_inline_object() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-public-overwrite").await;

    let http_base = actor.grpc_addr.trim_end_matches('/');
    let s3 = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("public-overwrite");
    let key = "models/gpt-oss-20b/anvil-index.json";
    let first = br#"{"files":[{"path":"config.json"}]}"#;
    let second = br#"{"files":[{"path":"config.json"},{"path":"README.md"}]}"#;

    s3.create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("CreateBucket should succeed");

    set_bucket_public_for_docker_app(&actor, &bucket).await;

    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(first))
        .send()
        .await
        .expect("first inline PUT should succeed");
    assert_public_get_body(
        http_base,
        &cluster.public_region_host,
        docker_actor_tenant_route(&actor),
        &bucket,
        key,
        first,
    )
    .await;

    s3.put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(second))
        .send()
        .await
        .expect("overwriting inline PUT should succeed");
    assert_public_get_body(
        http_base,
        &cluster.public_region_host,
        docker_actor_tenant_route(&actor),
        &bucket,
        key,
        second,
    )
    .await;
}

async fn assert_public_get_body(
    http_base: &str,
    public_region_host: &str,
    tenant: &str,
    bucket: &str,
    key: &str,
    expected: &[u8],
) {
    let response = reqwest::Client::new()
        .get(tenant_routed_public_url(http_base, tenant, bucket, key))
        .header(reqwest::header::HOST, public_region_host)
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
    let cluster = shared_docker_test_cluster().await;

    let default_actor = create_docker_app(&cluster, "tenant-scoped-default").await;
    let routed_actor = create_docker_app(&cluster, "tenant-scoped-routed").await;
    let http_base = routed_actor.grpc_addr.trim_end_matches('/');
    let s3 = s3_client_for_docker_app(&cluster, &default_actor);
    let routed_s3 = s3_client_for_docker_app(&cluster, &routed_actor);
    let bucket = unique_test_name("tenant-scoped");
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

    routed_s3
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("same bucket name should be allowed in another tenant");
    routed_s3
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from_static(routed_body))
        .send()
        .await
        .expect("routed tenant object should be written");
    routed_s3
        .put_object()
        .bucket(&bucket)
        .key(routed_only_key)
        .body(ByteStream::from_static(routed_only_body))
        .send()
        .await
        .expect("routed tenant-only object should be written");

    let mut object_client = ObjectServiceClient::connect(routed_actor.grpc_addr.clone())
        .await
        .unwrap();
    object_client
        .create_object_link(authorized(
            CreateObjectLinkRequest {
                context: Some(public_mutation_context("test-routed-link", 0)),
                tenant_id: String::new(),
                bucket_name: bucket.clone(),
                link_key: link_key.to_string(),
                target_key: key.to_string(),
                target_version: String::new(),
                resolution: ObjectLinkResolution::Follow as i32,
                allow_dangling: false,
            },
            &routed_actor.token,
        ))
        .await
        .expect("routed tenant object link should be written");
    set_bucket_public_for_docker_app(&routed_actor, &bucket).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{http_base}/{}/{bucket}/{key}",
            docker_actor_tenant_route(&routed_actor)
        ))
        .header(reqwest::header::HOST, &cluster.public_region_host)
        .send()
        .await
        .expect("tenant-routed public GET should send");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), routed_body);

    let versions = reqwest::Client::new()
        .get(format!(
            "{http_base}/{}/{bucket}?versions",
            docker_actor_tenant_route(&routed_actor)
        ))
        .header(reqwest::header::HOST, &cluster.public_region_host)
        .send()
        .await
        .expect("tenant-routed public version listing should send");
    assert_eq!(versions.status(), reqwest::StatusCode::OK);
    let versions_xml = versions.text().await.expect("version listing body");
    assert!(versions_xml.contains(routed_only_key));
    assert!(!versions_xml.contains(default_only_key));

    let link_metadata = reqwest::Client::new()
        .get(format!(
            "{http_base}/{}/{bucket}/{link_key}",
            docker_actor_tenant_route(&routed_actor)
        ))
        .header(reqwest::header::HOST, &cluster.public_region_host)
        .header("x-anvil-link-mode", "metadata")
        .send()
        .await
        .expect("tenant-routed link metadata GET should send");
    assert_eq!(link_metadata.status(), reqwest::StatusCode::OK);
    let link_metadata = link_metadata
        .json::<serde_json::Value>()
        .await
        .expect("link metadata JSON");
    assert_eq!(
        link_metadata["tenant_id"],
        routed_actor.tenant_id.to_string()
    );
    assert_eq!(link_metadata["link_key"], link_key);
    assert_eq!(link_metadata["target_key"], key);
}

#[tokio::test]
async fn test_s3_custom_host_alias_routes_to_bucket_prefix() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "host-alias").await;

    let http_base = actor.grpc_addr.trim_end_matches('/');
    let s3 = s3_client_for_docker_app(&cluster, &actor);
    let bucket = unique_test_name("host-alias");
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

    set_bucket_public_for_docker_app(&actor, &bucket).await;

    let hostname = format!("{}.example.test", unique_test_name("assets"));
    let mut object_client = ObjectServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let alias = object_client
        .create_host_alias(authorized(
            CreateHostAliasRequest {
                context: Some(public_mutation_context("host-alias-create", 0)),
                hostname: hostname.clone(),
                tenant_id: String::new(),
                bucket_name: bucket.clone(),
                region: cluster.region.clone(),
                prefix: "public/".to_string(),
            },
            &actor.token,
        ))
        .await
        .expect("host alias should be created")
        .into_inner()
        .host_alias
        .expect("host alias descriptor");
    let alias = object_client
        .verify_host_alias(authorized(
            VerifyHostAliasRequest {
                context: Some(public_mutation_context(
                    "host-alias-verify",
                    alias.generation,
                )),
                hostname: hostname.clone(),
                observed_challenge: alias.verification_challenge,
            },
            &actor.token,
        ))
        .await
        .expect("host alias should activate")
        .into_inner()
        .host_alias
        .expect("host alias descriptor");
    assert_eq!(alias.state, anvil::anvil_api::HostAliasState::Active as i32);

    let response = reqwest::Client::new()
        .get(format!("{http_base}/latest.exe"))
        .header(reqwest::header::HOST, hostname)
        .send()
        .await
        .expect("custom host alias GET should send");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.bytes().await.unwrap().as_ref(), body);
}
