use super::*;

#[tokio::test]
async fn test_streaming_upload_decoding() {
    let mut cluster = TestCluster::new_with_config(&["test-region-1"], |config| {
        configure_test_public_region(config);
    })
    .await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let (client_id, client_secret) = create_app(&cluster, "streaming-decode-app").await;

    // Grant the test app ownership of the storage tenant through the system realm.
    grant_storage_tenant_owner_for_test(&cluster, "streaming-decode-app").await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Configure S3 client
    let credentials =
        aws_sdk_s3::config::Credentials::new(&client_id, &client_secret, None, None, "static");
    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("test-region-1"))
        .endpoint_url(http_base)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    let client = Client::from_conf(config);

    let bucket_name = format!("streaming-decode-test-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let object_key = "my-streamed-object.txt";
    let original_content = "This is the content that will be streamed with aws-chunked encoding and should be decoded.";

    // 1. Upload the object using a true stream, which forces aws-chunked encoding.
    let stream = original_content.as_bytes().to_vec();
    let _content_len = stream.len();
    // let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(16);
    // tokio::spawn(async move {
    //     for chunk in stream.into_chunks::<5>() {
    //         tx.send(bytes::Bytes::copy_from_slice(&chunk)).await.unwrap();
    //     }
    // });
    // // turn the receiver into a Body that yields http-body 1.0 Frames
    // let stream = ReceiverStream::new(rx).map(|b| Ok::<hyper::body::Frame<bytes::Bytes>, Infallible>(Frame::data(b)));
    // let body = StreamBody::new(stream);
    let mut file = PathBuf::new();
    file.push(temp_dir());
    file.push(format!("anvil-test-streaming-{}", random::<i32>()));
    fs::write(file.as_path(), original_content).await.unwrap();
    let bytestream = ByteStream::read_from()
        .path(file.as_path())
        // Specify the size of the buffer used to read the file (in bytes, default is 4096)
        //.buffer_size(content_len as u64)
        // Specify the length of the file used (skips an additional call to retrieve the size)
        //.length(aws_sdk_s3::primitives::Length::Exact(content_len as i64))
        .build()
        .await
        .expect("valid path");

    client
        .put_object()
        .bucket(&bucket_name)
        .key(object_key)
        //.body(ByteStream::new(SdkBody::from_body_1_x(body)))
        .body(bytestream)
        .send()
        .await
        .expect("Failed to put streaming object");

    // 2. Make the bucket public so we can test with an unauthenticated client.
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let token = get_token(&cluster.grpc_addrs[0], &client_id, &client_secret).await;
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // 3. Download the object using a simple HTTP client (reqwest).
    let object_url = tenant_routed_public_url(http_base, "default", &bucket_name, object_key);
    let response = reqwest::Client::new()
        .get(&object_url)
        .header(reqwest::header::HOST, TEST_PUBLIC_REGION_HOST)
        .send()
        .await
        .unwrap();

    // 4. Verify the response is successful and the body is clean.
    assert_eq!(response.status(), 200, "Expected a successful GET request");
    let downloaded_content = response.text().await.unwrap();

    // This is the critical assertion: the downloaded content must be exactly what we
    // uploaded, with no chunked-encoding metadata.
    assert_eq!(downloaded_content, original_content);
}
