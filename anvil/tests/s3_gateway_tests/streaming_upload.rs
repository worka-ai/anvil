use super::*;

#[tokio::test]
async fn test_streaming_upload_decoding() {
    let cluster = shared_docker_test_cluster().await;

    let actor = create_docker_app(&cluster, "streaming-decode-app").await;

    // Configure S3 client
    let http_base = actor.grpc_addr.trim_end_matches('/');
    let client = s3_client_for_docker_app(&cluster, &actor);

    let bucket_name = unique_test_name("streaming-decode-test");
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
    set_bucket_public_for_docker_app(&actor, &bucket_name).await;

    tokio::time::sleep(Duration::from_secs(1)).await;

    // 3. Download the object using a simple HTTP client (reqwest).
    let tenant = actor.tenant_id.to_string();
    let object_url = tenant_routed_public_url(http_base, &tenant, &bucket_name, object_key);
    let response = reqwest::Client::new()
        .get(&object_url)
        .header(reqwest::header::HOST, &cluster.public_region_host)
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
