use super::*;
use anvil::anvil_api::stream_service_client::StreamServiceClient;
use anvil::anvil_api::{
    AppendStreamRecordRequest, CreateAppendStreamRequest, ReadAppendStreamRequest,
    SealAppendStreamSegmentRequest,
};

#[tokio::test]
async fn stream_capabilities_are_zanzibar_grants_not_object_or_scope_bypasses() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;
    cluster
        .create_bucket("stream-auth-bucket", "test-region-1")
        .await;

    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, "stream-auth-bucket")
        .await
        .unwrap()
        .expect("bucket exists");

    let owner_app_id = default_test_app_id(&cluster).await;
    let mut owner_stream = StreamServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut create = Request::new(CreateAppendStreamRequest {
        bucket_name: "stream-auth-bucket".to_string(),
        stream_key: "events/audit".to_string(),
        mutation_context: Some(native_mutation_context(
            bucket.id,
            &owner_app_id,
            "create-stream-auth",
        )),
    });
    add_bearer(&mut create, &cluster.token);
    let created = owner_stream
        .create_stream(create)
        .await
        .unwrap()
        .into_inner();
    assert!(!created.stream_id.is_empty());

    let (limited_app_id, limited_client_id, limited_secret) =
        create_app_with_id(&cluster, "stream-limited-app").await;
    let limited_token =
        get_token(&cluster.grpc_addrs[0], &limited_client_id, &limited_secret).await;
    let mut limited_stream = StreamServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let mut denied_append = Request::new(AppendStreamRecordRequest {
        bucket_name: "stream-auth-bucket".to_string(),
        stream_key: "events/audit".to_string(),
        stream_id: created.stream_id.clone(),
        payload: b"not yet".to_vec(),
        mutation_context: Some(native_mutation_context(
            bucket.id,
            &limited_app_id,
            "denied-stream-append",
        )),
        content_type: Some("text/plain".to_string()),
        user_metadata_json: String::new(),
        precondition: None,
    });
    add_bearer(&mut denied_append, &limited_token);
    let err = limited_stream
        .append_record(denied_append)
        .await
        .expect_err("limited app must not append before stream grant");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    let mut denied_read = Request::new(ReadAppendStreamRequest {
        bucket_name: "stream-auth-bucket".to_string(),
        stream_key: "events/audit".to_string(),
        stream_id: created.stream_id.clone(),
        after_sequence: 0,
        limit: 10,
        include_payload: true,
        consistency: None,
        page_token: String::new(),
    });
    add_bearer(&mut denied_read, &limited_token);
    let err = limited_stream
        .read_stream(denied_read)
        .await
        .expect_err("limited app must not read before stream grant");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    grant_policy(
        &cluster,
        "stream-limited-app",
        "stream:append",
        "stream-auth-bucket/events/audit",
    )
    .await;

    let mut append = Request::new(AppendStreamRecordRequest {
        bucket_name: "stream-auth-bucket".to_string(),
        stream_key: "events/audit".to_string(),
        stream_id: created.stream_id.clone(),
        payload: b"granted append".to_vec(),
        mutation_context: Some(native_mutation_context(
            bucket.id,
            &limited_app_id,
            "granted-stream-append",
        )),
        content_type: Some("text/plain".to_string()),
        user_metadata_json: String::new(),
        precondition: None,
    });
    add_bearer(&mut append, &limited_token);
    let appended = limited_stream
        .append_record(append)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(appended.record_sequence, 1);

    let mut still_denied_read = Request::new(ReadAppendStreamRequest {
        bucket_name: "stream-auth-bucket".to_string(),
        stream_key: "events/audit".to_string(),
        stream_id: created.stream_id.clone(),
        after_sequence: 0,
        limit: 10,
        include_payload: true,
        consistency: None,
        page_token: String::new(),
    });
    add_bearer(&mut still_denied_read, &limited_token);
    let err = limited_stream
        .read_stream(still_denied_read)
        .await
        .expect_err("append authority must not imply read authority");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    grant_policy(
        &cluster,
        "stream-limited-app",
        "stream:read",
        "stream-auth-bucket/events/audit",
    )
    .await;

    let mut read = Request::new(ReadAppendStreamRequest {
        bucket_name: "stream-auth-bucket".to_string(),
        stream_key: "events/audit".to_string(),
        stream_id: created.stream_id.clone(),
        after_sequence: 0,
        limit: 10,
        include_payload: true,
        consistency: None,
        page_token: String::new(),
    });
    add_bearer(&mut read, &limited_token);
    let read = limited_stream.read_stream(read).await.unwrap().into_inner();
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.records[0].payload, b"granted append".to_vec());

    let mut denied_seal = Request::new(SealAppendStreamSegmentRequest {
        bucket_name: "stream-auth-bucket".to_string(),
        stream_key: "events/audit".to_string(),
        stream_id: created.stream_id.clone(),
        mutation_context: Some(native_mutation_context(
            bucket.id,
            &limited_app_id,
            "denied-stream-seal",
        )),
        precondition: None,
    });
    add_bearer(&mut denied_seal, &limited_token);
    let err = limited_stream
        .seal_segment(denied_seal)
        .await
        .expect_err("append and read authority must not imply seal authority");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    grant_policy(
        &cluster,
        "stream-limited-app",
        "stream:seal_segment",
        "stream-auth-bucket/events/audit",
    )
    .await;

    let mut seal = Request::new(SealAppendStreamSegmentRequest {
        bucket_name: "stream-auth-bucket".to_string(),
        stream_key: "events/audit".to_string(),
        stream_id: created.stream_id,
        mutation_context: Some(native_mutation_context(
            bucket.id,
            &limited_app_id,
            "granted-stream-seal",
        )),
        precondition: None,
    });
    add_bearer(&mut seal, &limited_token);
    let sealed = limited_stream
        .seal_segment(seal)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(sealed.record_count, 1);
}
