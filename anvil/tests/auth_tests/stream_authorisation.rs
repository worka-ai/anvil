use super::*;
use anvil::anvil_api::stream_service_client::StreamServiceClient;
use anvil::anvil_api::{
    AppendStreamRecordRequest, CreateAppendStreamRequest, ReadAppendStreamRequest,
    SealAppendStreamSegmentRequest,
};

#[tokio::test]
async fn stream_capabilities_are_zanzibar_grants_not_object_or_scope_bypasses() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_storage_test_actor(&cluster, "stream-auth").await;
    let bucket_name = unique_test_name("stream-auth");
    let stream_key = "events/audit".to_string();

    let mut bucket_client = BucketServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),
        options: None,
    });
    add_bearer(&mut create_bucket, &actor.token);
    let bucket_id = bucket_client
        .create_bucket(create_bucket)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let mut owner_stream = StreamServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut create = Request::new(CreateAppendStreamRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        mutation_context: Some(native_mutation_context(
            actor.tenant_id,
            bucket_id,
            &actor.app_id,
            "create-stream-auth",
        )),
    });
    add_bearer(&mut create, &actor.token);
    let created = owner_stream
        .create_stream(create)
        .await
        .unwrap()
        .into_inner();
    assert!(!created.stream_id.is_empty());

    let limited = cluster
        .create_actor_in_tenant(actor.tenant_id, "stream-limited", &[])
        .await;
    let limited_token = limited.token.clone();
    let mut limited_stream = StreamServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();

    let mut denied_append = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: created.stream_id.clone(),
        payload: b"not yet".to_vec(),
        mutation_context: Some(native_mutation_context(
            limited.tenant_id,
            bucket_id,
            &limited.app_id,
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
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
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

    grant_docker_actor_policy(
        &cluster,
        &limited,
        "stream:append",
        &format!("{bucket_name}/{stream_key}"),
    )
    .await;

    let mut append = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: created.stream_id.clone(),
        payload: b"granted append".to_vec(),
        mutation_context: Some(native_mutation_context(
            limited.tenant_id,
            bucket_id,
            &limited.app_id,
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
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
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

    grant_docker_actor_policy(
        &cluster,
        &limited,
        "stream:read",
        &format!("{bucket_name}/{stream_key}"),
    )
    .await;

    let mut read = Request::new(ReadAppendStreamRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
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
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: created.stream_id.clone(),
        mutation_context: Some(native_mutation_context(
            limited.tenant_id,
            bucket_id,
            &limited.app_id,
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

    grant_docker_actor_policy(
        &cluster,
        &limited,
        "stream:seal_segment",
        &format!("{bucket_name}/{stream_key}"),
    )
    .await;

    let mut seal = Request::new(SealAppendStreamSegmentRequest {
        bucket_name,
        stream_key,
        stream_id: created.stream_id,
        mutation_context: Some(native_mutation_context(
            limited.tenant_id,
            bucket_id,
            &limited.app_id,
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
