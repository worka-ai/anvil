use super::*;

#[tokio::test]
async fn test_copy_object_creates_independent_destination_version() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "copy-object-creates-independent-destination-vers")
            .await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("copy-bucket");
    let source_key = "source.txt".to_string();
    let destination_key = "destination.txt".to_string();
    let content = b"copy native object";

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: source_key.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "object-metadata",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        storage_class: None,
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                content.to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let put_res = object_client
        .put_object(put_req)
        .await
        .unwrap()
        .into_inner();

    let mut copy_req = Request::new(CopyObjectRequest {
        source_bucket_name: bucket_name.clone(),
        source_object_key: source_key.clone(),
        source_version_id: Some(put_res.version_id.clone()),
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: destination_key.clone(),
        mutation_context: Some(native_mutation_context(&actor, bucket_id, "copy-object")),
    });
    copy_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let copy_res = object_client
        .copy_object(copy_req)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(copy_res.etag, put_res.etag);
    assert_ne!(copy_res.version_id, put_res.version_id);
    assert_native_mutation_response!(copy_res);
    assert_eq!(copy_res.payload_hash, put_res.payload_hash);
    assert!(copy_res.watch_cursor > put_res.watch_cursor);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: destination_key.clone(),
        version_id: Some(copy_res.version_id),
        range: None,

        ..Default::default()
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = object_client
        .get_object(get_req)
        .await
        .unwrap()
        .into_inner();
    let mut downloaded = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk.unwrap().data.unwrap() {
            anvil_api::get_object_response::Data::Metadata(metadata) => {
                assert_eq!(metadata.content_length, content.len() as i64);
            }
            anvil_api::get_object_response::Data::Chunk(bytes) => {
                downloaded.extend_from_slice(&bytes);
            }
        }
    }
    assert_eq!(downloaded, content);
}

#[tokio::test]
async fn test_private_object_read_denied_before_payload_load() {
    let cluster = shared_default_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "private-object-read-denied-before-payload-load").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("denied-payload");
    let object_key = "private/missing-payload.bin".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let _bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            &object_key,
            &hex::encode([42; 32]),
            999,
            "etag-missing-payload",
            Some("application/octet-stream"),
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let limited_reader = unique_test_name("limited-object-reader");
    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(limited_reader, claims.tenant_id)
        .unwrap();

    let mut denied_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    denied_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", limited_token).parse().unwrap(),
    );
    let denied = object_client
        .get_object(denied_req)
        .await
        .expect_err("read without object:read scope must be denied before payload load");
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert_eq!(denied.message(), "Permission denied");

    let mut denied_missing_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: "private/not-created.bin".to_string(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    denied_missing_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", limited_token).parse().unwrap(),
    );
    let denied_missing = object_client
        .get_object(denied_missing_req)
        .await
        .expect_err("unauthorized missing object lookup must not reveal absence");
    assert_eq!(denied_missing.code(), Code::PermissionDenied);
    assert_eq!(denied_missing.message(), "Permission denied");

    let mut denied_missing_head_req = Request::new(HeadObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: "private/not-created.bin".to_string(),
        version_id: None,

        ..Default::default()
    });
    denied_missing_head_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", limited_token).parse().unwrap(),
    );
    let denied_missing_head = object_client
        .head_object(denied_missing_head_req)
        .await
        .expect_err("unauthorized missing HEAD must not reveal absence");
    assert_eq!(denied_missing_head.code(), Code::PermissionDenied);
    assert_eq!(denied_missing_head.message(), "Permission denied");

    let mut allowed_req = Request::new(GetObjectRequest {
        bucket_name,
        object_key,
        version_id: None,
        range: None,

        ..Default::default()
    });
    allowed_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = object_client
        .get_object(allowed_req)
        .await
        .unwrap()
        .into_inner();
    let metadata = stream.next().await.unwrap().unwrap().data.unwrap();
    match metadata {
        anvil_api::get_object_response::Data::Metadata(metadata) => {
            assert_eq!(metadata.content_length, 999);
        }
        anvil_api::get_object_response::Data::Chunk(_) => panic!("first response must be metadata"),
    }
    let payload_error = stream
        .next()
        .await
        .expect("authorized read should attempt payload load")
        .expect_err("missing payload must be reported to authorized readers");
    assert_eq!(payload_error.code(), Code::NotFound);
    assert!(payload_error.message().contains("Object data unavailable"));
}

#[tokio::test]
async fn test_watch_prefix_streams_snapshot_and_live_events() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "watch-prefix-streams-snapshot-and-live-events").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut watch_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("watch-bucket");
    let object_key = "docs/a.txt".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "object-metadata",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        storage_class: None,
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"watch me".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    let mut watch_req = Request::new(WatchPrefixRequest {
        bucket_name: bucket_name.clone(),
        prefix: "docs/".to_string(),
        after_cursor: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = watch_client
        .watch_prefix(watch_req)
        .await
        .unwrap()
        .into_inner();

    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.bucket_name, bucket_name);
    assert_eq!(first.object_key, object_key);
    assert_eq!(first.event_type, "put");
    assert!(!first.is_delete_marker);
    let first_envelope = first.envelope.as_ref().expect("watch event envelope");
    assert_eq!(first_envelope.watch_stream_id, "object_prefix");
    assert_eq!(first_envelope.partition_family, "object_metadata");
    assert_eq!(first_envelope.cursor_low, first.cursor);
    assert_eq!(first_envelope.record_kind, "put");
    assert!(first_envelope.object_ref.ends_with(&object_key));
    assert!(!first_envelope.mutation_id.is_empty());
    assert!(!first_envelope.payload_hash.is_empty());
    let first_cursor = first.cursor;

    let mut delete_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        mutation_context: Some(native_mutation_context(&actor, bucket_id, "delete-object")),
    });
    delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.delete_object(delete_req).await.unwrap();

    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(second.cursor > first_cursor);
    assert_eq!(second.bucket_name, bucket_name);
    assert_eq!(second.object_key, object_key);
    assert_eq!(second.event_type, "delete");
    assert!(second.is_delete_marker);
}

#[tokio::test]
async fn test_append_stream_records_are_ordered_and_sealable() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "append-stream-records-are-ordered-and-sealable").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("append-bucket");
    let stream_key = "events/topic-a".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let mut create_stream_req = Request::new(CreateAppendStreamRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "create-append-stream",
        )),
    });
    create_stream_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let create_stream = object_client
        .create_append_stream(create_stream_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(create_stream);
    assert_eq!(create_stream.version_id, create_stream.stream_id);
    let stream_id = create_stream.stream_id;

    let mut first_req = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: stream_id.clone(),
        payload: b"first".to_vec(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "append-stream-record",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        precondition: None,
    });
    first_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first = object_client
        .append_stream_record(first_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(first);
    assert_eq!(first.version_id, "1");
    assert_eq!(first.record_sequence, 1);
    assert_eq!(first.payload_size, 5);
    assert!(!first.payload_hash.is_empty());

    let mut second_req = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: stream_id.clone(),
        payload: b"second".to_vec(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "append-stream-record",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        precondition: None,
    });
    second_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second = object_client
        .append_stream_record(second_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(second);
    assert_eq!(second.record_sequence, 2);
    assert!(second.watch_cursor > first.watch_cursor);

    let mut seal_req = Request::new(SealAppendStreamSegmentRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: stream_id.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "seal-append-stream",
        )),
        precondition: None,
    });
    seal_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let sealed = object_client
        .seal_append_stream_segment(seal_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(sealed);
    assert_eq!(sealed.version_id, stream_id);
    assert_eq!(sealed.record_count, 2);
    assert!(!sealed.segment_hash.is_empty());
    assert!(sealed.watch_cursor > second.watch_cursor);

    let mut append_after_seal = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: stream_id.clone(),
        payload: b"third".to_vec(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "append-stream-record",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        precondition: None,
    });
    append_after_seal.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let third = object_client
        .append_stream_record(append_after_seal)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(third.record_sequence, 3);

    let mut read_req = Request::new(ReadAppendStreamRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: stream_id.clone(),
        after_sequence: 0,
        limit: 10,
        include_payload: true,

        ..Default::default()
    });
    read_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let records = object_client
        .read_append_stream(read_req)
        .await
        .unwrap()
        .into_inner()
        .records;
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].record_sequence, 1);
    assert_eq!(records[0].payload, b"first".to_vec());
    assert_eq!(records[2].record_sequence, 3);
    assert_eq!(records[2].payload, b"third".to_vec());

    let mut tail_req = Request::new(TailAppendStreamRequest {
        bucket_name,
        stream_key,
        stream_id,
        from_sequence: 3,
        include_payload: true,
        poll_interval_ms: 100,
    });
    tail_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut tail = object_client
        .tail_append_stream(tail_req)
        .await
        .unwrap()
        .into_inner();
    let tailed = tokio::time::timeout(Duration::from_secs(2), tail.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .record
        .unwrap();
    assert_eq!(tailed.record_sequence, 3);
    assert_eq!(tailed.payload, b"third".to_vec());
}

#[tokio::test]
async fn test_grpc_object_metadata_round_trips_through_get_head_and_list() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "grpc-object-metadata-round-trips-through-get-hea")
            .await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = unique_test_name("metadata");
    let object_key = "catalog/item.json".to_string();
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let user_metadata = serde_json::json!({"queue": "outbound", "state": "pending"}).to_string();
    let metadata = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                mutation_context: Some(native_mutation_context(
                    &actor,
                    bucket_id,
                    "metadata-roundtrip",
                )),
                content_type: Some("application/json".to_string()),
                user_metadata_json: user_metadata.clone(),
                storage_class: None,
            },
        )),
    };
    let chunk = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Chunk(
            br#"{"ok":true}"#.to_vec(),
        )),
    };
    object_client
        .put_object(authorized(
            tokio_stream::iter(vec![metadata, chunk]),
            &token,
        ))
        .await
        .unwrap();

    let head = object_client
        .head_object(authorized(
            HeadObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                version_id: None,

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(head.content_type, "application/json");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&head.user_metadata_json).unwrap(),
        serde_json::from_str::<serde_json::Value>(&user_metadata).unwrap()
    );

    let listed = object_client
        .list_objects(authorized(
            ListObjectsRequest {
                bucket_name: bucket_name.clone(),
                prefix: "catalog/".to_string(),
                delimiter: String::new(),
                start_after: String::new(),
                max_keys: 10,

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .objects;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].content_type, "application/json");

    let mut stream = object_client
        .get_object(authorized(
            GetObjectRequest {
                bucket_name,
                object_key,
                version_id: None,
                range: None,

                ..Default::default()
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let metadata_frame = stream.next().await.unwrap().unwrap();
    let Some(anvil_api::get_object_response::Data::Metadata(info)) = metadata_frame.data else {
        panic!("first get_object frame was not metadata");
    };
    assert_eq!(info.content_type, "application/json");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&info.user_metadata_json).unwrap(),
        serde_json::from_str::<serde_json::Value>(&user_metadata).unwrap()
    );
}
