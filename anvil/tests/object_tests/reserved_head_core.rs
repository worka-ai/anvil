use super::*;

#[tokio::test]
async fn test_native_object_api_rejects_reserved_internal_namespaces() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-native-reserved-namespace".to_string();
    let visible_key = "visible/source.json".to_string();
    let reserved_key = "_anvil/authz/native-object-api".to_string();
    let reserved_prefix = "_anvil/authz/".to_string();

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

    let visible_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: visible_key.clone(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                br#"{"ok":true}"#.to_vec(),
            )),
        },
    ];
    let mut visible_put = Request::new(tokio_stream::iter(visible_chunks));
    visible_put.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(visible_put).await.unwrap();

    let reserved_put_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: reserved_key.clone(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"must not persist".to_vec(),
            )),
        },
    ];
    let mut reserved_put = Request::new(tokio_stream::iter(reserved_put_chunks));
    reserved_put.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    reserved_put.metadata_mut().insert(
        "x-anvil-internal-write-token",
        "caller-forged".parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.put_object(reserved_put).await);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        version_id: None,
        range: None,

        ..Default::default()
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.get_object(get_req).await);

    let mut head_req = Request::new(HeadObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        version_id: None,

        ..Default::default()
    });
    head_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.head_object(head_req).await);

    let mut delete_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        version_id: None,
        mutation_context: Some(native_mutation_context(bucket_id, "delete-object")),
    });
    delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.delete_object(delete_req).await);

    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: reserved_prefix.clone(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,

        ..Default::default()
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.list_objects(list_req).await);

    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: reserved_prefix.clone(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),

        ..Default::default()
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.list_object_versions(versions_req).await);

    for reserved_prefix in [
        "_anvil/meta/",
        "_anvil/index/",
        "_anvil/watch/",
        "_anvil/personaldb/",
        "_anvil/git/",
        "_anvil/tmp/",
    ] {
        let key = format!("{reserved_prefix}native-object-api");
        let reserved_put_chunks = vec![
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                    ObjectMetadata {
                        bucket_name: bucket_name.clone(),
                        object_key: key.clone(),
                        mutation_context: Some(native_mutation_context(
                            bucket_id,
                            "reserved-prefix-put",
                        )),
                        content_type: None,
                        user_metadata_json: String::new(),
                        storage_class: None,
                    },
                )),
            },
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    b"must not persist".to_vec(),
                )),
            },
        ];
        let mut reserved_put = Request::new(tokio_stream::iter(reserved_put_chunks));
        reserved_put.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        assert_reserved_namespace_status(object_client.put_object(reserved_put).await);

        let mut get_req = Request::new(GetObjectRequest {
            bucket_name: bucket_name.clone(),
            object_key: key.clone(),
            version_id: None,
            range: None,

            ..Default::default()
        });
        get_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        assert_reserved_namespace_status(object_client.get_object(get_req).await);

        let mut head_req = Request::new(HeadObjectRequest {
            bucket_name: bucket_name.clone(),
            object_key: key,
            version_id: None,

            ..Default::default()
        });
        head_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        assert_reserved_namespace_status(object_client.head_object(head_req).await);

        let mut list_req = Request::new(ListObjectsRequest {
            bucket_name: bucket_name.clone(),
            prefix: reserved_prefix.to_string(),
            delimiter: String::new(),
            start_after: String::new(),
            max_keys: 100,

            ..Default::default()
        });
        list_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        assert_reserved_namespace_status(object_client.list_objects(list_req).await);

        let mut watch_req = Request::new(WatchPrefixRequest {
            bucket_name: bucket_name.clone(),
            prefix: reserved_prefix.to_string(),
            after_cursor: 0,
        });
        watch_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        assert_reserved_namespace_status(object_client.watch_prefix(watch_req).await);
    }

    let mut copy_from_reserved = Request::new(CopyObjectRequest {
        source_bucket_name: bucket_name.clone(),
        source_object_key: reserved_key.clone(),
        source_version_id: None,
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: "visible/copied-from-reserved.json".to_string(),
        mutation_context: Some(native_mutation_context(bucket_id, "copy-object")),
    });
    copy_from_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.copy_object(copy_from_reserved).await);

    let mut copy_to_reserved = Request::new(CopyObjectRequest {
        source_bucket_name: bucket_name.clone(),
        source_object_key: visible_key.clone(),
        source_version_id: None,
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: reserved_key.clone(),
        mutation_context: Some(native_mutation_context(bucket_id, "copy-object")),
    });
    copy_to_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.copy_object(copy_to_reserved).await);

    let mut compose_to_reserved = Request::new(ComposeObjectRequest {
        sources: vec![ComposeObjectSource {
            bucket_name: bucket_name.clone(),
            object_key: visible_key.clone(),
            version_id: None,
        }],
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: reserved_key.clone(),
        mutation_context: Some(native_mutation_context(bucket_id, "compose-object")),
    });
    compose_to_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.compose_object(compose_to_reserved).await);

    let mut patch_reserved = Request::new(PatchJsonObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        base_version_id: None,
        merge_patch_json: r#"{"patched":true}"#.to_string(),
        mutation_context: Some(native_mutation_context(bucket_id, "patch-json-object")),
        precondition: None,
    });
    patch_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.patch_json_object(patch_reserved).await);

    let mut manifest_reserved = Request::new(CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: reserved_key.clone(),
        expected_revision: 0,
        manifest_json: "{}".to_string(),
        mutation_context: Some(native_mutation_context(
            bucket_id,
            "compare-and-swap-manifest",
        )),
        precondition: None,
    });
    manifest_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .compare_and_swap_manifest(manifest_reserved)
            .await,
    );

    let mut multipart_reserved = Request::new(InitiateMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        mutation_context: Some(native_mutation_context(bucket_id, "initiate-multipart")),
    });
    multipart_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .initiate_multipart_upload(multipart_reserved)
            .await,
    );

    let mut create_append_reserved = Request::new(CreateAppendStreamRequest {
        bucket_name: bucket_name.clone(),
        stream_key: reserved_key.clone(),
        mutation_context: Some(native_mutation_context(bucket_id, "create-append-stream")),
    });
    create_append_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .create_append_stream(create_append_reserved)
            .await,
    );

    let mut append_record_reserved = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: reserved_key.clone(),
        stream_id: uuid::Uuid::new_v4().to_string(),
        payload: b"reserved append payload".to_vec(),
        mutation_context: Some(native_mutation_context(bucket_id, "append-stream-record")),
        content_type: None,
        user_metadata_json: String::new(),
        precondition: None,
    });
    append_record_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .append_stream_record(append_record_reserved)
            .await,
    );

    let mut seal_append_reserved = Request::new(SealAppendStreamSegmentRequest {
        bucket_name: bucket_name.clone(),
        stream_key: reserved_key.clone(),
        stream_id: uuid::Uuid::new_v4().to_string(),
        mutation_context: Some(native_mutation_context(bucket_id, "seal-append-stream")),
        precondition: None,
    });
    seal_append_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .seal_append_stream_segment(seal_append_reserved)
            .await,
    );

    let mut watch_reserved = Request::new(WatchPrefixRequest {
        bucket_name,
        prefix: reserved_prefix,
        after_cursor: 0,
    });
    watch_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.watch_prefix(watch_reserved).await);

    let metrics = cluster.states[0].observability.snapshot();
    let reserved_rejections = metrics
        .iter()
        .filter(|(key, _)| {
            key.name == RESERVED_NAMESPACE_REJECTION_COUNT
                && key.labels.get("api").is_some_and(|value| value == "native")
        })
        .map(|(_, sample)| sample.count)
        .sum::<u64>();
    assert!(
        reserved_rejections >= 4,
        "native reserved namespace rejections should be counted"
    );
    for metric in [
        OBJECT_WRITE_LATENCY,
        OBJECT_READ_LATENCY,
        PREFIX_LIST_LATENCY,
    ] {
        assert!(
            metrics
                .iter()
                .any(|(key, sample)| key.name == metric && sample.count > 0),
            "expected {metric} to be observed during native object API calls"
        );
    }
}

#[tokio::test]
async fn test_head_object() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-head-bucket".to_string();
    let object_key = "test-head-object".to_string();
    let content = b"hello head";

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

    // 1. Put an object
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
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

    // 2. Head the object
    let mut head_req = Request::new(HeadObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,

        ..Default::default()
    });
    head_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let head_res = object_client
        .head_object(head_req)
        .await
        .unwrap()
        .into_inner();

    // 3. Assert metadata is correct
    assert_eq!(head_res.etag, put_res.etag);
    assert_eq!(head_res.size, content.len() as i64);
}

#[tokio::test]
async fn test_object_payloads_are_corestore_backed_and_readable() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-inline-payload-bucket".to_string();
    let inline_key = "inline-64k.bin".to_string();
    let external_key = "external-over-64k.bin".to_string();

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

    let inline_content = vec![7_u8; 64 * 1024];
    let inline_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: inline_key.clone(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                inline_content.clone(),
            )),
        },
    ];
    let mut inline_put_req = Request::new(tokio_stream::iter(inline_chunks));
    inline_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let inline_put = object_client
        .put_object(inline_put_req)
        .await
        .unwrap()
        .into_inner();

    let external_content = vec![9_u8; 128 * 1024 + 123];
    let mut external_chunks = vec![PutObjectRequest {
        data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.clone(),
                object_key: external_key.clone(),
                mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                content_type: None,
                user_metadata_json: String::new(),
                storage_class: None,
            },
        )),
    }];
    external_chunks.extend(
        external_content
            .chunks(1024 * 1024)
            .map(|chunk| PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    chunk.to_vec(),
                )),
            }),
    );
    let mut external_put_req = Request::new(tokio_stream::iter(external_chunks));
    external_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let external_put = object_client
        .put_object(external_put_req)
        .await
        .unwrap()
        .into_inner();

    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;
    let inline_object = cluster.states[0]
        .persistence
        .get_object_version(
            bucket_id,
            &inline_key,
            uuid::Uuid::parse_str(&inline_put.version_id).unwrap(),
        )
        .await
        .unwrap()
        .expect("inline object version should exist");
    let inline_shard_map = inline_object
        .shard_map
        .as_ref()
        .expect("inline object should record a CoreStore object data target");
    assert_eq!(
        inline_shard_map["schema"],
        "anvil.core.object_data_target.v1"
    );
    assert!(
        inline_shard_map["target"]
            .as_str()
            .is_some_and(|target| target.len() > 16),
        "inline object should store a canonical CoreStore object data target"
    );

    let external_object = cluster.states[0]
        .persistence
        .get_object_version(
            bucket_id,
            &external_key,
            uuid::Uuid::parse_str(&external_put.version_id).unwrap(),
        )
        .await
        .unwrap()
        .expect("external object version should exist");
    let external_shard_map = external_object
        .shard_map
        .as_ref()
        .expect("external object should record a CoreStore object data target");
    assert_eq!(
        external_shard_map["schema"],
        "anvil.core.object_data_target.v1"
    );
    assert!(
        external_shard_map["target"]
            .as_str()
            .is_some_and(|target| target.len() > 16),
        "external object should store a canonical CoreStore object data target"
    );

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: inline_key,
        version_id: Some(inline_put.version_id),
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
        if let anvil_api::get_object_response::Data::Chunk(bytes) = chunk.unwrap().data.unwrap() {
            downloaded.extend_from_slice(&bytes);
        }
    }
    assert_eq!(downloaded, inline_content);

    let mut external_get_req = Request::new(GetObjectRequest {
        bucket_name,
        object_key: external_key,
        version_id: Some(external_put.version_id),
        range: None,

        ..Default::default()
    });
    external_get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut external_stream = object_client
        .get_object(external_get_req)
        .await
        .unwrap()
        .into_inner();
    let mut external_downloaded = Vec::new();
    while let Some(chunk) = external_stream.next().await {
        if let anvil_api::get_object_response::Data::Chunk(bytes) = chunk.unwrap().data.unwrap() {
            external_downloaded.extend_from_slice(&bytes);
        }
    }
    assert_eq!(external_downloaded, external_content);
}

#[tokio::test]
async fn test_object_version_records_index_policy_snapshot_and_mutation_metadata() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "object-policy-snapshot-bucket".to_string();
    let object_key = "docs/policy-snapshot.txt".to_string();
    let content = b"policy snapshot content";

    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),

        options: None,
    });
    create_bucket.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_bucket)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let mut create_index = Request::new(CreateIndexRequest {
        bucket_name: bucket_name.clone(),
        name: "body-text".to_string(),
        kind: IndexKind::FullText as i32,
        selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
        extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
        authorization_mode: "inherit_object".to_string(),
        build_policy_json: serde_json::json!({"require_index_success": false}).to_string(),

        options: None,
    });
    create_index.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    index_client.create_index(create_index).await.unwrap();

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    let expected_policy_hash = cluster.states[0]
        .persistence
        .active_index_policy_snapshot_hash(claims.tenant_id, bucket.id)
        .await
        .unwrap();

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
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

    assert_eq!(put_res.index_policy_snapshot, expected_policy_hash);
    assert_eq!(put_res.payload_hash, put_res.etag);
    assert!(!put_res.mutation_id.is_empty());
    assert!(!put_res.record_hash.is_empty());

    let mut head_req = Request::new(HeadObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,

        ..Default::default()
    });
    head_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let head_res = object_client
        .head_object(head_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(head_res.version_id, put_res.version_id);
    assert_eq!(head_res.mutation_id, put_res.mutation_id);
    assert_eq!(head_res.record_hash, put_res.record_hash);
    assert_eq!(head_res.index_policy_snapshot, expected_policy_hash);
}
