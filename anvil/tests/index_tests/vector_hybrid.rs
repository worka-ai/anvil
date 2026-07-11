use super::*;

#[tokio::test]
async fn test_vector_index_builds_from_object_write_task() {
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
    let mut object_client = ObjectServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("vector-index-build-task-{}", uuid::Uuid::new_v4());
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "embedding".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_json_vector",
                    "caller_supplied",
                    "test-explicit-vector",
                    2,
                    "text",
                    "cosine",
                )
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/vector.json".to_string(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                br#"{"vector":[1.0,0.0],"source_start":4,"source_len":12}"#.to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    object_client.put_object(put_req).await.unwrap();

    let mut final_response = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let response = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket_name.clone(),
                    index_name: "embedding".to_string(),
                    query_text: String::new(),
                    query_vector: vec![1.0, 0.0],
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                    boundary_predicates_json: String::new(),
                    typed_predicates_json: String::new(),
                    typed_order_json: String::new(),
                    page_token: String::new(),
                    require_caught_up_to_watch_cursor: String::new(),
                    lag_timeout_ms: 0,
                },
                &token,
            ))
            .await;
        if let Ok(response) = response {
            let response = response.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/vector.json")
            {
                final_response = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let diagnostics = index_client
        .list_index_diagnostics(authorized(
            ListIndexDiagnosticsRequest {
                bucket_name: bucket_name.clone(),
                index_name: "embedding".to_string(),
                severity: String::new(),
                after_cursor: 0,
                limit: 100,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;
    let response = final_response.unwrap_or_else(|| {
        panic!("vector index build task should make object searchable; diagnostics={diagnostics:?}")
    });
    assert_eq!(response.index_kind, IndexKind::Vector as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/vector.json");
    assert_eq!(response.hits[0].vector_id, 1);
    let tasks = wait_for_index_build_task(&cluster, Duration::from_secs(60)).await;
    assert!(
        tasks.iter().any(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.status == anvil::tasks::TaskStatus::Completed
        }),
        "index build task should complete after searchable result; tasks={tasks:?}"
    );
    assert!(!tasks.iter().any(|task| {
        task.task_type == anvil::tasks::TaskType::IndexBuild
            && task.status == anvil::tasks::TaskStatus::Failed
    }));
}

#[tokio::test]
async fn test_vector_index_builds_required_media_modalities_from_object_write_tasks() {
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

    let bucket_name = format!("media-vector-index-{}", uuid::Uuid::new_v4());
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");

    let media_cases = [
        (
            "text",
            "text/plain",
            "media/text/notes.txt",
            b"plain text vector source".as_slice(),
        ),
        (
            "image",
            "image/png",
            "media/image/photo.bin",
            b"image bytes for deterministic embedding".as_slice(),
        ),
        (
            "audio",
            "audio/mpeg",
            "media/audio/clip.bin",
            b"audio bytes for deterministic embedding".as_slice(),
        ),
        (
            "video",
            "video/mp4",
            "media/video/movie.bin",
            b"video bytes for deterministic embedding".as_slice(),
        ),
    ];
    for (_modality, content_type, object_key, body) in media_cases {
        put_index_object_bytes(
            &cluster,
            claims.tenant_id,
            &bucket_name,
            object_key,
            Some(content_type),
            None,
            body,
        )
        .await;
    }

    for (modality, content_type, object_key, _body) in media_cases {
        let index_name = format!("{modality}-embedding");
        index_client
            .create_index(authorized(
                CreateIndexRequest {
                    bucket_name: bucket_name.clone(),
                    name: index_name.clone(),
                    kind: IndexKind::Vector as i32,
                    selector_json: serde_json::json!({
                        "prefix": format!("media/{modality}/"),
                        "content_type": content_type
                    })
                    .to_string(),
                    extractor_json: serde_json::json!({}).to_string(),
                    authorization_mode: "index_only".to_string(),
                    build_policy_json: rfc_vector_policy(
                        "object_body_utf8",
                        "test_only",
                        format!("test-{modality}-embedding"),
                        4,
                        modality,
                        "cosine",
                    )
                    .to_string(),

                    options: None,
                },
                &token,
            ))
            .await
            .unwrap();

        let response = wait_for_vector_hit(
            &mut index_client,
            &bucket_name,
            &index_name,
            object_key,
            vec![1.0, 0.0, 0.0, 0.0],
            &token,
        )
        .await;
        assert_eq!(response.index_kind, IndexKind::Vector as i32);
        assert_eq!(response.hits[0].object_key, object_key);
        let metadata: serde_json::Value =
            serde_json::from_str(&response.hits[0].metadata_json).unwrap();
        assert_eq!(metadata["modality"], modality);
    }

    let tasks = wait_for_index_build_task_count(&cluster, Duration::from_secs(60), 1).await;
    assert!(!tasks.iter().any(|task| {
        task.task_type == anvil::tasks::TaskType::IndexBuild
            && task.status == anvil::tasks::TaskStatus::Failed
    }));
}

#[tokio::test]
async fn test_vector_index_build_records_dimension_mismatch_diagnostic() {
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
    let mut object_client = ObjectServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("vector-diagnostic-task-{}", uuid::Uuid::new_v4());
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "embedding".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_json_vector",
                    "caller_supplied",
                    "test-explicit-vector",
                    3,
                    "text",
                    "cosine",
                )
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/bad-vector.json".to_string(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                br#"{"vector":[1.0,0.0]}"#.to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    object_client.put_object(put_req).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut found = false;
    while tokio::time::Instant::now() < deadline {
        let diagnostics = index_client
            .list_index_diagnostics(authorized(
                ListIndexDiagnosticsRequest {
                    bucket_name: bucket_name.clone(),
                    index_name: "embedding".to_string(),
                    severity: "error".to_string(),
                    after_cursor: 0,
                    limit: 10,
                },
                &token,
            ))
            .await
            .unwrap()
            .into_inner()
            .diagnostics;
        if diagnostics.iter().any(|diagnostic| {
            diagnostic.object_key == "docs/bad-vector.json"
                && diagnostic.code == "VectorDimensionMismatch"
        }) {
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(found, "dimension mismatch should write an index diagnostic");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let tasks = loop {
        let tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
        if tasks.iter().any(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.status == anvil::tasks::TaskStatus::Completed
        }) || tokio::time::Instant::now() >= deadline
        {
            break tasks;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        tasks.iter().any(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.status == anvil::tasks::TaskStatus::Completed
        }),
        "diagnostic should be followed by completed index build task"
    );
    assert!(!tasks.iter().any(|task| {
        task.task_type == anvil::tasks::TaskType::IndexBuild
            && task.status == anvil::tasks::TaskStatus::Failed
    }));
}

#[tokio::test]
async fn test_hybrid_index_builds_text_and_vector_segments_from_object_write_task() {
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
    let mut object_client = ObjectServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("hybrid-index-build-task-{}", uuid::Uuid::new_v4());
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();
    let mut vector_policy = rfc_vector_policy(
        "object_body_json_vector",
        "caller_supplied",
        "test-explicit-vector",
        2,
        "text",
        "cosine",
    );
    vector_policy["extractor"]["json_pointer"] = serde_json::json!("/embedding");
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body-and-vector".to_string(),
                kind: IndexKind::Hybrid as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({
                    "text": {
                        "source": "json_pointer",
                        "json_pointer": "/body"
                    }
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({
                    "full_text": {"positions": true},
                    "vector": vector_policy
                })
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let body = br#"{"body":"lease dashboard summary","embedding":[0.0,1.0]}"#.to_vec();
    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/hybrid.json".to_string(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                    storage_class: None,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(body)),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    object_client.put_object(put_req).await.unwrap();

    let mut final_response = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while tokio::time::Instant::now() < deadline {
        let response = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket_name.clone(),
                    index_name: "body-and-vector".to_string(),
                    query_text: "lease dashboard".to_string(),
                    query_vector: vec![0.0, 1.0],
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                    boundary_predicates_json: String::new(),
                    typed_predicates_json: String::new(),
                    typed_order_json: String::new(),
                    page_token: String::new(),
                    require_caught_up_to_watch_cursor: String::new(),
                    lag_timeout_ms: 0,
                },
                &token,
            ))
            .await;
        if let Ok(response) = response {
            let response = response.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/hybrid.json")
            {
                final_response = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let diagnostics = index_client
        .list_index_diagnostics(authorized(
            ListIndexDiagnosticsRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body-and-vector".to_string(),
                severity: String::new(),
                after_cursor: 0,
                limit: 100,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;
    let response = final_response.unwrap_or_else(|| {
        panic!("hybrid index build task should make object searchable; diagnostics={diagnostics:?}")
    });
    assert_eq!(response.index_kind, IndexKind::Hybrid as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/hybrid.json");
    assert!(response.hits[0].score > 0.0);
    let tasks = wait_for_index_build_task_count(&cluster, Duration::from_secs(60), 1).await;
    assert!(tasks.iter().any(|task| {
        task.task_type == anvil::tasks::TaskType::IndexBuild
            && task.status == anvil::tasks::TaskStatus::Completed
    }));
    assert!(!tasks.iter().any(|task| {
        task.task_type == anvil::tasks::TaskType::IndexBuild
            && task.status == anvil::tasks::TaskStatus::Failed
    }));
}

#[tokio::test]
async fn test_query_full_text_index_reads_latest_segment() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-query-full-text-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
                extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    let index_storage_id = anvil::index_journal::index_storage_id(
        claims.tenant_id,
        bucket.id,
        created.index_id as i64,
    );
    let indexed_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/alpha.txt",
        Some("text/plain"),
        None,
        b"alpha beta beta",
    )
    .await;
    let postings = build_full_text_postings(
        &[
            FullTextDocument {
                document_id: 11,
                field_id: 1,
                object_version_id: *indexed_object.version_id.as_bytes(),
                authz_label_hash: [1; 32],
                text: "alpha beta beta",
            },
            FullTextDocument {
                document_id: 22,
                field_id: 1,
                object_version_id: [22; 16],
                authz_label_hash: [2; 32],
                text: "gamma delta",
            },
        ],
        &Default::default(),
    );
    write_full_text_segment(
        &cluster.states[0].storage,
        FullTextSegmentWrite {
            index_id: &index_storage_id,
            generation: 7,
            tokenizer: serde_json::json!({}),
            scorer: serde_json::json!({"kind": "bm25"}),
            source_cursor: 44,
            authz_revision: 55,
            boundary_values: &[],
            built_postings: &postings,
            document_table: b"",
        },
    )
    .await
    .unwrap();

    let response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "body".to_string(),
                query_text: "alpha beta".to_string(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert_eq!(response.index_generation, 7);
    assert_eq!(response.authz_revision, 55);
    assert_eq!(response.hits.len(), 1);
    assert_eq!(response.hits[0].kind, IndexKind::FullText as i32);
    assert_eq!(response.hits[0].object_key, "docs/alpha.txt");
    assert_eq!(response.hits[0].document_id, 11);
    assert!(response.hits[0].score > 0.0);
}

#[tokio::test]
async fn test_query_full_text_phrase_requires_position_enabled_index() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-query-phrase-no-positions-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
                extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": false}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    let index_storage_id = anvil::index_journal::index_storage_id(
        claims.tenant_id,
        bucket.id,
        created.index_id as i64,
    );
    let postings = build_full_text_postings(
        &[FullTextDocument {
            document_id: 11,
            field_id: 1,
            object_version_id: [11; 16],
            authz_label_hash: [1; 32],
            text: "quick brown fox",
        }],
        &Default::default(),
    );
    write_full_text_segment(
        &cluster.states[0].storage,
        FullTextSegmentWrite {
            index_id: &index_storage_id,
            generation: 1,
            tokenizer: serde_json::json!({}),
            scorer: serde_json::json!({"kind": "bm25"}),
            source_cursor: 1,
            authz_revision: 1,
            boundary_values: &[],
            built_postings: &postings,
            document_table: b"",
        },
    )
    .await
    .unwrap();

    let status = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "body".to_string(),
                query_text: "quick brown".to_string(),
                query_vector: vec![],
                limit: 10,
                phrase: true,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .expect_err("phrase query should fail when index positions are disabled");

    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        status.message(),
        anvil::error_codes::AnvilErrorCode::IndexDoesNotSupportQuery.as_str()
    );
}

#[tokio::test]
async fn test_query_vector_index_reads_latest_segment() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-query-vector-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "embedding".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_utf8",
                    "test_only",
                    "test-embedding",
                    2,
                    "text",
                    "cosine",
                )
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    let index_storage_id = anvil::index_journal::index_storage_id(
        claims.tenant_id,
        bucket.id,
        created.index_id as i64,
    );
    let first_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/vector-a.txt",
        Some("text/plain"),
        None,
        b"vector a",
    )
    .await;
    let second_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/vector-b.txt",
        Some("text/plain"),
        None,
        b"vector b",
    )
    .await;
    let manual_generation = 10_000;
    let manual_authz_revision = 21;
    write_vector_segment(
        &cluster.states[0].storage,
        VectorSegmentWrite {
            index_id: &index_storage_id,
            definition_hash: "blake3:test-definition",
            generation: manual_generation,
            dimension: 2,
            metric: VectorMetric::Cosine,
            embedding_provider: "test_only",
            embedding_model_version: None,
            embedding_normalisation: "unit_l2",
            embedding_chunking_hash: "blake3:test-chunking",
            extractor_definition_hash: "blake3:test-extractor",
            embedding_provenance_hash: "blake3:test-provenance",
            embedding_model: "test-embedding",
            modality: VectorModality::Text,
            hnsw_m: 32,
            hnsw_ef_construction: 200,
            source_cursor: 20,
            authz_revision: manual_authz_revision,
            boundary_values: &[],
            entries: &[
                vector_entry(1, *first_object.version_id.as_bytes(), vec![1.0, 0.0]),
                vector_entry(2, *second_object.version_id.as_bytes(), vec![0.0, 1.0]),
            ],
            deleted_bitset: &[0],
        },
    )
    .await
    .unwrap();

    let first_page = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "embedding".to_string(),
                query_text: String::new(),
                query_vector: vec![1.0, 0.0],
                limit: 1,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(first_page.index_kind, IndexKind::Vector as i32);
    assert_eq!(first_page.index_generation, manual_generation);
    assert_eq!(first_page.authz_revision, manual_authz_revision);
    assert!(!first_page.next_page_token.is_empty());
    assert_eq!(
        first_page
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.object_key.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "docs/vector-a.txt")]
    );

    let second_page = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "embedding".to_string(),
                query_text: String::new(),
                query_vector: vec![1.0, 0.0],
                limit: 1,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: first_page.next_page_token.clone(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(second_page.next_page_token.is_empty());
    assert_eq!(
        second_page
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.object_key.as_str()))
            .collect::<Vec<_>>(),
        vec![(2, "docs/vector-b.txt")]
    );
}

#[tokio::test]
async fn test_query_hybrid_index_combines_full_text_and_vector_segments() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-query-hybrid-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let vector_policy = rfc_vector_policy(
        "object_body_utf8",
        "test_only",
        "test-embedding",
        2,
        "text",
        "cosine",
    );
    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body-and-vector".to_string(),
                kind: IndexKind::Hybrid as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({
                    "text": {"source": "object_body_utf8"}
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({
                    "full_text": {"positions": true},
                    "vector": vector_policy
                })
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created hybrid index");

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    let first_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/hybrid-a.txt",
        Some("text/plain"),
        Some(serde_json::json!({"tier": "gold", "kind": "note"})),
        b"alpha beta",
    )
    .await;
    let second_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/hybrid-b.txt",
        Some("text/plain"),
        Some(serde_json::json!({"tier": "silver", "kind": "note"})),
        b"gamma",
    )
    .await;
    let index_storage_id = anvil::index_journal::index_storage_id(
        claims.tenant_id,
        bucket.id,
        created.index_id as i64,
    );
    let postings = build_full_text_postings(
        &[
            FullTextDocument {
                document_id: 101,
                field_id: 1,
                object_version_id: *first_object.version_id.as_bytes(),
                authz_label_hash: [1; 32],
                text: "alpha beta",
            },
            FullTextDocument {
                document_id: 202,
                field_id: 1,
                object_version_id: *second_object.version_id.as_bytes(),
                authz_label_hash: [2; 32],
                text: "alpha gamma",
            },
        ],
        &Default::default(),
    );
    write_full_text_segment(
        &cluster.states[0].storage,
        FullTextSegmentWrite {
            index_id: &index_storage_id,
            generation: 5,
            tokenizer: serde_json::json!({}),
            scorer: serde_json::json!({"kind": "bm25"}),
            source_cursor: 30,
            authz_revision: 31,
            boundary_values: &[],
            built_postings: &postings,
            document_table: b"",
        },
    )
    .await
    .unwrap();
    write_vector_segment(
        &cluster.states[0].storage,
        VectorSegmentWrite {
            index_id: &index_storage_id,
            definition_hash: "blake3:test-definition",
            generation: 6,
            dimension: 2,
            metric: VectorMetric::Cosine,
            embedding_provider: "test_only",
            embedding_model_version: None,
            embedding_normalisation: "unit_l2",
            embedding_chunking_hash: "blake3:test-chunking",
            extractor_definition_hash: "blake3:test-extractor",
            embedding_provenance_hash: "blake3:test-provenance",
            embedding_model: "test-embedding",
            modality: VectorModality::Text,
            hnsw_m: 32,
            hnsw_ef_construction: 200,
            source_cursor: 31,
            authz_revision: 32,
            boundary_values: &[],
            entries: &[
                vector_entry(1, *first_object.version_id.as_bytes(), vec![1.0, 0.0]),
                vector_entry(2, *second_object.version_id.as_bytes(), vec![0.0, 1.0]),
            ],
            deleted_bitset: &[0],
        },
    )
    .await
    .unwrap();

    let response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body-and-vector".to_string(),
                query_text: "alpha".to_string(),
                query_vector: vec![1.0, 0.0],
                limit: 10,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.index_kind, IndexKind::Hybrid as i32);
    assert_eq!(response.index_generation, 6);
    assert_eq!(response.authz_revision, 32);
    assert_eq!(response.hits[0].kind, IndexKind::Hybrid as i32);
    assert_eq!(response.hits[0].object_key, "docs/hybrid-a.txt");
    assert_eq!(response.hits[0].document_id, 101);
    assert_eq!(response.hits[0].vector_id, 1);
    let recipe: serde_json::Value = serde_json::from_str(&response.scoring_recipe_json).unwrap();
    assert_eq!(recipe["kind"], "hybrid");
    assert!((recipe["text_weight"].as_f64().unwrap() - 0.55).abs() < 1e-6);
    assert!((recipe["vector_weight"].as_f64().unwrap() - 0.35).abs() < 1e-6);
    assert!((recipe["freshness_weight"].as_f64().unwrap() - 0.10).abs() < 1e-6);
    assert_eq!(recipe["index_generations"]["full_text"], 5);
    assert_eq!(recipe["index_generations"]["vector"], 6);
    assert_eq!(recipe["index_generations"]["max"], 6);
    let hit_metadata: serde_json::Value =
        serde_json::from_str(&response.hits[0].metadata_json).unwrap();
    assert_eq!(hit_metadata["normalized_text_score"].as_f64().unwrap(), 1.0);
    assert_eq!(
        hit_metadata["normalized_vector_score"].as_f64().unwrap(),
        1.0
    );
    assert!(hit_metadata["freshness_score"].as_f64().is_some());

    let filtered = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "body-and-vector".to_string(),
                query_text: "alpha".to_string(),
                query_vector: vec![0.0, 1.0],
                limit: 10,
                phrase: false,
                path_prefix: "docs/hybrid-a".to_string(),
                metadata_filters_json: serde_json::json!({"tier": "gold"}).to_string(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        filtered
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/hybrid-a.txt"]
    );
}

#[tokio::test]
async fn test_query_inherit_object_vector_filters_results_by_object_read_scope() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-query-inherit-vector-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "embedding".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_utf8",
                    "test_only",
                    "test-embedding",
                    2,
                    "text",
                    "cosine",
                )
                .to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    let allowed_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/vector-allowed.txt",
        Some("text/plain"),
        None,
        b"allowed",
    )
    .await;
    let denied_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/vector-denied.txt",
        Some("text/plain"),
        None,
        b"denied",
    )
    .await;
    let index_storage_id = anvil::index_journal::index_storage_id(
        claims.tenant_id,
        bucket.id,
        created.index_id as i64,
    );
    write_vector_segment(
        &cluster.states[0].storage,
        VectorSegmentWrite {
            index_id: &index_storage_id,
            definition_hash: "blake3:test-definition",
            generation: 100,
            dimension: 2,
            metric: VectorMetric::Cosine,
            embedding_provider: "test_only",
            embedding_model_version: None,
            embedding_normalisation: "unit_l2",
            embedding_chunking_hash: "blake3:test-chunking",
            extractor_definition_hash: "blake3:test-extractor",
            embedding_provenance_hash: "blake3:test-provenance",
            embedding_model: "test-embedding",
            modality: VectorModality::Text,
            hnsw_m: 32,
            hnsw_ef_construction: 200,
            source_cursor: 40,
            authz_revision: 41,
            boundary_values: &[],
            entries: &[
                vector_entry_with_authz_label(
                    1,
                    *allowed_object.version_id.as_bytes(),
                    vec![0.99, 0.0],
                    test_object_authz_label_hash(&bucket, &allowed_object),
                ),
                vector_entry_with_authz_label(
                    2,
                    *denied_object.version_id.as_bytes(),
                    vec![1.0, 0.0],
                    test_object_authz_label_hash(&bucket, &denied_object),
                ),
            ],
            deleted_bitset: &[0],
        },
    )
    .await
    .unwrap();

    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token("limited-vector-reader".to_string(), claims.tenant_id)
        .unwrap();
    grant_bucket_index_query_for_principal(&cluster, &bucket_name, "limited-vector-reader").await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "docs/vector-allowed.txt",
        "limited-vector-reader",
    )
    .await;
    let response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "embedding".to_string(),
                query_text: String::new(),
                query_vector: vec![1.0, 0.0],
                limit: 1,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &limited_token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(
        response
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.object_key.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "docs/vector-allowed.txt")]
    );
}

#[tokio::test]
async fn test_query_inherit_object_full_text_filters_results_by_object_read_scope() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-query-inherit-object-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
                extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),

                options: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    let allowed_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/allowed.txt",
        Some("text/plain"),
        None,
        b"alpha allowed",
    )
    .await;
    let denied_object = put_index_object_bytes(
        &cluster,
        claims.tenant_id,
        &bucket_name,
        "docs/denied.txt",
        Some("text/plain"),
        None,
        b"alpha denied",
    )
    .await;
    let index_storage_id = anvil::index_journal::index_storage_id(
        claims.tenant_id,
        bucket.id,
        created.index_id as i64,
    );
    let postings = build_full_text_postings(
        &[
            FullTextDocument {
                document_id: 1,
                field_id: 1,
                object_version_id: *allowed_object.version_id.as_bytes(),
                authz_label_hash: test_object_authz_label_hash(&bucket, &allowed_object),
                text: "alpha allowed",
            },
            FullTextDocument {
                document_id: 2,
                field_id: 1,
                object_version_id: *denied_object.version_id.as_bytes(),
                authz_label_hash: test_object_authz_label_hash(&bucket, &denied_object),
                text: "alpha alpha alpha denied",
            },
            FullTextDocument {
                document_id: 3,
                field_id: 1,
                object_version_id: [9; 16],
                authz_label_hash: [3; 32],
                text: "alpha missing metadata",
            },
        ],
        &Default::default(),
    );
    write_full_text_segment(
        &cluster.states[0].storage,
        FullTextSegmentWrite {
            index_id: &index_storage_id,
            generation: 100,
            tokenizer: serde_json::json!({}),
            scorer: serde_json::json!({"kind": "bm25"}),
            source_cursor: 3,
            authz_revision: 4,
            boundary_values: &[],
            built_postings: &postings,
            document_table: b"",
        },
    )
    .await
    .unwrap();

    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token("limited-index-reader".to_string(), claims.tenant_id)
        .unwrap();
    grant_bucket_index_query_for_principal(&cluster, &bucket_name, "limited-index-reader").await;
    grant_tenant_object_reader_for_principal(
        &cluster,
        &bucket_name,
        "docs/allowed.txt",
        "limited-index-reader",
    )
    .await;
    let response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body".to_string(),
                query_text: "alpha".to_string(),
                query_vector: vec![],
                limit: 1,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
                boundary_predicates_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: String::new(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &limited_token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.hits.len(), 1);
    assert_eq!(response.hits[0].object_key, "docs/allowed.txt");
    assert_eq!(response.hits[0].document_id, 1);

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    auth_client
        .write_authz_tuple(authorized(
            WriteAuthzTupleRequest {
                namespace: "object".to_string(),
                object_id: format!("{bucket_name}/docs/denied.txt"),
                relation: "reader".to_string(),
                subject_kind: "app".to_string(),
                subject_id: "tuple-index-reader".to_string(),
                caveat_hash: "".to_string(),
                operation: "add".to_string(),
                reason: "index query inherited object authz test".to_string(),
                scope: None,
            },
            &cluster.token,
        ))
        .await
        .unwrap();
    let tuple_token = cluster.states[0]
        .jwt_manager
        .mint_token("tuple-index-reader".to_string(), claims.tenant_id)
        .unwrap();
    grant_bucket_index_query_for_principal(&cluster, &bucket_name, "tuple-index-reader").await;
    let tuple_response = query_index_until_hits(
        &mut index_client,
        &tuple_token,
        QueryIndexRequest {
            bucket_name,
            index_name: "body".to_string(),
            query_text: "alpha".to_string(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: String::new(),
            metadata_filters_json: String::new(),
            boundary_predicates_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        1,
        Duration::from_secs(60),
    )
    .await;

    assert_eq!(tuple_response.hits.len(), 1);
    assert_eq!(tuple_response.hits[0].object_key, "docs/denied.txt");
    assert_eq!(tuple_response.hits[0].document_id, 2);
}
