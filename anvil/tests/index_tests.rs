use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    CreateBucketRequest, CreateIndexRequest, DisableIndexRequest, DropIndexRequest,
    ListIndexDiagnosticsRequest, ListIndexesRequest, ObjectMetadata, PutObjectRequest,
    QueryIndexRequest, UpdateIndexRequest, WatchIndexDefinitionRequest, WatchIndexPartitionRequest,
    WriteAuthzTupleRequest,
};
use anvil::formats::full_text::{FullTextDocument, build_full_text_postings};
use anvil::formats::vector::{VectorMetric, VectorModality, VectorPayload, VectorRecord};
use anvil::full_text_segment::{FullTextSegmentWrite, write_full_text_segment};
use anvil::search_query::{FullTextSegmentQuery, query_full_text_segment};
use anvil::vector_segment::{VectorSegmentEntry, VectorSegmentWrite, write_vector_segment};
use anvil_test_utils::*;
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

#[tokio::test]
async fn test_index_definition_lifecycle() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-definition-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
                kind: "full_text".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"fields": [{"path": "body", "source": "utf8"}]})
                    .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true, "language": "simple"})
                    .to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    assert_eq!(created.bucket_name, bucket_name);
    assert_eq!(created.name, "docs-full-text");
    assert_eq!(created.kind, "full_text");
    assert_eq!(created.authorization_mode, "inherit_object");
    assert!(created.enabled);
    assert_eq!(created.version, 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&created.selector_json).unwrap()["prefix"],
        "docs/"
    );

    let listed = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "docs-full-text");

    let updated = index_client
        .update_index(authorized(
            UpdateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/v2/"}).to_string(),
                extractor_json:
                    serde_json::json!({"fields": [{"path": "summary", "source": "utf8"}]})
                        .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": false, "language": "simple"})
                    .to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("updated index");
    assert_eq!(updated.authorization_mode, "index_only");
    assert_eq!(updated.version, 2);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&updated.selector_json).unwrap()["prefix"],
        "docs/v2/"
    );

    let disabled = index_client
        .disable_index(authorized(
            DisableIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("disabled index");
    assert!(!disabled.enabled);
    assert_eq!(disabled.version, 3);

    let active_only = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert!(active_only.is_empty());

    let with_disabled = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert_eq!(with_disabled.len(), 1);
    assert!(!with_disabled[0].enabled);

    index_client
        .drop_index(authorized(
            DropIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let after_drop = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name,
                include_disabled: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert!(after_drop.is_empty());

    let mut watch = index_client
        .watch_index_definition(authorized(
            WatchIndexDefinitionRequest {
                bucket_name: "index-definition-bucket".to_string(),
                after_cursor: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut events = Vec::new();
    for _ in 0..4 {
        events.push(watch.next().await.unwrap().unwrap());
    }
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["create", "update", "disable", "drop"]
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].cursor < pair[1].cursor)
    );
    assert_eq!(events[3].index.as_ref().unwrap().name, "docs-full-text");
}

#[tokio::test]
async fn test_full_text_index_builds_from_object_write_task() {
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

    let bucket_name = format!("index-build-task-{}", uuid::Uuid::new_v4());
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body".to_string(),
                kind: "full_text".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/alpha.txt".to_string(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"alpha beta automatic index build".to_vec(),
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
                    index_name: "body".to_string(),
                    query_text: "automatic index".to_string(),
                    query_vector: vec![],
                    limit: 10,
                    phrase: false,
                },
                &token,
            ))
            .await;
        if let Ok(response) = response {
            let response = response.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/alpha.txt")
            {
                final_response = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response =
        final_response.expect("full text index build task should make object searchable");
    assert_eq!(response.index_kind, "full_text");
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/alpha.txt");

    let mut watch = index_client
        .watch_index_partition(authorized(
            WatchIndexPartitionRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body".to_string(),
                partition_id: String::new(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let watch_event = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("index partition watch should yield a built segment event")
        .expect("index partition watch stream should stay open")
        .expect("index partition watch event should be successful");
    assert_eq!(watch_event.bucket_name, bucket_name);
    assert_eq!(watch_event.index_name, "body");
    assert_eq!(watch_event.event_type, "segment_built");
    assert_eq!(watch_event.index_kind, "full_text");
    assert_eq!(watch_event.generation, response.index_generation);
    assert!(!watch_event.index_storage_id.is_empty());
    assert!(!watch_event.partition_id.is_empty());
    assert!(!watch_event.source_manifest_hash.is_empty());
    assert!(!watch_event.proof_hash.is_empty());
    assert!(!watch_event.segment_hashes.is_empty());

    let tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
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
async fn test_full_text_index_build_extracts_json_pointer_from_object_write_task() {
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

    let bucket_name = format!("json-pointer-index-build-{}", uuid::Uuid::new_v4());
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "summary".to_string(),
                kind: "full_text".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "json_pointer",
                    "pointer": "/summary"
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/report.json".to_string(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                br#"{"summary":"quarterly tenant retention analysis","body":"ignored"}"#.to_vec(),
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
                    index_name: "summary".to_string(),
                    query_text: "tenant retention".to_string(),
                    query_vector: vec![],
                    limit: 10,
                    phrase: false,
                },
                &token,
            ))
            .await;
        if let Ok(response) = response {
            let response = response.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/report.json")
            {
                final_response = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response =
        final_response.expect("json_pointer text extraction should make object searchable");
    assert_eq!(response.index_kind, "full_text");
    assert_eq!(response.hits[0].object_key, "docs/report.json");
    assert!(response.hits[0].score > 0.0);
}

#[tokio::test]
async fn test_full_text_index_build_uses_source_cursor_snapshot() {
    let cluster = TestCluster::new(&["test-region-1"]).await;
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = format!("cursor-snapshot-{}", uuid::Uuid::new_v4());
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    let index = persistence
        .create_index_definition(
            tenant_id,
            bucket.id,
            "body",
            "full_text",
            serde_json::json!({"prefix": "docs/"}),
            serde_json::json!({"source": "object_body_utf8"}),
            "index_only",
            serde_json::json!({"positions": true}),
        )
        .await
        .unwrap();
    persistence
        .create_index_definition_event(tenant_id, bucket.id, &bucket.name, &index, "create")
        .await
        .unwrap();

    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "docs/alpha.txt",
            &hex::encode([1; 32]),
            20,
            "etag-alpha",
            Some("text/plain"),
            None,
            None,
            Some(b"alpha cursor visible".to_vec()),
        )
        .await
        .unwrap();
    let source_cursor = persistence
        .list_tasks()
        .await
        .unwrap()
        .into_iter()
        .find(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.payload["index_id"] == serde_json::json!(index.id)
        })
        .and_then(|task| task.payload["source_cursor"].as_u64())
        .expect("first index build task records source cursor");

    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "docs/future.txt",
            &hex::encode([2; 32]),
            27,
            "etag-future",
            Some("text/plain"),
            None,
            None,
            Some(b"future object must wait".to_vec()),
        )
        .await
        .unwrap();

    persistence
        .build_index_task(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(source_cursor),
        )
        .await
        .unwrap()
        .expect("index build succeeds");

    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let segment = anvil::full_text_segment::read_latest_full_text_segment(
        &cluster.states[0].storage,
        &index_storage_id,
    )
    .await
    .unwrap()
    .expect("full text segment exists");
    assert_eq!(segment.header.source_cursor, source_cursor);

    let definition = anvil::formats::full_text::FullTextIndexDefinition::from_json(
        &serde_json::json!({"positions": true}),
    )
    .unwrap();
    let alpha_hits = query_full_text_segment(
        &segment,
        FullTextSegmentQuery {
            query: "alpha",
            tokenizer: &definition.tokenizer,
            positions_enabled: definition.positions_enabled,
            phrase: false,
            bm25: anvil::formats::full_text::Bm25Config::default(),
            authorized_labels: None,
            limit: 10,
        },
    )
    .unwrap();
    let future_hits = query_full_text_segment(
        &segment,
        FullTextSegmentQuery {
            query: "future",
            tokenizer: &definition.tokenizer,
            positions_enabled: definition.positions_enabled,
            phrase: false,
            bm25: anvil::formats::full_text::Bm25Config::default(),
            authorized_labels: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(!alpha_hits.is_empty());
    assert!(future_hits.is_empty());

    let document_table: serde_json::Value =
        serde_json::from_slice(&segment.document_table).unwrap();
    assert!(document_table.to_string().contains("docs/alpha.txt"));
    assert!(!document_table.to_string().contains("docs/future.txt"));
}

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
                kind: "vector".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_json_vector"})
                    .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({
                    "dimension": 2,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "test-explicit-vector",
                    "chunking": {"kind": "whole_object"}
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/vector.json".to_string(),
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

    let response = final_response.expect("vector index build task should make object searchable");
    assert_eq!(response.index_kind, "vector");
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/vector.json");
    assert_eq!(response.hits[0].vector_id, 1);
    let tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
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
    for (modality, content_type, object_key, body) in media_cases {
        cluster.states[0]
            .persistence
            .create_object(
                claims.tenant_id,
                bucket.id,
                object_key,
                &hex::encode(anvil::formats::hash32(body)),
                i64::try_from(body.len()).unwrap(),
                &format!("etag-{modality}"),
                Some(content_type),
                None,
                None,
                Some(body.to_vec()),
            )
            .await
            .unwrap();
    }

    for (modality, content_type, object_key, _body) in media_cases {
        let index_name = format!("{modality}-embedding");
        index_client
            .create_index(authorized(
                CreateIndexRequest {
                    bucket_name: bucket_name.clone(),
                    name: index_name.clone(),
                    kind: "vector".to_string(),
                    selector_json: serde_json::json!({
                        "prefix": format!("media/{modality}/"),
                        "content_type": content_type
                    })
                    .to_string(),
                    extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                    authorization_mode: "index_only".to_string(),
                    build_policy_json: serde_json::json!({
                        "dimension": 4,
                        "metric": "cosine",
                        "modality": modality,
                        "embedding_model": format!("test-{modality}-embedding"),
                        "chunking": {"kind": "whole_object"}
                    })
                    .to_string(),
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
        assert_eq!(response.index_kind, "vector");
        assert_eq!(response.hits[0].object_key, object_key);
        let metadata: serde_json::Value =
            serde_json::from_str(&response.hits[0].metadata_json).unwrap();
        assert_eq!(metadata["modality"], modality);
    }

    let tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
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
                kind: "vector".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_json_vector"})
                    .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({
                    "dimension": 3,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "test-explicit-vector",
                    "chunking": {"kind": "whole_object"}
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/bad-vector.json".to_string(),
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
    let tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
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
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body-and-vector".to_string(),
                kind: "hybrid".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({
                    "text": {"source": "object_body_utf8"},
                    "vector": {
                        "source": "object_body_json_vector",
                        "json_pointer": "/embedding"
                    }
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({
                    "full_text": {"positions": true},
                    "vector": {
                        "dimension": 2,
                        "metric": "cosine",
                        "modality": "text",
                        "embedding_model": "test-explicit-vector",
                        "chunking": {"kind": "whole_object"}
                    }
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let body = br#"{"body":"lease dashboard summary","embedding":[0.0,1.0]}"#.to_vec();
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: "docs/hybrid.json".to_string(),
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
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

    let response = final_response.expect("hybrid index build task should make object searchable");
    assert_eq!(response.index_kind, "hybrid");
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/hybrid.json");
    assert!(response.hits[0].score > 0.0);
    let tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
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
                kind: "full_text".to_string(),
                selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
                extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
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
    let indexed_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/alpha.txt",
            &hex::encode([1; 32]),
            15,
            "etag-alpha",
            Some("text/plain"),
            None,
            None,
            Some(b"alpha beta beta".to_vec()),
        )
        .await
        .unwrap();
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
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.index_kind, "full_text");
    assert_eq!(response.index_generation, 7);
    assert_eq!(response.authz_revision, 55);
    assert_eq!(response.hits.len(), 1);
    assert_eq!(response.hits[0].kind, "full_text");
    assert_eq!(response.hits[0].object_key, "docs/alpha.txt");
    assert_eq!(response.hits[0].document_id, 11);
    assert!(response.hits[0].score > 0.0);
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
                kind: "vector".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({
                    "dimension": 2,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "test-embedding",
                    "chunking": {"kind": "whole_object"}
                })
                .to_string(),
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
    let first_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/vector-a.txt",
            &hex::encode([2; 32]),
            8,
            "etag-vector-a",
            Some("text/plain"),
            None,
            None,
            Some(b"vector a".to_vec()),
        )
        .await
        .unwrap();
    let second_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/vector-b.txt",
            &hex::encode([3; 32]),
            8,
            "etag-vector-b",
            Some("text/plain"),
            None,
            None,
            Some(b"vector b".to_vec()),
        )
        .await
        .unwrap();
    write_vector_segment(
        &cluster.states[0].storage,
        VectorSegmentWrite {
            index_id: &index_storage_id,
            generation: 3,
            dimension: 2,
            metric: VectorMetric::Cosine,
            embedding_model: "test-embedding",
            modality: VectorModality::Text,
            hnsw_m: 32,
            hnsw_ef_construction: 200,
            source_cursor: 20,
            authz_revision: 21,
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
                bucket_name,
                index_name: "embedding".to_string(),
                query_text: String::new(),
                query_vector: vec![1.0, 0.0],
                limit: 2,
                phrase: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.index_kind, "vector");
    assert_eq!(response.index_generation, 3);
    assert_eq!(response.authz_revision, 21);
    assert_eq!(
        response
            .hits
            .iter()
            .map(|hit| (hit.vector_id, hit.object_key.as_str()))
            .collect::<Vec<_>>(),
        vec![(1, "docs/vector-a.txt"), (2, "docs/vector-b.txt")]
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
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body-and-vector".to_string(),
                kind: "hybrid".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({
                    "text": {"source": "object_body_utf8"},
                    "vector": {"source": "object_body_utf8"}
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({
                    "full_text": {"positions": true},
                    "vector": {
                        "dimension": 2,
                        "metric": "cosine",
                        "modality": "text",
                        "embedding_model": "test-embedding",
                        "chunking": {"kind": "whole_object"}
                    }
                })
                .to_string(),
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
    let first_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/hybrid-a.txt",
            &hex::encode([8; 32]),
            8,
            "etag-hybrid-a",
            Some("text/plain"),
            None,
            None,
            Some(b"alpha beta".to_vec()),
        )
        .await
        .unwrap();
    let second_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/hybrid-b.txt",
            &hex::encode([9; 32]),
            8,
            "etag-hybrid-b",
            Some("text/plain"),
            None,
            None,
            Some(b"gamma".to_vec()),
        )
        .await
        .unwrap();
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
                text: "gamma",
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
            generation: 6,
            dimension: 2,
            metric: VectorMetric::Cosine,
            embedding_model: "test-embedding",
            modality: VectorModality::Text,
            hnsw_m: 32,
            hnsw_ef_construction: 200,
            source_cursor: 31,
            authz_revision: 32,
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
                bucket_name,
                index_name: "body-and-vector".to_string(),
                query_text: "alpha".to_string(),
                query_vector: vec![1.0, 0.0],
                limit: 10,
                phrase: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(response.index_kind, "hybrid");
    assert_eq!(response.index_generation, 6);
    assert_eq!(response.authz_revision, 32);
    assert_eq!(response.hits[0].kind, "hybrid");
    assert_eq!(response.hits[0].object_key, "docs/hybrid-a.txt");
    assert_eq!(response.hits[0].document_id, 101);
    assert_eq!(response.hits[0].vector_id, 1);
    let recipe: serde_json::Value = serde_json::from_str(&response.scoring_recipe_json).unwrap();
    assert_eq!(recipe["kind"], "hybrid");
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
                kind: "vector".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "dimension": 2,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "test-embedding",
                    "chunking": {"kind": "whole_object"}
                })
                .to_string(),
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
    let allowed_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/vector-allowed.txt",
            &hex::encode([6; 32]),
            8,
            "etag-vector-allowed",
            Some("text/plain"),
            None,
            None,
            Some(b"allowed".to_vec()),
        )
        .await
        .unwrap();
    let denied_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/vector-denied.txt",
            &hex::encode([7; 32]),
            7,
            "etag-vector-denied",
            Some("text/plain"),
            None,
            None,
            Some(b"denied".to_vec()),
        )
        .await
        .unwrap();
    let index_storage_id = anvil::index_journal::index_storage_id(
        claims.tenant_id,
        bucket.id,
        created.index_id as i64,
    );
    write_vector_segment(
        &cluster.states[0].storage,
        VectorSegmentWrite {
            index_id: &index_storage_id,
            generation: 4,
            dimension: 2,
            metric: VectorMetric::Cosine,
            embedding_model: "test-embedding",
            modality: VectorModality::Text,
            hnsw_m: 32,
            hnsw_ef_construction: 200,
            source_cursor: 40,
            authz_revision: 41,
            entries: &[
                vector_entry(1, *allowed_object.version_id.as_bytes(), vec![1.0, 0.0]),
                vector_entry(2, *denied_object.version_id.as_bytes(), vec![0.0, 1.0]),
            ],
            deleted_bitset: &[0],
        },
    )
    .await
    .unwrap();

    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "limited-vector-reader".to_string(),
            vec![
                format!("index:read|{bucket_name}"),
                format!("object:read|{bucket_name}/docs/vector-allowed.txt"),
            ],
            claims.tenant_id,
        )
        .unwrap();
    let response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "embedding".to_string(),
                query_text: String::new(),
                query_vector: vec![1.0, 0.0],
                limit: 2,
                phrase: false,
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
                kind: "full_text".to_string(),
                selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
                extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
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
    let allowed_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/allowed.txt",
            &hex::encode([4; 32]),
            15,
            "etag-allowed",
            Some("text/plain"),
            None,
            None,
            Some(b"alpha allowed".to_vec()),
        )
        .await
        .unwrap();
    let denied_object = cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            "docs/denied.txt",
            &hex::encode([5; 32]),
            14,
            "etag-denied",
            Some("text/plain"),
            None,
            None,
            Some(b"alpha denied".to_vec()),
        )
        .await
        .unwrap();
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
                authz_label_hash: [1; 32],
                text: "alpha allowed",
            },
            FullTextDocument {
                document_id: 2,
                field_id: 1,
                object_version_id: *denied_object.version_id.as_bytes(),
                authz_label_hash: [2; 32],
                text: "alpha denied",
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
            generation: 2,
            tokenizer: serde_json::json!({}),
            scorer: serde_json::json!({"kind": "bm25"}),
            source_cursor: 3,
            authz_revision: 4,
            built_postings: &postings,
            document_table: b"",
        },
    )
    .await
    .unwrap();

    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "limited-index-reader".to_string(),
            vec![
                format!("index:read|{bucket_name}"),
                format!("object:read|{bucket_name}/docs/allowed.txt"),
            ],
            claims.tenant_id,
        )
        .unwrap();
    let response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body".to_string(),
                query_text: "alpha".to_string(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
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
            },
            &cluster.token,
        ))
        .await
        .unwrap();
    let tuple_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "tuple-index-reader".to_string(),
            vec![format!("index:read|{bucket_name}")],
            claims.tenant_id,
        )
        .unwrap();
    let tuple_response = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "body".to_string(),
                query_text: "alpha".to_string(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
            },
            &tuple_token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(tuple_response.hits.len(), 1);
    assert_eq!(tuple_response.hits[0].object_key, "docs/denied.txt");
    assert_eq!(tuple_response.hits[0].document_id, 2);
}

#[tokio::test]
async fn test_index_definition_rejects_invalid_policy_shape() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-validation-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let invalid_kind = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-kind".to_string(),
                kind: "unsupported".to_string(),
                selector_json: "{}".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: "{}".to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_kind.code(), tonic::Code::InvalidArgument);

    let invalid_full_text_policy = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-full-text-policy".to_string(),
                kind: "full_text".to_string(),
                selector_json: "{}".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"max_token_chars": 129}).to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        invalid_full_text_policy.code(),
        tonic::Code::InvalidArgument
    );

    let valid_vector_policy = serde_json::json!({
        "dimension": 768,
        "metric": "cosine",
        "modality": "text",
        "embedding_model": "text-embedding-v1",
        "chunking": {
            "kind": "tokens",
            "max_tokens": 512,
            "overlap_tokens": 64
        }
    });
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "valid-vector".to_string(),
                kind: "vector".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: valid_vector_policy.to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let invalid_vector_policy = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-vector".to_string(),
                kind: "vector".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "dimension": 0,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "text-embedding-v1",
                    "chunking": {}
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_vector_policy.code(), tonic::Code::InvalidArgument);

    let invalid_vector_update = index_client
        .update_index(authorized(
            UpdateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "valid-vector".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/v2/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "dimension": 768,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "",
                    "chunking": {}
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_vector_update.code(), tonic::Code::InvalidArgument);

    let invalid_json = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name,
                name: "invalid-json".to_string(),
                kind: "full_text".to_string(),
                selector_json: "{".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: "{}".to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_list_index_diagnostics_filters_by_index_and_severity() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-diagnostics-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "body-text".to_string(),
                kind: "full_text".to_string(),
                selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
                extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"require_index_success": false}).to_string(),
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
    cluster.states[0]
        .persistence
        .create_index_diagnostic(
            claims.tenant_id,
            bucket.id,
            &bucket.name,
            Some(created.index_id as i64),
            "body-text",
            "docs/bad.txt",
            None,
            "warning",
            "ExtractionFailed",
            "object body was not valid UTF-8",
            serde_json::json!({"selector": "object_body_utf8"}),
        )
        .await
        .unwrap();
    cluster.states[0]
        .persistence
        .create_index_diagnostic(
            claims.tenant_id,
            bucket.id,
            &bucket.name,
            Some(created.index_id as i64),
            "body-text",
            "docs/too-large.txt",
            None,
            "error",
            "PayloadTooLarge",
            "payload exceeded extraction limit",
            serde_json::json!({"limit_bytes": 1048576}),
        )
        .await
        .unwrap();

    let warnings = index_client
        .list_index_diagnostics(authorized(
            ListIndexDiagnosticsRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body-text".to_string(),
                after_cursor: 0,
                limit: 100,
                severity: "warning".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;

    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].object_key, "docs/bad.txt");
    assert_eq!(warnings[0].code, "ExtractionFailed");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&warnings[0].details_json).unwrap()["selector"],
        "object_body_utf8"
    );

    let all = index_client
        .list_index_diagnostics(authorized(
            ListIndexDiagnosticsRequest {
                bucket_name,
                index_name: String::new(),
                after_cursor: warnings[0].cursor,
                limit: 100,
                severity: String::new(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .diagnostics;
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].severity, "error");
}

async fn wait_for_vector_hit(
    index_client: &mut IndexServiceClient<tonic::transport::Channel>,
    bucket_name: &str,
    index_name: &str,
    object_key: &str,
    query_vector: Vec<f32>,
    token: &str,
) -> anvil::anvil_api::QueryIndexResponse {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let response = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket_name.to_string(),
                    index_name: index_name.to_string(),
                    query_text: String::new(),
                    query_vector: query_vector.clone(),
                    limit: 10,
                    phrase: false,
                },
                token,
            ))
            .await;
        if let Ok(response) = response {
            let response = response.into_inner();
            if response.hits.iter().any(|hit| hit.object_key == object_key) {
                return response;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    panic!("vector index `{index_name}` did not return `{object_key}` before timeout");
}

fn vector_entry(
    vector_id: u64,
    object_version_id: [u8; 16],
    values: Vec<f32>,
) -> VectorSegmentEntry {
    VectorSegmentEntry {
        record: VectorRecord {
            vector_id,
            object_version_id,
            chunk_id: vector_id as u32,
            modality: VectorModality::Text as u8,
            metric: VectorMetric::Cosine as u8,
            dimension: 2,
            vector_payload_offset: 0,
            source_start: vector_id * 10,
            source_len: 10,
            authz_label_hash: [1; 32],
            metadata_filter_bits: 0,
        },
        payload: VectorPayload {
            dimension: 2,
            values,
        },
    }
}
