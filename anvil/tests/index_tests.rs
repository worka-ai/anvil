use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use anvil::anvil_api::{
    self, AppendStreamRecordRequest, CreateAppendStreamRequest, CreateBucketRequest,
    CreateIndexRequest, DisableIndexRequest, DropIndexRequest, IndexKind,
    ListIndexDiagnosticsRequest, ListIndexesRequest, ListRepairFindingsRequest,
    NativeMutationContext, ObjectMetadata, PutObjectRequest, QueryIndexRequest, QueryIndexResponse,
    QuerySpecRequest, RepairIndexRequest, UpdateIndexRequest, WatchIndexDefinitionRequest,
    WatchIndexPartitionRequest, WriteAuthzTupleRequest,
};
use anvil::authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace};
use anvil::authz_userset_index::{DEFAULT_DERIVED_USERSET_INDEX_ID, read_derived_userset_index};
use anvil::formats::full_text::{FullTextDocument, build_full_text_postings};
use anvil::formats::vector::{
    VECTOR_INDEX_SCHEMA, VectorMetric, VectorModality, VectorPayload, VectorRecord,
};
use anvil::full_text_segment::{FullTextSegmentWrite, write_full_text_segment};
use anvil::partition_fence::{
    AcquireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal, OwnershipResource,
    OwnershipResourceKind, acquire_ownership,
};
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

fn native_mutation_context(bucket_id: i64, tag: &str) -> NativeMutationContext {
    let nonce = uuid::Uuid::new_v4();
    NativeMutationContext {
        tenant_id: 1,
        bucket_id,
        principal: "test-app".to_string(),
        request_id: format!("{tag}-{nonce}-request"),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: format!("{tag}-{nonce}-idempotency"),
    }
}

fn rfc_vector_policy(
    extractor_kind: &str,
    provider: &str,
    model: impl Into<String>,
    dimension: u16,
    modality: &str,
    metric: &str,
) -> serde_json::Value {
    serde_json::json!({
        "schema": VECTOR_INDEX_SCHEMA,
        "source": {"kind": "object_current"},
        "extractor": {"kind": extractor_kind},
        "embedding": {
            "provider": provider,
            "model": model.into(),
            "dimension": dimension,
            "modality": modality,
            "normalisation": "unit_l2",
            "chunking": {"strategy": "whole_object"}
        },
        "ann": {
            "algorithm": "hnsw",
            "metric": metric
        }
    })
}

async fn wait_for_index_build_task(
    cluster: &TestCluster,
    timeout: Duration,
) -> Vec<anvil::persistence::TaskRecord> {
    wait_for_index_build_task_count(cluster, timeout, 1).await
}

async fn wait_for_index_build_task_count(
    cluster: &TestCluster,
    timeout: Duration,
    expected_completed: usize,
) -> Vec<anvil::persistence::TaskRecord> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut tasks = Vec::new();
    while tokio::time::Instant::now() < deadline {
        tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
        let completed = tasks
            .iter()
            .filter(|task| {
                task.task_type == anvil::tasks::TaskType::IndexBuild
                    && task.status == anvil::tasks::TaskStatus::Completed
            })
            .count();
        if completed >= expected_completed {
            return tasks;
        }
        assert!(
            !tasks.iter().any(|task| {
                task.task_type == anvil::tasks::TaskType::IndexBuild
                    && task.status == anvil::tasks::TaskStatus::Failed
            }),
            "index build task failed; tasks={tasks:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tasks
}

async fn persist_index_object(
    cluster: &TestCluster,
    bucket_id: i64,
    key: &str,
    hash_seed: u8,
    user_meta: Option<serde_json::Value>,
) {
    let payload = format!("payload for {key}").into_bytes();
    let content_hash = hex::encode([hash_seed; 32]);
    cluster.states[0]
        .persistence
        .create_object(
            1,
            bucket_id,
            key,
            &content_hash,
            payload.len() as i64,
            &content_hash,
            Some("text/plain"),
            user_meta,
            None,
            Some(payload),
        )
        .await
        .expect("persist index test object");
}

async fn put_json_object(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_id: i64,
    bucket_name: &str,
    key: &str,
    value: serde_json::Value,
    tag: &str,
) {
    let metadata = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.to_string(),
                object_key: key.to_string(),
                mutation_context: Some(native_mutation_context(bucket_id, tag)),
                content_type: Some("application/json".to_string()),
                user_metadata_json: String::new(),
            },
        )),
    };
    let chunk = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Chunk(
            serde_json::to_vec(&value).unwrap(),
        )),
    };
    object_client
        .put_object(authorized(tokio_stream::iter(vec![metadata, chunk]), token))
        .await
        .unwrap();
}

async fn query_index_until_hits(
    index_client: &mut IndexServiceClient<tonic::transport::Channel>,
    token: &str,
    request: QueryIndexRequest,
    expected_hits: usize,
    timeout: Duration,
) -> QueryIndexResponse {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        let response = match index_client
            .query_index(authorized(request.clone(), token))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                tokio::time::sleep(Duration::from_millis(150)).await;
                continue;
            }
            Err(status) => panic!("query failed while waiting for index: {status:?}"),
        };
        if response.hits.len() == expected_hits && response.is_caught_up {
            return response;
        }
        last = Some(response);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    last.unwrap_or_else(|| panic!("query did not execute before timeout"))
}

async fn query_spec_until_hits(
    index_client: &mut IndexServiceClient<tonic::transport::Channel>,
    token: &str,
    request: QuerySpecRequest,
    expected_hits: usize,
    timeout: Duration,
) -> anvil_api::QuerySpecResponse {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        let response = match index_client
            .query_spec(authorized(request.clone(), token))
            .await
        {
            Ok(response) => response.into_inner(),
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                tokio::time::sleep(Duration::from_millis(150)).await;
                continue;
            }
            Err(status) => panic!("query spec failed while waiting for indexes: {status:?}"),
        };
        let Some(result) = response.result.as_ref() else {
            panic!("query spec response omitted result");
        };
        if result.hits.len() == expected_hits && result.is_caught_up {
            return response;
        }
        last = Some(response);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    last.unwrap_or_else(|| panic!("query spec did not execute before timeout"))
}

async fn write_authz_tuple(
    auth_client: &mut AuthServiceClient<tonic::transport::Channel>,
    token: &str,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
) {
    auth_client
        .write_authz_tuple(authorized(
            WriteAuthzTupleRequest {
                namespace: namespace.to_string(),
                object_id: object_id.to_string(),
                relation: relation.to_string(),
                subject_kind: subject_kind.to_string(),
                subject_id: subject_id.to_string(),
                caveat_hash: String::new(),
                operation: "add".to_string(),
                reason: "query spec authz regression".to_string(),
                scope: None,
            },
            token,
        ))
        .await
        .unwrap();
}

async fn wait_for_derived_authz_entry(
    cluster: &TestCluster,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let maybe_index = read_derived_userset_index(
            &cluster.states[0].storage,
            1,
            DEFAULT_DERIVED_USERSET_INDEX_ID,
        )
        .await
        .unwrap();
        if maybe_index.is_some_and(|index| {
            index.entries.iter().any(|entry| {
                entry.namespace == namespace
                    && entry.object_id == object_id
                    && entry.relation == relation
                    && entry.subject_kind == subject_kind
                    && entry.subject_id == subject_id
                    && entry.caveat_hash.is_empty()
            })
        }) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    panic!("derived authz userset entry was not indexed before timeout");
}

#[tokio::test]
async fn test_typed_json_index_queries_canonical_object_body_with_range_order_and_page_token() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "typed-json-index-bucket".to_string();
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "queue/item-a.json",
        serde_json::json!({"state": {
            "queue_name": "outbound",
            "state": "pending",
            "available_at": "2026-07-03T10:00:00Z",
            "priority": 20,
            "item_id": "item-a"
        }}),
        "typed-a",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "queue/item-b.json",
        serde_json::json!({"state": {
            "queue_name": "outbound",
            "state": "failed",
            "available_at": "2026-07-03T10:00:00Z",
            "priority": 50,
            "item_id": "item-b"
        }}),
        "typed-b",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "queue/item-c.json",
        serde_json::json!({"state": {
            "queue_name": "outbound",
            "state": "pending",
            "available_at": "2026-07-04T10:00:00Z",
            "priority": 100,
            "item_id": "item-c"
        }}),
        "typed-c",
    )
    .await;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "due-work".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "queue/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "queue_name", "extractor": "/state/queue_name", "required": true},
                        {"name": "state", "extractor": "/state/state", "required": true},
                        {"name": "available_at", "extractor": "/state/available_at", "required": true},
                        {"name": "priority", "extractor": "/state/priority", "required": true},
                        {"name": "item_id", "extractor": "/state/item_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "available_at", "direction": "asc"},
                        {"field": "priority", "direction": "desc"},
                        {"field": "item_id", "direction": "asc"}
                    ]
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let first_page = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name: bucket_name.clone(),
            index_name: "due-work".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 1,
            phrase: false,
            path_prefix: "queue/".to_string(),
            metadata_filters_json: String::new(),
            typed_predicates_json: serde_json::json!([
                {"field": "queue_name", "op": "eq", "value": "outbound"},
                {"field": "state", "op": "in", "values": ["pending", "failed"]},
                {"field": "available_at", "op": "lte", "value": "2026-07-03T12:00:00Z"}
            ])
            .to_string(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        1,
        Duration::from_secs(10),
    )
    .await;
    assert_eq!(first_page.hits.len(), 1);
    assert_eq!(first_page.hits[0].object_key, "queue/item-b.json");
    assert!(!first_page.next_page_token.is_empty());
    assert!(first_page.is_caught_up);

    let second_page = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "due-work".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "queue/".to_string(),
                metadata_filters_json: String::new(),
                typed_predicates_json: serde_json::json!([
                    {"field": "queue_name", "op": "eq", "value": "outbound"},
                    {"field": "state", "op": "in", "values": ["pending", "failed"]},
                    {"field": "available_at", "op": "lte", "value": "2026-07-03T12:00:00Z"}
                ])
                .to_string(),
                typed_order_json: String::new(),
                page_token: first_page.next_page_token.clone(),
                require_caught_up_to_watch_cursor: first_page.source_watch_cursor_high.to_string(),
                lag_timeout_ms: 1000,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.hits.len(), 1);
    assert_eq!(second_page.hits[0].object_key, "queue/item-a.json");

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "queue/"},
                {"field": "queue_name", "op": "eq", "value": "outbound"},
                {"field": "state", "op": "in", "value": ["pending", "failed"]},
                {"field": "available_at", "op": "lte", "value": "2026-07-03T12:00:00Z"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "available_at", "direction": "asc"},
            {"field": "priority", "direction": "desc"},
            {"field": "item_id", "direction": "asc"}
        ],
        "limit": 1,
        "consistency": {
            "min_source_cursor": first_page.source_watch_cursor_high.to_string(),
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let spec_first = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: query_spec.clone(),
                page_token: String::new(),
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let spec_first_result = spec_first.result.expect("query spec result");
    assert_eq!(spec_first_result.hits.len(), 1);
    assert_eq!(spec_first_result.hits[0].object_key, "queue/item-b.json");
    assert!(!spec_first_result.next_page_token.is_empty());
    assert_eq!(spec_first.canonical_query_hash.len(), 64);
    assert!(
        spec_first.diagnostics.is_empty(),
        "bounded typed QuerySpec should not degrade: {:?}",
        spec_first.diagnostics
    );
    let plan: serde_json::Value = serde_json::from_str(&spec_first.plan_json).unwrap();
    assert_eq!(plan["schema"], "anvil.query.plan.v1");
    assert_eq!(plan["selected_index"]["name"], "due-work");
    assert_eq!(plan["selected_index"]["kind"], "typed_json");
    assert_eq!(plan["authz_relation"], "read");
    assert_eq!(plan["degraded"], false);

    let spec_second = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: query_spec,
                page_token: spec_first_result.next_page_token,
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("query spec second result");
    assert_eq!(spec_second.hits.len(), 1);
    assert_eq!(spec_second.hits[0].object_key, "queue/item-a.json");

    let missing_can_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "anvil_storage_tenant_id": "1",
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "queue/"},
                {"field": "state", "op": "in", "value": ["pending", "failed"]}
            ]
        },
        "limit": 10
    })
    .to_string();
    let missing_can = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: missing_can_spec,
                page_token: String::new(),
                lag_timeout_ms: 0,
                accept_degraded: false,
            },
            &token,
        ))
        .await;
    assert_eq!(
        missing_can.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "protected QuerySpec must fail closed without an explicit can predicate"
    );

    let unbounded_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "anvil_storage_tenant_id": "1",
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"can": {"relation": "read"}}
            ]
        },
        "limit": 10
    })
    .to_string();
    let unbounded = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: unbounded_spec,
                page_token: String::new(),
                lag_timeout_ms: 0,
                accept_degraded: false,
            },
            &token,
        ))
        .await;
    assert_eq!(
        unbounded.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "QuerySpec without a bounded primitive predicate must not scan the bucket"
    );

    let mismatched_predicate = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "due-work".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "queue/".to_string(),
                metadata_filters_json: String::new(),
                typed_predicates_json: serde_json::json!([
                    {"field": "queue_name", "op": "eq", "value": "outbound"},
                    {"field": "state", "op": "eq", "value": "pending"}
                ])
                .to_string(),
                typed_order_json: String::new(),
                page_token: first_page.next_page_token.clone(),
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await;
    assert_eq!(
        mismatched_predicate.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );

    let mut tampered_token = first_page.next_page_token.clone();
    tampered_token.push('A');
    let tampered = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name,
                index_name: "due-work".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "queue/".to_string(),
                metadata_filters_json: String::new(),
                typed_predicates_json: serde_json::json!([
                    {"field": "queue_name", "op": "eq", "value": "outbound"},
                    {"field": "state", "op": "in", "values": ["pending", "failed"]},
                    {"field": "available_at", "op": "lte", "value": "2026-07-03T12:00:00Z"}
                ])
                .to_string(),
                typed_order_json: String::new(),
                page_token: tampered_token,
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await;
    assert_eq!(tampered.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_query_spec_intersects_full_text_with_typed_filter_without_bucket_scan() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("query-spec-composite-{}", uuid::Uuid::new_v4());
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "case-text".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "cases/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "json_pointer",
                    "pointer": "/summary"
                })
                .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "case-state".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "cases/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true},
                        {"name": "priority", "extractor": "/priority", "required": true},
                        {"name": "case_id", "extractor": "/case_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "priority", "direction": "desc"},
                        {"field": "case_id", "direction": "asc"}
                    ]
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "cases/a.json",
        serde_json::json!({
            "summary": "payment failure requires operator follow up",
            "state": "pending",
            "priority": 30,
            "case_id": "case-a"
        }),
        "composite-a",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "cases/b.json",
        serde_json::json!({
            "summary": "payment failure already resolved",
            "state": "pending",
            "priority": 90,
            "case_id": "case-b"
        }),
        "composite-b",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "cases/c.json",
        serde_json::json!({
            "summary": "profile update needs review",
            "state": "pending",
            "priority": 100,
            "case_id": "case-c"
        }),
        "composite-c",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "cases/d.json",
        serde_json::json!({
            "summary": "payment failure needs later audit",
            "state": "closed",
            "priority": 10,
            "case_id": "case-d"
        }),
        "composite-d",
    )
    .await;

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "cases/"},
                {"full_text": {"query": "payment failure"}},
                {"field": "state", "op": "eq", "value": "pending"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "priority", "direction": "desc"},
            {"field": "case_id", "direction": "asc"}
        ],
        "limit": 1,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();

    let mut warmup_spec: serde_json::Value = serde_json::from_str(&query_spec).unwrap();
    warmup_spec["limit"] = serde_json::json!(10);
    query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: warmup_spec.to_string(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        2,
        Duration::from_secs(90),
    )
    .await;

    let response = query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: query_spec.clone(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        Duration::from_secs(90),
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].object_key, "cases/b.json");
    assert!(!result.next_page_token.is_empty());
    assert!(response.diagnostics.is_empty());

    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["planner"], "primitive-index-intersection");
    assert_eq!(plan["selected_index"]["name"], "case-text");
    assert_eq!(plan["selected_index"]["kind"], "full_text");
    assert_eq!(plan["filter_index"]["name"], "case-state");
    assert_eq!(plan["filter_index"]["kind"], "typed_json");

    let scoring: serde_json::Value = serde_json::from_str(&result.scoring_recipe_json).unwrap();
    assert_eq!(scoring["kind"], "query_spec_composite");
    assert_eq!(scoring["planner"], "primitive-index-intersection");
    assert_eq!(scoring["primary_index"], "case-text");
    assert_eq!(scoring["typed_filter_index"], "case-state");

    let second_page = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: query_spec.clone(),
                page_token: result.next_page_token.clone(),
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("second query spec page");
    assert_eq!(second_page.hits.len(), 1);
    assert_eq!(second_page.hits[0].object_key, "cases/a.json");

    let tampered_predicate = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "cases/"},
                {"full_text": {"query": "payment failure"}},
                {"field": "state", "op": "eq", "value": "closed"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "priority", "direction": "desc"},
            {"field": "case_id", "direction": "asc"}
        ],
        "limit": 1,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let tampered = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: tampered_predicate,
                page_token: result.next_page_token,
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await;
    assert_eq!(tampered.unwrap_err().code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_query_spec_intersection_filters_inherit_object_hits_by_read_scope() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("qsa-{}", uuid::Uuid::new_v4());
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "auth-text".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "auth/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "json_pointer",
                    "pointer": "/summary"
                })
                .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "auth-state".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "auth/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true},
                        {"name": "priority", "extractor": "/priority", "required": true}
                    ],
                    "default_order": [
                        {"field": "priority", "direction": "desc"}
                    ]
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "auth/allowed.json",
        serde_json::json!({
            "summary": "payment failure requires review",
            "state": "pending",
            "priority": 10
        }),
        "auth-composite-allowed",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "auth/denied.json",
        serde_json::json!({
            "summary": "payment failure high priority",
            "state": "pending",
            "priority": 100
        }),
        "auth-composite-denied",
    )
    .await;

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "query-spec-scope-reader".to_string(),
            vec![
                format!("index:read|{bucket_name}"),
                format!("object:read|{bucket_name}/auth/allowed.json"),
            ],
            claims.tenant_id,
        )
        .unwrap();

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "auth/"},
                {"full_text": {"query": "payment failure"}},
                {"field": "state", "op": "eq", "value": "pending"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "priority", "direction": "desc"}
        ],
        "limit": 10,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let response = query_spec_until_hits(
        &mut index_client,
        &limited_token,
        QuerySpecRequest {
            query_spec_json: query_spec,
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        Duration::from_secs(30),
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].object_key, "auth/allowed.json");

    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["planner"], "primitive-index-intersection");
    assert_eq!(
        plan["selected_index"]["authorization_mode"],
        "inherit_object"
    );
    assert_eq!(plan["filter_index"]["authorization_mode"], "inherit_object");
    assert_eq!(plan["authz_relation"], "read");
}

#[tokio::test]
async fn test_query_spec_path_filter_intersects_authz_before_results() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("qsp-{}", uuid::Uuid::new_v4());
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "by-path".to_string(),
                kind: IndexKind::Path as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "docs/allowed.json",
        serde_json::json!({"title": "allowed"}),
        "path-authz-allowed",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "docs/denied.json",
        serde_json::json!({"title": "denied"}),
        "path-authz-denied",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "other/allowed.json",
        serde_json::json!({"title": "outside path"}),
        "path-authz-outside",
    )
    .await;

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "query-spec-path-reader".to_string(),
            vec![
                format!("index:read|{bucket_name}"),
                format!("object:read|{bucket_name}/docs/allowed.json"),
            ],
            claims.tenant_id,
        )
        .unwrap();
    let prefix_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "query-spec-path-prefix-reader".to_string(),
            vec![
                format!("index:read|{bucket_name}"),
                format!("object:read|{bucket_name}/docs/*"),
            ],
            claims.tenant_id,
        )
        .unwrap();

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "docs/"},
                {"can": {"relation": "read"}}
            ]
        },
        "limit": 10,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let response = query_spec_until_hits(
        &mut index_client,
        &limited_token,
        QuerySpecRequest {
            query_spec_json: query_spec.clone(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        Duration::from_secs(60),
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/allowed.json"]
    );
    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["selected_index"]["kind"], "path");
    assert_eq!(plan["authz_relation"], "read");
    assert_eq!(plan["degraded"], false);

    let prefix_response = query_spec_until_hits(
        &mut index_client,
        &prefix_token,
        QuerySpecRequest {
            query_spec_json: query_spec,
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        2,
        Duration::from_secs(60),
    )
    .await
    .result
    .expect("query spec prefix result");
    assert_eq!(
        prefix_response
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/allowed.json", "docs/denied.json"]
    );
}

#[tokio::test]
async fn test_query_spec_inherit_object_filter_uses_derived_userset_grants() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("qsu-{}", uuid::Uuid::new_v4());
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "by-path".to_string(),
                kind: IndexKind::Path as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "docs/group-allowed.json",
        serde_json::json!({"title": "group allowed"}),
        "userset-authz-allowed",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "docs/group-denied.json",
        serde_json::json!({"title": "group denied"}),
        "userset-authz-denied",
    )
    .await;

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let reader_subject = "query-spec-userset-reader";
    write_authz_tuple(
        &mut auth_client,
        &token,
        "group",
        "engineering",
        "member",
        "app",
        reader_subject,
    )
    .await;
    write_authz_tuple(
        &mut auth_client,
        &token,
        "object",
        &format!("{bucket_name}/docs/group-allowed.json"),
        "reader",
        "userset",
        "group/engineering#member",
    )
    .await;

    wait_for_derived_authz_entry(
        &cluster,
        &encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "object"),
        &format!("{bucket_name}/docs/group-allowed.json"),
        "reader",
        "app",
        reader_subject,
        Duration::from_secs(10),
    )
    .await;

    let userset_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            reader_subject.to_string(),
            vec![format!("index:read|{bucket_name}")],
            claims.tenant_id,
        )
        .unwrap();
    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "docs/"},
                {"can": {"relation": "read"}}
            ]
        },
        "limit": 10,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();

    let response = query_spec_until_hits(
        &mut index_client,
        &userset_token,
        QuerySpecRequest {
            query_spec_json: query_spec,
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        Duration::from_secs(10),
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(
        result
            .hits
            .iter()
            .map(|hit| hit.object_key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/group-allowed.json"]
    );
    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["selected_index"]["kind"], "path");
    assert_eq!(plan["authz_relation"], "read");
    assert_eq!(plan["degraded"], false);
}

#[tokio::test]
async fn test_query_spec_intersects_vector_with_typed_filter_without_bucket_scan() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("qsv-{}", uuid::Uuid::new_v4());
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "case-vector".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "vectors/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_json_vector",
                    "caller_supplied",
                    "test-explicit-vector",
                    2,
                    "text",
                    "cosine",
                )
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "vector-state".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "vectors/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true},
                        {"name": "case_id", "extractor": "/case_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "case_id", "direction": "asc"}
                    ]
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "vectors/a.json",
        serde_json::json!({
            "vector": [1.0, 0.0],
            "source_start": 0,
            "source_len": 4,
            "state": "pending",
            "case_id": "case-a"
        }),
        "vector-composite-a",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "vectors/b.json",
        serde_json::json!({
            "vector": [1.0, 0.0],
            "source_start": 0,
            "source_len": 4,
            "state": "closed",
            "case_id": "case-b"
        }),
        "vector-composite-b",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "vectors/c.json",
        serde_json::json!({
            "vector": [0.0, 1.0],
            "source_start": 0,
            "source_len": 4,
            "state": "pending",
            "case_id": "case-c"
        }),
        "vector-composite-c",
    )
    .await;

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "vectors/"},
                {"vector": {"near": [1.0, 0.0]}},
                {"field": "state", "op": "eq", "value": "pending"},
                {"can": {"relation": "read"}}
            ]
        },
        "limit": 10,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();
    let response = query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: query_spec,
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        2,
        Duration::from_secs(90),
    )
    .await;
    let result = response.result.expect("query spec result");
    let hit_keys = result
        .hits
        .iter()
        .map(|hit| hit.object_key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(hit_keys, vec!["vectors/a.json", "vectors/c.json"]);

    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["planner"], "primitive-index-intersection");
    assert_eq!(plan["selected_index"]["name"], "case-vector");
    assert_eq!(plan["selected_index"]["kind"], "vector");
    assert_eq!(plan["filter_index"]["name"], "vector-state");
    assert_eq!(plan["filter_index"]["kind"], "typed_json");
}

#[tokio::test]
async fn test_query_spec_intersects_hybrid_with_typed_filter_without_bucket_scan() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("qsh-{}", uuid::Uuid::new_v4());
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

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
                name: "case-hybrid".to_string(),
                kind: IndexKind::Hybrid as i32,
                selector_json: serde_json::json!({"prefix": "hybrid/"}).to_string(),
                extractor_json: serde_json::json!({
                    "text": {
                        "source": "json_pointer",
                        "json_pointer": "/summary"
                    }
                })
                .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "full_text": {"positions": true},
                    "vector": vector_policy
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "hybrid-state".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "hybrid/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "object_current",
                    "fields": [
                        {"name": "state", "extractor": "/state", "required": true},
                        {"name": "priority", "extractor": "/priority", "required": true},
                        {"name": "case_id", "extractor": "/case_id", "required": true}
                    ],
                    "default_order": [
                        {"field": "priority", "direction": "desc"},
                        {"field": "case_id", "direction": "asc"}
                    ]
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "hybrid/a.json",
        serde_json::json!({
            "summary": "lease dashboard escalation",
            "embedding": [0.0, 1.0],
            "source_start": 0,
            "source_len": 12,
            "state": "pending",
            "priority": 20,
            "case_id": "case-a"
        }),
        "hybrid-composite-a",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "hybrid/b.json",
        serde_json::json!({
            "summary": "lease dashboard archived",
            "embedding": [0.0, 1.0],
            "source_start": 0,
            "source_len": 12,
            "state": "closed",
            "priority": 90,
            "case_id": "case-b"
        }),
        "hybrid-composite-b",
    )
    .await;
    put_json_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        "hybrid/c.json",
        serde_json::json!({
            "summary": "lease dashboard planning",
            "embedding": [0.0, 1.0],
            "source_start": 0,
            "source_len": 12,
            "state": "pending",
            "priority": 70,
            "case_id": "case-c"
        }),
        "hybrid-composite-c",
    )
    .await;

    let query_spec = serde_json::json!({
        "schema": "anvil.query.spec.v1",
        "scope": {
            "mesh_id": "local-mesh",
            "anvil_storage_tenant_id": "1",
            "authz_scope": {
                "anvil_storage_tenant_id": "1",
                "authz_realm_id": "tenant:1"
            },
            "bucket_name": bucket_name
        },
        "source_kind": "object_current",
        "where": {
            "all": [
                {"path_prefix": "hybrid/"},
                {"full_text": {"query": "lease dashboard"}},
                {"vector": {"near": [0.0, 1.0]}},
                {"field": "state", "op": "eq", "value": "pending"},
                {"can": {"relation": "read"}}
            ]
        },
        "order_by": [
            {"field": "priority", "direction": "desc"},
            {"field": "case_id", "direction": "asc"}
        ],
        "limit": 1,
        "consistency": {
            "min_authz_revision": 0,
            "allow_stale_index": false
        }
    })
    .to_string();

    let mut warmup_spec: serde_json::Value = serde_json::from_str(&query_spec).unwrap();
    warmup_spec["limit"] = serde_json::json!(10);
    query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: warmup_spec.to_string(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        2,
        Duration::from_secs(90),
    )
    .await;

    let response = query_spec_until_hits(
        &mut index_client,
        &token,
        QuerySpecRequest {
            query_spec_json: query_spec.clone(),
            page_token: String::new(),
            lag_timeout_ms: 1000,
            accept_degraded: false,
        },
        1,
        Duration::from_secs(90),
    )
    .await;
    let result = response.result.expect("query spec result");
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].object_key, "hybrid/c.json");
    assert!(!result.next_page_token.is_empty());

    let plan: serde_json::Value = serde_json::from_str(&response.plan_json).unwrap();
    assert_eq!(plan["planner"], "primitive-index-intersection");
    assert_eq!(plan["selected_index"]["name"], "case-hybrid");
    assert_eq!(plan["selected_index"]["kind"], "hybrid");
    assert_eq!(plan["filter_index"]["name"], "hybrid-state");
    assert_eq!(plan["filter_index"]["kind"], "typed_json");

    let scoring: serde_json::Value = serde_json::from_str(&result.scoring_recipe_json).unwrap();
    assert_eq!(scoring["kind"], "query_spec_composite");
    assert_eq!(scoring["primary_index"], "case-hybrid");
    assert_eq!(scoring["typed_filter_index"], "hybrid-state");
    assert_eq!(scoring["primary_scoring"]["kind"], "hybrid");

    let second_page = index_client
        .query_spec(authorized(
            QuerySpecRequest {
                query_spec_json: query_spec,
                page_token: result.next_page_token,
                lag_timeout_ms: 1000,
                accept_degraded: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .result
        .expect("second query spec page");
    assert_eq!(second_page.hits.len(), 1);
    assert_eq!(second_page.hits[0].object_key, "hybrid/a.json");
}

#[tokio::test]
async fn test_typed_json_index_queries_append_record_payloads() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "typed-json-append-index-bucket".to_string();
    let bucket_id = bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let stream = object_client
        .create_append_stream(authorized(
            CreateAppendStreamRequest {
                bucket_name: bucket_name.clone(),
                stream_key: "audit/tenant-a".to_string(),
                mutation_context: Some(native_mutation_context(bucket_id, "append-stream")),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    for (idx, payload) in [
        serde_json::json!({"event": {"kind": "attempt", "severity": "warn", "attempt": 2}}),
        serde_json::json!({"event": {"kind": "attempt", "severity": "info", "attempt": 1}}),
        serde_json::json!({"event": {"kind": "repair", "severity": "warn", "attempt": 3}}),
    ]
    .into_iter()
    .enumerate()
    {
        object_client
            .append_stream_record(authorized(
                AppendStreamRecordRequest {
                    bucket_name: bucket_name.clone(),
                    stream_key: "audit/tenant-a".to_string(),
                    stream_id: stream.stream_id.clone(),
                    payload: serde_json::to_vec(&payload).unwrap(),
                    mutation_context: Some(native_mutation_context(
                        bucket_id,
                        &format!("append-record-{idx}"),
                    )),
                    content_type: Some("application/json".to_string()),
                    user_metadata_json: serde_json::json!({"source": "test"}).to_string(),
                    precondition: None,
                },
                &token,
            ))
            .await
            .unwrap();
    }

    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "append-events".to_string(),
                kind: IndexKind::TypedJson as i32,
                selector_json: serde_json::json!({"prefix": "audit/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({
                    "source_kind": "append_record",
                    "fields": [
                        {"name": "stream", "extractor": "append_stream_key", "required": true},
                        {"name": "sequence", "extractor": "append_record_sequence", "required": true},
                        {"name": "kind", "extractor": "append_payload_json_pointer:/event/kind", "required": true},
                        {"name": "severity", "extractor": "append_payload_json_pointer:/event/severity", "required": true},
                        {"name": "attempt", "extractor": "append_payload_json_pointer:/event/attempt", "required": true},
                        {"name": "source", "extractor": "append_user_metadata_json_pointer:/source", "required": true}
                    ],
                    "default_order": [
                        {"field": "attempt", "direction": "asc"},
                        {"field": "sequence", "direction": "asc"}
                    ]
                })
                .to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let response = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name,
            index_name: "append-events".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: "audit/".to_string(),
            metadata_filters_json: String::new(),
            typed_predicates_json: serde_json::json!([
                {"field": "kind", "op": "eq", "value": "attempt"},
                {"field": "severity", "op": "in", "values": ["info", "warn"]},
                {"field": "source", "op": "eq", "value": "test"}
            ])
            .to_string(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        2,
        Duration::from_secs(10),
    )
    .await;

    assert_eq!(response.hits.len(), 2);
    assert_eq!(response.hits[0].object_key, "audit/tenant-a");
    let first_values: serde_json::Value =
        serde_json::from_str(&response.hits[0].metadata_json).unwrap();
    assert_eq!(first_values["typed_values"]["attempt"], 1);
    let second_values: serde_json::Value =
        serde_json::from_str(&response.hits[1].metadata_json).unwrap();
    assert_eq!(second_values["typed_values"]["attempt"], 2);
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
                kind: IndexKind::FullText as i32,
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
    assert_eq!(created.kind, IndexKind::FullText as i32);
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
    for event in &events {
        let envelope = event.envelope.as_ref().expect("index definition envelope");
        assert_eq!(envelope.watch_stream_id, "index_definition");
        assert_eq!(envelope.partition_family, "index_definition");
        assert_eq!(envelope.cursor_low, event.cursor);
        assert_eq!(envelope.record_kind, "index_definition");
        assert!(!envelope.payload_hash.is_empty());
    }
    assert_eq!(events[3].index.as_ref().unwrap().name, "docs-full-text");
}

#[tokio::test]
async fn test_query_path_and_metadata_filter_indexes_from_object_metadata() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("metadata-backed-index-{}", uuid::Uuid::new_v4());
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
                name: "by-path".to_string(),
                kind: IndexKind::Path as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),
            },
            &token,
        ))
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "by-meta".to_string(),
                kind: IndexKind::MetadataFilter as i32,
                selector_json: serde_json::json!({}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({}).to_string(),
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
    persist_index_object(
        &cluster,
        bucket_id,
        "docs/alpha.txt",
        1,
        Some(serde_json::json!({"tenant": "alpha", "nested": {"state": "open"}})),
    )
    .await;
    persist_index_object(
        &cluster,
        bucket_id,
        "docs/beta.txt",
        2,
        Some(serde_json::json!({"tenant": "beta", "nested": {"state": "open"}})),
    )
    .await;
    persist_index_object(
        &cluster,
        bucket_id,
        "images/logo.txt",
        3,
        Some(serde_json::json!({"tenant": "alpha", "nested": {"state": "open"}})),
    )
    .await;
    let tasks = wait_for_index_build_task_count(&cluster, Duration::from_secs(60), 2).await;
    assert!(
        tasks
            .iter()
            .filter(|task| {
                task.task_type == anvil::tasks::TaskType::IndexBuild
                    && task.status == anvil::tasks::TaskStatus::Completed
            })
            .count()
            >= 2,
        "path and metadata_filter build tasks should complete; tasks={tasks:?}"
    );

    let path_response = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name: bucket_name.clone(),
            index_name: "by-path".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: "docs/".to_string(),
            metadata_filters_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        2,
        Duration::from_secs(60),
    )
    .await;

    assert_eq!(path_response.index_kind, IndexKind::Path as i32);
    assert_eq!(path_response.hits.len(), 2);
    assert_eq!(path_response.hits[0].kind, IndexKind::Path as i32);
    assert_eq!(path_response.hits[0].object_key, "docs/alpha.txt");
    assert_eq!(path_response.hits[1].object_key, "docs/beta.txt");
    let path_recipe: serde_json::Value =
        serde_json::from_str(&path_response.scoring_recipe_json).unwrap();
    assert_eq!(path_recipe["source"], "corestore_typed_field_segment");

    let paged_path_first = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "by-path".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 1,
                phrase: false,
                path_prefix: "docs/".to_string(),
                metadata_filters_json: String::new(),
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
    assert_eq!(paged_path_first.hits.len(), 1);
    assert_eq!(paged_path_first.hits[0].object_key, "docs/alpha.txt");
    assert!(!paged_path_first.next_page_token.is_empty());

    let paged_path_second = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "by-path".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "docs/".to_string(),
                metadata_filters_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: paged_path_first.next_page_token.clone(),
                require_caught_up_to_watch_cursor: paged_path_first
                    .source_watch_cursor_high
                    .to_string(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(paged_path_second.hits.len(), 1);
    assert_eq!(paged_path_second.hits[0].object_key, "docs/beta.txt");

    let changed_predicate = index_client
        .query_index(authorized(
            QueryIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "by-path".to_string(),
                query_text: String::new(),
                query_vector: vec![],
                limit: 10,
                phrase: false,
                path_prefix: "images/".to_string(),
                metadata_filters_json: String::new(),
                typed_predicates_json: String::new(),
                typed_order_json: String::new(),
                page_token: paged_path_first.next_page_token,
                require_caught_up_to_watch_cursor: String::new(),
                lag_timeout_ms: 0,
            },
            &token,
        ))
        .await;
    assert_eq!(
        changed_predicate.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );

    let metadata_response = query_index_until_hits(
        &mut index_client,
        &token,
        QueryIndexRequest {
            bucket_name,
            index_name: "by-meta".to_string(),
            query_text: String::new(),
            query_vector: vec![],
            limit: 10,
            phrase: false,
            path_prefix: "docs/".to_string(),
            metadata_filters_json: serde_json::json!({
                "tenant": "alpha",
                "/nested/state": "open"
            })
            .to_string(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: String::new(),
            lag_timeout_ms: 0,
        },
        1,
        Duration::from_secs(10),
    )
    .await;

    assert_eq!(
        metadata_response.index_kind,
        IndexKind::MetadataFilter as i32
    );
    assert_eq!(metadata_response.hits.len(), 1);
    assert_eq!(
        metadata_response.hits[0].kind,
        IndexKind::MetadataFilter as i32
    );
    assert_eq!(metadata_response.hits[0].score, 1.0);
    assert_eq!(metadata_response.hits[0].object_key, "docs/alpha.txt");
    let hit_metadata: serde_json::Value =
        serde_json::from_str(&metadata_response.hits[0].metadata_json).unwrap();
    assert_eq!(hit_metadata["user_metadata"]["tenant"], "alpha");
    assert_eq!(hit_metadata["user_metadata"]["nested"]["state"], "open");
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
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
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
                    object_key: "docs/alpha.txt".to_string(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
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
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
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
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
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
    let watch_event = tokio::time::timeout(Duration::from_secs(30), watch.next())
        .await
        .expect("index partition watch should yield a built segment event")
        .expect("index partition watch stream should stay open")
        .expect("index partition watch event should be successful");
    assert_eq!(watch_event.bucket_name, bucket_name);
    assert_eq!(watch_event.index_name, "body");
    assert_eq!(watch_event.event_type, "segment_built");
    assert_eq!(watch_event.index_kind, IndexKind::FullText as i32);
    assert_eq!(watch_event.generation, response.index_generation);
    assert!(!watch_event.index_storage_id.is_empty());
    assert!(!watch_event.partition_id.is_empty());
    assert!(!watch_event.source_manifest_hash.is_empty());
    assert!(!watch_event.proof_hash.is_empty());
    assert!(!watch_event.segment_hashes.is_empty());
    let envelope = watch_event
        .envelope
        .as_ref()
        .expect("index partition envelope");
    assert_eq!(envelope.watch_stream_id, "index_partition");
    assert_eq!(envelope.partition_family, "index_partition");
    assert_eq!(envelope.cursor_low, watch_event.cursor_low);
    assert_eq!(envelope.cursor_high, watch_event.cursor_high);
    assert_eq!(envelope.index_generation, watch_event.generation);
    assert_eq!(envelope.record_kind, "index_partition");
    assert!(!envelope.payload_hash.is_empty());

    let mut tasks = Vec::new();
    let task_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while tokio::time::Instant::now() < task_deadline {
        tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
        if tasks.iter().any(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.status == anvil::tasks::TaskStatus::Completed
        }) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
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
                kind: IndexKind::FullText as i32,
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
                    object_key: "docs/report.json".to_string(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
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
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
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
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
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
    let index_tasks = persistence
        .list_tasks()
        .await
        .unwrap()
        .into_iter()
        .filter(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.payload["index_id"] == serde_json::json!(index.id)
        })
        .collect::<Vec<_>>();
    let pending_tasks = index_tasks
        .iter()
        .filter(|task| task.status == anvil::tasks::TaskStatus::Pending)
        .collect::<Vec<_>>();
    assert_eq!(
        pending_tasks.len(),
        1,
        "index build tasks for the same index should expose one pending build before execution"
    );
    assert!(
        pending_tasks[0].payload["source_cursor"].as_u64().unwrap() > source_cursor,
        "pending index build should advance to the latest source cursor"
    );
    assert!(
        index_tasks
            .iter()
            .any(|task| task.status == anvil::tasks::TaskStatus::Completed
                && task.payload["source_cursor"] == serde_json::json!(source_cursor)),
        "superseded index build should remain in the journal as a completed task"
    );

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
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let proof = anvil::derived_index_proof::read_latest_derived_index_proof(
        &cluster.states[0].storage,
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("derived index proof exists");
    assert_eq!(proof.source_cursor, u128::from(source_cursor));
    assert_eq!(proof.generation, segment.header.generation);
    assert_eq!(proof.segment_hashes.len(), 1);
    let checkpoint = anvil::watch_checkpoint::read_watch_checkpoint(
        &cluster.states[0].storage,
        "object_metadata",
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("index watch checkpoint exists");
    assert_eq!(checkpoint.cursor, u128::from(source_cursor));
    assert_eq!(checkpoint.source_manifest_hash, proof.source_manifest_hash);

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
async fn test_index_build_requires_current_rfc_ownership_fence() {
    let cluster = TestCluster::new(&["test-region-1"]).await;
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = format!("index-fence-{}", uuid::Uuid::new_v4());
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
            &hex::encode([33; 32]),
            20,
            "etag-alpha",
            Some("text/plain"),
            None,
            None,
            Some(b"alpha fence visible".to_vec()),
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
        .expect("index build task records source cursor");
    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::IndexPartition,
        resource_id: format!(
            "tenant/{tenant_id}/bucket/{}/index_build/{index_storage_id}",
            bucket.id
        ),
    };
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    acquire_ownership(
        &cluster.states[0].storage,
        AcquireOwnership {
            request_id: "other-node-index-owner".to_string(),
            idempotency_key: "other-node-index-owner".to_string(),
            resource,
            owner: OwnershipPrincipal {
                tenant_id: 0,
                principal_kind: "node".to_string(),
                principal_id: "other-node".to_string(),
                actor_instance_id: "other-node".to_string(),
                display_name: "other-node".to_string(),
                region: "test-region-1".to_string(),
                cell: "default".to_string(),
            },
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap()
                .saturating_mul(1_000_000),
        },
        &hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap(),
    )
    .await
    .unwrap();

    let err = persistence
        .build_index_task(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(source_cursor),
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("OwnershipHeld"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_index_enqueue_rebuilds_when_checkpoint_exists_but_proof_is_missing() {
    let cluster = TestCluster::new(&["test-region-1"]).await;
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = format!("index-missing-proof-{}", uuid::Uuid::new_v4());
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
            &hex::encode([41; 32]),
            32,
            "etag-alpha",
            Some("text/plain"),
            None,
            None,
            Some(b"alpha proves missing proof rebuild".to_vec()),
        )
        .await
        .unwrap();

    let initial_task = persistence
        .list_tasks()
        .await
        .unwrap()
        .into_iter()
        .find(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.payload["index_id"] == serde_json::json!(index.id)
        })
        .expect("initial index build task should exist");
    let source_cursor = initial_task
        .payload
        .get("source_cursor")
        .and_then(serde_json::Value::as_u64)
        .expect("initial index build task records source cursor");

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
    persistence
        .update_task_status(initial_task.id, anvil::tasks::TaskStatus::Completed)
        .await
        .unwrap();

    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let checkpoint = anvil::watch_checkpoint::read_watch_checkpoint(
        &cluster.states[0].storage,
        "object_metadata",
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("index build should checkpoint object metadata cursor");
    assert_eq!(checkpoint.cursor, u128::from(source_cursor));

    let core_store = anvil::core_store::CoreStore::new(cluster.states[0].storage.clone())
        .await
        .unwrap();
    core_store
        .delete_ref(
            &format!("derived_index_proof:index:{index_storage_id}:head"),
            None,
            None,
            true,
        )
        .await
        .expect("remove proof head to simulate lost derived proof");

    assert!(
        persistence
            .enqueue_index_build_for_index(&bucket, &index)
            .await
            .unwrap(),
        "missing proof must schedule a rebuild even when checkpoint cursor is current"
    );
    let rebuild_task = persistence
        .list_tasks()
        .await
        .unwrap()
        .into_iter()
        .find(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.status == anvil::tasks::TaskStatus::Pending
                && task.payload["index_id"] == serde_json::json!(index.id)
                && task.payload["source_cursor"] == serde_json::json!(source_cursor)
        })
        .expect("missing proof rebuild task should be pending");
    assert_eq!(
        rebuild_task.payload["catch_up_plan"]["RebuildFromManifest"]["reason"],
        serde_json::json!("MissingProof")
    );
    assert_eq!(
        rebuild_task.payload["catch_up_plan"]["RebuildFromManifest"]["resume_after_cursor"],
        serde_json::json!(0)
    );
    assert!(
        rebuild_task
            .payload
            .get("source_manifest_hash")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| value.len() == 64),
        "rebuild task should record the source checkpoint hash"
    );
}

#[tokio::test]
async fn test_repair_rebuilds_missing_full_text_segment_from_base_journal() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;
    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = format!("index-repair-{}", uuid::Uuid::new_v4());
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "docs/repair.txt",
            &hex::encode([31; 32]),
            32,
            "etag-repair",
            Some("text/plain"),
            None,
            None,
            Some(b"repair rebuilds derived full text segment".to_vec()),
        )
        .await
        .unwrap();
    persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata compaction writes manifest segments");
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
    assert!(
        persistence
            .enqueue_index_build_for_index(&bucket, &index)
            .await
            .unwrap(),
        "compacted source manifest must still schedule an index build"
    );
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
        .expect("index build task records source cursor");

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
        .expect("initial index build succeeds");
    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let proof = anvil::derived_index_proof::read_latest_derived_index_proof(
        &cluster.states[0].storage,
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("proof exists before deleting segment");
    assert!(!proof.segment_hashes.is_empty());
    let core_store = anvil::core_store::CoreStore::new(cluster.states[0].storage.clone())
        .await
        .unwrap();
    while let Some(segment_ref) = anvil::full_text_segment::latest_full_text_segment_ref(
        &cluster.states[0].storage,
        &index_storage_id,
    )
    .await
    .unwrap()
    {
        core_store
            .delete_ref(&segment_ref, None, None, true)
            .await
            .expect("remove segment to force repair");
    }
    assert!(
        anvil::full_text_segment::read_latest_full_text_segment(
            &cluster.states[0].storage,
            &index_storage_id
        )
        .await
        .unwrap()
        .is_none(),
        "segment deletion must remove the queryable derived index"
    );

    let mut repair_client = RepairServiceClient::connect(grpc_addr).await.unwrap();
    let report = repair_client
        .repair_index(authorized(
            RepairIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "body".to_string(),
                rebuild: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "rebuilt_derived_index");
    assert_eq!(report.reason, "DerivedIndexSegmentMissing");
    assert_eq!(report.index_storage_id, index_storage_id);
    assert_eq!(report.source_cursor_low, source_cursor);
    assert_eq!(report.source_cursor_high, 0);
    assert!(report.finding.is_some());
    assert!(report.build.is_some());

    let repaired = anvil::full_text_segment::read_latest_full_text_segment(
        &cluster.states[0].storage,
        &index_storage_id,
    )
    .await
    .unwrap()
    .expect("repair rebuilds segment");
    let definition = anvil::formats::full_text::FullTextIndexDefinition::from_json(
        &serde_json::json!({"positions": true}),
    )
    .unwrap();
    let hits = query_full_text_segment(
        &repaired,
        FullTextSegmentQuery {
            query: "repair rebuilds",
            tokenizer: &definition.tokenizer,
            positions_enabled: definition.positions_enabled,
            phrase: false,
            bm25: anvil::formats::full_text::Bm25Config::default(),
            authorized_labels: None,
            limit: 10,
        },
    )
    .unwrap();
    assert!(
        !hits.is_empty(),
        "rebuilt derived index must be queryable from base metadata and payload journals"
    );

    let findings = repair_client
        .list_repair_findings(authorized(
            ListRepairFindingsRequest {
                scope_kind: "bucket".to_string(),
                scope_id: format!("tenant-{tenant_id}-bucket-{}", bucket.id),
                limit: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .findings;
    assert!(findings.iter().any(|finding| {
        finding.code == "DerivedIndexSegmentMissing" && finding.status == "RebuiltDerivedIndex"
    }));
}

#[tokio::test]
async fn test_repair_rebuilds_missing_vector_segment_from_base_journal() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;
    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = format!("vector-index-repair-{}", uuid::Uuid::new_v4());
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    let vector_payload = br#"{"vector":[1.0,0.0],"source_start":4,"source_len":12}"#;
    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "vectors/repair.json",
            &hex::encode(anvil::formats::hash32(vector_payload)),
            i64::try_from(vector_payload.len()).unwrap(),
            "etag-vector-repair",
            Some("application/json"),
            None,
            None,
            Some(vector_payload.to_vec()),
        )
        .await
        .unwrap();
    persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata compaction writes manifest segments");
    let index = persistence
        .create_index_definition(
            tenant_id,
            bucket.id,
            "embedding",
            "vector",
            serde_json::json!({"prefix": "vectors/"}),
            serde_json::json!({}),
            "index_only",
            rfc_vector_policy(
                "object_body_json_vector",
                "caller_supplied",
                "test-explicit-vector",
                2,
                "text",
                "cosine",
            ),
        )
        .await
        .unwrap();
    persistence
        .create_index_definition_event(tenant_id, bucket.id, &bucket.name, &index, "create")
        .await
        .unwrap();
    assert!(
        persistence
            .enqueue_index_build_for_index(&bucket, &index)
            .await
            .unwrap(),
        "compacted source manifest must still schedule a vector index build"
    );
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
        .expect("index build task records source cursor");

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
        .expect("initial vector index build succeeds");
    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let signing_key = hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap();
    let proof = anvil::derived_index_proof::read_latest_derived_index_proof(
        &cluster.states[0].storage,
        &index_storage_id,
        &signing_key,
    )
    .await
    .unwrap()
    .expect("proof exists before deleting segment");
    assert!(!proof.segment_hashes.is_empty());
    let core_store = anvil::core_store::CoreStore::new(cluster.states[0].storage.clone())
        .await
        .unwrap();
    while let Some(segment_ref) = anvil::vector_segment::latest_vector_segment_ref(
        &cluster.states[0].storage,
        &index_storage_id,
    )
    .await
    .unwrap()
    {
        core_store
            .delete_ref(&segment_ref, None, None, true)
            .await
            .expect("remove vector segment to force repair");
    }
    assert!(
        anvil::vector_segment::read_latest_vector_segment(
            &cluster.states[0].storage,
            &index_storage_id
        )
        .await
        .unwrap()
        .is_none(),
        "segment deletion must remove the queryable vector derived index"
    );

    let mut repair_client = RepairServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let report = repair_client
        .repair_index(authorized(
            RepairIndexRequest {
                bucket_name: bucket_name.clone(),
                index_name: "embedding".to_string(),
                rebuild: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "rebuilt_derived_index");
    assert_eq!(report.reason, "DerivedIndexSegmentMissing");
    assert_eq!(report.index_storage_id, index_storage_id);
    assert_eq!(report.source_cursor_low, source_cursor);
    assert_eq!(report.source_cursor_high, 0);
    assert!(report.finding.is_some());
    assert!(report.build.is_some());

    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();
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
    assert_eq!(response.index_kind, IndexKind::Vector as i32);
    assert!(response.index_generation >= 1);
    assert!(
        response
            .hits
            .iter()
            .any(|hit| hit.object_key == "vectors/repair.json"),
        "rebuilt vector index must be queryable from base metadata and payload journals"
    );
}

#[tokio::test]
async fn test_index_build_followup_waits_for_running_build_and_catches_up_after_restart() {
    let cluster = TestCluster::new(&["test-region-1"]).await;
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = format!("index-handoff-{}", uuid::Uuid::new_v4());
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
            &hex::encode([11; 32]),
            20,
            "etag-alpha",
            Some("text/plain"),
            None,
            None,
            Some(b"alpha handoff first".to_vec()),
        )
        .await
        .unwrap();
    let running = persistence.claim_pending_tasks(10).await.unwrap();
    assert_eq!(running.len(), 1);
    assert_eq!(running[0].task_type, anvil::tasks::TaskType::IndexBuild);
    let first_cursor = running[0].payload["source_cursor"].as_u64().unwrap();

    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "docs/bravo.txt",
            &hex::encode([12; 32]),
            21,
            "etag-bravo",
            Some("text/plain"),
            None,
            None,
            Some(b"bravo handoff followup".to_vec()),
        )
        .await
        .unwrap();
    assert!(
        persistence
            .claim_pending_tasks(10)
            .await
            .unwrap()
            .is_empty(),
        "follow-up for a running index build must wait for the active build"
    );

    let restarted = anvil::persistence::Persistence::new(&cluster.states[0].config, None).unwrap();
    restarted
        .build_index_task(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(first_cursor),
        )
        .await
        .unwrap()
        .expect("first build succeeds after restart");
    restarted
        .update_task_status(running[0].id, anvil::tasks::TaskStatus::Completed)
        .await
        .unwrap();

    let followup = restarted.claim_pending_tasks(10).await.unwrap();
    assert_eq!(followup.len(), 1);
    let followup_cursor = followup[0].payload["source_cursor"].as_u64().unwrap();
    assert!(followup_cursor > first_cursor);
    restarted
        .build_index_task(
            tenant_id,
            bucket.id,
            index.id,
            index.version,
            u128::from(followup_cursor),
        )
        .await
        .unwrap()
        .expect("follow-up build succeeds");
    restarted
        .update_task_status(followup[0].id, anvil::tasks::TaskStatus::Completed)
        .await
        .unwrap();

    let index_storage_id = anvil::index_journal::index_storage_id(tenant_id, bucket.id, index.id);
    let segment = anvil::full_text_segment::read_latest_full_text_segment(
        &cluster.states[0].storage,
        &index_storage_id,
    )
    .await
    .unwrap()
    .expect("full text segment exists");
    assert_eq!(segment.header.source_cursor, followup_cursor);
    let definition = anvil::formats::full_text::FullTextIndexDefinition::from_json(
        &serde_json::json!({"positions": true}),
    )
    .unwrap();
    for term in ["alpha", "bravo"] {
        let hits = query_full_text_segment(
            &segment,
            FullTextSegmentQuery {
                query: term,
                tokenizer: &definition.tokenizer,
                positions_enabled: definition.positions_enabled,
                phrase: false,
                bm25: anvil::formats::full_text::Bm25Config::default(),
                authorized_labels: None,
                limit: 10,
            },
        )
        .unwrap();
        assert!(!hits.is_empty(), "{term} should be present after catch-up");
    }
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
    let tasks = wait_for_index_build_task(&cluster, Duration::from_secs(10)).await;
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

    let tasks = wait_for_index_build_task_count(&cluster, Duration::from_secs(10), 1).await;
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
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
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
    let tasks = wait_for_index_build_task_count(&cluster, Duration::from_secs(10), 1).await;
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
                kind: IndexKind::FullText as i32,
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
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
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
            definition_hash: "blake3:test-definition",
            generation: 3,
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
    assert_eq!(first_page.index_generation, 3);
    assert_eq!(first_page.authz_revision, 21);
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
            Some(serde_json::json!({"tier": "gold", "kind": "note"})),
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
            Some(serde_json::json!({"tier": "silver", "kind": "note"})),
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
                limit: 1,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
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
                limit: 1,
                phrase: false,
                path_prefix: String::new(),
                metadata_filters_json: String::new(),
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
        .mint_token(
            "tuple-index-reader".to_string(),
            vec![format!("index:read|{bucket_name}")],
            claims.tenant_id,
        )
        .unwrap();
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

    let unspecified_kind = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "unspecified-kind".to_string(),
                kind: IndexKind::Unspecified as i32,
                selector_json: "{}".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: "{}".to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(unspecified_kind.code(), tonic::Code::InvalidArgument);
    assert_eq!(unspecified_kind.message(), "index kind is required");

    let invalid_kind = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-kind".to_string(),
                kind: 999,
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
    assert_eq!(invalid_kind.message(), "Invalid index kind");

    let invalid_full_text_policy = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-full-text-policy".to_string(),
                kind: IndexKind::FullText as i32,
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

    let mut valid_vector_policy = rfc_vector_policy(
        "object_body_utf8",
        "test_only",
        "text-embedding-v1",
        768,
        "text",
        "cosine",
    );
    valid_vector_policy["embedding"]["chunking"] = serde_json::json!({
        "strategy": "token_window",
        "max_tokens": 512,
        "overlap_tokens": 64
    });
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "valid-vector".to_string(),
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
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
                kind: IndexKind::Vector as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_utf8",
                    "test_only",
                    "text-embedding-v1",
                    0,
                    "text",
                    "cosine",
                )
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
                extractor_json: serde_json::json!({}).to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: rfc_vector_policy(
                    "object_body_utf8",
                    "test_only",
                    "",
                    768,
                    "text",
                    "cosine",
                )
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
                kind: IndexKind::FullText as i32,
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
                kind: IndexKind::FullText as i32,
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
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                    typed_predicates_json: String::new(),
                    typed_order_json: String::new(),
                    page_token: String::new(),
                    require_caught_up_to_watch_cursor: String::new(),
                    lag_timeout_ms: 0,
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

fn test_object_authz_label_hash(
    bucket: &anvil::persistence::Bucket,
    object: &anvil::persistence::Object,
) -> [u8; 32] {
    anvil::formats::hash32(
        format!(
            "tenant:{}:bucket:{}:object:{}:authz:{}",
            bucket.tenant_id, bucket.id, object.key, object.authz_revision
        )
        .as_bytes(),
    )
}

fn vector_entry_with_authz_label(
    vector_id: u64,
    object_version_id: [u8; 16],
    values: Vec<f32>,
    authz_label_hash: [u8; 32],
) -> VectorSegmentEntry {
    let mut entry = vector_entry(vector_id, object_version_id, values);
    entry.record.authz_label_hash = authz_label_hash;
    entry
}

fn vector_entry(
    vector_id: u64,
    object_version_id: [u8; 16],
    values: Vec<f32>,
) -> VectorSegmentEntry {
    VectorSegmentEntry {
        source_id_binary: vec![vector_id as u8],
        source_generation: vector_id,
        labels: Vec::new(),
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
