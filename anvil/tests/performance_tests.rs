use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    self, CreateBucketRequest, CreateIndexRequest, GetObjectRequest, IndexKind, ListObjectsRequest,
    NativeMutationContext, ObjectMetadata, PutObjectRequest, QueryIndexRequest, QueryIndexResponse,
};
use anvil_core::core_store::{
    AcquireFence, AppendStreamRecord, CompareAndSwapRef, CoreMutationBatch, CoreMutationOperation,
    CoreMutationPrecondition, CoreStore, GetBlob, PutBlob, ReadStream, ReleaseFence,
};
use anvil_core::storage::Storage;
use anvil_test_utils::{TestCluster, emit_test_timing};
use serde::Serialize;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tonic::Request;

fn perf_enabled() -> bool {
    std::env::var_os("ANVIL_RUN_PERF_TESTS").is_some()
}

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

#[derive(Debug, Serialize)]
struct PerfSample {
    name: String,
    duration_ms: f64,
}

#[derive(Debug, Default, Serialize)]
struct PerfReport {
    samples: Vec<PerfSample>,
}

impl PerfReport {
    async fn measure<F, Fut, T>(&mut self, name: &str, operation: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        let started_at = Instant::now();
        let result = operation().await;
        let elapsed = started_at.elapsed();
        emit_test_timing(format!("perf.{name}"), elapsed);
        anvil::perf::record_duration("anvil_perf_case", &[("case", name)], elapsed);
        self.samples.push(PerfSample {
            name: name.to_string(),
            duration_ms: elapsed.as_secs_f64() * 1000.0,
        });
        result
    }

    async fn write(&self) {
        let path = std::env::var("ANVIL_PERF_REPORT_PATH").unwrap_or_else(|_| {
            workspace_perf_path("performance-summary.json")
                .to_string_lossy()
                .into_owned()
        });
        if let Some(parent) = std::path::Path::new(&path).parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(&path, serde_json::to_vec_pretty(self).unwrap())
            .await
            .unwrap();
        anvil::perf::flush().await;
        eprintln!("[perf] wrote {path}");
    }
}

fn workspace_perf_path(file_name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("target")
        .join("anvil")
        .join("perf")
        .join(file_name)
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
    let started_at = Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempts = 0_u64;
    let mut last = None;
    while tokio::time::Instant::now() < deadline {
        attempts += 1;
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
            let elapsed = started_at.elapsed();
            emit_test_timing(
                format!("perf.query_index_until_hits expected={expected_hits} attempts={attempts}"),
                elapsed,
            );
            anvil::perf::record_duration(
                "anvil_perf_case",
                &[("case", "query_index_until_hits")],
                elapsed,
            );
            return response;
        }
        last = Some(response);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    last.unwrap_or_else(|| panic!("query did not execute before timeout"))
}

#[tokio::test]
async fn performance_native_api_smoke() {
    if !perf_enabled() {
        eprintln!("skipping performance_native_api_smoke; set ANVIL_RUN_PERF_TESTS=1");
        return;
    }

    let mut report = PerfReport::default();

    let method_suite_started_at = Instant::now();
    {
        let temp = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let store = CoreStore::new(storage).await.unwrap();
        let payload = vec![42_u8; 64 * 1024];

        let object_ref = report
            .measure("corestore_put_blob_64k", || async {
                store
                    .put_blob(PutBlob {
                        logical_name: "perf/blob/64k".to_string(),
                        bytes: payload.clone(),
                        boundary_values: Vec::new(),
                        region_id: "perf-region-1".to_string(),
                        mutation_id: format!("perf-put-blob-{}", uuid::Uuid::new_v4()),
                    })
                    .await
                    .unwrap()
            })
            .await;

        let fetched = report
            .measure("corestore_get_blob_64k", || async {
                store
                    .get_blob(GetBlob {
                        object_ref: object_ref.clone(),
                    })
                    .await
                    .unwrap()
            })
            .await;
        assert_eq!(fetched, payload);

        report
            .measure("corestore_append_stream_20", || async {
                for idx in 0..20 {
                    store
                        .append_stream(AppendStreamRecord {
                            stream_id: "perf/stream/events".to_string(),
                            partition_id: "perf".to_string(),
                            record_kind: "perf.event".to_string(),
                            payload: serde_json::to_vec(&serde_json::json!({
                                "idx": idx,
                                "payload": "method-level event"
                            }))
                            .unwrap(),
                            fence: None,
                            transaction_id: None,
                            idempotency_key: Some(format!("perf-stream-{idx}")),
                        })
                        .await
                        .unwrap();
                }
            })
            .await;

        let stream_records = report
            .measure("corestore_read_stream_20", || async {
                store
                    .read_stream(ReadStream {
                        stream_id: "perf/stream/events".to_string(),
                        after_sequence: 0,
                        limit: 100,
                    })
                    .await
                    .unwrap()
            })
            .await;
        assert_eq!(stream_records.len(), 20);

        let receipt = report
            .measure("corestore_cas_ref_create", || async {
                store
                    .compare_and_swap_ref(CompareAndSwapRef {
                        ref_name: "perf/ref/current".to_string(),
                        expected_generation: None,
                        expected_target: None,
                        require_absent: true,
                        require_present: false,
                        fence: None,
                        authz_revision: None,
                        source_watch_cursor: None,
                        new_target: format!("sha256:{:064x}", 1),
                        transaction_id: Some(format!("perf-cas-{}", uuid::Uuid::new_v4())),
                    })
                    .await
                    .unwrap()
            })
            .await;
        assert_eq!(receipt.generation, 1);

        let permit = report
            .measure("corestore_acquire_fence", || async {
                store
                    .acquire_fence(AcquireFence {
                        fence_name: "perf/fence/work".to_string(),
                        authenticated_principal: "perf-principal".to_string(),
                        ttl_ms: 30_000,
                    })
                    .await
                    .unwrap()
            })
            .await;

        report
            .measure("corestore_mutation_batch_ref_and_stream", || async {
                store
                    .commit_mutation_batch(CoreMutationBatch {
                        transaction_id: format!("perf-batch-{}", uuid::Uuid::new_v4()),
                        scope_partition: "perf".to_string(),
                        committed_by_principal: "perf-principal".to_string(),
                        preconditions: vec![CoreMutationPrecondition::Ref {
                            ref_name: "perf/ref/batch".to_string(),
                            expected_generation: None,
                            expected_target: None,
                            require_absent: true,
                            require_present: false,
                            fence: None,
                            authz_revision: None,
                            source_watch_cursor: None,
                        }],
                        operations: vec![
                            CoreMutationOperation::RefUpdate {
                                partition_id: "perf".to_string(),
                                ref_name: "perf/ref/batch".to_string(),
                                new_target: format!("sha256:{:064x}", 2),
                            },
                            CoreMutationOperation::StreamAppend {
                                partition_id: "perf".to_string(),
                                stream_id: "perf/stream/batched".to_string(),
                                record_kind: "perf.batch".to_string(),
                                payload: br#"{"batched":true}"#.to_vec(),
                                idempotency_key: Some("perf-batch-stream-0".to_string()),
                            },
                        ],
                    })
                    .await
                    .unwrap()
            })
            .await;

        report
            .measure("corestore_release_fence", || async {
                store
                    .release_fence(ReleaseFence {
                        fence_name: permit.fence_name.clone(),
                        authenticated_principal: permit.owner_principal.clone(),
                        fence_token: permit.fence_token,
                    })
                    .await
                    .unwrap();
            })
            .await;
    }
    let method_suite_elapsed = method_suite_started_at.elapsed();
    emit_test_timing("perf.corestore_method_suite", method_suite_elapsed);
    anvil::perf::record_duration(
        "anvil_perf_case",
        &[("case", "corestore_method_suite")],
        method_suite_elapsed,
    );
    report.samples.push(PerfSample {
        name: "corestore_method_suite".to_string(),
        duration_ms: method_suite_elapsed.as_secs_f64() * 1000.0,
    });

    let mut cluster = report
        .measure("cluster_start_single_node", || async {
            let mut cluster = TestCluster::new(&["perf-region-1"]).await;
            cluster.start_and_converge(Duration::from_secs(5)).await;
            cluster
        })
        .await;

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

    let bucket_name = format!("perf-bucket-{}", uuid::Uuid::new_v4());
    let bucket_id = report
        .measure("grpc_create_bucket", || async {
            bucket_client
                .create_bucket(authorized(
                    CreateBucketRequest {
                        bucket_name: bucket_name.clone(),
                        region: "perf-region-1".to_string(),
                    },
                    &token,
                ))
                .await
                .unwrap()
                .into_inner()
                .bucket_id
        })
        .await;

    report
        .measure("grpc_put_json_objects_10", || async {
            for i in 0..10 {
                put_json_object(
                    &mut object_client,
                    &token,
                    bucket_id,
                    &bucket_name,
                    &format!("queue/item-{i:02}.json"),
                    serde_json::json!({"state": {
                        "queue_name": "outbound",
                        "state": if i % 2 == 0 {"pending"} else {"failed"},
                        "available_at": format!("2026-07-03T10:{i:02}:00Z"),
                        "priority": i,
                        "item_id": format!("item-{i:02}")
                    }}),
                    &format!("perf-put-{i}"),
                )
                .await;
            }
        })
        .await;

    report
        .measure("grpc_get_object", || async {
            let response = object_client
                .get_object(authorized(
                    GetObjectRequest {
                        bucket_name: bucket_name.clone(),
                        object_key: "queue/item-00.json".to_string(),
                        version_id: None,
                    },
                    &token,
                ))
                .await
                .unwrap();
            let chunks = response.into_inner().message().await.unwrap();
            assert!(chunks.is_some());
        })
        .await;

    report
        .measure("grpc_list_objects", || async {
            let response = object_client
                .list_objects(authorized(
                    ListObjectsRequest {
                        bucket_name: bucket_name.clone(),
                        prefix: "queue/".to_string(),
                        delimiter: String::new(),
                        start_after: String::new(),
                        max_keys: 50,
                    },
                    &token,
                ))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.objects.len(), 10);
        })
        .await;

    report
        .measure("grpc_create_typed_json_index", || async {
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
        })
        .await;

    let first_page = report
        .measure("grpc_query_typed_json_caught_up", || async {
            query_index_until_hits(
                &mut index_client,
                &token,
                QueryIndexRequest {
                    bucket_name: bucket_name.clone(),
                    index_name: "due-work".to_string(),
                    query_text: String::new(),
                    query_vector: vec![],
                    limit: 3,
                    phrase: false,
                    path_prefix: "queue/".to_string(),
                    metadata_filters_json: String::new(),
                    typed_predicates_json: serde_json::json!([
                        {"field": "queue_name", "op": "eq", "value": "outbound"},
                        {"field": "state", "op": "in", "values": ["pending", "failed"]},
                        {"field": "available_at", "op": "lte", "value": "2026-07-03T10:09:00Z"}
                    ])
                    .to_string(),
                    typed_order_json: String::new(),
                    page_token: String::new(),
                    require_caught_up_to_watch_cursor: String::new(),
                    lag_timeout_ms: 0,
                },
                3,
                Duration::from_secs(10),
            )
            .await
        })
        .await;
    assert_eq!(first_page.hits.len(), 3);

    report.write().await;
    for node in cluster.nodes.drain(..) {
        node.abort();
    }
}
