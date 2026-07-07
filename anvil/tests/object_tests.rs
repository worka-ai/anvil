use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::coordination_service_client::CoordinationServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use anvil::anvil_api::{
    self, AbortMultipartRequest, AcquireTaskLeaseRequest, AppendStreamRecordRequest,
    CompareAndSwapManifestRequest, CompleteMultipartPart, CompleteMultipartRequest,
    ComposeObjectRequest, ComposeObjectSource, CopyObjectRequest, CreateAppendStreamRequest,
    CreateBucketRequest, CreateIndexRequest, DeleteObjectRequest, GetObjectRequest,
    HeadObjectRequest, IndexKind, InitiateMultipartRequest, LeaseFencePrecondition,
    ListObjectVersionsRequest, ListObjectsRequest, MutationBatchAppendStreamRecord,
    MutationBatchOperation, MutationBatchPatchJsonObject, MutationBatchRequest,
    NativeMutationContext, ObjectMetadata, PatchJsonObjectRequest, PutObjectRequest,
    ReadAppendStreamRequest, RepairDirectoryIndexRequest, SealAppendStreamSegmentRequest,
    TailAppendStreamRequest, UploadPartMetadata, UploadPartRequest, WatchPrefixRequest,
    WritePrecondition,
};
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

use anvil::observability::{
    OBJECT_READ_LATENCY, OBJECT_WRITE_LATENCY, PREFIX_LIST_LATENCY,
    RESERVED_NAMESPACE_REJECTION_COUNT,
};
use anvil::routing::CrossRegionRoutingPolicy;
use anvil::{
    auth::Claims,
    core_store::CoreStore,
    mesh_directory::{
        self, BucketId, BucketLocatorDescriptor, BucketName, CellId, MeshControlWriteAuthority,
        MeshId, RegionName, RoutingRecordFamily, TenantId,
    },
    partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    },
};
use anvil_test_utils::*;
use tonic::{Code, Status};

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

fn assert_reserved_namespace_status<T>(result: Result<T, Status>) {
    let err = match result {
        Ok(_) => panic!("reserved namespace operation must fail"),
        Err(err) => err,
    };
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(
        err.message().contains("UnauthorizedReservedNamespace"),
        "expected UnauthorizedReservedNamespace, got {err:?}"
    );
}

#[tokio::test]
async fn native_object_routes_use_mesh_locator_before_local_bucket_metadata() {
    let cluster = TestCluster::new_with_config(&["us-west-2"], |config| {
        config.cross_region_routing_policy = CrossRegionRoutingPolicy::ProxyRequired;
    })
    .await;
    let state = &cluster.states[0];
    let signing_key = hex::decode(&state.config.anvil_secret_encryption_key).unwrap();
    let bucket_name = BucketName::canonicalize("remote-bucket").unwrap();
    let locator = BucketLocatorDescriptor::active(
        MeshId::new("default").unwrap(),
        TenantId::new("1").unwrap(),
        bucket_name.clone(),
        BucketId::new("bucket-remote").unwrap(),
        RegionName::new("eu-west-1").unwrap(),
        CellId::new("default").unwrap(),
        "regional-primary",
        "objects/1/remote-bucket/",
        "2026-07-02T00:00:00Z",
    )
    .unwrap();
    let partition = locator.partition();
    let control_partition_id = mesh_directory::control_partition_id(
        RoutingRecordFamily::BucketLocator.stream_family(),
        &partition,
    );
    let recovering = acquire_partition_recovery(
        &state.storage,
        PartitionRecoveryAcquire {
            partition_family: mesh_directory::CONTROL_PARTITION_FAMILY.to_string(),
            partition_id: control_partition_id,
            owner_node_id: state.config.node_id.clone(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: 1,
        },
        &signing_key,
    )
    .await
    .unwrap();
    let ready = publish_partition_ready(
        &state.storage,
        &recovering.partition_family,
        &recovering.partition_id,
        &state.config.node_id,
        recovering.fence_token,
        0,
        &hex::encode([0; 32]),
        2,
        &signing_key,
    )
    .await
    .unwrap();
    mesh_directory::write_bucket_locator(
        &state.storage,
        &locator,
        MeshControlWriteAuthority {
            permit: &ready.write_permit().unwrap(),
            signing_key: &signing_key,
        },
    )
    .await
    .unwrap();

    let err = state
        .object_manager
        .list_objects_for_tenant(
            Some(Claims {
                sub: "test-app".to_string(),
                exp: usize::MAX,
                scopes: vec!["*|*".to_string()],
                tenant_id: 1,
                jti: None,
            }),
            Some(1),
            bucket_name.as_str(),
            "",
            "",
            10,
            "",
        )
        .await
        .unwrap_err();

    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(
        err.metadata()
            .get("x-anvil-bucket-region")
            .unwrap()
            .to_str()
            .unwrap(),
        "eu-west-1"
    );
    assert_eq!(
        err.metadata()
            .get("x-anvil-cross-region-action")
            .unwrap()
            .to_str()
            .unwrap(),
        "proxy_unavailable"
    );

    let write_err = state
        .object_manager
        .initiate_multipart_upload(1, bucket_name.as_str(), "upload.bin", &["*|*".to_string()])
        .await
        .unwrap_err();
    assert_eq!(write_err.code(), Code::Unavailable);
    assert_eq!(
        write_err
            .metadata()
            .get("x-anvil-bucket-region")
            .unwrap()
            .to_str()
            .unwrap(),
        "eu-west-1"
    );
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

fn native_mutation_context_with_precondition(
    bucket_id: i64,
    tag: &str,
    precondition: &str,
) -> NativeMutationContext {
    let mut context = native_mutation_context(bucket_id, tag);
    context.precondition = precondition.to_string();
    context
}

fn put_object_chunks(
    bucket_name: &str,
    object_key: &str,
    payload: &[u8],
    mutation_context: Option<NativeMutationContext>,
) -> Vec<PutObjectRequest> {
    vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    mutation_context,
                    content_type: None,
                    user_metadata_json: String::new(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                payload.to_vec(),
            )),
        },
    ]
}

async fn put_object_for_test(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    object_key: &str,
    payload: &[u8],
    mutation_context: NativeMutationContext,
) -> Result<anvil_api::PutObjectResponse, Status> {
    let request = authorized(
        tokio_stream::iter(put_object_chunks(
            bucket_name,
            object_key,
            payload,
            Some(mutation_context),
        )),
        token,
    );
    object_client
        .put_object(request)
        .await
        .map(|response| response.into_inner())
}

async fn get_object_bytes_for_test(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    object_key: &str,
    version_id: Option<String>,
) -> Vec<u8> {
    let mut stream = object_client
        .get_object(authorized(
            GetObjectRequest {
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
                version_id,
                range: None,
            },
            token,
        ))
        .await
        .expect("get object")
        .into_inner();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("object chunk");
        if let Some(anvil_api::get_object_response::Data::Chunk(data)) = chunk.data {
            bytes.extend_from_slice(&data);
        }
    }
    bytes
}

async fn get_object_metadata_and_bytes_for_test(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_name: &str,
    object_key: &str,
    version_id: Option<String>,
) -> (anvil_api::ObjectInfo, Vec<u8>) {
    let mut stream = object_client
        .get_object(authorized(
            GetObjectRequest {
                bucket_name: bucket_name.to_string(),
                object_key: object_key.to_string(),
                version_id,
                range: None,
            },
            token,
        ))
        .await
        .expect("get object")
        .into_inner();
    let mut metadata = None;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.expect("object chunk");
        match chunk.data {
            Some(anvil_api::get_object_response::Data::Metadata(info)) => {
                metadata = Some(info);
            }
            Some(anvil_api::get_object_response::Data::Chunk(data)) => {
                bytes.extend_from_slice(&data);
            }
            None => {}
        }
    }
    (metadata.expect("get object metadata"), bytes)
}

macro_rules! assert_native_mutation_response {
    ($response:expr) => {{
        assert!(!$response.mutation_id.is_empty());
        assert!(!$response.payload_hash.is_empty());
        assert!(!$response.record_hash.is_empty());
        assert!($response.watch_cursor > 0);
    }};
}

#[tokio::test]
async fn native_object_routes_apply_cross_region_policy_before_local_metadata() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: "remote-bucket".to_string(),
                region: "test-region-2".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let err = object_client
        .get_object(authorized(
            GetObjectRequest {
                bucket_name: "remote-bucket".to_string(),
                object_key: "any.txt".to_string(),
                version_id: None,
                range: None,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(
        err.metadata().get("x-anvil-bucket-region").unwrap(),
        "test-region-2"
    );
    assert_eq!(
        err.metadata().get("x-anvil-cross-region-action").unwrap(),
        "redirect"
    );
}

#[tokio::test]
async fn native_object_routes_report_proxy_required_as_unavailable_when_proxy_is_absent() {
    let mut cluster = TestCluster::new_with_config(&["test-region-1"], |config| {
        config.cross_region_routing_policy =
            anvil::routing::CrossRegionRoutingPolicy::ProxyRequired;
    })
    .await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: "remote-bucket".to_string(),
                region: "test-region-2".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let err = object_client
        .list_objects(authorized(
            ListObjectsRequest {
                bucket_name: "remote-bucket".to_string(),
                prefix: String::new(),
                start_after: String::new(),
                delimiter: String::new(),
                max_keys: 100,
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unavailable);
    assert_eq!(
        err.metadata().get("x-anvil-bucket-region").unwrap(),
        "test-region-2"
    );
    assert_eq!(
        err.metadata().get("x-anvil-cross-region-action").unwrap(),
        "proxy_unavailable"
    );
}

#[tokio::test]
async fn test_native_mutations_require_valid_context() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();
    let bucket_name = format!("native-context-{}", uuid::Uuid::new_v4());

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let mut missing_context_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "missing-context.txt",
        b"missing",
        None,
    )));
    missing_context_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(missing_context_req)
        .await
        .expect_err("native mutation without context must fail");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("Missing native mutation context"));

    let mut wrong_principal = native_mutation_context(bucket_id, "wrong-principal");
    wrong_principal.principal = "other-app".to_string();
    let mut wrong_principal_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "wrong-principal.txt",
        b"wrong",
        Some(wrong_principal),
    )));
    wrong_principal_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(wrong_principal_req)
        .await
        .expect_err("native mutation with mismatched principal must fail");
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(err.message().contains("principal mismatch"));

    let mut wrong_tenant = native_mutation_context(bucket_id, "wrong-tenant");
    wrong_tenant.tenant_id = 2;
    let mut wrong_tenant_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "wrong-tenant.txt",
        b"wrong",
        Some(wrong_tenant),
    )));
    wrong_tenant_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(wrong_tenant_req)
        .await
        .expect_err("native mutation with mismatched tenant must fail");
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(err.message().contains("tenant mismatch"));

    let mut wrong_bucket = native_mutation_context(bucket_id + 1, "wrong-bucket");
    let mut wrong_bucket_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "wrong-bucket.txt",
        b"wrong",
        Some(wrong_bucket.clone()),
    )));
    wrong_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(wrong_bucket_req)
        .await
        .expect_err("native mutation with mismatched bucket must fail");
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(err.message().contains("bucket mismatch"));

    wrong_bucket.bucket_id = bucket_id;
    wrong_bucket.request_id.clear();
    let mut blank_field_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "blank-request-id.txt",
        b"wrong",
        Some(wrong_bucket),
    )));
    blank_field_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(blank_field_req)
        .await
        .expect_err("native mutation with blank request_id must fail");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("request_id"));

    let mut stale_zookie = native_mutation_context(bucket_id, "stale-zookie");
    stale_zookie.authz_zookie_optional = "authz:999999".to_string();
    let mut stale_zookie_req = Request::new(tokio_stream::iter(put_object_chunks(
        &bucket_name,
        "stale-zookie.txt",
        b"wrong",
        Some(stale_zookie),
    )));
    stale_zookie_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = object_client
        .put_object(stale_zookie_req)
        .await
        .expect_err("native mutation with unavailable authz revision must fail");
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("AuthzRevisionUnavailable"));
}

#[tokio::test]
async fn test_native_object_mutation_preconditions_are_enforced() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();
    let bucket_name = format!("native-preconditions-{}", uuid::Uuid::new_v4());
    let object_key = "docs/preconditioned.txt";

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

    let first = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"first",
        native_mutation_context_with_precondition(bucket_id, "first", "not_exists"),
    )
    .await
    .expect("not_exists precondition should allow initial object creation");
    assert_native_mutation_response!(first);

    let duplicate = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"duplicate",
        native_mutation_context_with_precondition(bucket_id, "duplicate", "not_exists"),
    )
    .await
    .expect_err("not_exists precondition must reject an existing object");
    assert_eq!(duplicate.code(), Code::FailedPrecondition);
    assert!(duplicate.message().contains("precondition failed"));

    let wrong_etag = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"wrong-etag",
        native_mutation_context_with_precondition(bucket_id, "wrong-etag", "etag:not-current"),
    )
    .await
    .expect_err("etag precondition must reject a mismatched object etag");
    assert_eq!(wrong_etag.code(), Code::FailedPrecondition);

    let second = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"second",
        native_mutation_context_with_precondition(
            bucket_id,
            "matching-etag",
            &format!("etag:\"{}\"", first.etag),
        ),
    )
    .await
    .expect("etag precondition should allow matching object replacement");
    assert_native_mutation_response!(second);

    let third = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"third",
        native_mutation_context_with_precondition(
            bucket_id,
            "matching-version",
            &format!("version:{}", second.version_id),
        ),
    )
    .await
    .expect("version precondition should allow matching object replacement");
    assert_native_mutation_response!(third);

    let unsupported = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"unsupported",
        native_mutation_context_with_precondition(bucket_id, "unsupported", "after:123"),
    )
    .await
    .expect_err("unsupported native precondition syntax must fail");
    assert_eq!(unsupported.code(), Code::InvalidArgument);
    assert!(unsupported.message().contains("Unsupported"));

    let delete_response = object_client
        .delete_object(authorized(
            DeleteObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
                mutation_context: Some(native_mutation_context_with_precondition(
                    bucket_id,
                    "delete-existing",
                    "exists",
                )),
            },
            &token,
        ))
        .await
        .expect("exists precondition should allow deleting current object")
        .into_inner();
    assert!(!delete_response.mutation_id.is_empty());
    assert!(!delete_response.record_hash.is_empty());
    assert!(delete_response.watch_cursor > third.watch_cursor);
    assert!(delete_response.delete_marker);

    let recreated = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"recreated",
        native_mutation_context_with_precondition(bucket_id, "recreated", "not_exists"),
    )
    .await
    .expect("not_exists should treat the current delete marker as absent");
    assert_native_mutation_response!(recreated);
}

#[tokio::test]
async fn test_native_object_mutation_idempotency_replays_without_duplicate_mutation() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();
    let bucket_name = format!("native-idempotency-{}", uuid::Uuid::new_v4());
    let object_key = "docs/idempotent.txt";

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

    let put_context = native_mutation_context(bucket_id, "idempotent-put");
    let put_idempotency_key = put_context.idempotency_key.clone();
    let first = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"first-payload",
        put_context.clone(),
    )
    .await
    .expect("first idempotent put should succeed");
    let replayed = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"second-payload-must-not-be-written",
        put_context,
    )
    .await
    .expect("second idempotent put should replay");
    assert_eq!(replayed, first);

    let downloaded = get_object_bytes_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        Some(first.version_id.clone()),
    )
    .await;
    assert_eq!(downloaded, b"first-payload");

    let versions = object_client
        .list_object_versions(authorized(
            ListObjectVersionsRequest {
                bucket_name: bucket_name.clone(),
                prefix: object_key.to_string(),
                key_marker: String::new(),
                max_keys: 10,
                version_id_marker: String::new(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(
        versions.len(),
        1,
        "idempotent replay must not add a version"
    );
    assert_eq!(versions[0].version_id, first.version_id);

    let mut reused_context = native_mutation_context(bucket_id, "reused-target");
    reused_context.idempotency_key = put_idempotency_key;
    let conflict = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        "docs/other-target.txt",
        b"other",
        reused_context,
    )
    .await
    .expect_err("idempotency key reuse against a different target must fail");
    assert_eq!(conflict.code(), Code::FailedPrecondition);
    assert!(conflict.message().contains("different mutation target"));

    let delete_context = native_mutation_context(bucket_id, "idempotent-delete");
    let delete_first = object_client
        .delete_object(authorized(
            DeleteObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
                mutation_context: Some(delete_context.clone()),
            },
            &token,
        ))
        .await
        .expect("first idempotent delete should succeed")
        .into_inner();
    let delete_replayed = object_client
        .delete_object(authorized(
            DeleteObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
                mutation_context: Some(delete_context),
            },
            &token,
        ))
        .await
        .expect("second idempotent delete should replay")
        .into_inner();
    assert_eq!(delete_replayed, delete_first);
    assert!(delete_replayed.delete_marker);

    let versions_after_delete = object_client
        .list_object_versions(authorized(
            ListObjectVersionsRequest {
                bucket_name,
                prefix: object_key.to_string(),
                key_marker: String::new(),
                max_keys: 10,
                version_id_marker: String::new(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(
        versions_after_delete.len(),
        2,
        "idempotent delete replay must not add another delete marker"
    );
    assert_eq!(
        versions_after_delete
            .iter()
            .filter(|version| version.is_delete_marker)
            .count(),
        1
    );
}

#[tokio::test]
async fn test_repair_rebuilds_missing_directory_segment_from_metadata_journal() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = format!("directory-repair-{}", uuid::Uuid::new_v4());
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "docs/a.txt",
            &hex::encode([41; 32]),
            12,
            "etag-a",
            Some("text/plain"),
            None,
            None,
            Some(b"directory-a".to_vec()),
        )
        .await
        .unwrap();
    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "docs/b.txt",
            &hex::encode([42; 32]),
            12,
            "etag-b",
            Some("text/plain"),
            None,
            None,
            Some(b"directory-b".to_vec()),
        )
        .await
        .unwrap();
    let sealed = persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata compaction writes directory segment");
    let directory_ref_name = sealed
        .directory_ref
        .strip_prefix("coreref:")
        .expect("directory segment ref should be a CoreStore ref");
    let store = CoreStore::new(cluster.states[0].storage.clone())
        .await
        .unwrap();
    store
        .delete_ref(directory_ref_name, None, None, true)
        .await
        .expect("remove directory segment ref to force repair");

    let mut repair_client = RepairServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let report = repair_client
        .repair_directory_index(authorized(
            RepairDirectoryIndexRequest {
                bucket_name: bucket_name.clone(),
                rebuild: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "needs_repair");
    assert_eq!(report.reason, "DirectoryIndexInvalid");
    assert_eq!(report.expected_entry_count, 2);
    assert!(report.finding.is_some());

    let rebuilt = repair_client
        .repair_directory_index(authorized(
            RepairDirectoryIndexRequest {
                bucket_name: bucket_name.clone(),
                rebuild: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rebuilt.status, "rebuilt_directory_index");
    assert_eq!(rebuilt.reason, "DirectoryIndexInvalid");
    assert_eq!(rebuilt.expected_entry_count, 2);
    assert!(!rebuilt.rebuilt_manifest_hash.is_empty());

    let mut object_client = ObjectServiceClient::connect(grpc_addr).await.unwrap();
    let listed = object_client
        .list_objects(authorized(
            ListObjectsRequest {
                bucket_name,
                prefix: "docs/".to_string(),
                start_after: String::new(),
                max_keys: 10,
                delimiter: String::new(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        listed
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/a.txt", "docs/b.txt"]
    );
}

#[tokio::test]
async fn test_delete_object_creates_delete_marker() {
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

    let bucket_name = "test-delete-bucket".to_string();
    let object_key = "test-delete-object".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"delete me".to_vec(),
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
    assert!(put_res.watch_cursor > 0);

    // 2. Verify it exists
    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res.objects.len(), 1);

    // 3. Delete the object
    let mut del_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        mutation_context: Some(native_mutation_context(bucket_id, "delete-object")),
    });
    del_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let delete_res = object_client
        .delete_object(del_req)
        .await
        .unwrap()
        .into_inner();
    assert!(delete_res.watch_cursor > put_res.watch_cursor);
    assert!(delete_res.delete_marker);
    assert!(!delete_res.version_id.is_empty());
    assert!(!delete_res.mutation_id.is_empty());
    assert!(!delete_res.record_hash.is_empty());

    // 4. Verify it is gone from listings (soft deleted)
    let mut list_req_after_delete = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    });
    list_req_after_delete.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res_after_delete = object_client
        .list_objects(list_req_after_delete)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res_after_delete.objects.len(), 0);

    // 5. Verify versions retain the original object plus a latest delete marker.
    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let versions = object_client
        .list_object_versions(versions_req)
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(versions.len(), 2);
    assert!(versions[0].is_delete_marker);
    assert!(versions[0].is_latest);
    assert!(!versions[1].is_delete_marker);
    assert!(!versions[1].is_latest);
}

#[tokio::test]
async fn test_delete_object_specific_version_removes_only_that_version() {
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

    let bucket_name = "test-delete-specific-version".to_string();
    let object_key = "versioned-object".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let first_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"v1".to_vec(),
            )),
        },
    ];
    let mut first_put_req = Request::new(tokio_stream::iter(first_chunks));
    first_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first_put = object_client
        .put_object(first_put_req)
        .await
        .unwrap()
        .into_inner();

    let second_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"v2".to_vec(),
            )),
        },
    ];
    let mut second_put_req = Request::new(tokio_stream::iter(second_chunks));
    second_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second_put = object_client
        .put_object(second_put_req)
        .await
        .unwrap()
        .into_inner();

    let mut first_page_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 1,
        version_id_marker: String::new(),
    });
    first_page_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first_page = object_client
        .list_object_versions(first_page_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_page.versions.len(), 1);
    assert_eq!(first_page.versions[0].version_id, second_put.version_id);
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker, object_key);
    assert_eq!(first_page.next_version_id_marker, second_put.version_id);

    let mut second_page_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: first_page.next_key_marker,
        max_keys: 1,
        version_id_marker: first_page.next_version_id_marker,
    });
    second_page_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second_page = object_client
        .list_object_versions(second_page_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.versions.len(), 1);
    assert_eq!(second_page.versions[0].version_id, first_put.version_id);
    assert!(!second_page.is_truncated);
    assert!(second_page.next_key_marker.is_empty());
    assert!(second_page.next_version_id_marker.is_empty());

    let mut delete_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(first_put.version_id.clone()),
        mutation_context: Some(native_mutation_context(bucket_id, "delete-object")),
    });
    delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.delete_object(delete_req).await.unwrap();

    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let versions = object_client
        .list_object_versions(versions_req)
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, second_put.version_id);
    assert!(versions[0].is_latest);
    assert!(!versions[0].is_delete_marker);
}

#[tokio::test]
async fn test_get_object_without_version_id_returns_latest_version() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = format!("latest-get-{}", uuid::Uuid::new_v4());
    let object_key = "docs/versioned.txt";

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

    let first = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"version-one",
        native_mutation_context(bucket_id, "put-first"),
    )
    .await
    .unwrap();
    let second = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"version-two",
        native_mutation_context(bucket_id, "put-second"),
    )
    .await
    .unwrap();
    let latest = put_object_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        b"version-three-latest",
        native_mutation_context(bucket_id, "put-latest"),
    )
    .await
    .unwrap();

    let head = object_client
        .head_object(authorized(
            HeadObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.to_string(),
                version_id: None,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(head.version_id, latest.version_id);

    let versions = object_client
        .list_object_versions(authorized(
            ListObjectVersionsRequest {
                bucket_name: bucket_name.clone(),
                prefix: object_key.to_string(),
                key_marker: String::new(),
                max_keys: 100,
                version_id_marker: String::new(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(
        versions
            .iter()
            .map(|version| version.version_id.as_str())
            .collect::<Vec<_>>(),
        vec![
            latest.version_id.as_str(),
            second.version_id.as_str(),
            first.version_id.as_str()
        ]
    );
    assert!(versions[0].is_latest);
    assert!(versions[1..].iter().all(|version| !version.is_latest));

    let (metadata, downloaded) = get_object_metadata_and_bytes_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        None,
    )
    .await;
    assert_eq!(metadata.version_id, latest.version_id);
    assert_eq!(metadata.content_length, "version-three-latest".len() as i64);
    assert_eq!(downloaded, b"version-three-latest");

    let (first_metadata, first_downloaded) = get_object_metadata_and_bytes_for_test(
        &mut object_client,
        &token,
        &bucket_name,
        object_key,
        Some(first.version_id.clone()),
    )
    .await;
    assert_eq!(first_metadata.version_id, first.version_id);
    assert_eq!(first_downloaded, b"version-one");
}

#[tokio::test]
async fn test_utf8_object_keys_with_spaces_round_trip() {
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

    let bucket_name = "test-utf8-object-keys".to_string();
    let object_key = "folder/my café document 📄.txt".to_string();
    let payload = b"utf8 object key payload".to_vec();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(payload.clone())),
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
    assert!(!put_res.version_id.is_empty());

    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: "folder/".to_string(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 10,
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res.objects.len(), 1);
    assert_eq!(list_res.objects[0].key, object_key);

    let literal_prefix = "literal/a%_";
    let literal_key = "literal/a%_object.txt";
    let wildcard_decoy_key = "literal/abc-object.txt";
    for (key, body) in [
        (literal_key, b"literal wildcard key".to_vec()),
        (wildcard_decoy_key, b"decoy key".to_vec()),
    ] {
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Metadata(
                    ObjectMetadata {
                        bucket_name: bucket_name.clone(),
                        object_key: key.to_string(),
                        mutation_context: Some(native_mutation_context(
                            bucket_id,
                            "object-metadata",
                        )),
                        content_type: None,
                        user_metadata_json: String::new(),
                    },
                )),
            },
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Chunk(body)),
            },
        ];
        let mut put_req = Request::new(tokio_stream::iter(chunks));
        put_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        object_client.put_object(put_req).await.unwrap();
    }

    let mut wildcard_prefix_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: literal_prefix.to_string(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 10,
    });
    wildcard_prefix_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let wildcard_prefix_list = object_client
        .list_objects(wildcard_prefix_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(wildcard_prefix_list.objects.len(), 1);
    assert_eq!(wildcard_prefix_list.objects[0].key, literal_key);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name,
        object_key,
        version_id: None,
        range: None,
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
    assert_eq!(downloaded, payload);
}

#[tokio::test]
async fn test_listing_omits_reserved_internal_object_keys() {
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

    let bucket_name = "test-listing-hides-internal".to_string();
    let visible_key = "visible/object.txt".to_string();
    let reserved_key = "_anvil/authz/tuples".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: visible_key.clone(),
                    mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
                    content_type: None,
                    user_metadata_json: String::new(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"visible".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    let bucket_id = bucket.id;
    let tenant_id = bucket.tenant_id;
    cluster.states[0]
        .persistence
        .create_object(
            tenant_id,
            bucket_id,
            &reserved_key,
            "reserved-payload-hash",
            0,
            "reserved-etag",
            None,
            None,
            None,
            Some(Vec::new()),
        )
        .await
        .unwrap();

    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let objects = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner()
        .objects;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key, visible_key);

    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name,
        prefix: String::new(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let versions = object_client
        .list_object_versions(versions_req)
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].key, visible_key);
}

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
        .expect("inline object should record a CoreStore object ref");
    assert_eq!(inline_shard_map["schema"], "anvil.core.object_ref.v1");
    assert_eq!(
        inline_shard_map["logical_size"].as_u64(),
        Some(inline_content.len() as u64)
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
        .expect("external object should record a CoreStore object ref");
    assert_eq!(external_shard_map["schema"], "anvil.core.object_ref.v1");
    assert_eq!(
        external_shard_map["logical_size"].as_u64(),
        Some(external_content.len() as u64)
    );

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: inline_key,
        version_id: Some(inline_put.version_id),
        range: None,
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

#[tokio::test]
async fn test_copy_object_creates_independent_destination_version() {
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

    let bucket_name = "test-copy-bucket".to_string();
    let source_key = "source.txt".to_string();
    let destination_key = "destination.txt".to_string();
    let content = b"copy native object";

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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
        mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
        content_type: None,
        user_metadata_json: String::new(),
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
        mutation_context: Some(native_mutation_context(bucket_id, "copy-object")),
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
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "test-denied-before-payload-load".to_string();
    let object_key = "private/missing-payload.bin".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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
        )
        .await
        .unwrap();

    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "limited-object-reader".to_string(),
            vec![format!("object:list|{bucket_name}")],
            claims.tenant_id,
        )
        .unwrap();

    let mut denied_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
        range: None,
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
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut watch_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-watch-bucket".to_string();
    let object_key = "docs/a.txt".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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
        mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
        content_type: None,
        user_metadata_json: String::new(),
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
        mutation_context: Some(native_mutation_context(bucket_id, "delete-object")),
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

    let bucket_name = "test-append-bucket".to_string();
    let stream_key = "events/topic-a".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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
        mutation_context: Some(native_mutation_context(bucket_id, "create-append-stream")),
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
        mutation_context: Some(native_mutation_context(bucket_id, "append-stream-record")),
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
        mutation_context: Some(native_mutation_context(bucket_id, "append-stream-record")),
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
        mutation_context: Some(native_mutation_context(bucket_id, "seal-append-stream")),
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
        mutation_context: Some(native_mutation_context(bucket_id, "append-stream-record")),
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
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "test-object-metadata-bucket".to_string();
    let object_key = "catalog/item.json".to_string();
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

    let user_metadata = serde_json::json!({"queue": "outbound", "state": "pending"}).to_string();
    let metadata = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                mutation_context: Some(native_mutation_context(bucket_id, "metadata-roundtrip")),
                content_type: Some("application/json".to_string()),
                user_metadata_json: user_metadata.clone(),
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

#[tokio::test]
async fn test_mutation_batch_rejects_stale_lease_fence_for_state_update() {
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
    let mut coordination_client = CoordinationServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "test-fenced-batch-bucket".to_string();
    let object_key = "queue/item-1.json".to_string();
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

    let metadata = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                mutation_context: Some(native_mutation_context(bucket_id, "fenced-seed")),
                content_type: Some("application/json".to_string()),
                user_metadata_json: String::new(),
            },
        )),
    };
    let chunk = PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Chunk(
            br#"{"state":{"state":"pending"}}"#.to_vec(),
        )),
    };
    object_client
        .put_object(authorized(
            tokio_stream::iter(vec![metadata, chunk]),
            &token,
        ))
        .await
        .unwrap();

    let task_id = "queue-item-1".to_string();
    let lease = coordination_client
        .acquire_task_lease(authorized(
            AcquireTaskLeaseRequest {
                task_id: task_id.clone(),
                task_kind: "queue_item".to_string(),
                partition_family: "queue".to_string(),
                partition_id: hex::encode([1_u8; 32]),
                owner_label: "worker-a".to_string(),
                source_cursor_low: 0,
                source_cursor_high: 0,
                requested_ttl_nanos: 60_000_000_000,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .lease
        .unwrap();

    let batch = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: bucket_name.clone(),
                mutation_context: Some(native_mutation_context(bucket_id, "fenced-batch-claim")),
                precondition: Some(WritePrecondition {
                    object_versions: vec![],
                    lease_fence: Some(LeaseFencePrecondition {
                        task_id: task_id.clone(),
                        fence_token: lease.fence_token,
                    }),
                }),
                operations: vec![MutationBatchOperation {
                    op: Some(anvil_api::mutation_batch_operation::Op::PatchJsonObject(
                        MutationBatchPatchJsonObject {
                            object_key: object_key.clone(),
                            base_version_id: None,
                            merge_patch_json: serde_json::json!({
                                "state": {"state": "leased"}
                            })
                            .to_string(),
                        },
                    )),
                }],
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(batch.operation_receipts.len(), 1);

    let stream_key = "queue/item-1-attempts".to_string();
    let create_stream = object_client
        .create_append_stream(authorized(
            CreateAppendStreamRequest {
                bucket_name: bucket_name.clone(),
                stream_key: stream_key.clone(),
                mutation_context: Some(native_mutation_context(
                    bucket_id,
                    "fenced-batch-create-stream",
                )),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let stream_id = create_stream.stream_id;

    coordination_client
        .commit_task_lease(authorized(
            anvil_api::CommitTaskLeaseRequest {
                task_id: task_id.clone(),
                fence_token: lease.fence_token,
                committed_cursor_low: 1,
                committed_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap();

    let stale = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name: bucket_name.clone(),
                mutation_context: Some(native_mutation_context(bucket_id, "fenced-batch-stale")),
                precondition: Some(WritePrecondition {
                    object_versions: vec![],
                    lease_fence: Some(LeaseFencePrecondition {
                        task_id: task_id.clone(),
                        fence_token: lease.fence_token,
                    }),
                }),
                operations: vec![MutationBatchOperation {
                    op: Some(anvil_api::mutation_batch_operation::Op::PatchJsonObject(
                        MutationBatchPatchJsonObject {
                            object_key: object_key.clone(),
                            base_version_id: None,
                            merge_patch_json: serde_json::json!({
                                "state": {"state": "completed"}
                            })
                            .to_string(),
                        },
                    )),
                }],
            },
            &token,
        ))
        .await;
    assert!(stale.is_err());

    let stale_append = object_client
        .mutation_batch(authorized(
            MutationBatchRequest {
                bucket_name,
                mutation_context: Some(native_mutation_context(
                    bucket_id,
                    "fenced-batch-stale-append",
                )),
                precondition: Some(WritePrecondition {
                    object_versions: vec![],
                    lease_fence: Some(LeaseFencePrecondition {
                        task_id,
                        fence_token: lease.fence_token,
                    }),
                }),
                operations: vec![MutationBatchOperation {
                    op: Some(anvil_api::mutation_batch_operation::Op::AppendStreamRecord(
                        MutationBatchAppendStreamRecord {
                            stream_key,
                            stream_id,
                            payload: br#"{"attempt":1}"#.to_vec(),
                            content_type: Some("application/json".to_string()),
                            user_metadata_json: String::new(),
                        },
                    )),
                }],
            },
            &token,
        ))
        .await
        .expect_err("stale lease fence must not append a protected stream record");
    assert_eq!(stale_append.code(), tonic::Code::FailedPrecondition);
    assert_eq!(stale_append.message(), "LeaseExpired");
}

#[tokio::test]
async fn test_compare_and_swap_manifest_enforces_expected_revision() {
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

    let bucket_name = "test-manifest-bucket".to_string();
    let manifest_key = "manifests/current.json".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let mut create_manifest = Request::new(CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: manifest_key.clone(),
        expected_revision: 0,
        manifest_json: serde_json::json!({"generation": 1}).to_string(),
        mutation_context: Some(native_mutation_context(
            bucket_id,
            "compare-and-swap-manifest",
        )),
        precondition: None,
    });
    create_manifest.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first = object_client
        .compare_and_swap_manifest(create_manifest)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(first);
    assert_eq!(first.revision, 1);
    assert_eq!(first.version_id, "1");
    assert!(!first.manifest_hash.is_empty());

    let mut stale_update = Request::new(CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: manifest_key.clone(),
        expected_revision: 0,
        manifest_json: serde_json::json!({"generation": 2}).to_string(),
        mutation_context: Some(native_mutation_context(
            bucket_id,
            "compare-and-swap-manifest",
        )),
        precondition: None,
    });
    stale_update.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert!(
        object_client
            .compare_and_swap_manifest(stale_update)
            .await
            .is_err()
    );

    let mut valid_update = Request::new(CompareAndSwapManifestRequest {
        bucket_name,
        manifest_key,
        expected_revision: first.revision,
        manifest_json: serde_json::json!({"generation": 2}).to_string(),
        mutation_context: Some(native_mutation_context(
            bucket_id,
            "compare-and-swap-manifest",
        )),
        precondition: None,
    });
    valid_update.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second = object_client
        .compare_and_swap_manifest(valid_update)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(second);
    assert_eq!(second.revision, 2);
    assert_eq!(second.version_id, "2");
    assert_ne!(second.manifest_hash, first.manifest_hash);
    assert!(second.watch_cursor > first.watch_cursor);
}

#[tokio::test]
async fn test_multipart_upload_completes_ordered_parts() {
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

    let bucket_name = "test-multipart-bucket".to_string();
    let object_key = "multipart.txt".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let mut initiate_req = Request::new(InitiateMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(bucket_id, "initiate-multipart")),
    });
    initiate_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let initiate_res = object_client
        .initiate_multipart_upload(initiate_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(initiate_res);
    assert_eq!(initiate_res.version_id, initiate_res.upload_id);
    let upload_id = initiate_res.upload_id;

    let part_payloads = [(1, b"multi".to_vec()), (2, b"part".to_vec())];
    let mut completed_parts = Vec::new();
    for (part_number, payload) in part_payloads {
        let chunks = vec![
            UploadPartRequest {
                data: Some(anvil_api::upload_part_request::Data::Metadata(
                    UploadPartMetadata {
                        bucket_name: bucket_name.clone(),
                        object_key: object_key.clone(),
                        upload_id: upload_id.clone(),
                        part_number,
                        mutation_context: Some(native_mutation_context(bucket_id, "upload-part")),
                    },
                )),
            },
            UploadPartRequest {
                data: Some(anvil_api::upload_part_request::Data::Chunk(payload)),
            },
        ];
        let mut upload_req = Request::new(tokio_stream::iter(chunks));
        upload_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let upload_part = object_client
            .upload_part(upload_req)
            .await
            .unwrap()
            .into_inner();
        assert_native_mutation_response!(upload_part);
        assert_eq!(upload_part.version_id, part_number.to_string());
        completed_parts.push(CompleteMultipartPart {
            part_number,
            etag: upload_part.etag,
        });
    }

    let mut complete_req = Request::new(CompleteMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        upload_id,
        parts: completed_parts,
        mutation_context: Some(native_mutation_context(bucket_id, "complete-multipart")),
    });
    complete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let complete_res = object_client
        .complete_multipart_upload(complete_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(complete_res);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(complete_res.version_id),
        range: None,
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

    assert_eq!(downloaded, b"multipart");
}

#[tokio::test]
async fn test_multipart_abort_returns_mutation_metadata() {
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

    let bucket_name = "test-multipart-abort-bucket".to_string();
    let object_key = "aborted.txt".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let mut initiate_req = Request::new(InitiateMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(bucket_id, "initiate-multipart")),
    });
    initiate_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let initiate_res = object_client
        .initiate_multipart_upload(initiate_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(initiate_res);

    let mut abort_req = Request::new(AbortMultipartRequest {
        bucket_name,
        object_key,
        upload_id: initiate_res.upload_id.clone(),
        mutation_context: Some(native_mutation_context(bucket_id, "abort-multipart")),
    });
    abort_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let abort_res = object_client
        .abort_multipart_upload(abort_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(abort_res);
    assert_eq!(abort_res.version_id, initiate_res.upload_id);
    assert!(abort_res.watch_cursor > initiate_res.watch_cursor);
}

#[tokio::test]
async fn test_compose_object_concatenates_sources_in_order() {
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

    let bucket_name = "test-compose-bucket".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let sources = vec![
        ("part-a.txt", b"hello ".to_vec()),
        ("part-b.txt", b"compose".to_vec()),
    ];
    let mut source_versions = Vec::new();
    for (key, content) in &sources {
        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: key.to_string(),
            mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
            content_type: None,
            user_metadata_json: String::new(),
        };
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                    metadata,
                )),
            },
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    content.clone(),
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
        source_versions.push((key.to_string(), put_res.version_id));
    }

    let mut compose_req = Request::new(ComposeObjectRequest {
        sources: source_versions
            .into_iter()
            .map(|(key, version_id)| ComposeObjectSource {
                bucket_name: bucket_name.clone(),
                object_key: key,
                version_id: Some(version_id),
            })
            .collect(),
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: "composed.txt".to_string(),
        mutation_context: Some(native_mutation_context(bucket_id, "compose-object")),
    });
    compose_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let compose_res = object_client
        .compose_object(compose_req)
        .await
        .unwrap()
        .into_inner();
    assert_native_mutation_response!(compose_res);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: "composed.txt".to_string(),
        version_id: Some(compose_res.version_id),
        range: None,
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
                assert_eq!(metadata.content_length, "hello compose".len() as i64);
            }
            anvil_api::get_object_response::Data::Chunk(bytes) => {
                downloaded.extend_from_slice(&bytes);
            }
        }
    }

    assert_eq!(downloaded, b"hello compose");
}

#[tokio::test]
async fn test_patch_json_object_writes_new_merged_version() {
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

    let bucket_name = "test-json-patch-bucket".to_string();
    let object_key = "document.json".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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
        mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
        content_type: None,
        user_metadata_json: String::new(),
    };
    let initial_json = br#"{"title":"old","stats":{"open":2,"closed":1},"remove_me":true}"#;
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                initial_json.to_vec(),
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

    let mut patch_req = Request::new(PatchJsonObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        base_version_id: Some(put_res.version_id.clone()),
        merge_patch_json: r#"{"title":"new","stats":{"open":3},"remove_me":null}"#.to_string(),
        mutation_context: Some(native_mutation_context(bucket_id, "patch-json-object")),
        precondition: None,
    });
    patch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let patch_res = object_client
        .patch_json_object(patch_req)
        .await
        .unwrap()
        .into_inner();

    assert_ne!(patch_res.version_id, put_res.version_id);
    assert_native_mutation_response!(patch_res);
    assert!(patch_res.watch_cursor > put_res.watch_cursor);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(patch_res.version_id),
        range: None,
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

    let patched: serde_json::Value = serde_json::from_slice(&downloaded).unwrap();
    assert_eq!(patched["title"], "new");
    assert_eq!(patched["stats"]["open"], 3);
    assert_eq!(patched["stats"]["closed"], 1);
    assert!(patched.get("remove_me").is_none());
}

#[tokio::test]
async fn test_list_objects_with_delimiter() {
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

    let bucket_name = "test-delimiter-bucket".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
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

    let keys = vec!["a/b.txt", "a/c.txt", "d.txt"];
    for key in keys {
        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: key.to_string(),
            mutation_context: Some(native_mutation_context(bucket_id, "object-metadata")),
            content_type: None,
            user_metadata_json: String::new(),
        };
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                    metadata,
                )),
            },
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    b"...".to_vec(),
                )),
            },
        ];
        let mut put_req = Request::new(tokio_stream::iter(chunks));
        put_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        object_client.put_object(put_req).await.unwrap();
    }

    // Listing with prefix and delimiter
    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: "a/".to_string(),
        delimiter: "/".to_string(),
        ..Default::default()
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(list_res.objects.len(), 2);
    let got_under_a: Vec<&str> = list_res.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(got_under_a, vec!["a/b.txt", "a/c.txt"]);
    assert!(list_res.common_prefixes.is_empty());

    // Listing with just a delimiter
    let mut list_req_2 = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        delimiter: "/".to_string(),
        ..Default::default()
    });
    list_req_2.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res_2 = object_client
        .list_objects(list_req_2)
        .await
        .unwrap()
        .into_inner();

    let top_level_objects: Vec<&str> = list_res_2.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(top_level_objects, vec!["d.txt"]);
    assert_eq!(list_res_2.common_prefixes, vec!["a/"]);
}
