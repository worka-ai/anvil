use std::time::Duration;

use anvil_storage::AnvilClient;
use anvil_storage::proto::get_object_response;
use anvil_storage::proto::put_object_request;
use anvil_storage::proto::{
    CreateBucketRequest, GetObjectRequest, ListBucketsRequest, NativeMutationContext,
    ObjectMetadata, PutObjectRequest,
};
use anvil_test_utils::TestCluster;

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

#[tokio::test]
async fn rust_client_calls_live_native_api() {
    let mut cluster = TestCluster::new(&["rust-client-region"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let client = AnvilClient::connect_with_bearer(cluster.grpc_addrs[0].clone(), &cluster.token)
        .await
        .expect("rust client should connect to live test node");

    let bucket_name = format!("rust-client-{}", uuid::Uuid::new_v4());
    let bucket = client
        .buckets()
        .create_bucket(CreateBucketRequest {
            bucket_name: bucket_name.clone(),
            region: "rust-client-region".to_string(),
        })
        .await
        .expect("rust client should create a bucket")
        .into_inner();

    let listed = client
        .buckets()
        .list_buckets(ListBucketsRequest {})
        .await
        .expect("rust client should list buckets")
        .into_inner();
    assert!(
        listed
            .buckets
            .iter()
            .any(|bucket| bucket.name == bucket_name)
    );

    let object_key = "notes/welcome.txt".to_string();
    let payload = b"hello from the Rust client".to_vec();
    let upload = vec![
        PutObjectRequest {
            data: Some(put_object_request::Data::Metadata(ObjectMetadata {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                mutation_context: Some(native_mutation_context(bucket.bucket_id, "put-object")),
            })),
        },
        PutObjectRequest {
            data: Some(put_object_request::Data::Chunk(payload.clone())),
        },
    ];

    let stored = client
        .objects()
        .put_object(tokio_stream::iter(upload))
        .await
        .expect("rust client should stream an object upload")
        .into_inner();
    assert!(!stored.version_id.is_empty());

    let mut download = client
        .objects()
        .get_object(GetObjectRequest {
            bucket_name,
            object_key,
            version_id: Some(stored.version_id),
        })
        .await
        .expect("rust client should stream an object download")
        .into_inner();

    let mut downloaded = Vec::new();
    while let Some(part) = download
        .message()
        .await
        .expect("download stream item should decode")
    {
        if let Some(get_object_response::Data::Chunk(bytes)) = part.data {
            downloaded.extend(bytes);
        }
    }

    assert_eq!(downloaded, payload);
}
