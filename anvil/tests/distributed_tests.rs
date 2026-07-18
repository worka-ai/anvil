#![recursion_limit = "256"]

use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    CreateBucketRequest, GetObjectRequest, NativeMutationContext, ObjectMetadata, PutObjectRequest,
};
use futures::stream::StreamExt;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tonic::Request;

use anvil_test_utils::*;

fn native_mutation_context(
    actor: &DockerTestStorageActor,
    bucket_id: i64,
    tag: &str,
) -> NativeMutationContext {
    let nonce = uuid::Uuid::new_v4();
    NativeMutationContext {
        tenant_id: actor.tenant_id,
        bucket_id,
        principal: actor.app_id.clone(),
        request_id: format!("{tag}-{nonce}-request"),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: format!("{tag}-{nonce}-idempotency"),
        transaction_id: None,
        saga_operation: None,
        saga_compensation_operation: None,
        write_visibility: None,
    }
}

#[tokio::test]
async fn test_distributed_reconstruction_on_node_failure() {
    let total_start = Instant::now();
    let stage_start = Instant::now();
    let cluster = isolated_docker_test_cluster("distributed-reconstruction", "test-region-1").await;
    emit_test_timing(
        "distributed_reconstruction cluster_ready",
        stage_start.elapsed(),
    );

    let stage_start = Instant::now();
    let actor = create_docker_storage_test_actor(&cluster, "reconstruction").await;
    emit_test_timing(
        "distributed_reconstruction actor_ready",
        stage_start.elapsed(),
    );

    let stage_start = Instant::now();
    let primary_addr = actor.grpc_addr.clone();
    let mut object_client = ObjectServiceClient::connect(primary_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(primary_addr).await.unwrap();
    emit_test_timing(
        "distributed_reconstruction public_clients_connected",
        stage_start.elapsed(),
    );

    let bucket_name = unique_test_name("reconstruction-bucket");
    let mut create_bucket_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),
        options: None,
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", actor.token).parse().unwrap(),
    );
    let stage_start = Instant::now();
    let bucket_id = bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;
    emit_test_timing(
        "distributed_reconstruction bucket_created",
        stage_start.elapsed(),
    );

    let object_key = "reconstruction-object".to_string();
    let content = (0..1024 * 256).map(|i| (i % 256) as u8).collect::<Vec<_>>();

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
    let mut chunks = vec![PutObjectRequest {
        data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
            metadata,
        )),
    }];
    for chunk in content.chunks(1024 * 64) {
        chunks.push(PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                chunk.to_vec(),
            )),
        });
    }
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", actor.token).parse().unwrap(),
    );
    let stage_start = Instant::now();
    object_client.put_object(put_req).await.unwrap();
    emit_test_timing(
        "distributed_reconstruction object_put",
        stage_start.elapsed(),
    );

    let stage_start = Instant::now();
    cluster.stop_node(2).await;
    emit_test_timing(
        "distributed_reconstruction node_stopped",
        stage_start.elapsed(),
    );
    let stage_start = Instant::now();
    tokio::time::sleep(Duration::from_secs(2)).await;
    emit_test_timing(
        "distributed_reconstruction failure_stabilised",
        stage_start.elapsed(),
    );

    let stage_start = Instant::now();
    let mut recovery_client = ObjectServiceClient::connect(cluster.grpc_addrs[2].clone())
        .await
        .unwrap();
    emit_test_timing(
        "distributed_reconstruction recovery_client_connected",
        stage_start.elapsed(),
    );
    let stage_start = Instant::now();
    let mut stream = {
        let mut attempt = 0;
        let mut last_failure = String::new();
        loop {
            let mut get_req = Request::new(GetObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                version_id: None,
                range: None,
                ..Default::default()
            });
            get_req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", actor.token).parse().unwrap(),
            );
            match timeout(Duration::from_secs(10), recovery_client.get_object(get_req)).await {
                Ok(Ok(resp)) => break resp.into_inner(),
                Ok(Err(status)) => {
                    last_failure =
                        format!("gRPC code={:?} message={}", status.code(), status.message());
                    attempt += 1;
                    if attempt > 8 {
                        panic!("get_object unavailable after node failure: {last_failure}");
                    }
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
                Err(_) => {
                    last_failure = "request timed out after 10 seconds".to_string();
                    attempt += 1;
                    if attempt > 8 {
                        panic!("get_object unavailable after node failure: {last_failure}");
                    }
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
            }
        }
    };
    emit_test_timing(
        "distributed_reconstruction get_stream_opened",
        stage_start.elapsed(),
    );

    let stage_start = Instant::now();
    let mut downloaded_data = Vec::new();
    let first_chunk = timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("timed out waiting for reconstructed object metadata")
        .expect("reconstructed object stream ended before metadata")
        .unwrap_or_else(|status| panic!("reconstructed object metadata failed: {status:?}"));
    assert!(
        matches!(
            first_chunk.data,
            Some(anvil::anvil_api::get_object_response::Data::Metadata(_))
        ),
        "reconstructed object stream did not begin with metadata"
    );
    while let Some(chunk) = timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("timed out waiting for reconstructed object data")
    {
        let chunk =
            chunk.unwrap_or_else(|status| panic!("reconstructed object data failed: {status:?}"));
        if let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.data {
            downloaded_data.extend_from_slice(&bytes);
        }
    }

    assert_eq!(downloaded_data.len(), content.len());
    assert_eq!(downloaded_data, content);
    emit_test_timing(
        "distributed_reconstruction stream_consumed",
        stage_start.elapsed(),
    );
    emit_test_timing("distributed_reconstruction total", total_start.elapsed());
}
