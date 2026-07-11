#![recursion_limit = "512"]

use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::{
    CreateIndexRequest, GetAccessTokenRequest, GrantAccessRequest, IndexKind, QueryIndexRequest,
    SetPublicAccessRequest,
};
use anvil::formats::vector::VECTOR_INDEX_SCHEMA;
use anvil::mesh_lifecycle::CreateHostAliasDescriptor;
use anvil::object_links::{ObjectLinkResolution, PutObjectLinkRequest};
use anvil::routing::{HostAliasState, RoutingConfig};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier,
    VersioningConfiguration,
};
use rand::random;
use std::env::temp_dir;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tonic::Request;

use anvil_test_utils::*;

const TEST_PUBLIC_REGION_HOST: &str = "test-region-1.anvil-storage.test";

fn configure_test_public_region(config: &mut anvil::config::Config) {
    config.public_region_base_domain = TEST_PUBLIC_REGION_HOST.to_string();
    config.mesh_id = "mesh-test".to_string();
}

fn tenant_routed_public_url(http_base: &str, tenant: &str, bucket: &str, key: &str) -> String {
    format!("{http_base}/{tenant}/{bucket}/{key}")
}

fn assert_reserved_namespace_error(error: impl std::fmt::Debug) {
    let rendered = format!("{error:?}");
    assert!(
        rendered.contains("UnauthorizedReservedNamespace"),
        "expected UnauthorizedReservedNamespace error, got {rendered}"
    );
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

fn rfc_vector_policy(
    extractor_kind: &str,
    provider: &str,
    model: &str,
    dimension: u16,
) -> serde_json::Value {
    serde_json::json!({
        "schema": VECTOR_INDEX_SCHEMA,
        "source": {"kind": "object_current"},
        "extractor": {"kind": extractor_kind},
        "embedding": {
            "provider": provider,
            "model": model,
            "dimension": dimension,
            "modality": "text",
            "normalisation": "unit_l2",
            "chunking": {"strategy": "whole_object"}
        },
        "ann": {
            "algorithm": "hnsw",
            "metric": "cosine"
        }
    })
}

const LARGE_OBJECT_RANGE_SPLIT_BYTES: usize = 1024 * 1024;

fn run_large_s3_gateway_test(future: Pin<Box<dyn Future<Output = ()> + Send>>) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(8 * 1024 * 1024)
        .enable_all()
        .build()
        .expect("build S3 gateway test runtime");
    runtime.block_on(async move {
        tokio::spawn(future)
            .await
            .expect("S3 gateway test task should not panic");
    });
}

async fn create_app(cluster: &TestCluster, app_name: &str) -> (String, String) {
    let (_, client_id, client_secret) = create_app_with_id(cluster, app_name).await;
    (client_id, client_secret)
}

async fn create_app_with_id(cluster: &TestCluster, app_name: &str) -> (String, String, String) {
    cluster
        .create_application_with_id("default", app_name)
        .await
}

async fn grant_storage_tenant_owner_for_test(cluster: &TestCluster, app_name: &str) {
    let app = cluster.states[0]
        .persistence
        .get_app_by_name(app_name)
        .await
        .unwrap()
        .expect("test app should exist before granting storage tenant owner");
    anvil::access_control::grant_storage_tenant_owner(
        &cluster.states[0].persistence,
        1,
        &app.id.to_string(),
        "test",
        "grant S3 test app storage tenant ownership",
    )
    .await
    .unwrap();
}

async fn grant_policy(cluster: &TestCluster, app_name: &str, action: &str, resource: &str) {
    cluster
        .grant_application_policy("default", app_name, action, resource)
        .await;
}

async fn wait_for_completed_index_build(cluster: &TestCluster, timeout: Duration) {
    let wait_start = std::time::Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut attempts = 0_u64;
    loop {
        attempts += 1;
        let tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
        assert!(
            !tasks.iter().any(|task| {
                task.task_type == anvil::tasks::TaskType::IndexBuild
                    && task.status == anvil::tasks::TaskStatus::Failed
            }),
            "index build task failed; tasks={tasks:?}"
        );
        if tasks.iter().any(|task| {
            task.task_type == anvil::tasks::TaskType::IndexBuild
                && task.status == anvil::tasks::TaskStatus::Completed
        }) {
            emit_test_timing(
                format!("wait_for_completed_index_build attempts={attempts}"),
                wait_start.elapsed(),
            );
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "index build task did not complete in time; tasks={tasks:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn s3_client(http_base: &str, client_id: &str, client_secret: &str) -> Client {
    let credentials = aws_sdk_s3::config::Credentials::new(
        client_id,
        client_secret,
        None, // session token
        None, // expiry
        "static",
    );

    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("test-region"))
        .endpoint_url(http_base)
        .behavior_version_latest()
        .build();
    Client::from_conf(config)
}

async fn get_token(grpc_addr: &str, client_id: &str, client_secret: &str) -> String {
    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .access_token
}

#[path = "s3_gateway_tests/public_private_large_object.rs"]
mod public_private_large_object;
#[path = "s3_gateway_tests/routing_public_alias.rs"]
mod routing_public_alias;
#[path = "s3_gateway_tests/streaming_upload.rs"]
mod streaming_upload;
#[path = "s3_gateway_tests/writes_indexes_compaction.rs"]
mod writes_indexes_compaction;
