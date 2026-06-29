use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::{
    CreateIndexRequest, GetAccessTokenRequest, IndexKind, QueryIndexRequest,
    SetPublicAccessRequest, WriteAuthzTupleRequest,
};
use anvil::storage::{DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES, ExternalChunkManifest};
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
use std::process::{Command, Output};
use std::time::Duration;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tonic::Request;

use anvil_test_utils::*;

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

// Helper function to create an app, since it's used in auth tests.
fn create_app(admin_state_path: &str, app_name: &str) -> (String, String) {
    let (_, client_id, client_secret) = create_app_with_id(admin_state_path, app_name);
    (client_id, client_secret)
}

fn create_app_with_id(admin_state_path: &str, app_name: &str) -> (String, String, String) {
    let app_output = run_admin(
        admin_state_path,
        &[
            "app",
            "create",
            "--tenant-name",
            "default",
            "--app-name",
            app_name,
        ],
    );
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let app_id = creds
        .lines()
        .find_map(|line| line.split_once("(ID: "))
        .and_then(|(_, rest)| rest.strip_suffix(')'))
        .expect("app id in admin output")
        .to_string();
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");
    (app_id, client_id, client_secret)
}

fn grant_wildcard_policy(admin_state_path: &str, app_name: &str) {
    grant_policy(admin_state_path, app_name, "*", "*");
}

fn grant_policy(admin_state_path: &str, app_name: &str, action: &str, resource: &str) {
    run_admin(
        admin_state_path,
        &[
            "policy",
            "grant",
            "--app-name",
            app_name,
            "--action",
            action,
            "--resource",
            resource,
        ],
    );
}

fn run_admin(admin_state_path: &str, args: &[&str]) -> Output {
    let mut command = admin_command(admin_state_path);
    let output = command.args(args).output().expect("run admin binary");
    assert!(
        output.status.success(),
        "admin command failed: status={:?}, args={:?}, stdout={}, stderr={}",
        output.status.code(),
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn admin_command(admin_state_path: &str) -> Command {
    let mut command = if let Some(admin_binary) = option_env!("CARGO_BIN_EXE_admin") {
        Command::new(admin_binary)
    } else {
        let mut fallback = Command::new("cargo");
        fallback.args(["run", "--bin", "admin", "--"]);
        fallback
    };
    command.args([
        "--anvil-secret-encryption-key",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "--storage-path",
        admin_state_path,
    ]);
    command
}

async fn wait_for_completed_index_build(cluster: &TestCluster, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
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

fn write_authz_tuple_request(
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    operation: &str,
) -> WriteAuthzTupleRequest {
    WriteAuthzTupleRequest {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        caveat_hash: String::new(),
        operation: operation.to_string(),
        reason: "test".to_string(),
    }
}

// Helper to get a token for specific scopes.
async fn get_token_for_scopes(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
    scopes: Vec<String>,
) -> String {
    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            scopes,
        })
        .await
        .unwrap()
        .into_inner()
        .access_token
}

#[tokio::test]
async fn test_s3_put_write_etag_preconditions() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-write-preconditions-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-write-preconditions-{}", uuid::Uuid::new_v4());
    let key = "preconditioned.txt";

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"created once"))
        .send()
        .await
        .expect("If-None-Match create should succeed when object is absent");
    let duplicate_create = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"created twice"))
        .send()
        .await;
    assert!(
        duplicate_create.is_err(),
        "If-None-Match create should reject existing object"
    );

    let head = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("HEAD should return current ETag");
    let etag = head.e_tag().expect("current ETag").to_string();

    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(&etag)
        .body(ByteStream::from_static(b"updated through If-Match"))
        .send()
        .await
        .expect("matching If-Match PUT should update the object");
    let stale_update = client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .if_match(&etag)
        .body(ByteStream::from_static(b"stale update"))
        .send()
        .await;
    assert!(
        stale_update.is_err(),
        "stale If-Match PUT should reject the update"
    );

    let updated_head = client
        .head_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("HEAD should return updated ETag");
    let updated_etag = updated_head.e_tag().expect("updated ETag").to_string();
    client
        .copy_object()
        .bucket(&bucket)
        .key("copied-through-source-if-match.txt")
        .copy_source(format!("{bucket}/{key}"))
        .copy_source_if_match(&updated_etag)
        .send()
        .await
        .expect("matching source If-Match CopyObject should succeed");
    let stale_copy = client
        .copy_object()
        .bucket(&bucket)
        .key("stale-copy.txt")
        .copy_source(format!("{bucket}/{key}"))
        .copy_source_if_match(&etag)
        .send()
        .await;
    assert!(
        stale_copy.is_err(),
        "stale source If-Match CopyObject should fail"
    );
    let matching_none_match_copy = client
        .copy_object()
        .bucket(&bucket)
        .key("none-match-copy.txt")
        .copy_source(format!("{bucket}/{key}"))
        .copy_source_if_none_match(&updated_etag)
        .send()
        .await;
    assert!(
        matching_none_match_copy.is_err(),
        "matching source If-None-Match CopyObject should fail"
    );
}

#[tokio::test]
async fn test_s3_list_versions_and_get_filter_by_relationship_authorization() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let writer_app = format!("s3-relationship-writer-{}", uuid::Uuid::new_v4());
    let (_, writer_client_id, writer_client_secret) =
        create_app_with_id(&cluster.admin_state_path, &writer_app);
    grant_wildcard_policy(&cluster.admin_state_path, &writer_app);

    let reader_app = format!("s3-relationship-reader-{}", uuid::Uuid::new_v4());
    let (reader_app_id, reader_client_id, reader_client_secret) =
        create_app_with_id(&cluster.admin_state_path, &reader_app);

    let bucket = format!("s3-relationship-filter-{}", uuid::Uuid::new_v4());
    grant_policy(
        &cluster.admin_state_path,
        &reader_app,
        "object:list",
        &bucket,
    );

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let writer = s3_client(http_base, &writer_client_id, &writer_client_secret);
    let reader = s3_client(http_base, &reader_client_id, &reader_client_secret);

    let allowed_key = "docs/allowed.txt";
    let denied_key = "docs/denied.txt";
    let visible_nested_key = "visible/nested.txt";
    let hidden_nested_key = "hidden/nested.txt";

    writer
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("writer should create private bucket");
    writer
        .put_object()
        .bucket(&bucket)
        .key(allowed_key)
        .body(ByteStream::from_static(b"allowed-v1"))
        .send()
        .await
        .expect("writer should put allowed v1");
    writer
        .put_object()
        .bucket(&bucket)
        .key(denied_key)
        .body(ByteStream::from_static(b"denied"))
        .send()
        .await
        .expect("writer should put denied object");
    writer
        .put_object()
        .bucket(&bucket)
        .key(allowed_key)
        .body(ByteStream::from_static(b"allowed-v2"))
        .send()
        .await
        .expect("writer should put allowed v2");
    writer
        .put_object()
        .bucket(&bucket)
        .key(visible_nested_key)
        .body(ByteStream::from_static(b"visible"))
        .send()
        .await
        .expect("writer should put visible nested object");
    writer
        .put_object()
        .bucket(&bucket)
        .key(hidden_nested_key)
        .body(ByteStream::from_static(b"hidden"))
        .send()
        .await
        .expect("writer should put hidden nested object");

    let ungranted = reader
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("docs/")
        .send()
        .await
        .expect("reader has bucket list permission");
    assert!(
        ungranted.contents().is_empty(),
        "list permission alone must not reveal object keys"
    );

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    for key in [allowed_key, visible_nested_key] {
        auth_client
            .write_authz_tuple(authorized(
                write_authz_tuple_request(
                    "object",
                    &format!("{bucket}/{key}"),
                    "reader",
                    "app",
                    &reader_app_id,
                    "add",
                ),
                &cluster.token,
            ))
            .await
            .unwrap();
    }

    let docs = reader
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("docs/")
        .send()
        .await
        .expect("relationship-filtered docs list should succeed");
    assert_eq!(docs.contents().len(), 1);
    assert_eq!(docs.contents()[0].key(), Some(allowed_key));

    let tree = reader
        .list_objects_v2()
        .bucket(&bucket)
        .delimiter("/")
        .send()
        .await
        .expect("relationship-filtered delimiter list should succeed");
    let prefixes = tree
        .common_prefixes()
        .iter()
        .filter_map(|prefix| prefix.prefix())
        .collect::<Vec<_>>();
    assert_eq!(prefixes, vec!["docs/", "visible/"]);

    let versions = reader
        .list_object_versions()
        .bucket(&bucket)
        .send()
        .await
        .expect("relationship-filtered version list should succeed");
    let version_keys = versions
        .versions()
        .iter()
        .filter_map(|version| version.key())
        .collect::<Vec<_>>();
    assert_eq!(
        version_keys,
        vec![allowed_key, allowed_key, visible_nested_key]
    );

    let allowed = reader
        .get_object()
        .bucket(&bucket)
        .key(allowed_key)
        .send()
        .await
        .expect("relationship grant should allow S3 GET");
    let allowed_bytes = allowed.body.collect().await.unwrap().into_bytes();
    assert_eq!(allowed_bytes.as_ref(), b"allowed-v2");

    let denied = reader
        .get_object()
        .bucket(&bucket)
        .key(denied_key)
        .send()
        .await;
    assert!(
        denied.is_err(),
        "S3 GET must not allow ungranted object through bucket list permission"
    );
}

#[tokio::test]
async fn test_s3_reads_and_lists_survive_object_metadata_compaction() {
    let mut cluster = TestCluster::new(&["compact-region"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-compact-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-compact-{}", uuid::Uuid::new_v4());

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    client
        .put_object()
        .bucket(&bucket)
        .key("logs/a.txt")
        .body(ByteStream::from_static(b"a-v1"))
        .send()
        .await
        .expect("put logs/a.txt v1 should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key("logs/a.txt")
        .body(ByteStream::from_static(b"a-v2"))
        .send()
        .await
        .expect("put logs/a.txt v2 should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key("logs/b.txt")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await
        .expect("put logs/b.txt should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key("logs/nested/c.txt")
        .body(ByteStream::from_static(b"c"))
        .send()
        .await
        .expect("put logs/nested/c.txt should succeed");
    client
        .delete_object()
        .bucket(&bucket)
        .key("logs/b.txt")
        .send()
        .await
        .expect("delete logs/b.txt should create a delete marker");

    let bucket_record = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    cluster.states[0]
        .persistence
        .compact_object_metadata(bucket_record.id)
        .await
        .unwrap()
        .expect("object metadata compaction should seal a manifest");

    let get_a = client
        .get_object()
        .bucket(&bucket)
        .key("logs/a.txt")
        .send()
        .await
        .expect("GET after compaction should succeed");
    let bytes = get_a
        .body
        .collect()
        .await
        .expect("collect compacted object body")
        .into_bytes();
    assert_eq!(bytes.as_ref(), b"a-v2");

    let compacted_listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("logs/")
        .delimiter("/")
        .send()
        .await
        .expect("delimiter LIST after compaction should succeed");
    assert_eq!(compacted_listing.contents().len(), 1);
    assert_eq!(compacted_listing.contents()[0].key(), Some("logs/a.txt"));
    assert_eq!(compacted_listing.common_prefixes().len(), 1);
    assert_eq!(
        compacted_listing.common_prefixes()[0].prefix(),
        Some("logs/nested/")
    );

    let compacted_versions = client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("logs/a.txt")
        .send()
        .await
        .expect("version LIST after compaction should succeed");
    assert_eq!(compacted_versions.versions().len(), 2);
    assert!(
        compacted_versions
            .versions()
            .iter()
            .any(|version| version.is_latest().unwrap_or(false))
    );

    client
        .put_object()
        .bucket(&bucket)
        .key("logs/d.txt")
        .body(ByteStream::from_static(b"d"))
        .send()
        .await
        .expect("post-compaction PUT should succeed");
    client
        .delete_object()
        .bucket(&bucket)
        .key("logs/nested/c.txt")
        .send()
        .await
        .expect("post-compaction DELETE should succeed");

    let overlay_listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("logs/")
        .delimiter("/")
        .send()
        .await
        .expect("LIST should merge compacted directory segment and active journal");
    let overlay_keys: Vec<_> = overlay_listing
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    assert_eq!(overlay_keys, vec!["logs/a.txt", "logs/d.txt"]);
    assert!(
        overlay_listing.common_prefixes().is_empty(),
        "post-compaction delete marker should remove now-empty nested prefix"
    );

    let deleted_get = client
        .get_object()
        .bucket(&bucket)
        .key("logs/b.txt")
        .send()
        .await;
    assert!(
        deleted_get.is_err(),
        "delete marker sealed during compaction must remain current"
    );
}

#[tokio::test]
async fn test_s3_active_get_survives_object_metadata_compaction() {
    let mut cluster = TestCluster::new(&["active-read-compact-region"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-active-compact-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-active-compact-{}", uuid::Uuid::new_v4());
    let key = "large/active-read.bin";
    let object_len = DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES + 257;
    let payload: Vec<u8> = (0..object_len)
        .map(|index| ((index * 31 + 17) % 251) as u8)
        .collect();

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key(key)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect("large S3 PUT should succeed");

    let get = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("active S3 GET should start");
    let mut reader = get.body.into_async_read();
    let mut first = vec![0_u8; 64 * 1024];
    reader
        .read_exact(&mut first)
        .await
        .expect("read first chunk before compaction");
    assert_eq!(first.as_slice(), &payload[..first.len()]);

    let bucket_record = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    cluster.states[0]
        .persistence
        .compact_object_metadata(bucket_record.id)
        .await
        .unwrap()
        .expect("object metadata compaction should seal a manifest");

    let mut observed = first;
    reader
        .read_to_end(&mut observed)
        .await
        .expect("active GET should drain after compaction");
    assert_eq!(observed, payload);

    let post_compaction_get = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .expect("subsequent GET after active-read compaction should succeed");
    let post_compaction_body = post_compaction_get
        .body
        .collect()
        .await
        .expect("collect subsequent compacted body")
        .into_bytes();
    assert_eq!(post_compaction_body.as_ref(), observed.as_slice());
}

#[tokio::test]
async fn test_s3_writes_trigger_worker_metadata_compaction() {
    let mut cluster = TestCluster::new_with_config(&["auto-compact-region"], |config| {
        config.object_metadata_compaction_frame_threshold = 2;
        config.object_metadata_compaction_bytes_threshold = 0;
        config.task_lease_ttl_secs = 60;
    })
    .await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-auto-compact-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-auto-compact-{}", uuid::Uuid::new_v4());

    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");
    client
        .put_object()
        .bucket(&bucket)
        .key("auto/a.txt")
        .body(ByteStream::from_static(b"automatic compaction"))
        .send()
        .await
        .expect("S3 PUT should schedule object metadata compaction");

    let bucket_record = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    let manifest_path = cluster.states[0]
        .storage
        .metadata_manifest_path(1, bucket_record.id);

    let completed_task = {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let tasks = cluster.states[0].persistence.list_tasks().await.unwrap();
            if let Some(task) = tasks.iter().find(|task| {
                task.task_type == anvil_core::tasks::TaskType::ObjectMetadataCompaction
                    && task.payload == serde_json::json!({ "bucket_id": bucket_record.id })
                    && task.status == anvil_core::tasks::TaskStatus::Completed
            }) {
                break task.clone();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "worker did not complete object metadata compaction task in time; tasks={tasks:?}"
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    };
    assert!(
        tokio::fs::metadata(&manifest_path).await.is_ok(),
        "worker-completed compaction should publish an object metadata manifest"
    );
    let lease = cluster.states[0]
        .persistence
        .read_task_execution_lease(completed_task.id)
        .await
        .unwrap()
        .expect("completed compaction task should have a task lease");
    assert_eq!(lease.partition_family, "object_metadata");
    assert_eq!(lease.checkpoint_cursor, lease.source_cursor);

    let get = client
        .get_object()
        .bucket(&bucket)
        .key("auto/a.txt")
        .send()
        .await
        .expect("GET should survive worker compaction");
    let bytes = get
        .body
        .collect()
        .await
        .expect("collect compacted body")
        .into_bytes();
    assert_eq!(bytes.as_ref(), b"automatic compaction");

    let listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("auto/")
        .send()
        .await
        .expect("LIST should survive worker compaction");
    assert_eq!(listing.contents().len(), 1);
    assert_eq!(listing.contents()[0].key(), Some("auto/a.txt"));
}

#[tokio::test]
async fn test_s3_put_triggers_full_text_index_build() {
    let mut cluster = TestCluster::new(&["s3-index-region"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-index-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-index-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "body".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"source": "object_body_utf8"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
            },
            &cluster.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("docs/s3-indexed.txt")
        .body(ByteStream::from_static(
            b"s3 writes should flow into full text indexing",
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "body".to_string(),
                    query_text: "full text indexing".to_string(),
                    query_vector: vec![],
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                },
                &cluster.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/s3-indexed.txt")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response = indexed.expect("S3 object should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
    wait_for_completed_index_build(&cluster, Duration::from_secs(20)).await;
}

#[tokio::test]
async fn test_s3_put_metadata_field_triggers_full_text_index_build() {
    let mut cluster = TestCluster::new(&["s3-metadata-index-region"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-metadata-index-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-metadata-index-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "owner".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "metadata_field",
                    "field": "owner"
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
            },
            &cluster.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("docs/s3-metadata.txt")
        .metadata("owner", "alice portfolio")
        .body(ByteStream::from_static(
            b"body intentionally does not contain the indexed owner",
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "owner".to_string(),
                    query_text: "alice portfolio".to_string(),
                    query_vector: vec![],
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                },
                &cluster.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/s3-metadata.txt")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response =
        indexed.expect("S3 metadata field should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/s3-metadata.txt");
}

#[tokio::test]
async fn test_s3_put_personaldb_table_column_triggers_full_text_index_build() {
    let mut cluster = TestCluster::new(&["s3-personaldb-column-index-region"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-personaldb-column-index-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-personaldb-column-index-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "row-name".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "rows/"}).to_string(),
                extractor_json: serde_json::json!({
                    "source": "personaldb_table_column",
                    "table": "items",
                    "column": "name"
                })
                .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
            },
            &cluster.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("rows/items/1.json")
        .content_type("application/json")
        .body(ByteStream::from_static(
            br#"{"table_name":"items","columns":{"id":1,"name":"alpha repair order"}}"#,
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "row-name".to_string(),
                    query_text: "alpha repair".to_string(),
                    query_vector: vec![],
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                },
                &cluster.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "rows/items/1.json")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response = indexed
        .expect("S3 PersonalDB table column should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "rows/items/1.json");
}

#[tokio::test]
async fn test_s3_put_media_transcript_triggers_full_text_index_build() {
    let mut cluster = TestCluster::new(&["s3-media-index-region"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-media-index-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-media-index-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "media".to_string(),
                kind: IndexKind::FullText as i32,
                selector_json: serde_json::json!({"prefix": "media/"}).to_string(),
                extractor_json: serde_json::json!({"source": "media_transcript"}).to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": true}).to_string(),
            },
            &cluster.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("media/audio/clip.bin")
        .content_type("audio/mpeg")
        .body(ByteStream::from_static(
            b"\x00\x01deterministic audio bytes",
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "media".to_string(),
                    query_text: "audio media object".to_string(),
                    query_vector: vec![],
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                },
                &cluster.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "media/audio/clip.bin")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response =
        indexed.expect("S3 media transcript should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::FullText as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "media/audio/clip.bin");
}

#[tokio::test]
async fn test_s3_put_triggers_vector_index_build() {
    let mut cluster = TestCluster::new(&["s3-vector-index-region"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let app_name = format!("s3-vector-index-{}", uuid::Uuid::new_v4());
    let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
    grant_wildcard_policy(&cluster.admin_state_path, &app_name);

    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);
    let bucket = format!("s3-vector-index-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("S3 CreateBucket should succeed");

    let mut index_client = IndexServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket.clone(),
                name: "embedding".to_string(),
                kind: IndexKind::Vector as i32,
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
            &cluster.token,
        ))
        .await
        .unwrap();

    client
        .put_object()
        .bucket(&bucket)
        .key("docs/s3-vector.json")
        .body(ByteStream::from_static(
            br#"{"vector":[0.0,1.0],"source_start":2,"source_len":16}"#,
        ))
        .send()
        .await
        .expect("S3 PUT should succeed");

    let mut indexed = None;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while tokio::time::Instant::now() < deadline {
        let query = index_client
            .query_index(authorized(
                QueryIndexRequest {
                    bucket_name: bucket.clone(),
                    index_name: "embedding".to_string(),
                    query_text: String::new(),
                    query_vector: vec![0.0, 1.0],
                    limit: 10,
                    phrase: false,
                    path_prefix: String::new(),
                    metadata_filters_json: String::new(),
                },
                &cluster.token,
            ))
            .await;
        if let Ok(query) = query {
            let response = query.into_inner();
            if response
                .hits
                .iter()
                .any(|hit| hit.object_key == "docs/s3-vector.json")
            {
                indexed = Some(response);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let response =
        indexed.expect("S3 object vector should be searchable after index task completes");
    assert_eq!(response.index_kind, IndexKind::Vector as i32);
    assert!(response.index_generation >= 1);
    assert_eq!(response.hits[0].object_key, "docs/s3-vector.json");
    wait_for_completed_index_build(&cluster, Duration::from_secs(20)).await;
}

#[test]
fn test_s3_public_and_private_access() {
    run_large_s3_gateway_test(Box::pin(run_s3_public_and_private_access()));
}

#[test]
fn test_s3_large_object_uses_external_chunks_and_ranges_across_chunk_boundary() {
    run_large_s3_gateway_test(Box::pin(async {
        let mut cluster = TestCluster::new(&["test-region-1"]).await;
        cluster.start_and_converge(Duration::from_secs(5)).await;

        let app_name = format!("s3-large-chunks-{}", uuid::Uuid::new_v4());
        let (client_id, client_secret) = create_app(&cluster.admin_state_path, &app_name);
        grant_wildcard_policy(&cluster.admin_state_path, &app_name);

        let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
        let client = s3_client(http_base, &client_id, &client_secret);
        let bucket_name = format!("s3-large-chunks-{}", uuid::Uuid::new_v4());
        let object_key = "large/chunked.bin";
        let object_len = DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES + 257;
        let content = (0..object_len)
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<_>>();

        client
            .create_bucket()
            .bucket(&bucket_name)
            .send()
            .await
            .expect("S3 CreateBucket should succeed");

        client
            .put_object()
            .bucket(&bucket_name)
            .key(object_key)
            .body(ByteStream::from(content.clone()))
            .send()
            .await
            .expect("large S3 PUT should succeed");

        let bucket_id = cluster.states[0]
            .persistence
            .get_bucket_by_name(1, &bucket_name)
            .await
            .unwrap()
            .expect("bucket metadata should exist")
            .id;
        let object = cluster.states[0]
            .persistence
            .get_object(bucket_id, object_key)
            .await
            .unwrap()
            .expect("large object metadata should exist");
        assert!(object.inline_payload.is_none());
        let manifest: ExternalChunkManifest = serde_json::from_value(
            object
                .shard_map
                .clone()
                .expect("large object should record external chunk manifest"),
        )
        .expect("external chunk manifest should decode");
        assert_eq!(manifest.kind, "external_chunks_v1");
        assert_eq!(manifest.chunk_size, DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES);
        assert_eq!(manifest.chunks.len(), 2);
        assert_eq!(
            manifest.chunks[0].plaintext_length as usize,
            DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES
        );
        assert_eq!(manifest.chunks[1].plaintext_length as usize, 257);
        for record in &manifest.chunks {
            assert!(record.storage_ref.starts_with("_anvil/payloads/chunks/"));
            let chunk_path = cluster.states[0].storage.external_chunk_path(
                &object.content_hash,
                record.chunk_index,
                &record.payload_chunk_hash,
            );
            assert!(
                chunk_path.exists(),
                "external chunk path should exist: {}",
                chunk_path.display()
            );
        }

        let full_resp = client
            .get_object()
            .bucket(&bucket_name)
            .key(object_key)
            .send()
            .await
            .expect("large S3 GET should succeed");
        let full = full_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(full.as_ref(), content.as_slice());

        let range_start = DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES - 8;
        let range_end = DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES + 8;
        let range_resp = client
            .get_object()
            .bucket(&bucket_name)
            .key(object_key)
            .range(format!("bytes={range_start}-{range_end}"))
            .send()
            .await
            .expect("S3 range GET across external chunk boundary should succeed");
        assert_eq!(
            range_resp.content_range(),
            Some(format!("bytes {range_start}-{range_end}/{object_len}").as_str())
        );
        let ranged = range_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(ranged.as_ref(), &content[range_start..=range_end]);
    }));
}

async fn run_s3_public_and_private_access() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let (client_id, client_secret) = create_app(&cluster.admin_state_path, "s3-test-app");

    // Grant wildcard policy to the app before getting a token
    grant_wildcard_policy(&cluster.admin_state_path, "s3-test-app");

    // Allow a moment for the policy change to propagate or be read by the server.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &client_id,
        &client_secret,
        vec![
            "bucket:create|*".to_string(),
            "policy:grant|*".to_string(),
            "object:write|*".to_string(),
            "object:read|*".to_string(),
        ],
    )
    .await;

    // 1. Create a private and a public bucket
    let private_bucket = "private-s3-bucket".to_string();
    let public_bucket = "public-s3-bucket".to_string();

    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: private_bucket.clone(),
        region: "test-region-1".to_string(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(req).await.unwrap();

    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: public_bucket.clone(),
        region: "test-region-1".to_string(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(req).await.unwrap();

    // 2. Set the public bucket to be public
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: public_bucket.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    // 3. Configure AWS S3 client to talk to our local server
    // TestCluster stores gRPC base at /grpc; S3 must hit HTTP root.
    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let client = s3_client(http_base, &client_id, &client_secret);

    let location = client
        .get_bucket_location()
        .bucket(&private_bucket)
        .send()
        .await
        .unwrap();
    assert!(
        format!("{:?}", location.location_constraint()).contains("test-region-1"),
        "bucket location response should include the stored bucket region"
    );

    client
        .put_bucket_versioning()
        .bucket(&private_bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let versioning = client
        .get_bucket_versioning()
        .bucket(&private_bucket)
        .send()
        .await
        .unwrap();
    assert!(
        matches!(versioning.status(), Some(BucketVersioningStatus::Enabled)),
        "bucket versioning should be reported as enabled"
    );

    let deleted_bucket = format!("delete-s3-bucket-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&deleted_bucket)
        .send()
        .await
        .unwrap();
    client
        .delete_bucket()
        .bucket(&deleted_bucket)
        .send()
        .await
        .unwrap();
    let deleted_head = client.head_bucket().bucket(&deleted_bucket).send().await;
    let deleted_head_debug = format!("{deleted_head:?}");
    assert!(
        deleted_head.is_err()
            && (deleted_head_debug.contains("StatusCode(404)")
                || deleted_head_debug.contains("NotFound")),
        "deleted bucket should no longer be visible: {deleted_head_debug}"
    );

    let active_multipart_bucket = format!("delete-s3-active-multipart-{}", uuid::Uuid::new_v4());
    let active_multipart_key = "active-upload.txt";
    client
        .create_bucket()
        .bucket(&active_multipart_bucket)
        .send()
        .await
        .unwrap();
    let active_upload = client
        .create_multipart_upload()
        .bucket(&active_multipart_bucket)
        .key(active_multipart_key)
        .send()
        .await
        .expect("create active multipart upload should succeed");
    let active_upload_id = active_upload
        .upload_id()
        .expect("active upload id")
        .to_string();
    let active_delete = client
        .delete_bucket()
        .bucket(&active_multipart_bucket)
        .send()
        .await;
    assert!(
        format!("{active_delete:?}").contains("BucketNotEmpty"),
        "S3 DeleteBucket must reject active multipart uploads"
    );
    client
        .abort_multipart_upload()
        .bucket(&active_multipart_bucket)
        .key(active_multipart_key)
        .upload_id(&active_upload_id)
        .send()
        .await
        .expect("abort active multipart upload should succeed");
    client
        .delete_bucket()
        .bucket(&active_multipart_bucket)
        .send()
        .await
        .expect("empty bucket should be deletable after aborting multipart upload");

    let unauthenticated_list_buckets = reqwest::get(format!("{}/", http_base)).await.unwrap();
    assert_eq!(unauthenticated_list_buckets.status(), 403);

    let private_key = "private.txt";
    let public_key = "public.txt";
    let private_content = b"this is private content";
    let public_content = b"this is public content";

    // 4. Put an object into each bucket using the S3 client (tests SigV4 auth)
    client
        .put_object()
        .bucket(&private_bucket)
        .key(private_key)
        .content_type("text/plain")
        .metadata("owner", "alice")
        .metadata("purpose", "metadata-test")
        .body(ByteStream::from(private_content.to_vec()))
        .send()
        .await
        .expect("Failed to put private object");

    let head_private = client
        .head_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .expect("HEAD should return object metadata");
    assert_eq!(head_private.content_type(), Some("text/plain"));
    let head_metadata = head_private.metadata().expect("HEAD metadata");
    assert_eq!(
        head_metadata.get("owner").map(String::as_str),
        Some("alice")
    );
    assert_eq!(
        head_metadata.get("purpose").map(String::as_str),
        Some("metadata-test")
    );
    let private_etag = head_private.e_tag().expect("private ETag").to_string();
    client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .if_match(&private_etag)
        .send()
        .await
        .expect("matching If-Match GET should succeed");
    let if_match_mismatch = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .if_match("\"definitely-not-the-current-etag\"")
        .send()
        .await;
    assert!(
        if_match_mismatch.is_err(),
        "mismatched If-Match GET should fail"
    );
    let if_none_match_hit = client
        .head_object()
        .bucket(&private_bucket)
        .key(private_key)
        .if_none_match(&private_etag)
        .send()
        .await;
    assert!(
        if_none_match_hit.is_err(),
        "matching If-None-Match HEAD should return not modified"
    );

    let create_only_key = "create-only.txt";
    client
        .put_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"created once"))
        .send()
        .await
        .expect("If-None-Match create should succeed when object is absent");
    let duplicate_create = client
        .put_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"created twice"))
        .send()
        .await;
    assert!(
        duplicate_create.is_err(),
        "If-None-Match create should reject existing object"
    );

    let create_only_head = client
        .head_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .send()
        .await
        .expect("HEAD should return create-only ETag");
    let create_only_etag = create_only_head
        .e_tag()
        .expect("create-only ETag")
        .to_string();
    client
        .put_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .if_match(&create_only_etag)
        .body(ByteStream::from_static(b"updated through If-Match"))
        .send()
        .await
        .expect("matching If-Match PUT should update the object");
    let stale_update = client
        .put_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .if_match(&create_only_etag)
        .body(ByteStream::from_static(b"stale update"))
        .send()
        .await;
    assert!(
        stale_update.is_err(),
        "stale If-Match PUT should reject the update"
    );

    let utf8_key = "folder/my café document 📄.txt";
    let utf8_content = b"utf8 key over s3";
    client
        .put_object()
        .bucket(&private_bucket)
        .key(utf8_key)
        .body(ByteStream::from_static(utf8_content))
        .send()
        .await
        .expect("put UTF-8 S3 key should succeed");
    let utf8_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(utf8_key)
        .send()
        .await
        .expect("UTF-8 S3 key should be readable");
    let utf8_data = utf8_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(utf8_data.as_ref(), utf8_content);
    let utf8_listing = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix("folder/")
        .send()
        .await
        .expect("UTF-8 S3 key should be listable");
    assert!(
        utf8_listing
            .contents()
            .iter()
            .any(|object| object.key() == Some(utf8_key)),
        "list_objects_v2 should include the UTF-8 key"
    );

    let literal_prefix = "literal/a%_";
    let literal_key = "literal/a%_object.txt";
    let wildcard_decoy_key = "literal/abc-object.txt";
    client
        .put_object()
        .bucket(&private_bucket)
        .key(literal_key)
        .body(ByteStream::from_static(b"literal wildcard key"))
        .send()
        .await
        .expect("put literal wildcard-like key should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key(wildcard_decoy_key)
        .body(ByteStream::from_static(b"decoy key"))
        .send()
        .await
        .expect("put wildcard decoy key should succeed");
    let literal_prefix_listing = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix(literal_prefix)
        .send()
        .await
        .expect("literal wildcard-like prefix listing should succeed");
    assert_eq!(literal_prefix_listing.contents().len(), 1);
    assert_eq!(
        literal_prefix_listing.contents()[0].key(),
        Some(literal_key)
    );

    client
        .put_object()
        .bucket(&public_bucket)
        .key(public_key)
        .body(ByteStream::from(public_content.to_vec()))
        .send()
        .await
        .expect("Failed to put public object");

    let delete_nonempty = client.delete_bucket().bucket(&private_bucket).send().await;
    assert!(
        format!("{delete_nonempty:?}").contains("BucketNotEmpty"),
        "S3 DeleteBucket must reject buckets with retained object versions"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Test Private Access (Success): Use S3 client to get from private bucket
    let resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .expect("Failed to get private object with S3 client");
    assert_eq!(resp.content_type(), Some("text/plain"));
    let get_metadata = resp.metadata().expect("GET metadata");
    assert_eq!(get_metadata.get("owner").map(String::as_str), Some("alice"));
    let data = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), private_content);

    let range_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .range("bytes=5-8")
        .send()
        .await
        .expect("range GET should succeed");
    assert_eq!(range_resp.content_range(), Some("bytes 5-8/23"));
    let range_data = range_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(range_data.as_ref(), b"is p");

    let suffix_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .range("bytes=-7")
        .send()
        .await
        .expect("suffix range GET should succeed");
    assert_eq!(suffix_resp.content_range(), Some("bytes 16-22/23"));
    let suffix_data = suffix_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(suffix_data.as_ref(), b"content");

    let copied_key = "copied-private.txt";
    client
        .copy_object()
        .bucket(&private_bucket)
        .key(copied_key)
        .copy_source(format!("{}/{}", private_bucket, private_key))
        .send()
        .await
        .expect("copy object should succeed");
    let copied_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(copied_key)
        .send()
        .await
        .expect("copied object should be readable");
    let copied_data = copied_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(copied_data.as_ref(), private_content);

    client
        .put_object()
        .bucket(&private_bucket)
        .key("bulk/one.txt")
        .body(ByteStream::from_static(b"one"))
        .send()
        .await
        .expect("put bulk/one.txt should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key("bulk/two.txt")
        .body(ByteStream::from_static(b"two"))
        .send()
        .await
        .expect("put bulk/two.txt should succeed");

    let bulk_delete = client
        .delete_objects()
        .bucket(&private_bucket)
        .delete(
            Delete::builder()
                .objects(
                    ObjectIdentifier::builder()
                        .key("bulk/one.txt")
                        .build()
                        .unwrap(),
                )
                .objects(
                    ObjectIdentifier::builder()
                        .key("bulk/two.txt")
                        .build()
                        .unwrap(),
                )
                .objects(
                    ObjectIdentifier::builder()
                        .key("bulk/missing.txt")
                        .build()
                        .unwrap(),
                )
                .objects(
                    ObjectIdentifier::builder()
                        .key("_anvil/authz/bulk-delete")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("multi-object delete should return a DeleteResult");
    assert_eq!(bulk_delete.deleted().len(), 3);
    assert_eq!(bulk_delete.errors().len(), 1);
    assert_eq!(
        bulk_delete.errors()[0].key(),
        Some("_anvil/authz/bulk-delete")
    );
    assert_eq!(
        bulk_delete.errors()[0].code(),
        Some("UnauthorizedReservedNamespace")
    );

    let bulk_deleted_get = client
        .get_object()
        .bucket(&private_bucket)
        .key("bulk/one.txt")
        .send()
        .await;
    assert!(
        bulk_deleted_get.is_err(),
        "multi-object delete should make bulk/one.txt unreadable"
    );

    client
        .put_object()
        .bucket(&private_bucket)
        .key("page/a.txt")
        .body(ByteStream::from_static(b"a"))
        .send()
        .await
        .expect("put page/a.txt should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key("page/b.txt")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await
        .expect("put page/b.txt should succeed");
    let first_page = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix("page/")
        .max_keys(1)
        .send()
        .await
        .expect("first paged list should succeed");
    assert!(first_page.is_truncated().unwrap_or(false));
    assert_eq!(first_page.contents().len(), 1);
    assert_eq!(first_page.contents()[0].key(), Some("page/a.txt"));
    let second_page = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix("page/")
        .max_keys(1)
        .continuation_token(first_page.next_continuation_token().expect("next token"))
        .send()
        .await
        .expect("second paged list should succeed");
    assert!(!second_page.is_truncated().unwrap_or(true));
    assert_eq!(second_page.contents().len(), 1);
    assert_eq!(second_page.contents()[0].key(), Some("page/b.txt"));
    let first_v1_page = client
        .list_objects()
        .bucket(&private_bucket)
        .prefix("page/")
        .max_keys(1)
        .send()
        .await
        .expect("first v1 paged list should succeed");
    assert!(first_v1_page.is_truncated().unwrap_or(false));
    assert_eq!(first_v1_page.contents().len(), 1);
    assert_eq!(first_v1_page.contents()[0].key(), Some("page/a.txt"));
    let second_v1_page = client
        .list_objects()
        .bucket(&private_bucket)
        .prefix("page/")
        .max_keys(1)
        .marker(first_v1_page.next_marker().expect("next marker"))
        .send()
        .await
        .expect("second v1 paged list should succeed");
    assert!(!second_v1_page.is_truncated().unwrap_or(true));
    assert_eq!(second_v1_page.contents().len(), 1);
    assert_eq!(second_v1_page.contents()[0].key(), Some("page/b.txt"));

    client
        .put_object()
        .bucket(&private_bucket)
        .key("tree/root.txt")
        .body(ByteStream::from_static(b"root"))
        .send()
        .await
        .expect("put tree/root.txt should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key("tree/a/file.txt")
        .body(ByteStream::from_static(b"a"))
        .send()
        .await
        .expect("put tree/a/file.txt should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key("tree/b/file.txt")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await
        .expect("put tree/b/file.txt should succeed");
    let tree_listing = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix("tree/")
        .delimiter("/")
        .send()
        .await
        .expect("delimiter list should succeed");
    assert_eq!(tree_listing.contents().len(), 1);
    assert_eq!(tree_listing.contents()[0].key(), Some("tree/root.txt"));
    assert_eq!(tree_listing.common_prefixes().len(), 2);
    assert_eq!(tree_listing.common_prefixes()[0].prefix(), Some("tree/a/"));
    assert_eq!(tree_listing.common_prefixes()[1].prefix(), Some("tree/b/"));

    let multipart_key = "multipart-private.txt";
    let multipart = client
        .create_multipart_upload()
        .bucket(&private_bucket)
        .key(multipart_key)
        .send()
        .await
        .expect("create multipart upload should succeed");
    let upload_id = multipart.upload_id().expect("upload id").to_string();
    let part_one = client
        .upload_part()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"multi"))
        .send()
        .await
        .expect("upload multipart part 1 should succeed");
    let part_two = client
        .upload_part()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from_static(b"part"))
        .send()
        .await
        .expect("upload multipart part 2 should succeed");
    let listed_parts = client
        .list_parts()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .send()
        .await
        .expect("list multipart parts should succeed");
    assert_eq!(listed_parts.parts().len(), 2);
    assert_eq!(listed_parts.parts()[0].part_number(), Some(1));
    assert_eq!(listed_parts.parts()[1].part_number(), Some(2));
    let listed_parts_page_one = client
        .list_parts()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .max_parts(1)
        .send()
        .await
        .expect("list multipart parts first page should succeed");
    assert_eq!(listed_parts_page_one.parts().len(), 1);
    assert_eq!(listed_parts_page_one.parts()[0].part_number(), Some(1));
    assert!(listed_parts_page_one.is_truncated().unwrap_or(false));
    let next_part_number_marker = listed_parts_page_one
        .next_part_number_marker()
        .expect("next part number marker");
    let listed_parts_page_two = client
        .list_parts()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .part_number_marker(next_part_number_marker)
        .max_parts(1)
        .send()
        .await
        .expect("list multipart parts second page should succeed");
    assert_eq!(listed_parts_page_two.parts().len(), 1);
    assert_eq!(listed_parts_page_two.parts()[0].part_number(), Some(2));
    assert!(!listed_parts_page_two.is_truncated().unwrap_or(false));
    client
        .complete_multipart_upload()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part_one.e_tag().expect("part 1 etag"))
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(part_two.e_tag().expect("part 2 etag"))
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("complete multipart upload should succeed");
    let multipart_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(multipart_key)
        .send()
        .await
        .expect("multipart object should be readable");
    let multipart_data = multipart_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(multipart_data.as_ref(), b"multipart");

    let aborted_key = "aborted-multipart-private.txt";
    let aborted = client
        .create_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .send()
        .await
        .expect("create multipart upload for abort should succeed");
    let aborted_upload_id = aborted.upload_id().expect("abort upload id").to_string();
    let second_aborted = client
        .create_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .send()
        .await
        .expect("create second multipart upload for pagination should succeed");
    let second_aborted_upload_id = second_aborted
        .upload_id()
        .expect("second abort upload id")
        .to_string();
    let active_uploads = client
        .list_multipart_uploads()
        .bucket(&private_bucket)
        .prefix(aborted_key)
        .send()
        .await
        .expect("list multipart uploads should succeed");
    assert_eq!(active_uploads.uploads().len(), 2);
    assert!(active_uploads.uploads().iter().any(|upload| {
        upload
            .upload_id()
            .is_some_and(|upload_id| upload_id == aborted_upload_id)
    }));
    assert!(active_uploads.uploads().iter().any(|upload| {
        upload
            .upload_id()
            .is_some_and(|upload_id| upload_id == second_aborted_upload_id)
    }));
    let active_uploads_page_one = client
        .list_multipart_uploads()
        .bucket(&private_bucket)
        .prefix(aborted_key)
        .max_uploads(1)
        .send()
        .await
        .expect("list multipart uploads first page should succeed");
    assert_eq!(active_uploads_page_one.uploads().len(), 1);
    assert!(active_uploads_page_one.is_truncated().unwrap_or(false));
    let next_key_marker = active_uploads_page_one
        .next_key_marker()
        .expect("next multipart key marker");
    let next_upload_id_marker = active_uploads_page_one
        .next_upload_id_marker()
        .expect("next multipart upload id marker");
    let active_uploads_page_two = client
        .list_multipart_uploads()
        .bucket(&private_bucket)
        .prefix(aborted_key)
        .key_marker(next_key_marker)
        .upload_id_marker(next_upload_id_marker)
        .max_uploads(1)
        .send()
        .await
        .expect("list multipart uploads second page should succeed");
    assert_eq!(active_uploads_page_two.uploads().len(), 1);
    assert!(!active_uploads_page_two.is_truncated().unwrap_or(false));
    client
        .abort_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .upload_id(&aborted_upload_id)
        .send()
        .await
        .expect("abort multipart upload should succeed");
    client
        .abort_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .upload_id(&second_aborted_upload_id)
        .send()
        .await
        .expect("abort second multipart upload should succeed");
    let upload_after_abort = client
        .upload_part()
        .bucket(&private_bucket)
        .key(aborted_key)
        .upload_id(&aborted_upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"must fail"))
        .send()
        .await;
    assert!(
        upload_after_abort.is_err(),
        "uploading a part after abort must fail"
    );
    let active_uploads_after_abort = client
        .list_multipart_uploads()
        .bucket(&private_bucket)
        .prefix(aborted_key)
        .send()
        .await
        .expect("list multipart uploads after abort should succeed");
    assert!(active_uploads_after_abort.uploads().is_empty());

    // 5b. S3 version listing returns overwritten versions and delete markers.
    client
        .put_object()
        .bucket(&private_bucket)
        .key(private_key)
        .body(ByteStream::from(b"this is private content v2".to_vec()))
        .send()
        .await
        .expect("Failed to overwrite private object");

    let versions_before_delete = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(private_key)
        .send()
        .await
        .expect("list object versions should succeed");
    assert_eq!(versions_before_delete.versions().len(), 2);
    assert!(
        versions_before_delete
            .versions()
            .iter()
            .any(|version| version.is_latest().unwrap_or(false))
    );
    let first_versions_page = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(private_key)
        .max_keys(1)
        .send()
        .await
        .expect("first paged version listing should succeed");
    assert_eq!(first_versions_page.versions().len(), 1);
    assert!(first_versions_page.is_truncated().unwrap_or(false));
    let next_key_marker = first_versions_page
        .next_key_marker()
        .expect("next key marker")
        .to_string();
    let next_version_id_marker = first_versions_page
        .next_version_id_marker()
        .expect("next version marker")
        .to_string();
    assert_eq!(next_key_marker, private_key);
    let second_versions_page = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(private_key)
        .key_marker(next_key_marker)
        .version_id_marker(next_version_id_marker)
        .max_keys(1)
        .send()
        .await
        .expect("second paged version listing should succeed");
    assert_eq!(second_versions_page.versions().len(), 1);
    assert!(!second_versions_page.is_truncated().unwrap_or(true));

    let version_specific_key = "version-specific-delete.txt";
    client
        .put_object()
        .bucket(&private_bucket)
        .key(version_specific_key)
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await
        .expect("put version-specific v1 should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key(version_specific_key)
        .body(ByteStream::from_static(b"v2"))
        .send()
        .await
        .expect("put version-specific v2 should succeed");
    let version_specific_before_delete = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(version_specific_key)
        .send()
        .await
        .expect("list version-specific object versions should succeed");
    let older_version_id = version_specific_before_delete
        .versions()
        .iter()
        .find(|version| !version.is_latest().unwrap_or(false))
        .and_then(|version| version.version_id())
        .expect("older version id")
        .to_string();
    client
        .delete_object()
        .bucket(&private_bucket)
        .key(version_specific_key)
        .version_id(older_version_id)
        .send()
        .await
        .expect("version-specific delete should succeed");
    let version_specific_after_delete = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(version_specific_key)
        .send()
        .await
        .expect("list after version-specific delete should succeed");
    assert_eq!(version_specific_after_delete.versions().len(), 1);
    assert!(version_specific_after_delete.delete_markers().is_empty());

    // 6. Test Public Access (Success): Use reqwest (no auth) to get from public bucket
    let public_url = format!("{}/{}/{}", http_base, public_bucket, public_key);
    let public_resp = reqwest::get(&public_url)
        .await
        .expect("Failed to make public request");
    assert_eq!(public_resp.status(), 200);
    let public_data = public_resp.bytes().await.unwrap();
    assert_eq!(public_data.as_ref(), public_content);

    // 7. Test Private Access (Failure): Use reqwest (no auth) to get from private bucket
    let private_url = format!("{}/{}/{}", http_base, private_bucket, private_key);
    let private_resp = reqwest::get(&private_url).await.unwrap();
    assert!(
        private_resp.status() == 403 || private_resp.status() == 404,
        "Private bucket should be blocked for anonymous access"
    );

    // 8. Reserved internal namespaces are never readable or writable through S3.
    let reserved_prefixes = [
        "_anvil/meta/",
        "_anvil/index/",
        "_anvil/authz/",
        "_anvil/watch/",
        "_anvil/personaldb/",
        "_anvil/git/",
        "_anvil/tmp/",
    ];
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &public_bucket)
        .await
        .unwrap()
        .expect("public bucket metadata should exist");

    for reserved_prefix in reserved_prefixes {
        let reserved_key = format!("{reserved_prefix}s3-compat-object");
        let reserved_url = format!("{}/{}/{}", http_base, public_bucket, reserved_key);

        let reserved_get = reqwest::get(&reserved_url).await.unwrap();
        assert_eq!(reserved_get.status(), 403);
        assert!(
            reserved_get
                .text()
                .await
                .unwrap()
                .contains("UnauthorizedReservedNamespace")
        );

        let reserved_head = reqwest::Client::new()
            .head(&reserved_url)
            .send()
            .await
            .unwrap();
        assert_eq!(reserved_head.status(), 403);

        let reserved_range_get = reqwest::Client::new()
            .get(&reserved_url)
            .header(reqwest::header::RANGE, "bytes=0-1")
            .send()
            .await
            .unwrap();
        assert_eq!(reserved_range_get.status(), 403);
        assert!(
            reserved_range_get
                .text()
                .await
                .unwrap()
                .contains("UnauthorizedReservedNamespace")
        );

        let put_err = client
            .put_object()
            .bucket(&public_bucket)
            .key(&reserved_key)
            .body(ByteStream::from(b"must not be stored".to_vec()))
            .send()
            .await
            .expect_err("reserved namespace PUT must fail");
        assert_reserved_namespace_error(put_err);

        let forged_internal_token_put = reqwest::Client::new()
            .put(format!("{reserved_url}?internal_write_token=caller-forged"))
            .header("x-anvil-internal-write-token", "caller-forged")
            .body("must not be stored")
            .send()
            .await
            .unwrap();
        assert_eq!(forged_internal_token_put.status(), 403);
        assert!(
            forged_internal_token_put
                .text()
                .await
                .unwrap()
                .contains("UnauthorizedReservedNamespace")
        );

        let list_err = client
            .list_objects_v2()
            .bucket(&public_bucket)
            .prefix(reserved_prefix)
            .send()
            .await
            .expect_err("reserved namespace LIST must fail");
        assert_reserved_namespace_error(list_err);

        let list_versions_err = client
            .list_object_versions()
            .bucket(&public_bucket)
            .prefix(reserved_prefix)
            .send()
            .await
            .expect_err("reserved namespace version LIST must fail");
        assert_reserved_namespace_error(list_versions_err);

        cluster.states[0]
            .persistence
            .create_object(
                bucket.tenant_id,
                bucket.id,
                &reserved_key,
                &hex::encode([9; 32]),
                0,
                "reserved-etag",
                None,
                None,
                None,
                Some(Vec::new()),
            )
            .await
            .unwrap();
        let root_listing = client
            .list_objects_v2()
            .bucket(&public_bucket)
            .send()
            .await
            .expect("root listing should succeed");
        assert!(
            root_listing
                .contents()
                .iter()
                .all(|object| object.key() != Some(reserved_key.as_str())),
            "S3 LIST must not reveal reserved namespace keys"
        );
        let root_versions = client
            .list_object_versions()
            .bucket(&public_bucket)
            .send()
            .await
            .expect("root version listing should succeed");
        assert!(
            root_versions
                .versions()
                .iter()
                .all(|object| !object.key().unwrap_or_default().starts_with("_anvil/"))
                && root_versions
                    .delete_markers()
                    .iter()
                    .all(|object| !object.key().unwrap_or_default().starts_with("_anvil/")),
            "S3 version LIST must not reveal reserved namespace keys"
        );

        let delete_err = client
            .delete_object()
            .bucket(&public_bucket)
            .key(&reserved_key)
            .send()
            .await
            .expect_err("reserved namespace DELETE must fail");
        assert_reserved_namespace_error(delete_err);

        let copy_from_reserved_err = client
            .copy_object()
            .bucket(&public_bucket)
            .key(format!(
                "copied-from-reserved-{}.txt",
                reserved_prefix.trim_matches('/').replace('/', "-")
            ))
            .copy_source(format!("{}/{}", public_bucket, reserved_key))
            .send()
            .await
            .expect_err("reserved namespace CopyObject source must fail");
        assert_reserved_namespace_error(copy_from_reserved_err);

        let copy_to_reserved_err = client
            .copy_object()
            .bucket(&public_bucket)
            .key(&reserved_key)
            .copy_source(format!("{}/{}", public_bucket, public_key))
            .send()
            .await
            .expect_err("reserved namespace CopyObject destination must fail");
        assert_reserved_namespace_error(copy_to_reserved_err);
    }

    // 9. Normal S3 DELETE remains compatible and idempotent.
    client
        .delete_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .expect("normal S3 delete should succeed");

    let versions_after_delete = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(private_key)
        .send()
        .await
        .expect("list object versions after delete should succeed");
    assert_eq!(versions_after_delete.versions().len(), 2);
    assert_eq!(versions_after_delete.delete_markers().len(), 1);
    assert!(
        versions_after_delete.delete_markers()[0]
            .is_latest()
            .unwrap_or(false),
        "delete marker should be latest after S3 delete"
    );

    let deleted_get = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await;
    assert!(
        deleted_get.is_err(),
        "deleted key must no longer be readable"
    );
}

#[tokio::test]
async fn test_streaming_upload_decoding() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let (client_id, client_secret) = create_app(&cluster.admin_state_path, "streaming-decode-app");

    // Grant wildcard policy to the app
    run_admin(
        &cluster.admin_state_path,
        &[
            "policy",
            "grant",
            "--app-name",
            "streaming-decode-app",
            "--action",
            "*",
            "--resource",
            "*",
        ],
    );
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Configure S3 client
    let credentials =
        aws_sdk_s3::config::Credentials::new(&client_id, &client_secret, None, None, "static");
    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("test-region-1"))
        .endpoint_url(http_base)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    let client = Client::from_conf(config);

    let bucket_name = format!("streaming-decode-test-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let object_key = "my-streamed-object.txt";
    let original_content = "This is the content that will be streamed with aws-chunked encoding and should be decoded.";

    // 1. Upload the object using a true stream, which forces aws-chunked encoding.
    let stream = original_content.as_bytes().to_vec();
    let _content_len = stream.len();
    // let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(16);
    // tokio::spawn(async move {
    //     for chunk in stream.into_chunks::<5>() {
    //         tx.send(bytes::Bytes::copy_from_slice(&chunk)).await.unwrap();
    //     }
    // });
    // // turn the receiver into a Body that yields http-body 1.0 Frames
    // let stream = ReceiverStream::new(rx).map(|b| Ok::<hyper::body::Frame<bytes::Bytes>, Infallible>(Frame::data(b)));
    // let body = StreamBody::new(stream);
    let mut file = PathBuf::new();
    file.push(temp_dir());
    file.push(format!("anvil-test-streaming-{}", random::<i32>()));
    fs::write(file.as_path(), original_content).await.unwrap();
    let bytestream = ByteStream::read_from()
        .path(file.as_path())
        // Specify the size of the buffer used to read the file (in bytes, default is 4096)
        //.buffer_size(content_len as u64)
        // Specify the length of the file used (skips an additional call to retrieve the size)
        //.length(aws_sdk_s3::primitives::Length::Exact(content_len as i64))
        .build()
        .await
        .expect("valid path");

    client
        .put_object()
        .bucket(&bucket_name)
        .key(object_key)
        //.body(ByteStream::new(SdkBody::from_body_1_x(body)))
        .body(bytestream)
        .send()
        .await
        .expect("Failed to put streaming object");

    // 2. Make the bucket public so we can test with an unauthenticated client.
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &client_id,
        &client_secret,
        vec!["policy:grant|*".to_string()],
    )
    .await;
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // 3. Download the object using a simple HTTP client (reqwest).
    let object_url = format!("{}/{}/{}", http_base, bucket_name, object_key);
    let response = reqwest::get(&object_url).await.unwrap();

    // 4. Verify the response is successful and the body is clean.
    assert_eq!(response.status(), 200, "Expected a successful GET request");
    let downloaded_content = response.text().await.unwrap();

    // This is the critical assertion: the downloaded content must be exactly what we
    // uploaded, with no chunked-encoding metadata.
    assert_eq!(downloaded_content, original_content);
}
