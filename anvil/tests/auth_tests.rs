use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use anvil::anvil_api::{
    CheckPermissionRequest, CreateBucketRequest, GetAccessTokenRequest, GetObjectRequest,
    GrantAccessRequest, ListBucketsRequest, ListObjectVersionsRequest, ListObjectsRequest,
    ListRepairFindingsRequest, NativeMutationContext, ObjectMetadata, PutObjectRequest,
    RepairAuthzDerivedIndexRequest, RevokeAccessRequest, SetPublicAccessRequest,
    WatchAuthzDerivedLagRequest, WatchAuthzNamespaceRequest, WatchAuthzTupleLogRequest,
    WriteAuthzTupleRequest,
};
use anvil::authz_derived_lag_watch::{
    AuthzDerivedLagWatchPayload, append_authz_derived_lag_watch_record,
};
use anvil::authz_namespace_watch::{
    AuthzNamespaceWatchPayload, append_authz_namespace_watch_record,
};
use anvil::authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID;
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

use anvil_test_utils::*;

#[tokio::test]
async fn grpc_error_responses_include_server_request_id() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let err = bucket_client
        .list_buckets(Request::new(ListBucketsRequest {}))
        .await
        .expect_err("unauthenticated request must fail");

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let request_id = err
        .metadata()
        .get("x-anvil-request-id")
        .expect("gRPC error metadata must include x-anvil-request-id")
        .to_str()
        .expect("request id must be ASCII metadata");
    assert_eq!(request_id.len(), 32);
    assert!(request_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
}

// Helper function to create an app, since it's used in auth tests.
fn create_app(admin_state_path: &str, app_name: &str) -> (String, String) {
    let (_, client_id, client_secret) = create_app_with_id(admin_state_path, app_name);
    (client_id, client_secret)
}

fn create_app_with_id(admin_state_path: &str, app_name: &str) -> (String, String, String) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let app_output = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--storage-path",
            admin_state_path,
            "app",
            "create",
            "--tenant-name",
            "default",
            "--app-name",
            app_name,
        ]))
        .output()
        .unwrap();
    assert!(app_output.status.success());
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

fn grant_policy(admin_state_path: &str, app_name: &str, action: &str, resource: &str) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let output = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--storage-path",
            admin_state_path,
            "policy",
            "grant",
            "--app-name",
            app_name,
            "--action",
            action,
            "--resource",
            resource,
        ]))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "policy grant failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

// Helper to get a token for specific scopes.
async fn get_token_for_scopes(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
    scopes: Vec<String>,
) -> String {
    try_get_token_for_scopes(grpc_addr, client_id, client_secret, scopes)
        .await
        .unwrap()
}

async fn try_get_token_for_scopes(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
    scopes: Vec<String>,
) -> Result<String, tonic::Status> {
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
        .map(|r| r.into_inner().access_token)
}

fn add_bearer<T>(request: &mut Request<T>, token: &str) {
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
}

fn native_mutation_context(bucket_id: i64, principal: &str, tag: &str) -> NativeMutationContext {
    let nonce = uuid::Uuid::new_v4();
    NativeMutationContext {
        tenant_id: 1,
        bucket_id,
        principal: principal.to_string(),
        request_id: format!("{tag}-{nonce}-request"),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: format!("{tag}-{nonce}-idempotency"),
    }
}

async fn put_test_object(
    object_client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
    bucket_id: i64,
    bucket_name: &str,
    object_key: &str,
    payload: &[u8],
) {
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.to_string(),
                    object_key: object_key.to_string(),
                    mutation_context: Some(native_mutation_context(
                        bucket_id,
                        "test-app",
                        "put-test-object",
                    )),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                payload.to_vec(),
            )),
        },
    ];
    let mut request = Request::new(tokio_stream::iter(chunks));
    add_bearer(&mut request, token);
    object_client.put_object(request).await.unwrap();
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

fn check_permission_request(
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    consistency: &str,
    zookie: &str,
) -> CheckPermissionRequest {
    CheckPermissionRequest {
        namespace: namespace.to_string(),
        object_id: object_id.to_string(),
        relation: relation.to_string(),
        subject_kind: subject_kind.to_string(),
        subject_id: subject_id.to_string(),
        caveat_hash: String::new(),
        consistency: consistency.to_string(),
        zookie: zookie.to_string(),
    }
}

#[tokio::test]
async fn test_grant_and_revoke_access() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let (granter_client_id, granter_client_secret) =
        create_app(&cluster.admin_state_path, "granter-app");

    // Grant the granter app the ability to grant policies
    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policy",
        "grant",
        "--app-name",
        "granter-app",
        "--action",
        "policy:grant",
        "--resource",
        "*",
    ];
    let output = std::process::Command::new("cargo")
        .args(
            admin_args
                .iter()
                .chain(&[
                    "--anvil-secret-encryption-key",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--storage-path",
                    &cluster.admin_state_path,
                ])
                .chain(policy_args.iter()),
        )
        .output()
        .unwrap();
    assert!(output.status.success());

    let revoke_policy_args = &[
        "policy",
        "grant",
        "--app-name",
        "granter-app",
        "--action",
        "policy:revoke",
        "--resource",
        "*",
    ];
    let revoke_output = std::process::Command::new("cargo")
        .args(
            admin_args
                .iter()
                .chain(&[
                    "--anvil-secret-encryption-key",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--storage-path",
                    &cluster.admin_state_path,
                ])
                .chain(revoke_policy_args.iter()),
        )
        .output()
        .unwrap();
    assert!(revoke_output.status.success());

    tokio::time::sleep(Duration::from_secs(2)).await;

    let granter_token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &granter_client_id,
        &granter_client_secret,
        vec!["*"].into_iter().map(|s| s.to_string()).collect(),
    )
    .await;

    let (grantee_client_id, grantee_client_secret) =
        create_app(&cluster.admin_state_path, "grantee-app");

    let bucket_name = "grant-test-bucket".to_string();
    let resource = format!("bucket:{}", bucket_name);

    // 2. Grant access
    let mut grant_req = Request::new(GrantAccessRequest {
        grantee_app_id: "grantee-app".to_string(),
        resource: resource.clone(),
        action: "bucket:read".to_string(),
    });
    grant_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", granter_token).parse().unwrap(),
    );
    auth_client.grant_access(grant_req).await.unwrap();

    // 3. Verify grantee can now get a token and access the resource
    let grantee_token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &grantee_client_id,
        &grantee_client_secret,
        vec![format!("bucket:read|{}", resource)],
    )
    .await;
    assert!(!grantee_token.is_empty());

    // 4. Revoke access
    let mut revoke_req = Request::new(RevokeAccessRequest {
        grantee_app_id: "grantee-app".to_string(),
        resource: resource.clone(),
        action: "bucket:read".to_string(),
    });
    revoke_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", granter_token).parse().unwrap(),
    );
    auth_client.revoke_access(revoke_req).await.unwrap();

    // 5. Verify grantee can no longer get a token for that scope
    let res = try_get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &grantee_client_id,
        &grantee_client_secret,
        vec![format!("bucket:read|{}", resource)],
    )
    .await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_authz_tuple_write_check_and_watch() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut watch_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let mut write_add = Request::new(WriteAuthzTupleRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: "add".to_string(),
        reason: "grant viewer".to_string(),
    });
    write_add.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let add = auth_client
        .write_authz_tuple(write_add)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(add.revision, 1);
    assert_eq!(add.zookie, "authz:1");
    assert!(!add.record_hash.is_empty());

    let mut watch_req = Request::new(WatchAuthzTupleLogRequest {
        after_revision: 0,
        namespace: "document".to_string(),
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = watch_client
        .watch_authz_tuple_log(watch_req)
        .await
        .unwrap()
        .into_inner();
    let watched_add = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(watched_add.revision, add.revision);
    assert_eq!(watched_add.namespace, "document");
    assert_eq!(watched_add.operation, "add");
    let envelope = watched_add.envelope.as_ref().expect("authz watch envelope");
    assert_eq!(envelope.watch_stream_id, "authz_tuple_log");
    assert_eq!(envelope.partition_family, "authz_tuple");
    assert_eq!(envelope.cursor_low, watched_add.revision);
    assert_eq!(envelope.authz_revision, watched_add.revision);
    assert_eq!(envelope.record_kind, "authz_tuple");
    assert!(envelope.object_ref.contains("document:alpha#viewer"));
    assert!(!envelope.payload_hash.is_empty());

    let mut check_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
    });
    check_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let allowed = auth_client
        .check_permission(check_req)
        .await
        .unwrap()
        .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.revision, add.revision);

    let mut exact_add_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "exact".to_string(),
        zookie: add.zookie.clone(),
    });
    exact_add_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let exact_add = auth_client
        .check_permission(exact_add_req)
        .await
        .unwrap()
        .into_inner();
    assert!(exact_add.allowed);
    assert_eq!(exact_add.revision, add.revision);

    let mut write_remove = Request::new(WriteAuthzTupleRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: "remove".to_string(),
        reason: "revoke viewer".to_string(),
    });
    write_remove.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let remove = auth_client
        .write_authz_tuple(write_remove)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(remove.revision, 2);

    let watched_remove = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(watched_remove.revision, remove.revision);
    assert_eq!(watched_remove.operation, "remove");

    let mut check_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "latest".to_string(),
        zookie: String::new(),
    });
    check_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let denied = auth_client
        .check_permission(check_req)
        .await
        .unwrap()
        .into_inner();
    assert!(!denied.allowed);
    assert_eq!(denied.revision, remove.revision);

    let mut at_least_add_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "at_least".to_string(),
        zookie: add.zookie,
    });
    at_least_add_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let at_least_add = auth_client
        .check_permission(at_least_add_req)
        .await
        .unwrap()
        .into_inner();
    assert!(!at_least_add.allowed);
    assert_eq!(at_least_add.revision, remove.revision);

    let mut exact_add_after_remove_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "exact".to_string(),
        zookie: "authz:1".to_string(),
    });
    exact_add_after_remove_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let exact_add_after_remove = auth_client
        .check_permission(exact_add_after_remove_req)
        .await
        .unwrap()
        .into_inner();
    assert!(exact_add_after_remove.allowed);
    assert_eq!(exact_add_after_remove.revision, 1);

    let mut unavailable_req = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        consistency: "exact".to_string(),
        zookie: "authz:999".to_string(),
    });
    unavailable_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let unavailable = auth_client
        .check_permission(unavailable_req)
        .await
        .unwrap_err();
    assert_eq!(unavailable.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn test_authz_tuple_rejects_invalid_caveat_hash_before_writing() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let mut invalid_write = Request::new(WriteAuthzTupleRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: "not-a-hex32-caveat".to_string(),
        operation: "add".to_string(),
        reason: "invalid caveat".to_string(),
    });
    add_bearer(&mut invalid_write, &token);
    let status = auth_client
        .write_authz_tuple(invalid_write)
        .await
        .expect_err("invalid caveat hash must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        cluster.states[0]
            .persistence
            .latest_authz_revision(1)
            .await
            .unwrap(),
        0,
        "invalid caveat input must not append an authz revision"
    );

    let mut invalid_check = Request::new(CheckPermissionRequest {
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: "not-a-hex32-caveat".to_string(),
        consistency: "latest".to_string(),
        zookie: String::new(),
    });
    add_bearer(&mut invalid_check, &token);
    let status = auth_client
        .check_permission(invalid_check)
        .await
        .expect_err("invalid caveat hash must be rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_authz_permission_resolves_nested_usersets() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let tuples = [
        write_authz_tuple_request("group", "engineering", "member", "user", "alice", "add"),
        write_authz_tuple_request(
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        write_authz_tuple_request(
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
    ];
    let mut zookies = Vec::new();
    for tuple in tuples {
        let mut request = Request::new(tuple);
        add_bearer(&mut request, &token);
        let response = auth_client
            .write_authz_tuple(request)
            .await
            .unwrap()
            .into_inner();
        zookies.push(response.zookie);
    }

    let mut allowed_request = Request::new(check_permission_request(
        "document", "alpha", "viewer", "user", "alice", "latest", "",
    ));
    add_bearer(&mut allowed_request, &token);
    let allowed = auth_client
        .check_permission(allowed_request)
        .await
        .unwrap()
        .into_inner();
    assert!(allowed.allowed);
    assert_eq!(allowed.explanation_ref, "tuple_or_userset_match");

    let mut denied_request = Request::new(check_permission_request(
        "document", "alpha", "viewer", "user", "bob", "latest", "",
    ));
    add_bearer(&mut denied_request, &token);
    let denied = auth_client
        .check_permission(denied_request)
        .await
        .unwrap()
        .into_inner();
    assert!(!denied.allowed);
    assert_eq!(denied.explanation_ref, "no_current_tuple_or_userset");

    let mut remove_request = Request::new(write_authz_tuple_request(
        "folder",
        "platform",
        "viewer",
        "userset",
        "group/engineering#member",
        "remove",
    ));
    add_bearer(&mut remove_request, &token);
    let remove = auth_client
        .write_authz_tuple(remove_request)
        .await
        .unwrap()
        .into_inner();

    let mut exact_before_remove = Request::new(check_permission_request(
        "document",
        "alpha",
        "viewer",
        "user",
        "alice",
        "exact",
        &zookies[2],
    ));
    add_bearer(&mut exact_before_remove, &token);
    let exact_before_remove = auth_client
        .check_permission(exact_before_remove)
        .await
        .unwrap()
        .into_inner();
    assert!(exact_before_remove.allowed);

    let mut latest_after_remove = Request::new(check_permission_request(
        "document", "alpha", "viewer", "user", "alice", "latest", "",
    ));
    add_bearer(&mut latest_after_remove, &token);
    let latest_after_remove = auth_client
        .check_permission(latest_after_remove)
        .await
        .unwrap()
        .into_inner();
    assert!(!latest_after_remove.allowed);
    assert_eq!(latest_after_remove.zookie, remove.zookie);

    let mut lag_request = Request::new(WatchAuthzDerivedLagRequest {
        derived_index_id: DEFAULT_DERIVED_USERSET_INDEX_ID.to_string(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    add_bearer(&mut lag_request, &token);
    let mut lag_stream = auth_client
        .watch_authz_derived_lag(lag_request)
        .await
        .unwrap()
        .into_inner();
    for expected_revision in 1..=4 {
        let event = tokio::time::timeout(Duration::from_secs(5), lag_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event.derived_index_id, DEFAULT_DERIVED_USERSET_INDEX_ID);
        assert_eq!(event.derived_index_kind, "userset");
        assert_eq!(event.processed_revision, expected_revision);
        assert_eq!(event.latest_revision, expected_revision);
        assert_eq!(event.revision_lag, 0);
    }
}

#[tokio::test]
async fn test_object_read_uses_relationship_authorization_before_streaming_bytes() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let bucket_name = "relationship-read-bucket".to_string();
    let object_key = "private/report.txt".to_string();
    let payload = b"relationship authorized object".to_vec();

    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    add_bearer(&mut create_bucket, &token);
    let bucket_id = bucket_client
        .create_bucket(create_bucket)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let put_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                    mutation_context: Some(native_mutation_context(
                        bucket_id,
                        "test-app",
                        "object-metadata",
                    )),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                payload.clone(),
            )),
        },
    ];
    let mut put_request = Request::new(tokio_stream::iter(put_chunks));
    add_bearer(&mut put_request, &token);
    object_client.put_object(put_request).await.unwrap();

    let (reader_app_id, reader_client_id, reader_client_secret) =
        create_app_with_id(&cluster.admin_state_path, "relationship-reader-app");
    grant_policy(
        &cluster.admin_state_path,
        "relationship-reader-app",
        "bucket:read",
        "unrelated-bucket",
    );
    let reader_token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &reader_client_id,
        &reader_client_secret,
        vec!["bucket:read|unrelated-bucket".to_string()],
    )
    .await;

    let mut denied_get = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    add_bearer(&mut denied_get, &reader_token);
    let denied = object_client.get_object(denied_get).await.unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);

    let mut grant_reader = Request::new(write_authz_tuple_request(
        "object",
        &format!("{bucket_name}/{object_key}"),
        "reader",
        "app",
        &reader_app_id,
        "add",
    ));
    add_bearer(&mut grant_reader, &token);
    auth_client.write_authz_tuple(grant_reader).await.unwrap();

    let mut allowed_get = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    add_bearer(&mut allowed_get, &reader_token);
    let mut stream = object_client
        .get_object(allowed_get)
        .await
        .unwrap()
        .into_inner();
    let first = stream.next().await.unwrap().unwrap();
    assert!(matches!(
        first.data,
        Some(anvil::anvil_api::get_object_response::Data::Metadata(_))
    ));
    let second = stream.next().await.unwrap().unwrap();
    let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) = second.data else {
        panic!("second get_object response must be payload bytes");
    };
    assert_eq!(bytes, payload);
}

#[tokio::test]
async fn test_object_list_and_versions_filter_entries_by_read_relationship() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let bucket_name = "relationship-list-bucket".to_string();
    let allowed_key = "docs/allowed.txt".to_string();
    let denied_key = "docs/denied.txt".to_string();
    let visible_nested_key = "visible/nested.txt".to_string();
    let hidden_nested_key = "hidden/nested.txt".to_string();

    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    add_bearer(&mut create_bucket, &token);
    let bucket_id = bucket_client
        .create_bucket(create_bucket)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    put_test_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        &allowed_key,
        b"allowed-v1",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        &denied_key,
        b"denied",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        &allowed_key,
        b"allowed-v2",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        &visible_nested_key,
        b"visible",
    )
    .await;
    put_test_object(
        &mut object_client,
        &token,
        bucket_id,
        &bucket_name,
        &hidden_nested_key,
        b"hidden",
    )
    .await;

    let (reader_app_id, reader_client_id, reader_client_secret) =
        create_app_with_id(&cluster.admin_state_path, "relationship-list-reader-app");
    grant_policy(
        &cluster.admin_state_path,
        "relationship-list-reader-app",
        "object:list",
        &bucket_name,
    );
    let reader_token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &reader_client_id,
        &reader_client_secret,
        vec![format!("object:list|{bucket_name}")],
    )
    .await;

    let mut ungranted_list = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,
    });
    add_bearer(&mut ungranted_list, &reader_token);
    let ungranted_list = object_client
        .list_objects(ungranted_list)
        .await
        .unwrap()
        .into_inner();
    assert!(ungranted_list.objects.is_empty());
    assert!(ungranted_list.common_prefixes.is_empty());

    for key in [&allowed_key, &visible_nested_key] {
        let mut grant_reader = Request::new(write_authz_tuple_request(
            "object",
            &format!("{bucket_name}/{key}"),
            "reader",
            "app",
            &reader_app_id,
            "add",
        ));
        add_bearer(&mut grant_reader, &token);
        auth_client.write_authz_tuple(grant_reader).await.unwrap();
    }

    let mut list_docs = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: "docs/".to_string(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,
    });
    add_bearer(&mut list_docs, &reader_token);
    let list_docs = object_client
        .list_objects(list_docs)
        .await
        .unwrap()
        .into_inner();
    let listed_keys = list_docs
        .objects
        .iter()
        .map(|object| object.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(listed_keys, vec![allowed_key.as_str()]);

    let mut delimiter_list = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        delimiter: "/".to_string(),
        start_after: String::new(),
        max_keys: 100,
    });
    add_bearer(&mut delimiter_list, &reader_token);
    let delimiter_list = object_client
        .list_objects(delimiter_list)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(delimiter_list.common_prefixes, vec!["docs/", "visible/"]);
    assert!(
        !delimiter_list
            .common_prefixes
            .iter()
            .any(|prefix| prefix == "hidden/")
    );

    let mut list_versions = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),
    });
    add_bearer(&mut list_versions, &reader_token);
    let list_versions = object_client
        .list_object_versions(list_versions)
        .await
        .unwrap()
        .into_inner();
    let version_keys = list_versions
        .versions
        .iter()
        .map(|version| version.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        version_keys,
        vec![
            allowed_key.as_str(),
            allowed_key.as_str(),
            visible_nested_key.as_str()
        ]
    );
    assert!(!version_keys.contains(&denied_key.as_str()));
    assert!(!version_keys.contains(&hidden_nested_key.as_str()));
    assert!(!list_versions.is_truncated);
}

#[tokio::test]
async fn test_authz_namespace_watch_streams_snapshot_and_new_events() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    append_authz_namespace_watch_record(
        &cluster.states[0].storage,
        1,
        1,
        [1; 16],
        namespace_watch_payload(10),
    )
    .await
    .unwrap();

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut watch_req = Request::new(WatchAuthzNamespaceRequest {
        namespace: "document".to_string(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    let mut stream = auth_client
        .watch_authz_namespace(watch_req)
        .await
        .unwrap()
        .into_inner();

    let snapshot = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.cursor_low, 1);
    assert_eq!(snapshot.cursor_high, 0);
    assert_eq!(snapshot.namespace, "document");
    assert_eq!(snapshot.event_type, "schema_changed");
    assert_eq!(snapshot.authz_revision, 10);
    assert!(snapshot.invalidates_derived_usersets);

    append_authz_namespace_watch_record(
        &cluster.states[0].storage,
        1,
        2,
        [2; 16],
        namespace_watch_payload(11),
    )
    .await
    .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.cursor_low, 2);
    assert_eq!(live.authz_revision, 11);
}

#[tokio::test]
async fn test_authz_derived_lag_watch_streams_snapshot_and_new_events() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    append_authz_derived_lag_watch_record(
        &cluster.states[0].storage,
        1,
        1,
        [1; 16],
        derived_lag_watch_payload(90, 100, 1),
    )
    .await
    .unwrap();

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut watch_req = Request::new(WatchAuthzDerivedLagRequest {
        derived_index_id: "derived-userset-primary".to_string(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    let mut stream = auth_client
        .watch_authz_derived_lag(watch_req)
        .await
        .unwrap()
        .into_inner();

    let snapshot = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.cursor_low, 1);
    assert_eq!(snapshot.derived_index_id, "derived-userset-primary");
    assert_eq!(snapshot.derived_index_kind, "userset");
    assert_eq!(snapshot.processed_revision, 90);
    assert_eq!(snapshot.latest_revision, 100);
    assert_eq!(snapshot.revision_lag, 10);
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.authz_revision, 100);

    append_authz_derived_lag_watch_record(
        &cluster.states[0].storage,
        1,
        2,
        [2; 16],
        derived_lag_watch_payload(100, 100, 2),
    )
    .await
    .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.cursor_low, 2);
    assert_eq!(live.revision_lag, 0);
    assert_eq!(live.generation, 2);
}

#[tokio::test]
async fn test_repair_authz_derived_index_rebuilds_from_tuple_log() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut repair_client = RepairServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let mut direct_grant = Request::new(write_authz_tuple_request(
        "document",
        "repair-alpha",
        "viewer",
        "user",
        "alice",
        "add",
    ));
    add_bearer(&mut direct_grant, &token);
    auth_client.write_authz_tuple(direct_grant).await.unwrap();

    let path = cluster.states[0]
        .storage
        .authz_derived_userset_index_path(1, DEFAULT_DERIVED_USERSET_INDEX_ID)
        .unwrap();
    tokio::fs::remove_file(&path)
        .await
        .expect("remove derived userset index to force repair");

    let mut repair_request = Request::new(RepairAuthzDerivedIndexRequest {
        derived_index_id: DEFAULT_DERIVED_USERSET_INDEX_ID.to_string(),
        rebuild: true,
    });
    add_bearer(&mut repair_request, &token);
    let repaired = repair_client
        .repair_authz_derived_index(repair_request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(repaired.status, "rebuilt_derived_index");
    assert_eq!(repaired.reason, "AuthzDerivedIndexMissing");
    assert_eq!(repaired.latest_revision, 1);
    assert_eq!(repaired.processed_revision, 1);
    assert_eq!(
        repaired.derived_index_id,
        DEFAULT_DERIVED_USERSET_INDEX_ID.to_string()
    );
    assert!(repaired.finding.is_some());

    let mut check = Request::new(check_permission_request(
        "document",
        "repair-alpha",
        "viewer",
        "user",
        "alice",
        "exact",
        "authz:1",
    ));
    add_bearer(&mut check, &token);
    let allowed = auth_client
        .check_permission(check)
        .await
        .unwrap()
        .into_inner();
    assert!(allowed.allowed);

    let mut list_findings = Request::new(ListRepairFindingsRequest {
        scope_kind: "authz".to_string(),
        scope_id: "tenant-1".to_string(),
        limit: 10,
    });
    add_bearer(&mut list_findings, &token);
    let findings = repair_client
        .list_repair_findings(list_findings)
        .await
        .unwrap()
        .into_inner()
        .findings;
    assert!(findings.iter().any(|finding| {
        finding.code == "AuthzDerivedIndexMissing"
            && finding.proposed_action == "RebuildDerivedIndex"
    }));

    let mut second_repair = Request::new(RepairAuthzDerivedIndexRequest {
        derived_index_id: DEFAULT_DERIVED_USERSET_INDEX_ID.to_string(),
        rebuild: false,
    });
    add_bearer(&mut second_repair, &token);
    let up_to_date = repair_client
        .repair_authz_derived_index(second_repair)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(up_to_date.status, "up_to_date");
    assert!(up_to_date.finding.is_none());
}

fn namespace_watch_payload(authz_revision: u64) -> AuthzNamespaceWatchPayload {
    AuthzNamespaceWatchPayload {
        namespace: "document".to_string(),
        event_type: "schema_changed".to_string(),
        authz_revision,
        schema_hash: hex::encode([4; 32]),
        invalidates_derived_usersets: true,
        emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    }
}

fn derived_lag_watch_payload(
    processed_revision: u64,
    latest_revision: u64,
    generation: u64,
) -> AuthzDerivedLagWatchPayload {
    AuthzDerivedLagWatchPayload {
        derived_index_id: "derived-userset-primary".to_string(),
        derived_index_kind: "userset".to_string(),
        processed_revision,
        latest_revision,
        source_cursor: u128::from(latest_revision),
        source_manifest_hash: hex::encode([9; 32]),
        generation,
        emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    }
}

#[tokio::test]
async fn test_set_public_access_and_get() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let token = cluster.token.clone();
    let bucket_name = "public-access-bucket".to_string();
    let object_key = "public-object".to_string();

    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    // Put an object
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            bucket_id,
            "test-app",
            "object-metadata",
        )),
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"public data".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    // Set bucket to public
    let mut public_req = Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    // Get object without auth
    let get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    let _res = object_client.get_object(get_req).await.unwrap();

    // Set bucket to private
    let mut private_req = Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: false,
    });
    private_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(private_req).await.unwrap();

    // Get object without auth should now fail
    let get_req_2 = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    let res_2 = object_client.get_object(get_req_2).await;
    assert!(res_2.is_err());
}

#[tokio::test]
async fn test_reset_app_secret() {
    let mut cluster = TestCluster::new(&["eu-west-1"]).await;
    cluster
        .start_and_converge_no_new_token(Duration::from_secs(5), false)
        .await;

    let app_name = "app-to-reset";

    // 1. Create an app and get original credentials
    let (client_id, original_secret) = create_app(&cluster.admin_state_path, app_name);

    // Grant it permissions
    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policy",
        "grant",
        "--app-name",
        app_name,
        "--action",
        "*",
        "--resource",
        "*",
    ];
    let grant_status = std::process::Command::new("cargo")
        .args(
            admin_args
                .iter()
                .chain(&[
                    "--anvil-secret-encryption-key",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--storage-path",
                    &cluster.admin_state_path,
                ])
                .chain(policy_args.iter()),
        )
        .status()
        .unwrap();
    assert!(grant_status.success());

    // 2. Reset the secret using the new admin command
    let reset_output = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--storage-path",
            &cluster.admin_state_path,
            "app",
            "reset-secret",
            "--app-name",
            app_name,
        ]))
        .output()
        .unwrap();

    assert!(reset_output.status.success());
    let reset_creds = String::from_utf8(reset_output.stdout).unwrap();
    let new_secret = extract_credential(&reset_creds, "Client Secret");

    // 3. Verify the secret has changed
    assert_ne!(original_secret, new_secret);

    // 4. Restart the cluster to ensure it picks up the new secret, clearing any cache.
    cluster.restart(Duration::from_secs(10)).await;

    // 5. Verify the NEW secret works against the restarted node
    let s3_client_new = cluster
        .get_s3_client("eu-west-1", &client_id, &new_secret)
        .await;
    match s3_client_new.list_buckets().send().await {
        Ok(_list_bucket_output) => {}
        Err(e) => {
            panic!("List buckets failed with the new secret: {:?}", e);
        }
    }

    // 6. Verify the OLD secret fails
    let s3_client_old = cluster
        .get_s3_client("eu-west-1", &client_id, &original_secret)
        .await;
    let list_buckets_old = s3_client_old.list_buckets().send().await;
    assert!(
        list_buckets_old.is_err(),
        "List buckets should fail with the old secret"
    );
}

#[tokio::test]
async fn test_admin_cli_set_public_access() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();

    let token = cluster.token.clone();
    let bucket_name = "cli-public-bucket".to_string();
    let object_key = "cli-public-object".to_string();

    // 1. Create a bucket and upload an object to it.
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            bucket_id,
            "test-app",
            "object-metadata",
        )),
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"public data from cli test".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    // 2. Verify the object is NOT public yet.
    let object_url = format!("{}/{}/{}", cluster.grpc_addrs[0], bucket_name, object_key);
    let http_client = reqwest::Client::new();
    let resp_before = http_client.get(&object_url).send().await.unwrap();
    assert_eq!(
        resp_before.status(),
        403,
        "Object should be private initially"
    );

    // 3. Use the admin CLI to make the bucket public.
    let admin_args = &["run", "--bin", "admin", "--"];
    let set_public_status = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--storage-path",
            &cluster.admin_state_path,
            "bucket",
            "set-public-access",
            "--bucket",
            &bucket_name,
            "--allow",
        ]))
        .status()
        .unwrap();
    assert!(set_public_status.success());

    // 4. Verify the object IS public now, with retries for cache consistency.
    let mut resp_after = None;
    for i in 0..5 {
        // Retry up to 5 times
        let resp = http_client.get(&object_url).send().await.unwrap();
        if resp.status() == 200 {
            resp_after = Some(resp);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await; // Wait 500ms before retrying
        println!("Retry {} for public access check...", i + 1);
    }
    let resp_after =
        resp_after.expect("Object should be public after CLI command, but never became public");

    assert_eq!(
        resp_after.status(),
        200,
        "Object should be public after CLI command"
    );
    let body = resp_after.text().await.unwrap();
    assert_eq!(body, "public data from cli test");
}
