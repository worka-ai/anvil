use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    CheckPermissionRequest, CreateBucketRequest, GetAccessTokenRequest, GetObjectRequest,
    GrantAccessRequest, ListBucketsRequest, ObjectMetadata, PutObjectRequest, RevokeAccessRequest,
    SetPublicAccessRequest, WatchAuthzDerivedLagRequest, WatchAuthzNamespaceRequest,
    WatchAuthzTupleLogRequest, WriteAuthzTupleRequest,
};
use anvil::authz_derived_lag_watch::{
    AuthzDerivedLagWatchPayload, append_authz_derived_lag_watch_record,
};
use anvil::authz_namespace_watch::{
    AuthzNamespaceWatchPayload, append_authz_namespace_watch_record,
};
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
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");
    (client_id, client_secret)
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
    bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap();

    // Put an object
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
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
    bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap();

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
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
