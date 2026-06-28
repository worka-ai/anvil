use anvil::anvil_api::git_source_service_client::GitSourceServiceClient;
use anvil::anvil_api::{
    GetGitBlobByPathRequest, GetGitObjectRequest, ListGitTreeRequest, WatchGitSourceRequest,
};
use anvil::formats::git::{GitHashAlgorithm, GitSourceRecord};
use anvil::git_source_index::{GitSourceIndexWrite, write_git_source_index};
use anvil::git_source_watch::{GitSourceWatchPayload, append_git_source_watch_record};
use anvil_test_utils::TestCluster;
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

#[tokio::test]
async fn test_git_source_watch_streams_snapshot_and_new_events() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    append_git_source_watch_record(
        &cluster.states[0].storage,
        1,
        "repo-alpha",
        1,
        [1; 16],
        5,
        git_watch_payload(1),
    )
    .await
    .unwrap();

    let mut client = GitSourceServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut watch_req = Request::new(WatchGitSourceRequest {
        repository_id: "repo-alpha".to_string(),
        after_cursor_low: 0,
        after_cursor_high: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    let mut stream = client
        .watch_git_source(watch_req)
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
    assert_eq!(snapshot.repository_id, "repo-alpha");
    assert_eq!(snapshot.event_type, "index_published");
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.source_hash, hex::encode([1; 32]));
    assert_eq!(
        snapshot.pack_object_version_id,
        "00000000-0000-0000-0000-000000000001"
    );
    assert_eq!(snapshot.authz_revision, 5);

    append_git_source_watch_record(
        &cluster.states[0].storage,
        1,
        "repo-alpha",
        2,
        [2; 16],
        6,
        git_watch_payload(2),
    )
    .await
    .unwrap();
    let live = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(live.cursor_low, 2);
    assert_eq!(live.generation, 2);
    assert_eq!(live.authz_revision, 6);
}

#[tokio::test]
async fn test_git_source_query_apis_use_latest_index_and_enforce_read_authz() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    write_git_source_index(
        &cluster.states[0].storage,
        GitSourceIndexWrite {
            tenant_id: 1,
            repository_id: "repo-alpha",
            generation: 1,
            source_hash: [7; 32],
            hash_algorithm: GitHashAlgorithm::Sha1,
            records: &[
                git_record(1, 10, "src/lib.rs", 100, 44),
                git_record(1, 11, "src/main.rs", 200, 55),
                git_record(1, 12, "README.md", 300, 66),
                git_record(2, 13, "src/lib.rs", 400, 77),
            ],
        },
    )
    .await
    .unwrap();

    let mut client = GitSourceServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let blob = client
        .get_git_blob_by_path(authorized(
            GetGitBlobByPathRequest {
                repository_id: "repo-alpha".to_string(),
                commit_id: hex::encode([1_u8; 20]),
                tree_path: "/src/lib.rs".to_string(),
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .location
        .expect("blob location");
    assert_eq!(blob.object_id, hex::encode([10_u8; 20]));
    assert_eq!(blob.tree_path, "src/lib.rs");
    assert_eq!(blob.blob_start, 100);
    assert_eq!(blob.blob_len, 44);
    assert_eq!(
        blob.pack_object_version_id,
        "0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a"
    );

    let tree = client
        .list_git_tree(authorized(
            ListGitTreeRequest {
                repository_id: "repo-alpha".to_string(),
                commit_id: hex::encode([1_u8; 20]),
                prefix: "src".to_string(),
                limit: 10,
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .entries;
    assert_eq!(
        tree.iter()
            .map(|entry| entry.tree_path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/lib.rs", "src/main.rs"]
    );

    let object_locations = client
        .get_git_object(authorized(
            GetGitObjectRequest {
                repository_id: "repo-alpha".to_string(),
                object_id: hex::encode([10_u8; 20]),
            },
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner()
        .locations;
    assert_eq!(object_locations.len(), 1);
    assert_eq!(object_locations[0].commit_id, hex::encode([1_u8; 20]));

    let read_denied_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "watch-only".to_string(),
            vec!["git_source:watch|repository:repo-alpha".to_string()],
            1,
        )
        .unwrap();
    let denied = client
        .get_git_object(authorized(
            GetGitObjectRequest {
                repository_id: "repo-alpha".to_string(),
                object_id: hex::encode([10_u8; 20]),
            },
            &read_denied_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

fn git_record(commit: u8, object: u8, path: &str, start: u64, len: u64) -> GitSourceRecord {
    GitSourceRecord::new(
        GitHashAlgorithm::Sha1,
        b"repo-alpha".to_vec(),
        vec![commit; 20],
        vec![object; 20],
        path.as_bytes().to_vec(),
        start,
        len,
        [object; 16],
    )
    .unwrap()
}

fn git_watch_payload(generation: u64) -> GitSourceWatchPayload {
    GitSourceWatchPayload {
        repository_id: "repo-alpha".to_string(),
        event_type: "index_published".to_string(),
        generation,
        source_hash: hex::encode([generation as u8; 32]),
        index_path: format!(
            "_anvil/git/tenants/tenant-1/repositories/repo-alpha/indexes/generation-{generation:020}-source.angit"
        ),
        pack_object_version_id: Some("00000000-0000-0000-0000-000000000001".to_string()),
        emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    }
}
