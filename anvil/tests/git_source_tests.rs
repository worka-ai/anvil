use anvil::anvil_api::WatchGitSourceRequest;
use anvil::anvil_api::git_source_service_client::GitSourceServiceClient;
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
