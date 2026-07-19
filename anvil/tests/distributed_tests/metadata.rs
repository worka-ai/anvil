use anvil_test_utils::isolated_docker_test_cluster;

use super::common::*;

#[tokio::test]
async fn docker_coremeta_r3q2_restart_and_catch_up() {
    let cluster = isolated_docker_test_cluster("r3q2-catch-up", "test-region-1").await;
    let fixture = create_fixture(&cluster, "r3q2-catch-up").await;
    let peer_one = cluster.equal_peer(1);

    let baseline = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        "before-restart",
        b"before restart",
        &MutationIdentity::unique("before-restart"),
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            "before-restart",
            &baseline,
            DISTRIBUTED_WAIT,
        )
        .await;

    cluster.stop_node(3).await;
    let mut missed = Vec::new();
    for ordinal in 0..3 {
        let key = format!("missed-generation-{ordinal}");
        let content = format!("generation committed while peer 3 was offline: {ordinal}");
        let observation = put_and_observe(
            &peer_one.grpc_addr,
            &fixture,
            &key,
            content.as_bytes(),
            &MutationIdentity::unique(&key),
        )
        .await;
        missed.push((key, observation));
    }

    cluster.start_node(3).await;
    for (key, observation) in &missed {
        cluster
            .wait_for_object_convergence(
                &fixture.actor,
                &fixture.bucket_name,
                key,
                observation,
                &[3],
                DISTRIBUTED_WAIT,
            )
            .await;
    }

    cluster.stop_node(2).await;
    let participation_key = "caught-up-peer-participates";
    let participation_content = b"peer 1 and caught-up peer 3 form Q2";
    let participation = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        participation_key,
        participation_content,
        &MutationIdentity::unique(participation_key),
    )
    .await;
    cluster
        .wait_for_object_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            participation_key,
            &participation,
            &[1, 3],
            DISTRIBUTED_WAIT,
        )
        .await;
    cluster.start_node(2).await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            participation_key,
            &participation,
            DISTRIBUTED_WAIT,
        )
        .await;
}

#[tokio::test]
async fn docker_coremeta_quorum_loss_has_no_visibility() {
    let cluster = isolated_docker_test_cluster("r3q2-quorum-loss", "test-region-1").await;
    let fixture = create_fixture(&cluster, "r3q2-quorum-loss").await;
    let peer_one = cluster.equal_peer(1);
    let object_key = "quorum-loss-object";
    let content = b"must not become visible without Q2";
    let identity = MutationIdentity::unique("quorum-loss-object");

    cluster.stop_node(2).await;
    cluster.stop_node(3).await;
    let failure = put_object_at(
        &peer_one.grpc_addr,
        &fixture,
        object_key,
        content,
        &identity,
    )
    .await
    .expect_err("metadata mutation must fail after losing two R3 replicas");
    assert_retryable_closed_failure(&failure, "R3Q2 quorum-loss write");
    cluster
        .wait_for_object_absent(
            &fixture.actor,
            &fixture.bucket_name,
            object_key,
            &[1],
            DISTRIBUTED_WAIT,
        )
        .await;

    cluster.start_node(2).await;
    cluster.start_node(3).await;
    cluster
        .wait_for_object_absent(
            &fixture.actor,
            &fixture.bucket_name,
            object_key,
            &[1, 2, 3],
            DISTRIBUTED_WAIT,
        )
        .await;

    let response = put_object_at(
        &peer_one.grpc_addr,
        &fixture,
        object_key,
        content,
        &identity,
    )
    .await
    .expect("retry after quorum recovery must commit");
    let observation =
        anvil_test_utils::DockerObjectObservation::from_put_response(&response, content.len());
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            object_key,
            &observation,
            DISTRIBUTED_WAIT,
        )
        .await;
    let versions = list_object_versions_at(&peer_one.grpc_addr, &fixture, object_key)
        .await
        .expect("list versions after idempotent quorum-loss retry");
    assert_eq!(
        versions
            .versions
            .iter()
            .filter(|version| version.key == object_key)
            .count(),
        1,
        "quorum-loss retry published exactly one object version"
    );
}

#[tokio::test]
async fn docker_late_equal_peer_bootstraps_and_catches_up() {
    let cluster = isolated_docker_test_cluster("late-empty-peer", "test-region-1").await;
    let fixture = create_fixture(&cluster, "late-empty-peer").await;
    let peer_one = cluster.equal_peer(1);
    let mut history = Vec::new();
    for ordinal in 0..3 {
        let key = format!("before-empty-peer-{ordinal}");
        let content = format!("history entry {ordinal}");
        let observation = put_and_observe(
            &peer_one.grpc_addr,
            &fixture,
            &key,
            content.as_bytes(),
            &MutationIdentity::unique(&key),
        )
        .await;
        history.push((key, observation));
    }

    cluster.recreate_node_with_empty_volume(3).await;
    for (key, observation) in &history {
        cluster
            .wait_for_object_convergence(
                &fixture.actor,
                &fixture.bucket_name,
                key,
                observation,
                &[3],
                DISTRIBUTED_WAIT,
            )
            .await;
    }

    cluster.stop_node(2).await;
    let key = "empty-peer-joined-r3";
    let content = b"empty peer caught up and acknowledged a later Q2 commit";
    let observation = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        key,
        content,
        &MutationIdentity::unique(key),
    )
    .await;
    cluster
        .wait_for_object_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            key,
            &observation,
            &[1, 3],
            DISTRIBUTED_WAIT,
        )
        .await;
    cluster.start_node(2).await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            key,
            &observation,
            DISTRIBUTED_WAIT,
        )
        .await;
}
