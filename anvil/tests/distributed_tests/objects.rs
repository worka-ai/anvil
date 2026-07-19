use std::collections::BTreeSet;

use anvil_test_utils::isolated_docker_test_cluster;

use super::common::*;

#[tokio::test]
async fn docker_equal_peers_replicate_and_converge() {
    let cluster = isolated_docker_test_cluster("equal-peer-convergence", "test-region-1").await;
    let fixture = create_fixture(&cluster, "equal-peer-convergence").await;
    let peer_one = cluster.equal_peer(1);
    let shard_counts_before = shard_counts(&cluster).await;

    let inline_key = "inline-convergence";
    let inline_content = deterministic_bytes(4 * 1024, 11);
    let inline_observation = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        inline_key,
        &inline_content,
        &MutationIdentity::unique("inline-convergence"),
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            inline_key,
            &inline_observation,
            DISTRIBUTED_WAIT,
        )
        .await;

    let large_key = "large-convergence";
    let large_content = deterministic_bytes(512 * 1024, 29);
    let large_observation = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        large_key,
        &large_content,
        &MutationIdentity::unique("large-convergence"),
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            large_key,
            &large_observation,
            DISTRIBUTED_WAIT,
        )
        .await;
    cluster
        .wait_for_all_peer_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            large_key,
            &large_observation,
            DISTRIBUTED_WAIT,
        )
        .await;

    for peer in cluster.equal_peers() {
        let inline = get_object_at(&peer.grpc_addr, &fixture, inline_key)
            .await
            .unwrap_or_else(|status| panic!("peer {} inline read: {status:?}", peer.ordinal));
        assert_eq!(inline, inline_content, "peer {} inline bytes", peer.ordinal);
        let large = get_object_at(&peer.grpc_addr, &fixture, large_key)
            .await
            .unwrap_or_else(|status| panic!("peer {} large read: {status:?}", peer.ordinal));
        assert_eq!(large, large_content, "peer {} large bytes", peer.ordinal);
    }

    let shard_counts_after = shard_counts(&cluster).await;
    for (index, (before, after)) in shard_counts_before
        .iter()
        .zip(&shard_counts_after)
        .enumerate()
    {
        assert!(
            after > before,
            "equal peer {} received no EC4+2 shard: before={before}, after={after}",
            index + 1
        );
    }
    let cells = cluster
        .equal_peers()
        .into_iter()
        .map(|peer| peer.cell_id)
        .collect::<BTreeSet<_>>();
    assert_eq!(cells.len(), 6, "EC4+2 placements span six cells");
}

#[tokio::test]
async fn docker_ec42_degraded_read_write_threshold_and_repair() {
    let cluster = isolated_docker_test_cluster("ec42-repair", "test-region-1").await;
    let fixture = create_fixture(&cluster, "ec42-repair").await;
    let peer_one = cluster.equal_peer(1);
    let object_key = "degraded-object";
    let content = deterministic_bytes(768 * 1024, 47);
    let observation = put_and_observe(
        &peer_one.grpc_addr,
        &fixture,
        object_key,
        &content,
        &MutationIdentity::unique("degraded-object"),
    )
    .await;
    cluster
        .wait_for_metadata_replica_convergence(
            &fixture.actor,
            &fixture.bucket_name,
            object_key,
            &observation,
            DISTRIBUTED_WAIT,
        )
        .await;
    let node_six_shards = cluster.node_block_shard_count(6).await;
    assert!(node_six_shards > 0, "peer 6 received no object shard");

    cluster.stop_node(4).await;
    assert_eq!(
        get_object_at(&peer_one.grpc_addr, &fixture, object_key)
            .await
            .expect("EC4+2 read with one missing shard"),
        content
    );
    cluster.stop_node(5).await;
    assert_eq!(
        get_object_at(&peer_one.grpc_addr, &fixture, object_key)
            .await
            .expect("EC4+2 read with two missing shards"),
        content
    );

    let failed_write = put_object_at(
        &peer_one.grpc_addr,
        &fixture,
        "write-without-six-acks",
        &deterministic_bytes(256 * 1024, 53),
        &MutationIdentity::unique("write-without-six-acks"),
    )
    .await
    .expect_err("EC4+2 publication must fail without all six shard receipts");
    assert_retryable_closed_failure(&failed_write, "EC4+2 degraded write");

    cluster.stop_node(6).await;
    let below_quorum = get_object_at(&peer_one.grpc_addr, &fixture, object_key)
        .await
        .expect_err("EC4+2 read must fail below four shards");
    assert_retryable_closed_failure(&below_quorum, "EC4+2 below-quorum read");

    cluster.start_node(4).await;
    cluster.start_node(5).await;
    cluster.start_node(6).await;
    cluster.erase_node_block_shards_and_restart(6).await;

    assert_eq!(
        get_object_at(&peer_one.grpc_addr, &fixture, object_key)
            .await
            .expect("degraded read should enqueue repair"),
        content
    );
    cluster
        .wait_for_node_block_shard_count_at_least(6, node_six_shards, DISTRIBUTED_WAIT)
        .await;

    cluster.stop_node(4).await;
    cluster.stop_node(5).await;
    assert_eq!(
        get_object_at(&peer_one.grpc_addr, &fixture, object_key)
            .await
            .expect("repaired peer 6 must supply the fourth shard"),
        content
    );
    cluster.start_node(4).await;
    cluster.start_node(5).await;
}

async fn shard_counts(cluster: &anvil_test_utils::DockerTestCluster) -> Vec<u64> {
    let mut counts = Vec::new();
    for ordinal in 1..=6 {
        counts.push(cluster.node_block_shard_count(ordinal).await);
    }
    counts
}
