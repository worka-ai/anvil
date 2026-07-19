use super::*;

const DOCKER_EQUAL_PEER_COUNT: u8 = 6;
const DOCKER_METADATA_REPLICA_COUNT: u8 = 3;
const BLOCK_SHARD_ROOT: &str = "/var/lib/anvil/corestore/blocks/local-cache";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DockerPeer {
    pub ordinal: u8,
    pub node_id: String,
    pub cell_id: String,
    pub grpc_addr: String,
    pub admin_addr: String,
}

impl DockerPeer {
    pub fn service_name(&self) -> String {
        docker_node_service(self.ordinal)
    }
}

impl DockerTestCluster {
    pub fn equal_peers(&self) -> Vec<DockerPeer> {
        assert_eq!(
            self.grpc_addrs.len(),
            DOCKER_EQUAL_PEER_COUNT as usize,
            "distributed Docker acceptance requires six equal peers"
        );
        assert_eq!(
            self.admin_addrs.len(),
            DOCKER_EQUAL_PEER_COUNT as usize,
            "distributed Docker acceptance requires six equal admin endpoints"
        );
        (1..=DOCKER_EQUAL_PEER_COUNT)
            .map(|ordinal| self.equal_peer(ordinal))
            .collect()
    }

    pub fn equal_peer(&self, ordinal: u8) -> DockerPeer {
        assert!(
            (1..=DOCKER_EQUAL_PEER_COUNT).contains(&ordinal),
            "Docker equal-peer ordinal must be between 1 and 6"
        );
        let index = usize::from(ordinal - 1);
        DockerPeer {
            ordinal,
            node_id: format!("anvil-test-node-{ordinal}"),
            cell_id: format!("test-cell-{ordinal}"),
            grpc_addr: self.grpc_addrs[index].clone(),
            admin_addr: self.admin_addrs[index].clone(),
        }
    }

    /// The default metadata-r3-q2 profile selects the first three active
    /// candidates after the production region/cell/node ordering is applied.
    pub fn selected_metadata_replicas(&self) -> Vec<DockerPeer> {
        self.equal_peers()
            .into_iter()
            .take(DOCKER_METADATA_REPLICA_COUNT as usize)
            .collect()
    }

    pub async fn restart_node_same_volume(&self, ordinal: u8) {
        self.stop_node(ordinal).await;
        self.start_node(ordinal).await;
    }

    /// Recreates one equal peer with a fresh named volume. This intentionally
    /// does not call the test bootstrap snapshot installer: recovery must use
    /// the same production join and catch-up path as a real late peer.
    pub async fn recreate_node_with_empty_volume(&self, ordinal: u8) {
        self.stop_node(ordinal).await;
        let compose_file = self.compose_file.clone();
        let project_name = self.project_name.clone();
        let compose_env = self.compose_env.clone();
        tokio::task::spawn_blocking(move || {
            docker_recreate_node_with_empty_volume(
                &compose_file,
                &project_name,
                &compose_env,
                ordinal,
            );
        })
        .await
        .expect("recreate Docker equal peer with empty volume panicked");
        self.start_node(ordinal).await;
    }

    /// Removes only final block shards while preserving the peer's metadata,
    /// identity, and volume. Restarting must cause production repair to restore
    /// any shard that is still required by a published manifest.
    pub async fn erase_node_block_shards_and_restart(&self, ordinal: u8) {
        self.stop_node(ordinal).await;
        let project_name = self.project_name.clone();
        let compose_env = self.compose_env.clone();
        tokio::task::spawn_blocking(move || {
            docker_clear_node_block_shards(&project_name, &compose_env, ordinal);
        })
        .await
        .expect("clear Docker equal-peer block shards panicked");
        self.start_node(ordinal).await;
    }

    pub async fn node_block_shard_count(&self, ordinal: u8) -> u64 {
        let output = self
            .exec_node_output(
                ordinal,
                &[
                    "sh",
                    "-c",
                    &format!("find {BLOCK_SHARD_ROOT} -type f -name '*.anb' 2>/dev/null | wc -l"),
                ],
            )
            .await;
        assert!(
            output.status.success(),
            "count Docker peer {ordinal} block shards: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("Docker shard count is utf-8")
            .trim()
            .parse::<u64>()
            .expect("Docker shard count is numeric")
    }

    pub async fn wait_for_node_block_shard_count_at_least(
        &self,
        ordinal: u8,
        minimum: u64,
        timeout: Duration,
    ) {
        let deadline = Instant::now() + timeout;
        let mut observed = 0;
        while Instant::now() < deadline {
            observed = self.node_block_shard_count(ordinal).await;
            if observed >= minimum {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        panic!(
            "Docker peer {ordinal} did not recover {minimum} block shards before timeout; observed {observed}"
        );
    }

    /// Splits all six peers into two disjoint Docker bridge networks. Each
    /// partition retains normal service aliases and host-published endpoints,
    /// while cross-partition peer traffic is impossible until `heal` runs.
    pub async fn partition_equal_peers(
        &self,
        first: &[u8],
        second: &[u8],
    ) -> DockerNetworkPartition {
        validate_partition_groups(first, second);
        let state = DockerNetworkPartitionState::new(self, first, second);
        let apply = state.clone();
        tokio::task::spawn_blocking(move || apply_partition(&apply))
            .await
            .expect("apply Docker equal-peer network partition panicked");
        DockerNetworkPartition { state: Some(state) }
    }
}

#[derive(Debug)]
pub struct DockerNetworkPartition {
    state: Option<DockerNetworkPartitionState>,
}

impl DockerNetworkPartition {
    pub async fn heal(mut self) {
        let state = self
            .state
            .clone()
            .expect("Docker network partition was already healed");
        tokio::task::spawn_blocking(move || heal_partition(&state))
            .await
            .expect("heal Docker equal-peer network partition panicked");
        self.state = None;
    }
}

impl Drop for DockerNetworkPartition {
    fn drop(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        let _ = std::panic::catch_unwind(|| heal_partition(&state));
    }
}

#[derive(Debug, Clone)]
struct DockerNetworkPartitionState {
    original_network: String,
    first_network: String,
    second_network: String,
    first: Vec<(String, String)>,
    second: Vec<(String, String)>,
}

impl DockerNetworkPartitionState {
    fn new(cluster: &DockerTestCluster, first: &[u8], second: &[u8]) -> Self {
        let suffix = uuid::Uuid::new_v4().simple();
        let original_network = docker_compose_network_name(&cluster.project_name);
        let peers = |ordinals: &[u8]| {
            ordinals
                .iter()
                .map(|ordinal| {
                    (
                        docker_container_id(&cluster.project_name, *ordinal),
                        docker_node_service(*ordinal),
                    )
                })
                .collect::<Vec<_>>()
        };
        Self {
            original_network,
            first_network: format!("{}-partition-a-{suffix}", cluster.project_name),
            second_network: format!("{}-partition-b-{suffix}", cluster.project_name),
            first: peers(first),
            second: peers(second),
        }
    }
}

fn validate_partition_groups(first: &[u8], second: &[u8]) {
    assert!(!first.is_empty(), "first Docker network partition is empty");
    assert!(
        !second.is_empty(),
        "second Docker network partition is empty"
    );
    let actual = first.iter().chain(second).copied().collect::<BTreeSet<_>>();
    let expected = (1..=DOCKER_EQUAL_PEER_COUNT).collect::<BTreeSet<_>>();
    assert_eq!(
        first.len() + second.len(),
        expected.len(),
        "Docker network partitions contain duplicate peers"
    );
    assert_eq!(
        actual, expected,
        "Docker network partitions must contain every equal peer exactly once"
    );
}

fn apply_partition(state: &DockerNetworkPartitionState) {
    docker_create_bridge_network(&state.first_network, "anvil-distributed-test");
    docker_create_bridge_network(&state.second_network, "anvil-distributed-test");
    for (container, alias) in &state.first {
        docker_connect_network(&state.first_network, container, alias);
    }
    for (container, alias) in &state.second {
        docker_connect_network(&state.second_network, container, alias);
    }
    for (container, _) in state.first.iter().chain(&state.second) {
        docker_disconnect_network(&state.original_network, container);
    }
}

fn heal_partition(state: &DockerNetworkPartitionState) {
    for (container, alias) in state.first.iter().chain(&state.second) {
        docker_connect_network(&state.original_network, container, alias);
    }
    for (container, _) in &state.first {
        docker_disconnect_network(&state.first_network, container);
    }
    for (container, _) in &state.second {
        docker_disconnect_network(&state.second_network, container);
    }
    docker_remove_network(&state.first_network);
    docker_remove_network(&state.second_network);
}
