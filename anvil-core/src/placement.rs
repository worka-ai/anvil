use crate::cluster::ClusterState;
use blake3::Hasher;
use libp2p::PeerId;

#[derive(Debug, Clone, Default)]
pub struct PlacementManager;

impl PlacementManager {
    /// Calculates the placement of shards for a given object key using Rendezvous Hashing.
    pub async fn calculate_placement(
        &self,
        object_key: &str,
        cluster_state: &ClusterState,
        count: usize,
    ) -> Vec<PeerId> {
        let nodes = cluster_state.read().await;
        if nodes.is_empty() {
            return vec![];
        }

        let mut scores: Vec<([u8; 32], PeerId)> = nodes
            .keys()
            .map(|peer_id| {
                let mut hasher = Hasher::new();
                // Hash both the object key and the peer id to get a unique score
                hasher.update(object_key.as_bytes());
                hasher.update(&peer_id.to_bytes());
                (hasher.finalize().into(), peer_id.clone())
            })
            .collect();

        // Sort by score in descending order. The hash bytes are compared lexicographically.
        scores.sort_by(|a, b| b.0.cmp(&a.0));

        // Take the top `count` nodes
        scores
            .into_iter()
            .map(|(_, peer_id)| peer_id)
            .take(count)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::PeerInfo;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_placement_determinism_and_balancing() {
        let manager = PlacementManager::default();
        let cluster_state: ClusterState = Arc::new(RwLock::new(HashMap::new()));

        // Add some nodes to the cluster state
        let peers: Vec<PeerId> = (0..10).map(|_| PeerId::random()).collect();
        {
            let mut state = cluster_state.write().await;
            for peer in &peers {
                state.insert(
                    peer.clone(),
                    PeerInfo {
                        p2p_addrs: vec![],
                        grpc_addr: String::new(),
                    },
                );
            }
        }

        let object_key1 = uuid::Uuid::new_v4().to_string();
        let object_key2 = uuid::Uuid::new_v4().to_string();

        // Calculate placement twice for the same key
        let placement1 = manager
            .calculate_placement(&object_key1, &cluster_state, 3)
            .await;
        let placement2 = manager
            .calculate_placement(&object_key1, &cluster_state, 3)
            .await;

        // Assert that the placement is deterministic
        assert_eq!(placement1, placement2, "Placement should be deterministic");
        assert_eq!(placement1.len(), 3, "Should return 3 nodes");

        // Calculate placement for a different key
        let placement3 = manager
            .calculate_placement(&object_key2, &cluster_state, 3)
            .await;
        assert_eq!(placement3.len(), 3, "Should return 3 nodes");

        // Assert that the placement for a different key is different
        assert_ne!(
            placement1, placement3,
            "Placement for different keys should be different"
        );
    }
}
