use anyhow::Result;
use futures_util::StreamExt;
use libp2p::{
    gossipsub::{self, IdentTopic as Topic},
    identity,
    mdns,
    swarm::{NetworkBehaviour, SwarmEvent},
    Multiaddr,
    PeerId,
    Swarm,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// Rich information about a peer in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub p2p_addrs: Vec<String>,
    pub grpc_addr: String,
}

// The shared state of the cluster membership.
pub type ClusterState = Arc<RwLock<HashMap<PeerId, PeerInfo>>>;

// The message format for gossip-based cluster membership.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMessage {
    #[serde(with = "serde_peer_id")]
    pub peer_id: PeerId,
    pub p2p_addrs: Vec<String>,
    pub grpc_addr: String,
}

// A module for custom PeerId serialization
mod serde_peer_id {
    use libp2p::PeerId;
    use serde::{de::Error, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(peer_id: &PeerId, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&peer_id.to_base58())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PeerId, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(Error::custom)
    }
}

// The network behaviour that combines gossip and mDNS.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "ClusterEvent")]
pub struct ClusterBehaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
}

// Events emitted by our ClusterBehaviour.
pub enum ClusterEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
}

impl From<gossipsub::Event> for ClusterEvent {
    fn from(event: gossipsub::Event) -> Self {
        ClusterEvent::Gossipsub(event)
    }
}

impl From<mdns::Event> for ClusterEvent {
    fn from(event: mdns::Event) -> Self {
        ClusterEvent::Mdns(event)
    }
}

pub async fn create_swarm() -> Result<Swarm<ClusterBehaviour>> {
    let local_key = identity::Keypair::generate_ed25519();

    let swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            Default::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let gossipsub_config = gossipsub::Config::default();
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;
            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            Ok(ClusterBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(std::time::Duration::from_secs(60)))
        .build();

    Ok(swarm)
}

// Function to configure and run the gossip service.
pub async fn run_gossip(
    mut swarm: Swarm<ClusterBehaviour>,
    cluster_state: ClusterState,
    grpc_addr: String,
) -> Result<()> {
    let topic = Topic::new("anvil-cluster");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    let local_peer_id = *swarm.local_peer_id();
    let mut broadcast_interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = broadcast_interval.tick() => {
                let p2p_addrs = swarm.listeners().map(|addr| addr.to_string()).collect::<Vec<_>>();
                if p2p_addrs.is_empty() {
                    continue;
                }

                let message = ClusterMessage {
                    peer_id: local_peer_id,
                    p2p_addrs: p2p_addrs.clone(),
                    grpc_addr: grpc_addr.clone(),
                };

                if let Ok(encoded_message) = serde_json::to_vec(&message) {
                    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), encoded_message) {
                        println!("[GOSSIP] Failed to publish gossip message: {:?}", e);
                    }
                }
            }

            event = swarm.select_next_some() => {
                match event {
                    SwarmEvent::NewListenAddr { address, .. } => {
                        println!("[GOSSIP] Listening on {address}");
                        let mut state = cluster_state.write().await;
                        let info = state.entry(local_peer_id).or_insert_with(|| PeerInfo {
                            p2p_addrs: Vec::new(),
                            grpc_addr: grpc_addr.clone(),
                        });
                        let addr_string = address.to_string();
                        if !info.p2p_addrs.contains(&addr_string) {
                            info.p2p_addrs.push(addr_string);
                        }
                    }
                    SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Message {
                        message,
                        ..
                    })) => {
                        if let Ok(cluster_message) = serde_json::from_slice::<ClusterMessage>(&message.data) {
                            println!("[GOSSIP] Received cluster message from peer: {}", cluster_message.peer_id);
                            let mut state = cluster_state.write().await;
                            let info = state.entry(cluster_message.peer_id).or_insert_with(|| PeerInfo {
                                p2p_addrs: Vec::new(),
                                grpc_addr: cluster_message.grpc_addr,
                            });
                            for addr in cluster_message.p2p_addrs {
                                if !info.p2p_addrs.contains(&addr) {
                                    info.p2p_addrs.push(addr);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
