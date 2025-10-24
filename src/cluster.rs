use anyhow::Result;
use chrono::Utc;
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use sha2::Sha256;
use libp2p::{
    PeerId, Swarm,
    gossipsub::{self, IdentTopic as Topic},
    identity, mdns,
    swarm::{NetworkBehaviour, SwarmEvent},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;

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
    pub timestamp: i64,
    #[serde(with = "serde_bytes")]
    pub signature: Vec<u8>,
}

impl ClusterMessage {
    // Sign the message with the given secret.
    pub fn sign(&mut self, secret: &str) -> Result<()> {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
        mac.update(&self.peer_id.to_bytes());
        mac.update(self.p2p_addrs.join(",").as_bytes());
        mac.update(self.grpc_addr.as_bytes());
        mac.update(&self.timestamp.to_le_bytes());
        self.signature = mac.finalize().into_bytes().to_vec();
        Ok(())
    }

    // Verify the message's signature.
    pub fn verify(&self, secret: &str) -> Result<()> {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())?;
        mac.update(&self.peer_id.to_bytes());
        mac.update(self.p2p_addrs.join(",").as_bytes());
        mac.update(self.grpc_addr.as_bytes());
        mac.update(&self.timestamp.to_le_bytes());
        mac.verify_slice(&self.signature)?;
        Ok(())
    }
}

// A module for custom PeerId serialization
mod serde_peer_id {
    use libp2p::PeerId;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

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

// A module for custom byte array serialization
mod serde_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&hex::encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        hex::decode(s).map_err(serde::de::Error::custom)
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

pub async fn create_swarm(config: Arc<crate::config::Config>) -> Result<Swarm<ClusterBehaviour>> {
    let local_key = identity::Keypair::generate_ed25519();

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_quic()
        .with_behaviour(|key| {
            let gossipsub_config = gossipsub::Config::default();
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )
            .unwrap();
            // Conditionally enable mDNS
            let mdns = if config.enable_mdns {
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?
            } else {
                // If mDNS is disabled, create a disabled behaviour with a long query interval and short TTL
                mdns::tokio::Behaviour::new(
                    mdns::Config {
                        ttl: std::time::Duration::from_secs(0),
                        query_interval: std::time::Duration::from_secs(3600),
                        ..mdns::Config::default()
                    },
                    key.public().to_peer_id(),
                )?
            };
            Ok(ClusterBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(std::time::Duration::from_secs(60)))
        .build();

    // Parse the QUIC bind address from config
    let quic_bind_addr: libp2p::Multiaddr = config.quic_bind_addr.parse()?;
    swarm.listen_on(quic_bind_addr)?;

    Ok(swarm)
}

// Function to configure and run the gossip service.
pub async fn run_gossip(
    mut swarm: Swarm<ClusterBehaviour>,
    cluster_state: ClusterState,
    grpc_addr: String,
    cluster_secret: Option<String>,
) -> Result<()> {
    let topic = Topic::new("anvil-cluster");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    let local_peer_id = *swarm.local_peer_id();

    // Add self to the cluster state immediately
    {
        let mut state = cluster_state.write().await;
        state.entry(local_peer_id).or_insert_with(|| PeerInfo {
            p2p_addrs: Vec::new(),
            grpc_addr: grpc_addr.clone(),
        });
    }

    let mut broadcast_interval = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            _ = broadcast_interval.tick() => {
                let p2p_addrs = swarm.listeners().map(|addr| addr.to_string()).collect::<Vec<_>>();
                if p2p_addrs.is_empty() {
                    continue;
                }

                let mut message = ClusterMessage {
                    peer_id: local_peer_id,
                    p2p_addrs: p2p_addrs.clone(),
                    grpc_addr: grpc_addr.clone(),
                    timestamp: Utc::now().timestamp(),
                    signature: Vec::new(),
                };

                if let Some(secret) = &cluster_secret {
                    if let Err(e) = message.sign(secret) {
                        info!("[GOSSIP] Failed to sign gossip message: {:?}", e);
                        continue;
                    }
                }

                if let Ok(encoded_message) = serde_json::to_vec(&message) {
                    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic.clone(), encoded_message) {
                        info!("[GOSSIP] Failed to publish gossip message: {:?}", e);
                    }
                }
            }

            event = swarm.select_next_some() => {
                handle_swarm_event(event, &mut swarm, &cluster_state, &grpc_addr, &cluster_secret).await;
            }
        }
    }
}


pub async fn handle_swarm_event(
    event: SwarmEvent<ClusterEvent>,
    swarm: &mut Swarm<ClusterBehaviour>,
    cluster_state: &ClusterState,
    grpc_addr: &str,
    cluster_secret: &Option<String>,
) {
    let local_peer_id = *swarm.local_peer_id();
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            info!("[GOSSIP] Listening on {address}");
            let mut state = cluster_state.write().await;
            let info = state.entry(local_peer_id).or_insert_with(|| PeerInfo {
                p2p_addrs: Vec::new(),
                grpc_addr: grpc_addr.to_string(),
            });
            let addr_string = address.to_string();
            if !info.p2p_addrs.contains(&addr_string) {
                info.p2p_addrs.push(addr_string);
            }
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            info!("[GOSSIP] Connection established with: {peer_id}");
            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
        }
        SwarmEvent::Behaviour(ClusterEvent::Mdns(mdns::Event::Discovered(list))) => {
            for (peer_id, _multiaddr) in list {
                info!("[GOSSIP] mDNS discovered: {peer_id}");
                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            }
        }
        SwarmEvent::Behaviour(ClusterEvent::Mdns(mdns::Event::Expired(list))) => {
            for (peer_id, _multiaddr) in list {
                info!("[GOSSIP] mDNS expired: {peer_id}");
                swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
            }
        }
        SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Message {
            message,
            ..
        })) => {
            if let Ok(cluster_message) = serde_json::from_slice::<ClusterMessage>(&message.data) {
                if let Some(secret) = cluster_secret {
                    if let Err(e) = cluster_message.verify(secret) {
                        info!("[GOSSIP] Invalid signature from peer: {}, error: {:?}", cluster_message.peer_id, e);
                        return;
                    }
                    // Check timestamp to prevent replay attacks
                    let now = Utc::now().timestamp();
                    if (now - cluster_message.timestamp).abs() > 60 {
                        info!("[GOSSIP] Stale message from peer: {}, timestamp: {}", cluster_message.peer_id, cluster_message.timestamp);
                        return;
                    }
                }

                info!("[GOSSIP] Received cluster message from peer: {}", cluster_message.peer_id);
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
