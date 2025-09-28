use anyhow::Result;
use futures_util::StreamExt;
use libp2p::{
    gossipsub::{self, IdentTopic as Topic},
    identity,
    mdns,
    swarm::{NetworkBehaviour, SwarmEvent},
    PeerId,
    Swarm,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

// The shared state of the cluster membership.
pub type ClusterState = Arc<RwLock<HashMap<PeerId, Vec<String>>>>; // PeerId -> Multiaddrs

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

// Function to configure and run the gossip service.
pub async fn run_gossip(cluster_state: ClusterState) -> Result<()> {
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    println!("[GOSSIP] Local peer id: {local_peer_id}");

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key)
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
            let mdns = mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            Ok(ClusterBehaviour { gossipsub, mdns })
        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(std::time::Duration::from_secs(60)))
        .build();

    let topic = Topic::new("anvil-cluster");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                println!("[GOSSIP] Listening on {address}");
            }
            SwarmEvent::Behaviour(ClusterEvent::Mdns(mdns::Event::Discovered(list))) => {
                for (peer_id, multiaddr) in list {
                    println!("[GOSSIP] mDNS discovered: {peer_id}");
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                    let mut state = cluster_state.write().await;
                    let addrs = state.entry(peer_id).or_default();
                    addrs.push(multiaddr.to_string());
                }
            }
            SwarmEvent::Behaviour(ClusterEvent::Mdns(mdns::Event::Expired(list))) => {
                for (peer_id, _multiaddr) in list {
                    println!("[GOSSIP] mDNS expired: {peer_id}");
                    swarm.behaviour_mut().gossipsub.remove_explicit_peer(&peer_id);
                    let mut state = cluster_state.write().await;
                    state.remove(&peer_id);
                }
            }
            SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Message {
                propagation_source: peer_id,
                message_id: id,
                message,
            })) => {
                println!(
                    "[GOSSIP] Got message: '{}' with id: {id} from peer: {peer_id}",
                    String::from_utf8_lossy(&message.data),
                );
            }
            _ => {}
        }
    }
}
