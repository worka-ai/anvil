use std::{sync::Arc, time::Duration};

use anvil::cluster::{ClusterBehaviour, ClusterEvent, ClusterMessage, create_swarm};
use chrono::Utc;
use futures_util::StreamExt;
use libp2p::{Swarm, gossipsub, swarm::SwarmEvent};
use tempfile::TempDir;

fn cluster_test_config(secret: &str) -> (TempDir, Arc<anvil::config::Config>) {
    let storage = tempfile::tempdir().unwrap();
    let storage_path = storage.path().join("storage");
    let config = Arc::new(anvil::config::Config {
        jwt_secret: "test-secret".to_string(),
        anvil_secret_encryption_key:
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        cluster_listen_addr: "/ip4/127.0.0.1/udp/0/quic-v1".to_string(),
        public_cluster_addrs: vec![],
        public_api_addr: "".to_string(),
        api_listen_addr: "127.0.0.1:0".to_string(),
        region: "test-region".to_string(),
        bootstrap_addrs: vec![],
        init_cluster: false,
        enable_mdns: false,
        cluster_secret: Some(secret.to_string()),
        metadata_cache_ttl_secs: 1,
        storage_path: storage_path.to_string_lossy().into_owned(),
        personaldb_snapshot_entry_threshold: 1024,
        personaldb_snapshot_payload_bytes_threshold: 64 * 1024 * 1024,
        ..anvil::config::Config::default()
    });
    (storage, config)
}

fn observe_cluster_event(
    event: &SwarmEvent<ClusterEvent>,
    swarm: &mut Swarm<ClusterBehaviour>,
    connected_nodes: &mut usize,
    subscribed_nodes: &mut usize,
) {
    match event {
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            *connected_nodes += 1;
            swarm.behaviour_mut().gossipsub.add_explicit_peer(peer_id);
        }
        SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Subscribed { .. })) => {
            *subscribed_nodes += 1;
        }
        _ => {}
    }
}

async fn first_listen_addr(swarm: &mut Swarm<ClusterBehaviour>) -> libp2p::Multiaddr {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let SwarmEvent::NewListenAddr { address, .. } = swarm.select_next_some().await {
                return address;
            }
        }
    })
    .await
    .expect("cluster swarm did not start listening")
}

async fn wait_for_gossip_ready(
    swarm1: &mut Swarm<ClusterBehaviour>,
    swarm2: &mut Swarm<ClusterBehaviour>,
) {
    let mut connected_nodes = 0;
    let mut subscribed_nodes = 0;

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = swarm1.select_next_some() => {
                    observe_cluster_event(&event, swarm1, &mut connected_nodes, &mut subscribed_nodes);
                },
                event = swarm2.select_next_some() => {
                    observe_cluster_event(&event, swarm2, &mut connected_nodes, &mut subscribed_nodes);
                },
            }

            if connected_nodes >= 2 && subscribed_nodes >= 2 {
                break;
            }
        }
    })
    .await
    .expect("cluster peers did not become gossip-ready");
}

async fn publish_with_retry(
    swarm1: &mut Swarm<ClusterBehaviour>,
    swarm2: &mut Swarm<ClusterBehaviour>,
    topic: gossipsub::IdentTopic,
    payload: Vec<u8>,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match swarm1
                .behaviour_mut()
                .gossipsub
                .publish(topic.clone(), payload.clone())
            {
                Ok(_) => break,
                Err(gossipsub::PublishError::NoPeersSubscribedToTopic) => {
                    tokio::select! {
                        event = swarm1.select_next_some() => {
                            let mut connected = 0;
                            let mut subscribed = 0;
                            observe_cluster_event(&event, swarm1, &mut connected, &mut subscribed);
                        },
                        event = swarm2.select_next_some() => {
                            let mut connected = 0;
                            let mut subscribed = 0;
                            observe_cluster_event(&event, swarm2, &mut connected, &mut subscribed);
                        },
                        _ = tokio::time::sleep(Duration::from_millis(25)) => {}
                    }
                }
                Err(err) => panic!("failed to publish cluster gossip message: {err:?}"),
            }
        }
    })
    .await
    .expect("cluster message could not be published");
}

async fn receive_gossip_message(
    swarm1: &mut Swarm<ClusterBehaviour>,
    swarm2: &mut Swarm<ClusterBehaviour>,
) -> ClusterMessage {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            tokio::select! {
                event = swarm1.select_next_some() => {
                    let mut connected = 0;
                    let mut subscribed = 0;
                    observe_cluster_event(&event, swarm1, &mut connected, &mut subscribed);
                },
                event = swarm2.select_next_some() => {
                    if let SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Message {
                        message,
                        ..
                    })) = event
                    {
                        return serde_json::from_slice::<ClusterMessage>(&message.data).unwrap();
                    }

                    let mut connected = 0;
                    let mut subscribed = 0;
                    observe_cluster_event(&event, swarm2, &mut connected, &mut subscribed);
                },
            }
        }
    })
    .await
    .expect("cluster gossip message was not received")
}

#[tokio::test]
async fn test_cluster_gossip() {
    let (_storage1, config1) = cluster_test_config("test-secret");
    let (_storage2, config2) = cluster_test_config("test-secret");
    // 1. Create two swarms
    let mut swarm1 = create_swarm(config1).await.unwrap();
    let mut swarm2 = create_swarm(config2).await.unwrap();

    let topic = gossipsub::IdentTopic::new("anvil-test");
    swarm1.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
    swarm2.behaviour_mut().gossipsub.subscribe(&topic).unwrap();

    // 2. Dial swarm1 from swarm2
    let listen_addr = first_listen_addr(&mut swarm1).await;
    swarm2.dial(listen_addr).unwrap();

    // 3. Wait for connection and gossip subscription
    wait_for_gossip_ready(&mut swarm1, &mut swarm2).await;

    // 4. Publish a message from swarm1
    let mut message = ClusterMessage {
        peer_id: *swarm1.local_peer_id(),
        p2p_addrs: vec!["/ip4/127.0.0.1/udp/1234/quic-v1".to_string()],
        grpc_addr: "127.0.0.1:50051".to_string(),
        timestamp: Utc::now().timestamp(),
        signature: vec![],
    };
    message.sign("test-secret").unwrap();
    let encoded_message = serde_json::to_vec(&message).unwrap();

    publish_with_retry(&mut swarm1, &mut swarm2, topic, encoded_message).await;

    // 5. Assert that swarm2 receives and can verify the message
    let received_message = receive_gossip_message(&mut swarm1, &mut swarm2).await;
    assert!(received_message.verify("test-secret").is_ok());
    assert_eq!(received_message.peer_id, *swarm1.local_peer_id());
}

#[tokio::test]
async fn test_cluster_gossip_invalid_secret() {
    let (_storage1, config1) = cluster_test_config("secret-1");
    let (_storage2, config2) = cluster_test_config("secret-2");

    let mut swarm1 = create_swarm(config1).await.unwrap();
    let mut swarm2 = create_swarm(config2).await.unwrap();

    let topic = gossipsub::IdentTopic::new("anvil-test-invalid");
    swarm1.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
    swarm2.behaviour_mut().gossipsub.subscribe(&topic).unwrap();

    let listen_addr = first_listen_addr(&mut swarm1).await;
    swarm2.dial(listen_addr).unwrap();
    wait_for_gossip_ready(&mut swarm1, &mut swarm2).await;

    // Publish a message from swarm1
    let mut message = ClusterMessage {
        peer_id: *swarm1.local_peer_id(),
        p2p_addrs: vec!["/ip4/127.0.0.1/udp/1234/quic-v1".to_string()],
        grpc_addr: "127.0.0.1:50051".to_string(),
        timestamp: Utc::now().timestamp(),
        signature: vec![],
    };
    message.sign("secret-1").unwrap();
    let encoded_message = serde_json::to_vec(&message).unwrap();

    publish_with_retry(&mut swarm1, &mut swarm2, topic, encoded_message).await;

    // Assert that swarm2 receives the message but verification fails
    let received_message = receive_gossip_message(&mut swarm1, &mut swarm2).await;
    assert!(received_message.verify("secret-2").is_err());
}
