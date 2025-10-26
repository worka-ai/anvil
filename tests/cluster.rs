use std::sync::Arc;

use anvil::cluster::{ClusterEvent, ClusterMessage, create_swarm};
use chrono::Utc;
use futures_util::StreamExt;
use libp2p::{gossipsub, swarm::SwarmEvent};

#[tokio::test]
async fn test_cluster_gossip() {
    let config = Arc::new(anvil::config::Config {
        global_database_url: "".to_string(),
        regional_database_url: "".to_string(),
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
        enable_mdns: true, // Enable for this specific gossip test
        cluster_secret: Some("test-secret".to_string()),
    });
    // 1. Create two swarms
    let mut swarm1 = create_swarm(config.clone()).await.unwrap();
    let mut swarm2 = create_swarm(config).await.unwrap();

    let topic = gossipsub::IdentTopic::new("anvil-test");
    swarm1.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
    swarm2.behaviour_mut().gossipsub.subscribe(&topic).unwrap();

    // 2. Start listening on swarm1 and dial from swarm2
    swarm1
        .listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap())
        .unwrap();

    let listen_addr = match swarm1.select_next_some().await {
        SwarmEvent::NewListenAddr { address, .. } => address,
        _ => panic!("Expected NewListenAddr event"),
    };

    swarm2.dial(listen_addr).unwrap();

    // 3. Wait for connection and gossip subscription
    let mut connected_nodes = 0;
    let mut subscribed_nodes = 0;
    loop {
        tokio::select! {
            event = swarm1.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { .. } = event {
                    connected_nodes += 1;
                }
                if let SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Subscribed { .. })) = event {
                    subscribed_nodes += 1;
                }
            },
            event = swarm2.select_next_some() => {
                if let SwarmEvent::ConnectionEstablished { .. } = event {
                    connected_nodes += 1;
                }
                 if let SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Subscribed { .. })) = event {
                    subscribed_nodes += 1;
                }
            },
        }
        if connected_nodes >= 2 && subscribed_nodes >= 2 {
            break;
        }
    }

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

    swarm1
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), encoded_message)
        .unwrap();

    // 5. Assert that swarm2 receives and can verify the message
    loop {
        if let SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Message {
            message,
            ..
        })) = swarm2.select_next_some().await
        {
            let received_message: ClusterMessage = serde_json::from_slice(&message.data).unwrap();
            assert!(received_message.verify("test-secret").is_ok());
            assert_eq!(received_message.peer_id, *swarm1.local_peer_id());
            break;
        }
    }
}

#[tokio::test]
async fn test_cluster_gossip_invalid_secret() {
    let config1 = Arc::new(anvil::config::Config {
        global_database_url: "".to_string(),
        regional_database_url: "".to_string(),
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
        enable_mdns: true,
        cluster_secret: Some("secret-1".to_string()),
    });
    let config2 = Arc::new(anvil::config::Config {
        global_database_url: "".to_string(),
        regional_database_url: "".to_string(),
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
        enable_mdns: true,
        cluster_secret: Some("secret-2".to_string()),
    });

    let mut swarm1 = create_swarm(config1).await.unwrap();
    let mut swarm2 = create_swarm(config2).await.unwrap();

    let topic = gossipsub::IdentTopic::new("anvil-test-invalid");
    swarm1.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
    swarm2.behaviour_mut().gossipsub.subscribe(&topic).unwrap();

    swarm1
        .listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap())
        .unwrap();

    let listen_addr = match swarm1.select_next_some().await {
        SwarmEvent::NewListenAddr { address, .. } => address,
        _ => panic!("Expected NewListenAddr event"),
    };

    swarm2.dial(listen_addr).unwrap();

    // Wait for connection
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            tokio::select! {
                _ = swarm1.select_next_some() => {},
                _ = swarm2.select_next_some() => {},
            }
        }
    })
    .await
    .err();

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

    swarm1
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), encoded_message)
        .unwrap();

    // Assert that swarm2 receives the message but verification fails
    loop {
        if let SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Message {
            message,
            ..
        })) = swarm2.select_next_some().await
        {
            let received_message: ClusterMessage = serde_json::from_slice(&message.data).unwrap();
            assert!(received_message.verify("secret-2").is_err());
            break;
        }
    }
}
