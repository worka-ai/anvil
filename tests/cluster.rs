use anvil::cluster::{create_swarm, ClusterBehaviour, ClusterEvent};
use libp2p::{
    gossipsub,
    swarm::{Swarm, SwarmEvent},
};
use futures_util::StreamExt;

#[tokio::test]
async fn test_cluster_gossip() {
    // 1. Create two swarms
    let mut swarm1 = create_swarm().await.unwrap();
    let mut swarm2 = create_swarm().await.unwrap();

    let topic = gossipsub::IdentTopic::new("anvil-test");
    swarm1.behaviour_mut().gossipsub.subscribe(&topic).unwrap();
    swarm2.behaviour_mut().gossipsub.subscribe(&topic).unwrap();

    // 2. Start listening on swarm1 and dial from swarm2
    swarm1.listen_on("/ip4/127.0.0.1/tcp/0".parse().unwrap()).unwrap();

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
    swarm1
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), b"hello world".to_vec())
        .unwrap();

    // 5. Assert that swarm2 receives the message
    loop {
        if let SwarmEvent::Behaviour(ClusterEvent::Gossipsub(gossipsub::Event::Message { message, .. })) = swarm2.select_next_some().await {
            assert_eq!(message.data, b"hello world");
            break;
        }
    }
}