---
slug: /architecture/clustering
title: 'Deep Dive: Clustering and Membership'
description: A detailed look at how Anvil nodes discover each other and maintain cluster state using a SWIM-like gossip protocol over QUIC.
tags: [architecture, deep-dive, clustering, gossip, libp2p]
---

# Chapter 12: Deep Dive: Clustering and Membership

> **TL;DR:** Nodes use a SWIM-like gossip protocol over QUIC to discover each other and detect failures, maintaining a shared view of the cluster's state.

Anvil's ability to scale from a single node to a large cluster relies on a robust and decentralized mechanism for peer discovery and failure detection. This is achieved using a **SWIM-like gossip protocol** implemented using the `libp2p` framework.

### The `ClusterBehaviour`

At the heart of Anvil's networking is the `ClusterBehaviour`, a custom `NetworkBehaviour` from `libp2p`. It combines two key components:

1.  **`gossipsub`:** This is `libp2p`'s implementation of a pub-sub protocol over a gossip network. Anvil uses a single, well-known topic (`anvil-cluster`) to broadcast and receive membership information.
2.  **`mdns`:** For local networks, multicast DNS is used as a zero-configuration discovery mechanism. It allows nodes on the same LAN (or Docker network) to find each other without needing a predefined bootstrap node. This is primarily for development and testing convenience.

### The Membership Gossip Message

Periodically, each node in the cluster broadcasts a `ClusterMessage` to all other peers on the `anvil-cluster` topic. This message, defined in `src/cluster.rs`, contains all the information another node needs to know about the sender:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMessage {
    pub peer_id: PeerId,       // The unique libp2p identifier of the node
    pub p2p_addrs: Vec<String>,// The public QUIC multiaddresses it is listening on
    pub grpc_addr: String,     // The public gRPC address for API calls
    pub timestamp: i64,        // A timestamp to prevent replay attacks
    pub signature: Vec<u8>,    // A signature to verify authenticity
}
```

#### Security

To prevent unauthorized nodes from joining the cluster and to ensure message integrity, every `ClusterMessage` is signed using an HMAC-SHA256 signature. The `ANVIL_CLUSTER_SECRET` environment variable provides the shared secret used for signing and verification. When a node receives a gossip message, it first verifies the signature. If the signature is invalid or the message timestamp is too old, the message is discarded.

### The Cluster State

Each node maintains its own view of the cluster's state in a thread-safe, in-memory map:

```rust
pub type ClusterState = Arc<RwLock<HashMap<PeerId, PeerInfo>>>;
```

When a node receives a valid `ClusterMessage`, it updates this map with the information about the sending peer. This map is the source of truth for all other parts of the system that need to know about the cluster topology, such as the `PlacementManager`.

### The Lifecycle of a Node

1.  **Startup:** An Anvil node initializes its `libp2p` swarm, which includes the `ClusterBehaviour`.

2.  **Listening:** The node starts listening for incoming QUIC connections on the address specified by `CLUSTER_LISTEN_ADDR`.

3.  **Bootstrapping:**
    *   If `BOOTSTRAP_ADDRS` is provided, the node immediately attempts to dial one of the specified peers.
    *   If no bootstrap addresses are given, it relies on mDNS to find peers on the local network.

4.  **Joining the Gossip:** Once connected to a peer, it subscribes to the `anvil-cluster` topic. It immediately starts receiving gossip messages from other nodes and, in turn, starts broadcasting its own `ClusterMessage` periodically.

5.  **Convergence:** Within a short time, the gossip protocol ensures that the new node learns about all other members of the cluster, and all other members learn about the new node. The cluster has now **converged** on a new state.

6.  **Failure Detection:** `libp2p`'s underlying connection management handles failure detection. If a node becomes unreachable, its connection will be dropped. While Anvil's current implementation doesn't explicitly remove peers from the `ClusterState` on disconnect, a production-ready system would add a mechanism to periodically prune peers that haven't been seen in a gossip message for a certain amount of time, thus completing the SWIM protocol's failure detection loop.
