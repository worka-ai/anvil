---
slug: /anvil/operational-guide/scaling
title: 'Operational Guide: Scaling and Cluster Management'
description: Learn how to scale your Anvil deployment by adding new nodes and deploying across multiple regions.
tags: [operational-guide, scaling, cluster, high-availability, multi-region]
---

# Chapter 8: Scaling and Cluster Management

> **TL;DR:** Scale horizontally by adding new peers. Point new peers to an existing node using the `BOOTSTRAP_ADDRS` flag. The cluster uses a gossip protocol to automatically discover and integrate new members.

Anvil is designed to scale from a single node to a large, distributed cluster. This chapter covers the concepts and procedures for scaling your Anvil deployment.

### 8.1. Adding a Node to an Existing Cluster

Scaling an Anvil cluster horizontally is straightforward. The process involves launching a new Anvil peer and pointing it to at least one existing member of the cluster.

**Steps to Add a New Node:**

1.  **Provision a New Host:** Set up a new server (or container) for the Anvil peer.
2.  **Configure the Node:** Configure the new node using the same environment variables as the existing cluster members, with two key differences:
    *   Ensure it connects to the **same `GLOBAL_DATABASE_URL`** and the correct **`REGIONAL_DATABASE_URL`** for the region it will operate in.
    *   **Do not** use the `--init-cluster` flag.
    *   Set the `BOOTSTRAP_ADDRS` environment variable to the QUIC address of one or more existing nodes.

**Example Configuration for a New Node:**

```yaml
# In your docker-compose.yml or other deployment configuration
environment:
  # ... (same database URLs, secrets, region, etc. as the main cluster)

  # Point to the first node to join the cluster
  - BOOTSTRAP_ADDRS=/dns4/anvil1/udp/7443/quic-v1

  # Ensure each node has a unique gRPC and QUIC port if running on the same host
  - GRPC_BIND_ADDR=0.0.0.0:50052
  - QUIC_BIND_ADDR=/ip4/0.0.0.0/udp/7444/quic-v1
  - PUBLIC_ADDRS=/dns4/anvil2/udp/7444/quic-v1
```

Once launched, the new node will contact the bootstrap peer, join the cluster, and automatically discover all other members via the gossip protocol. It will then begin participating in object storage and retrieval.

### 8.2. Multi-Region Deployments

For large-scale, geographically distributed deployments, Anvil supports a multi-region architecture. This allows you to place data closer to your users, reducing latency and improving performance.

**Key Principles:**

-   **Single Global Database:** All regions share one `GLOBAL_DATABASE_URL`. This database acts as the central authority for tenants, buckets, and policies.
-   **Separate Regional Databases:** Each region has its own, independent `REGIONAL_DATABASE_URL`. This is where the high-volume object metadata for that specific region is stored.
-   **Regional Peering:** Nodes within a region form a cluster. While cross-region communication is possible for future features like replication, the primary storage and retrieval operations happen within a single region.

**To Deploy a New Region:**

1.  **Set up a new PostgreSQL database** for the new region.
2.  **Run the regional database migrations** against this new database.
3.  **Register the new region** in the global database using the admin CLI:
    ```bash
    anvil admin regions create --name <NEW_REGION_NAME>
    ```
4.  **Launch new Anvil peers** in the new geographical location, configuring them with:
    *   The shared `GLOBAL_DATABASE_URL`.
    *   The new region's `REGIONAL_DATABASE_URL`.
    *   The new region's name in the `REGION` variable.

### 8.3. Understanding Peer Discovery (Gossip & mDNS)

Anvil uses a **SWIM-like gossip protocol** to manage cluster membership. This is a highly efficient and decentralized approach.

-   **Gossip:** Each peer periodically sends its state to a few other random peers. This information spreads virally through the cluster, ensuring that all nodes eventually converge on the same view of the cluster's membership.
-   **Failure Detection:** If a peer fails to respond to pings from its neighbors, it is marked as "suspect." If it remains unresponsive, it is eventually removed from the cluster state by all peers.
-   **mDNS (Multicast DNS):** For local networks (like a single data center or a Docker network), Anvil can use mDNS to automatically discover peers without needing a bootstrap address. This is enabled by default (`ENABLE_MDNS=true`) and is useful for simplifying local development and testing.
