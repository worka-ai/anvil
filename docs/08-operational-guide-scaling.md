---
slug: /anvil/operational-guide/scaling
title: 'Operational Guide: Scaling and Cluster Management'
description: Learn how to scale your Anvil deployment by adding new nodes and deploying across multiple regions.
tags: [operational-guide, scaling, cluster, high-availability, multi-region]
---

# Chapter 8: Scaling and Cluster Management

> **TL;DR:** Scale horizontally by adding new peers. Point new peers to an existing node using the `BOOTSTRAP_ADDRS` flag. The cluster uses a gossip protocol to automatically discover and integrate new members.

Anvil is designed to scale from a single node to a large, distributed cluster. This chapter covers the concepts and procedures for scaling your Anvil deployment.

### 8.1. Tutorial: Adding a New Node to a Cluster

This tutorial provides a concrete, step-by-step example of how to scale an Anvil cluster from one node to two, with each node running on a separate host machine. This directly answers the question: *"How do I have a subsequent node join the cluster?"*

**The Scenario:**

-   **Host A (IP: `203.0.113.1`):** Runs the initial Anvil node (`anvil1`), the global Postgres database, and the regional Postgres database.
-   **Host B (IP: `203.0.113.2`):** Will run our new Anvil node (`anvil2`).

#### Step 1: Initial Setup on Host A

On Host A, you have your initial `docker-compose.yml` file, similar to the one in the Getting Started guide. The databases are running, and `anvil1` is configured with its public IP.

*Key `anvil1` environment variables on Host A:*
```yaml
environment:
  # ... other config
  REGION: "europe-west-1"
  # This node is the first in the cluster
  command: ["anvil", "--init-cluster"]
  # --- Networking Configuration ---
  PUBLIC_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
  PUBLIC_GRPC_ADDR: "http://203.0.113.1:50051"
```

#### Step 2: Prepare Host B

On Host B, you will create a new, much simpler Docker Compose file. Let's call it `docker-compose.node.yml`.

> **Important Note on `docker-compose`:** `docker-compose` is designed for single-host applications. The following example is a conceptual guide showing how you would configure a second node. For true multi-host production deployments, you should use an orchestration tool like **Kubernetes**, **Docker Swarm**, or **Nomad**. However, the Anvil configuration principles remain the same.

Create the following `docker-compose.node.yml` on **Host B**:

```yaml
# docker-compose.node.yml (for Host B)
version: "3.8"

services:
  anvil2:
    image: ghcr.io/worka-ai/anvil:main
    # We don't run databases here; we point to the ones on Host A
    environment:
      RUST_LOG: "info"
      # --- CRITICAL: Point to the existing databases on Host A ---
      # The password must be URL-encoded if it contains special characters.
      GLOBAL_DATABASE_URL: "postgres://worka:a-secure-password@203.0.113.1:5433/anvil_global"
      REGIONAL_DATABASE_URL: "postgres://worka:a-secure-password@203.0.113.1:5432/anvil_regional_europe"
      REGION: "europe-west-1"

      # --- Use the SAME secrets as the rest of the cluster ---
      JWT_SECRET: "must-be-a-long-and-random-secret-for-signing-jwts"
      ANVIL_SECRET_ENCRYPTION_KEY: "must-be-a-64-character-hex-string-generate-with-openssl-rand-hex-32"
      ANVIL_CLUSTER_SECRET: "must-be-a-long-and-random-secret-for-cluster-gossip"

      # --- Networking for Host B ---
      HTTP_BIND_ADDR: "0.0.0.0:9000"
      GRPC_BIND_ADDR: "0.0.0.0:50051"
      QUIC_BIND_ADDR: "/ip4/0.0.0.0/udp/7443/quic-v1"
      PUBLIC_ADDRS: "/ip4/203.0.113.2/udp/7443/quic-v1"
      PUBLIC_GRPC_ADDR: "http://203.0.113.2:50051"
      ENABLE_MDNS: "false"

      # --- BOOTSTRAP from Host A ---
      BOOTSTRAP_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
    # Note: No `command` is needed, as we are NOT initializing a cluster
    ports:
      - "9000:9000"
      - "50051:50051"
      - "7443:7443/udp"
```

#### Step 3: Launch the New Node

On **Host B**, run the following command:

```bash
docker-compose -f docker-compose.node.yml up -d
```

#### What Happens Next?

1.  `anvil2` starts up on Host B.
2.  It reads its `BOOTSTRAP_ADDRS` and connects to `anvil1` on `203.0.113.1:7443/udp`.
3.  `anvil1` accepts the connection. The two nodes exchange information via the gossip protocol.
4.  `anvil2` learns about all other nodes `anvil1` knows about (if any), and vice-versa.
5.  Within seconds, `anvil2` is a fully integrated member of the cluster and will be considered by the `PlacementManager` for storing new object shards.

This process can be repeated for `anvil3`, `anvil4`, and so on, allowing you to scale your cluster horizontally across many machines.

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
