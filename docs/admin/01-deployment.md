---
slug: /admin/deployment
title: 'Administrator's Guide: Deployment'
description: A comprehensive guide to deploying Anvil, from a single node for development to a multi-region cluster for production.
tags: [admin, deployment, cluster, scaling, multi-region, production]
---

# Administrator's Guide: Deployment

This guide covers the fundamentals of deploying Anvil. The architecture is flexible, allowing you to start with a simple single-node setup for development and scale out to a distributed, multi-region cluster for production.

## 1. Core Deployment Concepts

- **Single vs. Multi-Node:** Anvil can run as a single instance or as a cluster of multiple peer nodes that work together for durability and scale.
- **Configuration:** All Anvil nodes are configured entirely through environment variables.
- **Database Layout:** The architecture relies on two types of PostgreSQL databases:
    - A **single Global Database** used by all nodes in all regions. This stores low-volume, critical data like tenants, apps, and bucket information.
    - One **Regional Database per region**. This stores the high-volume object metadata for that specific region, allowing for massive horizontal scaling.

## 2. Single-Node Deployment

A single-node deployment is the simplest way to run Anvil and is perfect for development and testing. It consists of one Anvil instance and its required PostgreSQL databases.

For a complete, working `docker-compose.yml` and a step-by-step tutorial for this setup, please refer to the [**Getting Started Guide**](../fundamentals/getting-started).

## 3. Multi-Node Deployment (Single Region)

For production, you will run Anvil as a cluster of multiple nodes. This tutorial covers scaling from one to two nodes within the same region.

**The Scenario:**
-   **Host A (IP: `203.0.113.1`):** Runs the initial Anvil node (`anvil1`) and the databases.
-   **Host B (IP: `203.0.113.2`):** Will run our new Anvil node (`anvil2`).

#### Step 1: Launch the First Node

On Host A, launch the first Anvil node using the `--init-cluster` command flag. This tells the node it is the first peer and should not seek others to join.

*Key `anvil1` environment variables on Host A:*
```yaml
environment:
  REGION: "europe-west-1"
  # ... other config ...
  # This node is the first in the cluster
  command: ["anvil", "--init-cluster"]
  # --- Networking Configuration ---
  PUBLIC_CLUSTER_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
  BOOTSTRAP_ADDRS: ""
```

#### Step 2: Launch the Second Node

On Host B, configure the second Anvil node (`anvil2`). It must point to the same databases as `anvil1` and use the same shared secrets. Crucially, you must provide the address of the first node in the `BOOTSTRAP_ADDRS` variable.

*Key `anvil2` environment variables on Host B:*
```yaml
environment:
  REGION: "europe-west-1"
  # --- Point to existing databases on Host A ---
  GLOBAL_DATABASE_URL: "postgres://worka:a-secure-password@203.0.113.1:5433/anvil_global"
  REGIONAL_DATABASE_URL: "postgres://worka:a-secure-password@203.0.113.1:5432/anvil_regional_europe"
  # --- Use the SAME secrets as the rest of the cluster ---
  JWT_SECRET: "..."
  ANVIL_SECRET_ENCRYPTION_KEY: "..."
  # --- BOOTSTRAP from Host A --- 
  BOOTSTRAP_ADDRS: "/ip4/203.0.113.1/udp/7443/quic-v1"
# Note: No `command` is needed, as we are NOT initializing a cluster
```

When `anvil2` starts, it will connect to `anvil1`, join the cluster via the gossip protocol, and become a fully integrated member, ready to store object shards.

## 4. Multi-Region Deployment

For large-scale, geographically distributed deployments, you can deploy Anvil nodes across multiple regions.

**The Scenario:**
-   **3 Regions:** `us-east-1`, `europe-west-1`, and `ap-southeast-1`.
-   Multiple Anvil nodes per region.
-   **1 Global Database** and **3 Regional Databases** (one for each region).

#### Step 1: Set Up Databases
Provision your four PostgreSQL databases. Ensure the Global DB is accessible from all regions, and each Regional DB is accessible from hosts within its region. Run the `migrations_global` scripts on the Global DB and the `migrations_regional` scripts on each of the three Regional DBs.

#### Step 2: Launch Initial Node in Each Region
Just like in the multi-node setup, you must launch the *first* node in each region with the `--init-cluster` flag. 

For the first node in `us-east-1`, the configuration would include:
```yaml
REGION: "us-east-1"
REGIONAL_DATABASE_URL: "postgres://.../anvil_regional_us_east"
command: ["anvil", "--init-cluster"]
BOOTSTRAP_ADDRS: ""
```

#### Step 3: Launch Subsequent Nodes in Each Region
Launch additional nodes in each region *without* the `--init-cluster` flag. These nodes should point to the first node **in their own region** as the `BOOTSTRAP_ADDRS`.

For a second node in `us-east-1`:
```yaml
REGION: "us-east-1"
REGIONAL_DATABASE_URL: "postgres://.../anvil_regional_us_east"
BOOTSTRAP_ADDRS: "/ip4/FIRST_US_EAST_NODE_IP/udp/7443/quic-v1"
```

Once deployed, you can create buckets in any of the registered regions. When a client uploads an object to a bucket, the data will automatically be sharded and stored on the nodes within that bucket's designated region.

## 5. Firewall Configuration

If your Anvil hosts run a firewall, you must open the necessary ports.

-   **50051/tcp:** The unified API endpoint for the S3 Gateway and gRPC service.
-   **7443/udp:** The QUIC endpoint for peer-to-peer gossip and data transfer.

#### UFW (Ubuntu/Debian)
```bash
sudo ufw allow 50051/tcp
sudo ufw allow 7443/udp
sudo ufw reload
```

#### firewalld (RHEL/CentOS/Fedora)
```bash
sudo firewall-cmd --zone=public --add-port=50051/tcp --permanent
sudo firewall-cmd --zone=public --add-port=7443/udp --permanent
sudo firewall-cmd --reload
```
