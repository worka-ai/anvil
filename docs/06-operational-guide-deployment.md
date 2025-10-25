---
slug: /anvil/operational-guide/deployment
title: 'Operational Guide: Deployment'
description: Learn how to deploy Anvil, from a single node for development to a multi-node cluster for production.
tags: [operational-guide, deployment, cluster, configuration, postgres]
---

# Chapter 6: Deployment

> **TL;DR:** Deploy a single node for development or a multi-node cluster for production. Configuration is managed via environment variables. A cluster requires a shared global Postgres and one regional Postgres per region.

This chapter covers the fundamentals of deploying Anvil. The architecture is flexible, allowing you to start with a simple single-node setup and scale out to a distributed, multi-node cluster as your needs grow.

### 6.1. Single-Node Deployment

A single-node deployment is the simplest way to run Anvil and is perfect for development, testing, or small-scale use cases. It consists of one Anvil instance and two PostgreSQL databases (which can run on the same Postgres server).

See the `docker-compose.yml` in the [Getting Started](/docs/anvil/getting-started) guide for a complete, working example.

**Key Configuration Parameters:**

-   `GLOBAL_DATABASE_URL`: The connection string for the global metadata database.
-   `REGIONAL_DATABASE_URL`: The connection string for the regional metadata database.
-   `REGION`: The name of the region this node operates in.
-   `JWT_SECRET`: A secret key for signing JWTs.
-   `ANVIL_SECRET_ENCRYPTION_KEY`: A secret key for encrypting sensitive data at rest.
-   `HTTP_BIND_ADDR`, `GRPC_BIND_ADDR`, `QUIC_BIND_ADDR`: The local addresses and ports for the various services.
-   `command: ["anvil", "--init-cluster"]`: The `--init-cluster` flag tells this node that it is the first node and should not try to bootstrap from another peer.

### 6.2. Multi-Node Cluster Deployment

For production environments requiring high availability and durability, you will run Anvil as a multi-node cluster. 

**Architectural Requirements:**

1.  **Shared Global Database:** All nodes in the cluster, regardless of region, **must** connect to the **same** global PostgreSQL database. This database holds shared information like tenants, buckets, and apps.
2.  **Regional Databases:** All nodes within the *same region* **must** connect to the **same** regional PostgreSQL database for that region. Different regions must have different regional databases.
3.  **Peer-to-Peer Networking:** Anvil nodes must be able to communicate with each other over their QUIC port (default: `7443/udp`).

**Launching the First Node:**

The first node in a new cluster is launched with the `--init-cluster` flag, just like a single-node deployment.

**Launching Subsequent Nodes:**

Additional nodes are launched *without* the `--init-cluster` flag. Instead, you must provide the address of one or more existing nodes in the cluster using the `BOOTSTRAP_ADDRS` environment variable.

```yaml
# Example environment for a second node
environment:
  # ... (same database URLs, secrets, etc.)
  - BOOTSTRAP_ADDRS=/dns4/anvil1/udp/7443/quic-v1
```

When this second node starts, it will connect to `anvil1`, join the cluster, and begin discovering all other peers through the gossip protocol.

### Firewall Configuration

If your Anvil nodes are running on hosts with a firewall, you must open the necessary ports to allow traffic. By default, Anvil uses the following ports:

-   **9000/tcp:** The S3-compatible HTTP gateway.
-   **50051/tcp:** The gRPC API service.
-   **7443/udp:** The QUIC endpoint for peer-to-peer gossip and data transfer.

These ports can be changed via their respective environment variables.

#### UFW (Ubuntu/Debian)

```bash
# Allow S3 Gateway traffic
sudo ufw allow 9000/tcp

# Allow gRPC traffic
sudo ufw allow 50051/tcp

# Allow QUIC peer-to-peer traffic
sudo ufw allow 7443/udp

# Apply the rules
sudo ufw enable
sudo ufw reload
```

#### firewalld (RHEL/CentOS/Fedora)

```bash
# Allow S3 Gateway traffic
sudo firewall-cmd --zone=public --add-port=9000/tcp --permanent

# Allow gRPC traffic
sudo firewall-cmd --zone=public --add-port=50051/tcp --permanent

# Allow QUIC peer-to-peer traffic
sudo firewall-cmd --zone=public --add-port=7443/udp --permanent

# Apply the rules
sudo firewall-cmd --reload
```

### 6.3. Configuration Reference

Anvil is configured entirely through environment variables. The following is a reference for the most important variables, defined in `src/config.rs`.

| Variable                        | Description                                                                 |
| ------------------------------- | --------------------------------------------------------------------------- |
| `GLOBAL_DATABASE_URL`           | **Required.** Connection URL for the global Postgres database.              |
| `REGIONAL_DATABASE_URL`         | **Required.** Connection URL for the regional Postgres database.            |
| `REGION`                        | **Required.** The name of the region this node belongs to.                  |
| `JWT_SECRET`                    | **Required.** Secret key for minting and verifying JWTs.                    |
| `ANVIL_SECRET_ENCRYPTION_KEY`   | **Required.** A 64-character hex-encoded string for AES-256 encryption. <br/><br/> **CRITICAL:** This key is used to encrypt sensitive data at rest. It **MUST** be a cryptographically secure, 64-character hexadecimal string (representing 32 bytes). Loss of this key will result in permanent data loss. <br/><br/> Generate a secure key with: <br/> `openssl rand -hex 32` |
| `ANVIL_CLUSTER_SECRET`          | A shared secret to authenticate and encrypt inter-node gossip messages.     |
| `HTTP_BIND_ADDR`                | The local IP and port for the S3 gateway (e.g., `0.0.0.0:9000`).             |
| `GRPC_BIND_ADDR`                | The local IP and port for the gRPC service (e.g., `0.0.0.0:50051`).           |
| `QUIC_BIND_ADDR`                | The local multiaddress for the QUIC P2P listener.                           |
| `PUBLIC_ADDRS`                  | Comma-separated list of public-facing multiaddresses for this node.         |
| `PUBLIC_GRPC_ADDR`              | The public-facing address for the gRPC service.                             |
| `BOOTSTRAP_ADDRS`               | Comma-separated list of bootstrap peer addresses for joining a cluster.     |
| `INIT_CLUSTER`                  | Set to `true` for the first node in a cluster. Defaults to `false`.         |
| `ENABLE_MDNS`                   | Set to `true` to enable local peer discovery via mDNS. Defaults to `true`.  |

### 6.4. Understanding the Database Layout

The separation of databases is a key scaling feature.

-   **Global Database:** This is the single source of truth for low-volume, globally relevant data. It contains tables for `tenants`, `buckets`, `apps`, `policies`, and `regions`. Because all nodes access this, it can become a bottleneck if not managed correctly, but the data it holds changes infrequently.

-   **Regional Database:** This database handles the high-volume traffic of object metadata. Each region has its own, containing the `objects` table. This allows object listing and searching to be handled locally within a region, preventing a single database from having to index billions of objects from around the world.
