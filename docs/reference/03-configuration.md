---
slug: /reference/configuration
title: 'Reference: Configuration'
description: A detailed reference of all environment variables used to configure an Anvil node.
tags: [reference, configuration, environment, variables]
---

# Reference: Configuration

Anvil is configured entirely through environment variables. The following is a reference for the most important variables required to launch and operate an Anvil node.

| Variable                        | Description                                                                 |
| ------------------------------- | --------------------------------------------------------------------------- |
| `GLOBAL_DATABASE_URL`           | **Required.** Connection URL for the global Postgres database.              |
| `REGIONAL_DATABASE_URL`         | **Required.** Connection URL for the regional Postgres database.            |
| `REGION`                        | **Required.** The name of the region this node belongs to.                  |
| `JWT_SECRET`                    | **Required.** Secret key for minting and verifying JWTs.                    |
| `ANVIL_SECRET_ENCRYPTION_KEY`   | **Required.** A 64-character hex-encoded string for AES-256 encryption. <br/><br/> **CRITICAL:** This key is used to encrypt sensitive data at rest. It **MUST** be a cryptographically secure, 64-character hexadecimal string (representing 32 bytes). Loss of this key will result in permanent data loss. <br/><br/> Generate a secure key with: <br/> `openssl rand -hex 32` |
| `CLUSTER_SECRET`          | A shared secret to authenticate and encrypt inter-node gossip messages.     |
| `API_LISTEN_ADDR`               | The local IP and port for the unified S3 Gateway and gRPC service (e.g., `0.0.0.0:50051`). |
| `CLUSTER_LISTEN_ADDR`           | The local multiaddress for the QUIC P2P listener.                           |
| `PUBLIC_CLUSTER_ADDRS`          | Comma-separated list of public-facing multiaddresses for this node.         |
| `PUBLIC_API_ADDR`               | The public-facing address for the gRPC service.                             |
| `BOOTSTRAP_ADDRS`               | Comma-separated list of bootstrap peer addresses for joining a cluster.     |
| `INIT_CLUSTER`                  | Set to `true` for the first node in a cluster. Defaults to `false`.         |
| `ENABLE_MDNS`                   | Set to `true` to enable local peer discovery via mDNS. Defaults to `true`.  |
| `METADATA_CACHE_TTL_SECS`       | Time-to-live (in seconds) for cached global metadata (buckets, tenants, policies). Defaults to `300` (5 minutes). |
