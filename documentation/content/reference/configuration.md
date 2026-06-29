---
title: Configuration
description: Runtime configuration for Anvil server, admin CLI, and clients.
---

# Configuration

**Goal:** know which settings configure an Anvil node and what each one controls.

## Server variables

| Variable | Required | Description |
| --- | --- | --- |
| `REGION` | Yes | Logical region name for this node. |
| `JWT_SECRET` | Yes | Secret used to validate bearer tokens in deployments using shared symmetric token signing. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Hex-encoded key used for encrypted control-plane secrets. |
| `ANVIL_CLUSTER_SECRET` | Yes in clusters | Shared secret for authenticated cluster messages. |
| `API_LISTEN_ADDR` | No | Address for gRPC and S3-compatible traffic. Defaults to `0.0.0.0:50051`. |
| `PUBLIC_API_ADDR` | Yes | Public API endpoint advertised to clients and peers. |
| `CLUSTER_LISTEN_ADDR` | No | Cluster listen multiaddress. |
| `PUBLIC_CLUSTER_ADDRS` | Clustered deployments | Comma-separated public cluster addresses for this node. |
| `BOOTSTRAP_ADDRS` | Joining nodes | Comma-separated peer addresses used to join an existing deployment. |
| `INIT_CLUSTER` | First node | `true` when initializing a new deployment. |
| `ENABLE_MDNS` | No | Enables local discovery for development and controlled networks. |
| `STORAGE_PATH` | No | Durable Anvil state directory. Defaults to `anvil-data`. |
| `METADATA_CACHE_TTL_SECS` | No | TTL for in-process metadata cache entries. |
| `OBJECT_METADATA_COMPACTION_FRAME_THRESHOLD` | No | Number of metadata journal frames before compaction is scheduled. |
| `OBJECT_METADATA_COMPACTION_BYTES_THRESHOLD` | No | Encoded journal bytes before compaction is scheduled. |
| `TASK_LEASE_TTL_SECS` | No | Lease duration for in-process background tasks. |

## Admin CLI variables

| Variable | Required | Description |
| --- | --- | --- |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Same key as the target node. |
| `STORAGE_PATH` | Yes for direct admin operations | Target node storage path to mutate or inspect. |

## Client profile fields

| Field | Description |
| --- | --- |
| `host` | Native gRPC endpoint. |
| `client_id` | Application client id. |
| `client_secret` | Application client secret. |
| `default_region` | Default region for bucket creation commands. |

Keep production secrets out of shell history and source control. Prefer secret managers or deployment-platform secret injection.
