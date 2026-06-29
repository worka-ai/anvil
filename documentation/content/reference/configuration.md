---
title: Configuration
description: Runtime configuration for Anvil nodes, admins, and clients.
---

# Configuration

**What this page achieves:** you will know what each major configuration setting controls and why it exists. Use this page after reading the deployment guide.

Configuration defines identity, network addresses, durable state, secrets, cache behavior, and background work. Treat configuration as part of the deployment contract. A wrong endpoint, weak secret, or ephemeral storage path can make a healthy binary unsafe to run.

## Server configuration

| Variable | Required | What it controls |
| --- | --- | --- |
| `REGION` | Yes | Logical region name for the node. Region identity appears in bucket placement, replication, and operational diagnostics. |
| `JWT_SECRET` | Yes | Secret used to validate bearer tokens in deployments using shared symmetric signing. Protect it as credential material. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Hex-encoded key for encrypted control-plane secrets. Losing it can make stored secrets unrecoverable. |
| `ANVIL_CLUSTER_SECRET` | Yes in clusters | Shared secret for authenticated cluster messages. All trusted nodes need compatible cluster trust. |
| `API_LISTEN_ADDR` | No | Address for native gRPC and S3-compatible traffic. Defaults to `0.0.0.0:50051`. |
| `PUBLIC_API_ADDR` | Yes | Address advertised to clients and peers. It must be reachable by the intended callers. |
| `CLUSTER_LISTEN_ADDR` | Clustered deployments | Address for node-to-node traffic. |
| `PUBLIC_CLUSTER_ADDRS` | Clustered deployments | Comma-separated addresses advertised to other nodes. |
| `BOOTSTRAP_ADDRS` | Joining nodes | Peer addresses used to join an existing deployment. |
| `INIT_CLUSTER` | First node | Initializes a new deployment when true. Do not set casually on joining nodes. |
| `ENABLE_MDNS` | Development/local networks | Enables local discovery where appropriate. Avoid relying on it for controlled production topology. |
| `STORAGE_PATH` | No | Durable Anvil state directory. Defaults to `anvil-data`. Production deployments should set it explicitly. |
| `METADATA_CACHE_TTL_SECS` | No | Time-to-live for in-process metadata cache entries. |
| `OBJECT_METADATA_COMPACTION_FRAME_THRESHOLD` | No | Number of metadata journal frames before compaction is scheduled. |
| `OBJECT_METADATA_COMPACTION_BYTES_THRESHOLD` | No | Encoded journal byte threshold before compaction is scheduled. |
| `TASK_LEASE_TTL_SECS` | No | Lease duration for in-process background responsibilities. |

## Address rules

Listen addresses answer "where does this process bind?" Public addresses answer "what should another process use to reach it?" They are often different behind containers, proxies, or load balancers.

If clients cannot connect, inspect public addresses first. If the process cannot start, inspect listen addresses and port conflicts first.

## Storage path rules

`STORAGE_PATH` is not a cache. It contains durable state. Back it up, monitor capacity, and avoid sharing one mutable storage path between unrelated nodes.

Changing `STORAGE_PATH` changes which durable state the node sees. That is a recovery operation, not a harmless refactor.

## Admin CLI configuration

| Variable | Required | What it controls |
| --- | --- | --- |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Lets admin tooling read or write encrypted control-plane secrets. |
| `STORAGE_PATH` | Direct local admin operations | Points admin tooling at the node storage path when operating out of band. |

Prefer API-based administrative operations where possible. Direct storage operations are powerful and should be reserved for controlled maintenance.

## Client profile fields

| Field | Meaning |
| --- | --- |
| `host` | Native gRPC endpoint. |
| `client_id` | Application id used for token exchange. |
| `client_secret` | Application secret. |
| `default_region` | Region used when commands create regional resources. |

Keep client secrets out of shell history and source control. For CI, inject them through the CI secret mechanism.

## What you can do after this page

You should be able to read an Anvil environment file and identify which settings affect identity, networking, storage durability, cluster trust, and background work.
