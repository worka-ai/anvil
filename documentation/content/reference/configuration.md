---
title: Configuration
description: Runtime configuration for Anvil nodes, admins, and clients.
---

# Configuration

**What this page gives you:** a reference for the major configuration settings and the reason each exists. Read the deployment guide first if the deployment model is unfamiliar.

Configuration defines identity, networking, durable state, secrets, caches, and background work. Treat configuration as part of the production contract. A wrong address, weak secret, or ephemeral storage path can make a healthy binary unsafe.

## Server configuration

| Variable | Required | What it controls |
| --- | --- | --- |
| `REGION` | Yes | Logical region name used in placement and diagnostics. |
| `JWT_SECRET` | Yes | Secret used to validate bearer tokens in symmetric-token deployments. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Hex key for encrypted control-plane secrets. Losing it can make stored secrets unrecoverable. |
| `ANVIL_CLUSTER_SECRET` | Clustered deployments | Shared secret for authenticated node-to-node traffic. |
| `API_LISTEN_ADDR` | No | Local bind address for native and S3-compatible traffic. |
| `PUBLIC_API_ADDR` | Yes | Address clients should use to reach this deployment. |
| `CLUSTER_LISTEN_ADDR` | Clustered deployments | Local bind address for cluster traffic. |
| `PUBLIC_CLUSTER_ADDRS` | Clustered deployments | Advertised peer addresses. |
| `BOOTSTRAP_ADDRS` | Joining nodes | Existing node addresses used when joining. |
| `INIT_CLUSTER` | First node | Initializes a new deployment. Do not set on ordinary joining nodes. |
| `ENABLE_MDNS` | Development/local networks | Enables local discovery where appropriate. |
| `STORAGE_PATH` | No | Durable Anvil state directory. Defaults to `anvil-data`. Set explicitly in production. |
| `METADATA_CACHE_TTL_SECS` | No | Time-to-live for in-process metadata cache entries. |
| `OBJECT_METADATA_COMPACTION_FRAME_THRESHOLD` | No | Journal frame threshold before metadata compaction is scheduled. |
| `OBJECT_METADATA_COMPACTION_BYTES_THRESHOLD` | No | Encoded journal byte threshold before metadata compaction is scheduled. |
| `TASK_LEASE_TTL_SECS` | No | Lease duration for in-process background responsibilities. |

## Storage path rules

`STORAGE_PATH` is durable state, not temporary storage. Back it up, monitor capacity, and avoid sharing one mutable path between unrelated deployments.

Changing `STORAGE_PATH` changes which state the node sees. Treat that as a recovery or migration action.

## Address rules

Listen addresses answer "where does this process bind?" Public addresses answer "what should another process use to reach it?" They differ in containers, service meshes, and proxy deployments.

## Client profile fields

| Field | Meaning |
| --- | --- |
| `host` | Native API endpoint. |
| `client_id` | Application id used for token exchange. |
| `client_secret` | Application secret. |
| `default_region` | Region used by commands that create regional resources. |

Keep client secrets out of shell history and source control. Use CI or secret-manager injection for automation.

## What you can do after this page

You should be able to read an Anvil environment and identify settings that affect identity, networking, durable storage, cluster trust, and background work.
