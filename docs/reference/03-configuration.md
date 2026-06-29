---
title: Configuration Reference
description: Runtime configuration for Anvil nodes and administrative tooling.
---

# Configuration Reference

Anvil stores object bytes, metadata journals, indexes, manifests, and local control state under the configured storage path. A node does not require an external metadata database.

## Node Variables

| Variable | Required | Description |
| --- | --- | --- |
| `JWT_SECRET` | Yes | Secret used to sign access tokens. Use a strong value shared by nodes that must validate each other's tokens. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Hex-encoded 32-byte key used to encrypt stored application secrets and other sensitive control-plane values. |
| `REGION` | Yes | Logical region name for the node. |
| `PUBLIC_API_ADDR` | Yes | Public gRPC endpoint advertised to clients and peers. |
| `API_LISTEN_ADDR` | No | Local gRPC bind address. Defaults to `0.0.0.0:50051`. |
| `CLUSTER_LISTEN_ADDR` | No | libp2p QUIC listen multiaddr. Defaults to `/ip4/0.0.0.0/udp/7443/quic-v1`. |
| `PUBLIC_CLUSTER_ADDRS` | No | Comma-separated public libp2p multiaddrs for this node. |
| `BOOTSTRAP_ADDRS` | No | Comma-separated peer multiaddrs used when joining an existing cluster. |
| `INIT_CLUSTER` | No | Set to `true` for the first node that initializes a cluster. |
| `ENABLE_MDNS` | No | Enables local peer discovery. Defaults to `true`; disable it for controlled deployments. |
| `ANVIL_CLUSTER_SECRET` | No | Shared secret used for cluster message authentication. |
| `METADATA_CACHE_TTL_SECS` | No | TTL for in-process metadata cache entries. Defaults to `300`. |
| `STORAGE_PATH` | No | Directory containing Anvil-owned object bytes and metadata state. Defaults to `anvil-data`. |
| `OBJECT_METADATA_COMPACTION_FRAME_THRESHOLD` | No | Number of uncompacted object metadata journal frames allowed before Anvil schedules an object metadata compaction task. Defaults to `4096`; set to `0` to disable frame-count scheduling. |
| `OBJECT_METADATA_COMPACTION_BYTES_THRESHOLD` | No | Encoded size of uncompacted object metadata journal frames allowed before Anvil schedules an object metadata compaction task. Defaults to `67108864`; set to `0` to disable byte-count scheduling. |
| `TASK_LEASE_TTL_SECS` | No | Seconds that an in-process background task lease remains valid without renewal. Defaults to `300`. |

## Admin CLI Variables

The admin CLI writes to the native storage path used by the target node.

| Variable | Required | Description |
| --- | --- | --- |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Same encryption key as the target node. |
| `STORAGE_PATH` | No | Native storage path to mutate. Defaults to `anvil-data`; pass `--storage-path` explicitly for scripts. |

Example:

```bash
cargo run -p anvil-storage --bin admin -- \
  --anvil-secret-encryption-key aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --storage-path /var/lib/anvil \
  tenant create default
```
