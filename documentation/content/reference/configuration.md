---
title: Configuration
description: Runtime configuration for Anvil nodes, admins, and clients.
---

# Configuration

**What this page gives you:** a reference for the configuration values that define an Anvil deployment, what each value protects or controls, and how to operate the secret-encryption key safely.

Configuration defines identity, networking, durable state, encryption, clustering, caches, and background work. Treat it as production state. A weak key, ephemeral storage path, wrong public address, or exposed admin listener can make a healthy process unsafe.

## Server Configuration

| Variable | Required | What it controls |
| --- | --- | --- |
| `REGION` | Yes | Logical region name used in placement, routing, diagnostics, and lifecycle records. |
| `JWT_SECRET` | Yes | Secret used to sign and verify bearer tokens in symmetric-token deployments. Rotate carefully because existing bearer tokens depend on it. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Active 32-byte hex key used only by Anvil server processes to encrypt stored server-side secrets and encrypted shard payloads. |
| `ANVIL_SECRET_ENCRYPTION_KEY_ID` | No | Non-secret identifier written into new encryption envelopes. Defaults to `primary`. Change it when changing the active key. |
| `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` | Rotation only | Comma-separated previous keys as `key_id:hex`. Used while rotating existing envelopes to the active key. |
| `ANVIL_BOOTSTRAP_ADMIN_TOKEN` | Bootstrap only | Fixed bearer token accepted by the admin API for first setup. Remove it after permanent admin credentials are created. |
| `ANVIL_CLUSTER_SECRET` | Clustered deployments | Shared secret for authenticated node-to-node cluster traffic. |
| `API_LISTEN_ADDR` | No | Local bind address for native API and S3-compatible traffic. Defaults to `0.0.0.0:50051`. |
| `ADMIN_LISTEN_ADDR` | No | Local bind address for the admin API. Defaults to `127.0.0.1:50052`. Keep it internal. |
| `PUBLIC_API_ADDR` | Yes | Address clients and peers should use to reach the public API for this node or deployment. |
| `CLUSTER_LISTEN_ADDR` | Clustered deployments | Local libp2p QUIC listen multiaddr. |
| `PUBLIC_CLUSTER_ADDRS` | Clustered deployments | Advertised peer multiaddrs for this node. |
| `BOOTSTRAP_ADDRS` | Joining nodes | Existing peer multiaddrs used when joining a cluster. |
| `INIT_CLUSTER` | First node | Initialises a new deployment. Do not set on ordinary joining nodes. |
| `ENABLE_MDNS` | Development/local networks | Enables local peer discovery. Disable it in controlled production deployments unless intentionally used. |
| `STORAGE_PATH` | No | Durable Anvil state directory. Defaults to `anvil-data`. Set explicitly in production. |
| `METADATA_CACHE_TTL_SECS` | No | Time-to-live for in-process metadata cache entries. |
| `OBJECT_METADATA_COMPACTION_FRAME_THRESHOLD` | No | Journal frame threshold before metadata compaction is scheduled. |
| `OBJECT_METADATA_COMPACTION_BYTES_THRESHOLD` | No | Encoded journal byte threshold before metadata compaction is scheduled. |
| `TASK_LEASE_TTL_SECS` | No | Lease duration for in-process background responsibilities. |

## Secret Encryption Key

`ANVIL_SECRET_ENCRYPTION_KEY` is not an admin credential and it is not a client secret. It is server-side key material used by Anvil processes to encrypt secrets that Anvil persists for later use. Examples include application client secrets, stored Hugging Face tokens, and encrypted distributed shard files.

The key must be a 64-character hex string representing 32 random bytes. Generate one with the admin CLI helper:

```bash
admin key generate-secret-encryption-key
```

The command writes the key to stdout and writes a warning to stderr. Generate it once for a storage cluster, store it in a secret manager, inject it into Anvil server processes, and never commit it or paste it into tickets, logs, shell history, or client configuration. If the key is lost, encrypted records that depend on it are unrecoverable. If the key leaks, rotate it.

The network admin CLI does not need this key. Administrative provisioning now goes through the admin API, so operators should not run admin tooling with direct access to `STORAGE_PATH` or `ANVIL_SECRET_ENCRYPTION_KEY`.

### Key IDs and Envelopes

New encrypted records are written as Anvil encryption envelopes. The envelope stores a key id, nonce, and ciphertext. The key id is not secret; it tells the server which configured key should decrypt the record.

`ANVIL_SECRET_ENCRYPTION_KEY_ID` sets the key id for new envelopes. Use a stable, descriptive id such as `2026-07-primary`. When changing the active key, change both `ANVIL_SECRET_ENCRYPTION_KEY` and `ANVIL_SECRET_ENCRYPTION_KEY_ID`.

`ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` is used only during a rotation window. Its format is:

```text
old-key-id-1:old-key-hex,old-key-id-2:old-key-hex
```

Do not leave previous keys configured after rotation has been verified. Keeping old keys around unnecessarily extends the blast radius of a leaked historical key.

### Rotation

A normal rotation is:

1. Generate a new key with `admin key generate-secret-encryption-key`.
2. Restart Anvil with:
   - `ANVIL_SECRET_ENCRYPTION_KEY` set to the new key;
   - `ANVIL_SECRET_ENCRYPTION_KEY_ID` set to a new id;
   - `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` containing the old key id and old key.
3. Run a dry run against the internal admin API:
   ```bash
   ANVIL_AUTH_TOKEN="$ADMIN_TOKEN" \
     admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
     --dry-run \
     --audit-reason "dry-run secret encryption key rotation"
   ```
4. Run the real rotation:
   ```bash
   ANVIL_AUTH_TOKEN="$ADMIN_TOKEN" \
     admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
     --audit-reason "rotate secret encryption key"
   ```
5. Verify the counters and relevant smoke tests.
6. Restart Anvil without the old key in `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS`.

The rotation command re-encrypts records reachable through the node: application client secrets, stored ingestion tokens, and local committed distributed shard files. It is an admin API operation, not a direct filesystem rewrite tool.

## Bootstrap Admin Token

`ANVIL_BOOTSTRAP_ADMIN_TOKEN` exists to solve first setup. When set, the admin API accepts that bearer token and grants it the cluster-wide `anvil_admin:*` capability for the configured mesh.

Use it to create the first tenant, create an administrative application, and grant that application explicit admin scopes. Then remove the bootstrap token from the deployment.

Example:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"

admin --host http://127.0.0.1:50052 tenant create \
  --name default \
  --home-region eu-west-1 \
  --audit-reason "initial tenant"

admin --host http://127.0.0.1:50052 app create \
  --tenant-id default \
  --app-name ops-admin \
  --audit-reason "initial admin app"

admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id default \
  --app-name ops-admin \
  --action 'anvil_admin:*' \
  --resource 'anvil_admin:cluster:default' \
  --audit-reason "grant admin capability"
```

Do not expose the admin listener publicly just because it has authentication. Put it on an internal network and apply normal network policy around it.

## Storage Path Rules

`STORAGE_PATH` is durable state, not temporary storage. It contains Anvil-owned object bytes, metadata journals, indexes, manifests, authz data, PersonalDB state, and control records. Back it up, monitor capacity, and avoid sharing one mutable path between unrelated deployments.

Changing `STORAGE_PATH` changes which state the node sees. Treat that as a recovery or migration action.

## Address Rules

Listen addresses answer "where does this process bind?" Public addresses answer "what should another process use to reach it?" They differ in containers, service meshes, and proxy deployments.

`API_LISTEN_ADDR` may be exposed through a public service when serving object traffic. `ADMIN_LISTEN_ADDR` should be internal. If both are exposed the same way, a deployment mistake has expanded the attack surface.

## Client Profile Fields

| Field | Meaning |
| --- | --- |
| `host` | Native API endpoint. |
| `client_id` | Application id used for token exchange. |
| `client_secret` | Application secret. |
| `default_region` | Region used by commands that create regional resources. |

Keep client secrets out of shell history and source control. Use CI or secret-manager injection for automation.

## What You Can Do After This Page

You should be able to read an Anvil environment and identify settings that affect identity, networking, durable storage, server-side secret encryption, cluster trust, and background work.
