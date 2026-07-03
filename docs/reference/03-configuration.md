---
title: Configuration Reference
description: Runtime configuration for Anvil nodes and administrative tooling.
---

# Configuration Reference

Anvil stores object bytes, metadata journals, indexes, manifests, and control-plane records under the configured storage path. A node does not require an external metadata database. Configuration is supplied by environment variables or matching CLI flags.

## Node Variables

| Variable | Required | Description |
| --- | --- | --- |
| `JWT_SECRET` | Yes | Secret used to sign and verify bearer tokens minted by Anvil. |
| `ANVIL_BOOTSTRAP_ADMIN_TOKEN` | No | Fixed bearer token accepted for first administrative bootstrap on the admin API. Remove it after permanent app credentials and policies are created. |
| `ANVIL_SECRET_ENCRYPTION_KEY` | Yes | Active hex-encoded 32-byte key used by the server to encrypt stored application secrets, Hugging Face keys, and encrypted distributed shard data. |
| `ANVIL_SECRET_ENCRYPTION_KEY_ID` | No | Stable identifier written into new encryption envelopes. Defaults to `primary`. Change it when introducing a new active encryption key. |
| `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` | No | Comma-separated previous keys as `key_id:hex`. Used only while rotating existing encrypted records to the active key. |
| `REGION` | Yes | Logical region name for the node. |
| `PUBLIC_API_ADDR` | Yes | Public gRPC endpoint advertised to clients and peers. |
| `API_LISTEN_ADDR` | No | Local public API bind address. Defaults to `0.0.0.0:50051`. |
| `ADMIN_LISTEN_ADDR` | No | Local admin API bind address. Defaults to `127.0.0.1:50052`. Keep this on an internal network. |
| `CLUSTER_LISTEN_ADDR` | No | libp2p QUIC listen multiaddr. Defaults to `/ip4/0.0.0.0/udp/7443/quic-v1`. |
| `PUBLIC_CLUSTER_ADDRS` | No | Comma-separated public libp2p multiaddrs for this node. |
| `BOOTSTRAP_ADDRS` | No | Comma-separated peer multiaddrs used when joining an existing cluster. |
| `INIT_CLUSTER` | No | Set to `true` for the first node that initialises a cluster. |
| `ENABLE_MDNS` | No | Enables local peer discovery. Defaults to `true`; disable it for controlled deployments. |
| `ANVIL_CLUSTER_SECRET` | No | Shared secret used for cluster message authentication. |
| `METADATA_CACHE_TTL_SECS` | No | TTL for in-process metadata cache entries. Defaults to `300`. |
| `STORAGE_PATH` | No | Directory containing Anvil-owned object bytes and metadata state. Defaults to `anvil-data`. |
| `OBJECT_METADATA_COMPACTION_FRAME_THRESHOLD` | No | Uncompacted object metadata journal frames allowed before scheduling compaction. Defaults to `4096`; set to `0` to disable frame-count scheduling. |
| `OBJECT_METADATA_COMPACTION_BYTES_THRESHOLD` | No | Uncompacted object metadata journal bytes allowed before scheduling compaction. Defaults to `67108864`; set to `0` to disable byte-count scheduling. |
| `TASK_LEASE_TTL_SECS` | No | Seconds that an in-process background task lease remains valid without renewal. Defaults to `300`. |

## Secret Encryption Key

`ANVIL_SECRET_ENCRYPTION_KEY` is server-only secret material. It must be a 64-character hex string representing 32 random bytes. The admin CLI must not be given this key; administrative provisioning now goes through the admin API, so only Anvil server processes need the encryption key.

Generate a key with:

```bash
admin key generate-secret-encryption-key
```

The command prints the key on stdout and a security warning on stderr. Generate it once for a storage cluster, place it in a secret manager, and keep it out of source control, shell history, tickets, and logs. Losing the key makes encrypted records unrecoverable. If the key leaks, rotate it immediately.

`ANVIL_SECRET_ENCRYPTION_KEY_ID` is written into each new encrypted envelope. It is not secret. Use a new id whenever you introduce a new active key, for example `2026-07-primary`. `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` is only for rotation windows. It lets the server decrypt old envelopes while writing new envelopes with the active key.

The normal rotation sequence is:

1. Generate a new key.
2. Restart Anvil with the new `ANVIL_SECRET_ENCRYPTION_KEY`, a new `ANVIL_SECRET_ENCRYPTION_KEY_ID`, and the old key listed in `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS`.
3. Run a dry run:
   ```bash
   ANVIL_AUTH_TOKEN="$BOOTSTRAP_OR_ADMIN_TOKEN" \
     admin --host http://127.0.0.1:50052 \
     secret-encryption-key rotate \
     --audit-reason "dry-run secret key rotation" \
     --dry-run
   ```
4. Run the rotation without `--dry-run`.
5. Restart Anvil without the old key in `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` after verifying the rotation counters.

The rotation command rewrites encrypted envelopes it can reach through the local node: application client secrets, stored Hugging Face tokens, and local committed distributed shard files.

## Bootstrap Admin Token

`ANVIL_BOOTSTRAP_ADMIN_TOKEN` is an operational bootstrap escape hatch, not a long-term user credential. When set, a request to the admin API with `Authorization: Bearer <token>` receives the `anvil_admin:*` capability for the configured mesh. Use it to create the first tenant, application credential, and policy grants through the network admin API.

After bootstrap, create a permanent administrative application with explicit admin policies and remove `ANVIL_BOOTSTRAP_ADMIN_TOKEN` from the deployment. Do not expose the admin port publicly even when a bootstrap token is configured.

## Admin CLI Variables

The `admin` binary is a network client. It talks to `ADMIN_LISTEN_ADDR`; it does not mount or write the storage directory.

| Variable | Required | Description |
| --- | --- | --- |
| `ANVIL_ADMIN_ENDPOINT` | No | Admin API endpoint used by `admin --host` when the flag is omitted. Example: `http://127.0.0.1:50052`. |
| `ANVIL_AUTH_TOKEN` | Usually | Bearer token to send to the admin API. Use the bootstrap token only for first setup, then use a minted token with explicit admin scopes. |

Example bootstrap setup:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"
admin --host http://127.0.0.1:50052 tenant create \
  --name default \
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
