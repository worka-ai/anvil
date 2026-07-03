---
title: Deployment
description: Deploying Anvil nodes.
---

# Deployment

Anvil deployment requires Anvil nodes and durable storage for each node's `STORAGE_PATH`. The storage path contains object bytes, metadata journals, indexes, manifests, and control-plane records.

## Single-Node Development

```yaml
services:
  anvil:
    image: ghcr.io/anvil-storage/anvil:latest
    environment:
      REGION: local
      JWT_SECRET: change-me
      ANVIL_BOOTSTRAP_ADMIN_TOKEN: change-me-bootstrap-admin-token
      ANVIL_SECRET_ENCRYPTION_KEY: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      ANVIL_SECRET_ENCRYPTION_KEY_ID: local-primary
      API_LISTEN_ADDR: 0.0.0.0:50051
      ADMIN_LISTEN_ADDR: 0.0.0.0:50052
      PUBLIC_API_ADDR: http://localhost:50051
      STORAGE_PATH: /var/lib/anvil
    command: ["anvil", "--init-cluster"]
    ports:
      - "50051:50051"
      # Keep this bound to loopback or an internal network outside development.
      - "127.0.0.1:50052:50052"
    volumes:
      - anvil_data:/var/lib/anvil
volumes:
  anvil_data:
```

Generate a real encryption key before deploying anything persistent:

```bash
admin key generate-secret-encryption-key
```

Put the value in a secret manager and inject it as `ANVIL_SECRET_ENCRYPTION_KEY`. Do not pass this key to the admin CLI.

## Multi-Node Deployment

For a cluster, deploy multiple Anvil nodes with:

- the same `JWT_SECRET` where tokens must be mutually valid;
- the same `ANVIL_CLUSTER_SECRET` for cluster message authentication;
- the same active secret encryption key configuration during a rotation window;
- unique `STORAGE_PATH` volumes per node;
- `PUBLIC_CLUSTER_ADDRS` set to each node's reachable libp2p address;
- `BOOTSTRAP_ADDRS` set on joining nodes;
- `ADMIN_LISTEN_ADDR` reachable only by trusted internal operators and provisioners.

The first node starts with `--init-cluster`; later nodes join using bootstrap addresses.

## Administrative Setup

Use the network admin CLI against the admin API. The CLI must not mount the storage path and must not receive `ANVIL_SECRET_ENCRYPTION_KEY`.

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"

admin --host http://127.0.0.1:50052 tenant create \
  --name default \
  --home-region local \
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
  --audit-reason "grant admin API access"
```

After bootstrap, remove `ANVIL_BOOTSTRAP_ADMIN_TOKEN` from the deployment and use minted tokens for the permanent administrative application.
