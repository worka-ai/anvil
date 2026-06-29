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
      ANVIL_SECRET_ENCRYPTION_KEY: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      API_LISTEN_ADDR: 0.0.0.0:50051
      PUBLIC_API_ADDR: http://localhost:50051
      STORAGE_PATH: /var/lib/anvil
    command: ["anvil", "--init-cluster"]
    ports:
      - "50051:50051"
    volumes:
      - anvil_data:/var/lib/anvil
volumes:
  anvil_data:
```

## Multi-Node Deployment

For a cluster, deploy multiple Anvil nodes with:

- the same `JWT_SECRET` where tokens must be mutually valid;
- the same `ANVIL_CLUSTER_SECRET` for cluster message authentication;
- unique `STORAGE_PATH` volumes per node;
- `PUBLIC_CLUSTER_ADDRS` set to each node's reachable libp2p address;
- `BOOTSTRAP_ADDRS` set on joining nodes.

The first node starts with `--init-cluster`; later nodes join using bootstrap addresses.

## Administrative Setup

Use the admin CLI against the native storage path of the node being configured:

```bash
cargo run -p anvil-storage --bin admin -- \
  --anvil-secret-encryption-key aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --storage-path /var/lib/anvil \
  tenant create default
```
