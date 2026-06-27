---
title: Getting Started
description: Run Anvil locally and create a bucket.
---

# Getting Started

Run a local Anvil node with native storage:

```bash
cargo run -p anvil -- \
  --jwt-secret local-jwt-secret \
  --anvil-secret-encryption-key aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --cluster-secret local-cluster-secret \
  --region local \
  --public-api-addr http://127.0.0.1:50051 \
  --api-listen-addr 127.0.0.1:50051 \
  --storage-path ./.anvil-data \
  --init-cluster true
```

Create initial control-plane records with the admin CLI:

```bash
cargo run -p anvil --bin admin -- \
  --anvil-secret-encryption-key aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --storage-path ./.anvil-data \
  tenant create default

cargo run -p anvil --bin admin -- \
  --anvil-secret-encryption-key aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --storage-path ./.anvil-data \
  app create --tenant-name default --app-name demo
```

Use the printed client id and client secret with the Anvil CLI or S3-compatible client.
