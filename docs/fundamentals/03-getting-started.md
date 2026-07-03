---
title: Getting Started
description: Run Anvil locally and create a bucket.
---

# Getting Started

Generate a local encryption key first:

```bash
admin key generate-secret-encryption-key
```

Keep the value securely. For a throwaway local node you can export it into your shell:

```bash
export ANVIL_SECRET_ENCRYPTION_KEY=<printed-64-character-hex-key>
export ANVIL_BOOTSTRAP_ADMIN_TOKEN=local-bootstrap-admin-token
```

Run a local Anvil node with native storage:

```bash
cargo run -p anvil-server --bin anvil -- \
  --jwt-secret local-jwt-secret \
  --anvil-bootstrap-admin-token "$ANVIL_BOOTSTRAP_ADMIN_TOKEN" \
  --anvil-secret-encryption-key "$ANVIL_SECRET_ENCRYPTION_KEY" \
  --anvil-secret-encryption-key-id local-primary \
  --cluster-secret local-cluster-secret \
  --region local \
  --public-api-addr http://127.0.0.1:50051 \
  --api-listen-addr 127.0.0.1:50051 \
  --admin-listen-addr 127.0.0.1:50052 \
  --storage-path ./.anvil-data \
  --init-cluster true
```

Create initial control-plane records with the network admin CLI:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"

cargo run -p anvil-storage-cli --bin admin -- \
  --host http://127.0.0.1:50052 \
  tenant create \
  --name default \
  --home-region local \
  --audit-reason "initial local tenant"

cargo run -p anvil-storage-cli --bin admin -- \
  --host http://127.0.0.1:50052 \
  app create \
  --tenant-id default \
  --app-name demo \
  --audit-reason "initial local app"
```

Use the printed client id and client secret with the Anvil CLI or S3-compatible client. Remove the bootstrap token from real deployments after creating permanent administrative credentials and policies.
