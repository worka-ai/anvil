---
slug: /scenarios/admin-tasks
title: 'Scenario: Administrative Tasks'
description: A guide covering common administrative commands for credentials, policies, and secret rotation.
tags: [scenario, cli, admin, credentials]
---

# Scenario: Common Administrative Tasks

Anvil administration is performed through the network `admin` CLI. The CLI connects to the admin API and never writes the storage directory directly.

## 1. Rotate an App's Client Secret

If an application's client secret is compromised, invalidate it and generate a new one:

```bash
admin app rotate-secret \
  --tenant-id acme-corp \
  --app-name data-science-app \
  --expected-generation 1 \
  --audit-reason "rotate compromised app secret"
```

The command returns a new `client_secret`. The old secret stops working once the mutation is committed.

## 2. Rotate the Server Secret Encryption Key

Generate a replacement key:

```bash
admin key generate-secret-encryption-key
```

Restart the server with the new key as `ANVIL_SECRET_ENCRYPTION_KEY`, a new `ANVIL_SECRET_ENCRYPTION_KEY_ID`, and the old key in `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS`. Then run:

```bash
admin secret-encryption-key rotate \
  --dry-run \
  --audit-reason "dry-run secret key rotation"

admin secret-encryption-key rotate \
  --audit-reason "rotate secret key"
```

After verification, remove the old key from `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS` and restart the server.

## 3. Interactive Client CLI Configuration

While non-interactive configuration is recommended for scripts, users can use the client CLI `configure` command for a wizard-style setup experience:

```bash
anvil-cli configure
```

## 4. Getting a Raw Bearer Token

For developers who need to interact with the gRPC API directly using tools like `grpcurl`, the client CLI can mint a bearer token for the configured profile:

```bash
anvil-cli auth get-token
```

Use the returned token in the `authorization` metadata header, for example `authorization: Bearer <token>`.
