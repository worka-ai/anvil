---
slug: /reference/admin-cli
title: 'Reference: Admin CLI (`admin`)'
description: Network administrative client for operating Anvil through the admin API.
tags: [reference, cli, admin]
---

# Reference: Admin CLI (`admin`)

`admin` is the network administrative client for Anvil. It talks to the admin gRPC API, normally bound to `ADMIN_LISTEN_ADDR` on an internal network. It does not write directly to `STORAGE_PATH` and it must not be given `ANVIL_SECRET_ENCRYPTION_KEY`.

Every mutating network command requires an audit reason. The CLI sends this reason to the server as part of `AdminRequestContext`, where it is written to the admin audit log.

## Authentication

For first bootstrap, set `ANVIL_AUTH_TOKEN` to the server-side `ANVIL_BOOTSTRAP_ADMIN_TOKEN` and connect to the internal admin endpoint:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"
admin --host http://127.0.0.1:50052 tenant create \
  --name default \
  --audit-reason "initial tenant"
```

For normal operation, configure a profile with an application client id and client secret, or keep using `ANVIL_AUTH_TOKEN` with a short-lived token minted for an application that has the relevant `anvil_admin:*` scopes.

## `key`

Local helper commands for generating server secrets. These commands do not contact Anvil.

- **`generate-secret-encryption-key`**: Generates a 32-byte random key suitable for `ANVIL_SECRET_ENCRYPTION_KEY`.
  ```bash
  admin key generate-secret-encryption-key
  ```
  The printed key should be created once per storage cluster and kept securely in a secret manager. Losing it makes encrypted records unrecoverable. If it leaks, rotate immediately.

## `tenant`

Manages Anvil storage tenants.

- **`create`**: Creates a tenant.
  ```bash
  admin --host http://127.0.0.1:50052 tenant create \
    --name acme \
    --home-region eu-west-1 \
    --audit-reason "create acme tenant"
  ```

## `app`

Manages application credentials inside a tenant.

- **`create`**: Creates an application credential and prints the client id and client secret once.
  ```bash
  admin --host http://127.0.0.1:50052 app create \
    --tenant-id acme \
    --app-name ingest-worker \
    --audit-reason "create ingest app"
  ```
- **`rotate-secret`**: Rotates an existing application secret.
  ```bash
  admin --host http://127.0.0.1:50052 app rotate-secret \
    --tenant-id acme \
    --app-name ingest-worker \
    --expected-generation 1 \
    --audit-reason "rotate leaked app secret"
  ```

## `policy`

Manages application permission scopes. A policy is an action and resource pair granted to an application.

- **`grant`**: Grants a permission scope.
  ```bash
  admin --host http://127.0.0.1:50052 policy grant \
    --tenant-id acme \
    --app-name ingest-worker \
    --action object:write \
    --resource 'raw-events/*' \
    --audit-reason "allow ingest writes"
  ```
- **`revoke`**: Revokes a permission scope.
  ```bash
  admin --host http://127.0.0.1:50052 policy revoke \
    --tenant-id acme \
    --app-name ingest-worker \
    --action object:write \
    --resource 'raw-events/*' \
    --audit-reason "remove ingest writes"
  ```

## `secret-encryption-key`

Rotates stored encrypted envelopes after the server has been restarted with a new active key and the old key listed in `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS`.

- **`rotate`**: Re-encrypts reachable encrypted records with the active key.
  ```bash
  admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
    --audit-reason "rotate secret encryption key"
  ```
- **`rotate --dry-run`**: Counts what would be rotated without writing changes.
  ```bash
  admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
    --dry-run \
    --audit-reason "dry-run secret encryption key rotation"
  ```

## `bucket`

Performs administrative bucket operations.

- **`create`**: Creates a bucket for a tenant in a region.
  ```bash
  admin --host http://127.0.0.1:50052 bucket create \
    --tenant-id acme \
    --bucket-name assets \
    --region eu-west-1 \
    --audit-reason "create assets bucket"
  ```
- **`public-access set`**: Enables or disables anonymous reads for a bucket.
  ```bash
  admin --host http://127.0.0.1:50052 bucket public-access set \
    --tenant-id acme \
    --bucket-name assets \
    --allow true \
    --expected-generation 1 \
    --audit-reason "publish static assets"
  ```

## Mesh and Lifecycle Commands

The network admin CLI also exposes mesh lifecycle operations:

- `region create|activate|set-read-only|drain|remove|list`
- `cell register|activate|drain|remove|list`
- `node register|activate|drain|force-offline|remove|list`
- `link create|update|delete|read|list`
- `host-alias create|activate|suspend|delete|read|list`
- `routing list|repair`
- `repair run`
- `diagnostics list`
- `audit list`

Use `admin <command> --help` for the complete flag list for each operation.
