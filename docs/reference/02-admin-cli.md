---
slug: /reference/admin-cli
title: 'Reference: Admin CLI (`admin`)'
description: Network administrative client for operating Anvil through the admin API.
tags: [reference, cli, admin]
---

# Reference: Admin CLI (`admin`)

`admin` is the network administrative client for Anvil. It talks to the admin gRPC API, normally bound to `ADMIN_LISTEN_ADDR` on an internal network. It does not write directly to `STORAGE_PATH` and it must not be given `ANVIL_SECRET_ENCRYPTION_KEY`.

Every mutating command requires `--audit-reason`. Most mutating commands also accept `--request-id`, `--idempotency-key`, and `--expected-generation`.

## Authentication

For first bootstrap:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"
admin --host http://127.0.0.1:50052 tenant create \
  --name default \
  --home-region eu-west-1 \
  --audit-reason "initial tenant"
```

For normal operation, use a short-lived token minted for an application that has the required admin capability.

## Local key helper

```bash
admin key generate-secret-encryption-key
```

The printed key is server-only material for `ANVIL_SECRET_ENCRYPTION_KEY`. Store it securely. If it leaks, rotate it.

## Tenant and application commands

```bash
admin tenant create --name acme --home-region eu-west-1 --audit-reason "create acme tenant"
admin app create --tenant-id acme --app-name ingest-worker --audit-reason "create ingest app"
admin app rotate-secret --tenant-id acme --app-name ingest-worker --expected-generation 1 --audit-reason "rotate ingest app secret"
```

## Policy commands

```bash
admin policy grant \
  --tenant-id acme \
  --app-name ingest-worker \
  --action object:write \
  --resource 'raw-events/*' \
  --audit-reason "allow ingest writes"

admin policy revoke \
  --tenant-id acme \
  --app-name ingest-worker \
  --action object:write \
  --resource 'raw-events/*' \
  --audit-reason "remove ingest writes"
```

## Secret envelope rotation

```bash
admin secret-encryption-key rotate --dry-run --audit-reason "dry-run key rotation"
admin secret-encryption-key rotate --audit-reason "rotate key"
```

The server re-encrypts stored envelopes. The CLI does not read encrypted storage files.

## Bucket commands

```bash
admin bucket create \
  --tenant-id acme \
  --bucket-name assets \
  --region eu-west-1 \
  --audit-reason "create assets bucket"

admin bucket public-access set \
  --tenant-id acme \
  --bucket-name assets \
  --allow true \
  --expected-generation 1 \
  --audit-reason "publish public assets"
```

## Mesh and lifecycle commands

| Command family | Commands |
| --- | --- |
| Regions | `create`, `activate`, `set-read-only`, `drain`, `remove`, `list` |
| Cells | `register`, `activate`, `drain`, `remove`, `list` |
| Nodes | `register`, `activate`, `drain`, `force-offline`, `remove`, `list` |
| Links | `create`, `update`, `delete`, `read`, `list` |
| Host aliases | `create`, `activate`, `suspend`, `delete`, `read`, `list` |
| Routing | `list`, `repair` |
| Repair | `run` |
| Diagnostics | `list` |
| Audit | `list` |

Use `admin <family> <command> --help` for the exact flag list. Lifecycle updates require generation checks so stale operator commands cannot overwrite newer state.
