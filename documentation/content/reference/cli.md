---
title: CLI
description: Command-line tasks for Anvil users, administrators, and release operators.
---

# CLI

**What this page gives you:** a reference for Anvil's two command-line surfaces, when to use each one, and the safety model behind user operations and administrative operations.

Anvil ships two CLIs.

| CLI | Audience | Endpoint | Storage access |
| --- | --- | --- | --- |
| `anvil-cli` | Application developers, scripts, data import jobs, and ordinary authorised users. | Public native API on `API_LISTEN_ADDR`. | Never direct. |
| `admin` | Operators, provisioners, and trusted automation. | Internal admin API on `ADMIN_LISTEN_ADDR`. | Never direct. |

The split is deliberate. `anvil-cli` can only do what its application credentials and bearer token allow. `admin` can mutate control-plane state, but it still goes through the server, authorisation, request context, and audit logging. Neither CLI should mount or edit `STORAGE_PATH` directly.

## User CLI: `anvil-cli`

Global options:

```bash
anvil-cli --profile production <command>
anvil-cli --config ./anvil-cli.toml <command>
```

### Configuration

| Command | Purpose |
| --- | --- |
| `anvil-cli configure` | Interactive profile setup. |
| `anvil-cli static-config` | Non-interactive profile setup for scripts and CI. |

Example:

```bash
anvil-cli static-config \
  --name production \
  --host https://storage.example.com \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

This stores connection information. It does not grant permissions.

### Authentication and delegated policy

| Command | Purpose |
| --- | --- |
| `anvil-cli auth get-token` | Request a bearer token for the active profile. |
| `anvil-cli auth grant <app> <action> <resource>` | Grant a scope when the caller is already allowed to grant it. |
| `anvil-cli auth revoke <app> <action> <resource>` | Revoke a scope when the caller is already allowed to revoke it. |

Delegated grants are bounded. If the caller cannot grant `object:read` on `bucket:documents/*`, this command cannot create that authority.

### Buckets

| Command | Purpose |
| --- | --- |
| `anvil-cli bucket create <name> <region>` | Create a bucket in a region. |
| `anvil-cli bucket rm <name>` | Remove an empty bucket. |
| `anvil-cli bucket ls` | List buckets visible to the caller. |
| `anvil-cli bucket set-public <name> --allow <true|false>` | Toggle public bucket reads when authorised. |

### Objects

Anvil object commands use S3-style paths because they are familiar and compact:

```text
s3://bucket/key/prefix/object.ext
```

| Command | Purpose |
| --- | --- |
| `anvil-cli object put <local> <s3://bucket/key>` | Upload bytes and metadata. |
| `anvil-cli object get <s3://bucket/key> [local]` | Download bytes or print to stdout. |
| `anvil-cli object head <s3://bucket/key>` | Read metadata for one object version. |
| `anvil-cli object ls <s3://bucket/prefix>` | List visible objects under a prefix. |
| `anvil-cli object rm <s3://bucket/key>` | Delete an object head. |

Reserved `_anvil/` paths are rejected even if a token has broad object scopes.

### Hugging Face artefact ingestion

| Command | Purpose |
| --- | --- |
| `anvil-cli hf key add` | Store an ingestion token in Anvil. |
| `anvil-cli hf key ls` | List stored ingestion keys. |
| `anvil-cli hf key rm` | Remove a stored ingestion key. |
| `anvil-cli hf ingest start` | Start a model/source ingestion job. |
| `anvil-cli hf ingest status` | Inspect ingestion progress. |
| `anvil-cli hf ingest cancel` | Cancel a running ingestion job. |

Ingestion state, fetched artefacts, indexes, diagnostics, and completion records are persisted through CoreStore.

## Admin CLI: `admin`

`admin` is a network client for the admin API. It needs an admin endpoint and a bearer token. It does not need `STORAGE_PATH` and must not be given `ANVIL_SECRET_ENCRYPTION_KEY`.

Set the token explicitly for bootstrap or automation:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"
```

Every mutating admin command requires `--audit-reason`. Use a concrete sentence. The server writes it to the admin audit stream.

Most mutating commands also accept:

| Flag | Meaning |
| --- | --- |
| `--request-id` | Caller-supplied request id. Defaults to a generated id. |
| `--idempotency-key` | Retry key for safe replay. Defaults to a generated id. |
| `--expected-generation` | Required for update/delete lifecycle commands; create commands default to generation `0`. |
| `--audit-reason` | Required reason stored in the audit log. |

### Local key helper

```bash
admin key generate-secret-encryption-key
```

This command is local. It prints a 32-byte hex key suitable for `ANVIL_SECRET_ENCRYPTION_KEY`. Generate one once for a storage cluster, store it securely, and rotate if leaked.

### Tenant and application provisioning

```bash
admin --host http://127.0.0.1:50052 tenant create \
  --name acme \
  --home-region eu-west-1 \
  --audit-reason "create acme tenant"

admin --host http://127.0.0.1:50052 app create \
  --tenant-id acme \
  --app-name docs-writer \
  --audit-reason "create docs writer credential"

admin --host http://127.0.0.1:50052 app rotate-secret \
  --tenant-id acme \
  --app-name docs-writer \
  --expected-generation 1 \
  --audit-reason "rotate docs writer secret"
```

`app create` prints the client id and client secret once. Store them in a secret manager.

### Policy grants and revocation

```bash
admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id acme \
  --app-name docs-writer \
  --action object:write \
  --resource 'documents/*' \
  --audit-reason "allow document uploads"

admin --host http://127.0.0.1:50052 policy revoke \
  --tenant-id acme \
  --app-name docs-writer \
  --action object:write \
  --resource 'documents/*' \
  --audit-reason "remove document upload access"
```

Policy scopes are coarse API gates. Relationship authorisation still decides object-level permissions where a path uses relationship checks.

### Server-side secret envelope rotation

```bash
admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
  --dry-run \
  --audit-reason "dry-run secret encryption key rotation"

admin --host http://127.0.0.1:50052 secret-encryption-key rotate \
  --audit-reason "rotate secret encryption key"
```

The server performs the re-encryption. The CLI does not read or write encrypted storage files.

### Bucket and public access operations

```bash
admin --host http://127.0.0.1:50052 bucket create \
  --tenant-id acme \
  --bucket-name documents \
  --region eu-west-1 \
  --audit-reason "create documents bucket"

admin --host http://127.0.0.1:50052 bucket public-access set \
  --tenant-id acme \
  --bucket-name public-assets \
  --allow true \
  --expected-generation 1 \
  --audit-reason "publish public assets"
```

### Mesh lifecycle

| Command family | Purpose |
| --- | --- |
| `admin region create|activate|set-read-only|drain|remove|list` | Manage region descriptors and region lifecycle. |
| `admin cell register|activate|drain|remove|list` | Manage cells inside regions. |
| `admin node register|activate|drain|force-offline|remove|list` | Manage nodes and their capabilities. |
| `admin link create|update|delete|read|list` | Manage symlink-like object links. |
| `admin host-alias create|activate|suspend|delete|read|list` | Manage custom host aliases. |
| `admin routing list|repair` | Inspect and repair routing projections. |
| `admin repair run` | Run repair backends. |
| `admin diagnostics list` | List diagnostics. |
| `admin audit list` | List administrative audit events. |

Example node registration:

```bash
admin --host http://127.0.0.1:50052 node register \
  --node-id node-a \
  --region eu-west-1 \
  --cell-id cell-a \
  --libp2p-peer-id 12D3KooW... \
  --public-api-addr https://node-a.storage.example.com \
  --public-cluster-addr /ip4/10.0.0.10/udp/7000/quic-v1 \
  --capability object,index,gateway,admin \
  --audit-reason "register first node"
```

## Scripting rules

- Prefer least-privilege credentials.
- Use idempotency keys for automation.
- Capture request ids in logs.
- Put admin traffic on an internal network.
- Keep `ANVIL_SECRET_ENCRYPTION_KEY` out of CLI environments.
- Treat wildcard grants as exceptional.
- Do not use public APIs to probe `_anvil/` paths.
- Verify denied paths as well as successful paths during deployment smoke tests.

## What you can do after this page

You should be able to choose the correct CLI, understand which endpoint it talks to, bootstrap a tenant, create credentials, grant and revoke access, and prove access changes through ordinary object commands.
