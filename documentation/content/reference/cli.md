---
title: CLI
description: Command-line tasks for Anvil users, administrators, and release operators.
---

# CLI

**What this page gives you:** a reference for the command families exposed by Anvil's user CLI and network admin CLI, and the safety model behind them.

Anvil ships two command-line surfaces:

- `anvil-cli` is the application/client CLI. It talks to the public native API with application credentials.
- `admin` is the administrative CLI. It talks to the internal admin API and never writes directly to the storage directory.

## User-Oriented Commands

| Task | Command family | Why it exists |
| --- | --- | --- |
| Configure a profile | `anvil-cli configure` or `anvil-cli static-config` | Stores endpoint and credential settings. |
| Get a token | `anvil-cli auth get-token` | Verifies credentials and obtains a bearer token. |
| Manage buckets | `anvil-cli bucket ...` | Creates, lists, or deletes bucket boundaries where authorised. |
| Manage objects | `anvil-cli object ...` | Puts, gets, heads, lists, and deletes objects. |
| Delegated auth | `anvil-cli auth ...` | Performs permitted auth operations. |
| Ingestion keys | `anvil-cli hf key ...` | Manages credentials for source/model ingestion workflows. |
| Ingestion jobs | `anvil-cli hf ingest ...` | Starts and inspects ingestion into buckets. |

## Network Admin Commands

`admin` is a network client for the admin API. It needs an admin endpoint and a bearer token. It does not need `STORAGE_PATH` and must not be given `ANVIL_SECRET_ENCRYPTION_KEY`.

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"
admin --host http://127.0.0.1:50052 tenant create \
  --name default \
  --audit-reason "initial tenant"
```

| Task | Command family | Safety note |
| --- | --- | --- |
| Generate server encryption keys | `admin key generate-secret-encryption-key` | Local helper only. Store the printed key securely. |
| Create tenants | `admin tenant create ...` | Creates administrative boundaries. |
| Create or rotate apps | `admin app create ...`, `admin app rotate-secret ...` | Creates or changes credentials. |
| Grant or revoke policy | `admin policy grant ...`, `admin policy revoke ...` | Changes what callers can do. Review carefully. |
| Rotate secret envelopes | `admin secret-encryption-key rotate ...` | Re-encrypts server-side secret envelopes after a key change. |
| Create buckets | `admin bucket create ...` | Creates buckets for a tenant through the admin API. |
| Set public access | `admin bucket public-access set ...` | Can expose data if misused. |
| Manage regions/cells/nodes | `admin region ...`, `admin cell ...`, `admin node ...` | Changes placement and lifecycle records. |
| Manage object links | `admin link ...` | Creates symlink-like object aliases. |
| Manage host aliases | `admin host-alias ...` | Controls virtual-host routing. |
| Repair and inspect | `admin routing ...`, `admin repair ...`, `admin diagnostics ...`, `admin audit ...` | Operator diagnostics and repair actions. |

Every mutating admin command requires `--audit-reason`. Use a concrete reason that explains why the change was made; it is written to the audit log.

## Secret Key Generation

Generate a server encryption key with:

```bash
admin key generate-secret-encryption-key
```

The command prints one 64-character hex key to stdout and a warning to stderr. Create it once for a storage cluster, put it in a secret manager, and keep it secure. Losing it makes encrypted records unrecoverable. If it leaks, configure a new active key and run `admin secret-encryption-key rotate`.

## Profile Setup Example

```bash
anvil-cli static-config \
  --name production \
  --host https://anvil.example.com \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

This configures the application CLI. It does not grant permissions by itself.

## Admin Bootstrap Example

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"

admin --host http://127.0.0.1:50052 tenant create \
  --name acme \
  --home-region eu-west-1 \
  --audit-reason "create acme tenant"

admin --host http://127.0.0.1:50052 app create \
  --tenant-id acme \
  --app-name ingest-worker \
  --audit-reason "create ingest app"

admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id acme \
  --app-name ingest-worker \
  --action object:write \
  --resource 'raw-events/*' \
  --audit-reason "allow ingest writes"
```

After bootstrap, remove `ANVIL_BOOTSTRAP_ADMIN_TOKEN` and use minted admin tokens or application credentials with explicit admin scopes.

## Scripting Rules

When using the CLI in automation:

- use idempotency keys where supported;
- capture request ids;
- avoid wildcard grants unless creating a deliberate cluster admin;
- prefer least-privilege credentials;
- fail closed on authorisation errors;
- do not probe reserved namespaces;
- keep admin traffic on the internal admin endpoint;
- keep `ANVIL_SECRET_ENCRYPTION_KEY` out of CLI environments.

## What You Can Do After This Page

You should be able to choose the correct command family, understand whether a command is ordinary client work or privileged administration, and bootstrap a deployment without direct filesystem writes.
