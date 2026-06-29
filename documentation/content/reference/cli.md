---
title: CLI
description: Command-line tasks for Anvil users, administrators, and release operators.
---

# CLI

**What this page gives you:** a reference for the command families exposed by the Anvil CLI and the safety model behind them.

The CLI is a client for Anvil APIs. Some commands act as ordinary application clients. Some commands are administrative and should be treated as privileged production changes.

## User-oriented commands

| Task | Command family | Why it exists |
| --- | --- | --- |
| Configure a profile | `anvil configure` or `anvil static-config` | Stores endpoint and credential settings. |
| Get a token | `anvil auth get-token` | Verifies credentials and obtains a bearer token. |
| Manage buckets | `anvil bucket ...` | Creates, lists, or deletes bucket boundaries where authorized. |
| Manage objects | `anvil object ...` | Puts, gets, heads, lists, and deletes objects. |
| Delegated auth | `anvil auth ...` | Performs permitted auth operations. |
| Ingestion keys | `anvil hf key ...` | Manages credentials for source/model ingestion workflows. |
| Ingestion jobs | `anvil hf ingest ...` | Starts and inspects ingestion into buckets. |

## Admin commands

| Task | Command family | Safety note |
| --- | --- | --- |
| Create tenants | `admin tenant ...` | Changes administrative boundaries. |
| Create applications | `admin app ...` | Creates credentialed callers. |
| Grant policy | `admin policy grant ...` | Expands what callers may do. Review carefully. |
| Register regions | `admin region ...` | Affects placement and cluster behavior. |
| Set bucket public access | `admin bucket set-public-access ...` | Can expose data if misused. |
| Create admin users | `admin user ...` | Grants human administrative entry points. |

## Profile setup example

```bash
anvil static-config \
  --name production \
  --host https://anvil.example.com \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

This configures the CLI. It does not grant permissions by itself.

## Scripting rules

When using the CLI in automation:

- use idempotency keys where supported;
- capture request ids;
- avoid wildcard grants;
- prefer least-privilege credentials;
- fail closed on authorization errors;
- do not probe reserved namespaces;
- use machine-readable output where commands support it.

## What you can do after this page

You should be able to choose the correct command family and understand whether a command is ordinary user work or a privileged administrative change.
