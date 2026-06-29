---
title: CLI
description: Command-line tasks for Anvil users, administrators, and release operators.
---

# CLI

**What this page achieves:** you will understand which command family to use for common Anvil tasks and what safety model applies to each family.

The CLI is a convenient client for Anvil APIs. Some commands act as ordinary application clients. Some are administrative and should be treated as privileged production changes. The difference matters because commands can create credentials, grant policy, mutate buckets, and expose data.

## User-oriented commands

User commands authenticate through configured profiles and call the running Anvil API.

| Task | Command family | Why it exists |
| --- | --- | --- |
| Configure a profile | `anvil configure` or `anvil static-config` | Stores endpoint and credential settings for later commands. |
| Get a token | `anvil auth get-token` | Verifies credentials and obtains a bearer token. |
| Manage buckets | `anvil bucket ...` | Creates, lists, or deletes bucket boundaries where authorized. |
| Manage objects | `anvil object ...` | Puts, gets, heads, lists, and deletes objects. |
| Delegated auth | `anvil auth ...` | Performs permitted auth operations without direct storage access. |
| Ingestion keys | `anvil hf key ...` | Manages credentials for model/source ingestion workflows. |
| Ingestion jobs | `anvil hf ingest ...` | Starts and inspects ingestion into buckets. |

## Admin commands

Admin commands operate on privileged control-plane surfaces.

| Task | Command family | Safety note |
| --- | --- | --- |
| Create tenants | `admin tenant ...` | Changes administrative boundaries. |
| Create applications | `admin app ...` | Creates credentialed callers. |
| Grant policy | `admin policy grant ...` | Expands what callers may do. Review carefully. |
| Register regions | `admin region ...` | Affects placement and cluster behavior. |
| Set bucket access | `admin bucket set-public-access ...` | Can expose data if misused. |
| Create admin users | `admin user ...` | Grants human administrative entry points. |

Run admin commands from controlled environments and keep request ids in change records.

## Profile setup example

```bash
anvil static-config \
  --name production \
  --host https://anvil.example.com \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

This does not create permissions. It tells the CLI how to authenticate. The credentials must already be valid and scoped correctly.

## Scripting rules

When using the CLI in automation:

- use idempotency keys for retryable mutations where supported;
- capture request ids;
- avoid broad wildcard grants;
- use machine-readable output where commands support it;
- prefer least-privilege application credentials;
- fail closed on authorization errors;
- do not probe reserved namespaces.

## What you can do after this page

You should be able to choose the right CLI command family and understand whether a command is an ordinary user operation or a privileged administrative change.
