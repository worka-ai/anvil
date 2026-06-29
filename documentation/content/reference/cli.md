---
title: CLI
description: Command-line surfaces for users and administrators.
---

# CLI

**Goal:** identify which CLI command family to use for each task.

Anvil ships user-facing and administrative command surfaces. User commands talk to the running Anvil API with application credentials. Admin commands operate on privileged control-plane state and require stronger operational controls.

## User CLI tasks

| Task | Command family |
| --- | --- |
| Configure a profile | `anvil configure` or `anvil static-config` |
| Get a bearer token | `anvil auth get-token` |
| Create/list/delete buckets | `anvil bucket ...` |
| Put/get/head/list/delete objects | `anvil object ...` |
| Manage delegated auth where permitted | `anvil auth ...` |
| Manage model ingestion credentials | `anvil hf key ...` |
| Start and inspect model ingestion | `anvil hf ingest ...` |

## Admin tasks

| Task | Command family |
| --- | --- |
| Create tenants | `admin tenant ...` |
| Create applications | `admin app ...` |
| Grant policy | `admin policy grant ...` |
| Register regions | `admin region ...` |
| Set bucket public access | `admin bucket set-public-access ...` |
| Create admin users | `admin user ...` |

## Profile setup

```bash
anvil static-config \
  --name production \
  --host https://anvil.example.com \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

## Scripting rules

- Capture request ids in logs.
- Use idempotency keys for retryable mutations where supported.
- Avoid broad wildcard grants in automation.
- Prefer machine-readable output for CI workflows where commands support it.
- Treat admin commands as privileged production changes.
