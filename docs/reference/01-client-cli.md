---
slug: /reference/client-cli
title: 'Reference: Client CLI (`anvil-cli`)'
description: A reference guide for the user-facing Anvil CLI.
tags: [reference, cli, client]
---

# Reference: Client CLI (`anvil-cli`)

`anvil-cli` is the user and application command-line client. It talks to the public native API and can only perform operations authorised by its profile credentials and token scopes.

## Global flags

- `--profile <NAME>`: use a named profile from the CLI configuration file.
- `--config <PATH>`: use a custom configuration file.

## Configuration

```bash
anvil-cli configure
anvil-cli static-config \
  --name production \
  --host https://storage.example.com \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

## Authentication

```bash
anvil-cli auth get-token
anvil-cli auth grant <app_name> <action> <resource>
anvil-cli auth revoke <app_name> <action> <resource>
```

Grants and revokes only work when the current caller is already authorised to delegate that scope.

## Buckets

```bash
anvil-cli bucket create <bucket> <region>
anvil-cli bucket rm <bucket>
anvil-cli bucket ls
anvil-cli bucket set-public <bucket> --allow true
```

## Objects

```bash
anvil-cli object put ./local.txt s3://bucket/path/local.txt
anvil-cli object get s3://bucket/path/local.txt ./downloaded.txt
anvil-cli object head s3://bucket/path/local.txt
anvil-cli object ls s3://bucket/path/
anvil-cli object rm s3://bucket/path/local.txt
```

Object paths under `_anvil/` are reserved and fail closed.

## Hugging Face ingestion

```bash
anvil-cli hf key add --name hf-prod --token "$HF_TOKEN"
anvil-cli hf key ls
anvil-cli hf key rm --name hf-prod

anvil-cli hf ingest start \
  --key hf-prod \
  --repo org/model \
  --bucket model-artefacts \
  --target-region eu-west-1
anvil-cli hf ingest status --id 1
anvil-cli hf ingest cancel --id 1
```

Ingestion writes artefacts, metadata, derived indexes, diagnostics, and completion records through CoreStore.
