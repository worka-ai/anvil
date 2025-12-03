---
slug: /reference/client-cli
title: 'Reference: Client CLI (`anvil`)'
description: A complete reference guide for all commands and subcommands available in the `anvil` client CLI.
tags: [reference, cli, client]
---

# Reference: Client CLI (`anvil`)

This page provides a complete reference for all commands available in the `anvil` client CLI. This tool is used by end-users to interact with the Anvil storage system.

## Global Flags

- `--profile <NAME>`: Use a specific profile from your configuration file.
- `--config <PATH>`: Path to a custom configuration file.

## `configure`

Starts an interactive wizard to create or update a connection profile.

```bash
anvil configure
```

## `static-config`

Creates a configuration profile non-interactively. Useful for scripts.

```bash
anvil static-config --name <profile_name> --host <host> --client-id <id> --client-secret <secret> [--default]
```

## `auth`

Manages authentication and app-to-app permissions.

- **`get-token`**: Retrieves a raw bearer token (JWT) for the current profile.
  ```bash
  anvil auth get-token [--client-id <id>] [--client-secret <secret>]
  ```
- **`grant`**: Grants a permission from your app to another app.
  ```bash
  anvil auth grant <app_name> <action> <resource>
  ```
- **`revoke`**: Revokes a permission from another app.
  ```bash
  anvil auth revoke <app_name> <action> <resource>
  ```

## `bucket`

Manages buckets.

- **`create <name> <region>`**: Creates a new bucket in the specified region.
- **`rm <name>`**: Deletes an empty bucket.
- **`ls`**: Lists all buckets you have permission to see.
- **`set-public --name <name> --allow <true|false>`**: Sets public read access for a bucket.

## `object`

Manages objects within buckets using S3-style paths.

- **`put <local_path> <s3://bucket/key>`**: Uploads a file.
- **`get <s3://bucket/key> [local_path]`**: Downloads a file. Prints to stdout if `local_path` is omitted.
- **`rm <s3://bucket/key>`**: Deletes an object.
- **`ls <s3://bucket/[prefix]>`**: Lists objects in a bucket. Can be filtered by a prefix.
- **`head <s3://bucket/key>`**: Shows an object's metadata (size, ETag, etc.).

## `hf`

Integrates with the Hugging Face Hub.

#### `hf key`

Manages Hugging Face API tokens stored in Anvil.

- **`add --name <key_name> --token <hf_token> [--note <note>]`**: Adds a named HF token.
- **`ls`**: Lists all stored HF keys by name.
- **`rm --name <key_name>`**: Removes a stored HF key.

#### `hf ingest`

Manages the asynchronous ingestion of models from Hugging Face.

- **`start`**: Begins a new ingestion job. Returns a job ID.
  ```bash
  anvil hf ingest start --key <key_name> --repo <repo_id> --bucket <bucket_name> --target-region <region> [OPTIONS]
  ```
- **`status --id <ingestion_id>`**: Checks the status of an ingestion job.
- **`cancel --id <ingestion_id>`**: Cancels a running ingestion job.
