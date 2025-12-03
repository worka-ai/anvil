---
slug: /reference/admin-cli
title: 'Reference: Admin CLI (`admin`)'
description: A complete reference guide for all commands available in the `admin` CLI for system administrators.
tags: [reference, cli, admin]
---

# Reference: Admin CLI (`admin`)

This page provides a complete reference for all commands available in the `admin` CLI. This tool is used by system administrators to manage the foundational resources of the Anvil deployment.

**Note:** These commands typically require direct access to the Anvil node (e.g., via `docker compose exec`) and connect directly to the global database.

## `tenant`

Manages tenants, the top-level organizational unit.

- **`create <name>`**: Creates a new tenant.
  ```bash
  admin tenant create my-new-tenant
  ```

## `app`

Manages apps, which are entities within a tenant that receive API credentials.

- **`create`**: Creates a new app and generates its credentials.
  ```bash
  admin app create --tenant-name <tenant_name> --app-name <app_name>
  ```
- **`reset-secret`**: Invalidates an app's client secret and generates a new one.
  ```bash
  admin app reset-secret --app-name <app_name>
  ```

## `policy`

Manages permissions granted to apps.

- **`grant`**: Grants a permission to an app.
  ```bash
  admin policy grant --app-name <app_name> --action <action> --resource <resource>
  ```
  - **Example Action:** `object:write`
  - **Example Resource:** `my-bucket/*`
## `region`

Manages geographical regions.

- **`create <name>`**: Registers a new region (idempotent).
  ```bash
  admin region create us-west-2
  ```

## `bucket`

Performs administrative tasks on buckets.

- **`set-public-access`**: Sets a bucket to be publicly readable or private.
  ```bash
  # To ENABLE public access:
  admin bucket set-public-access --bucket <bucket_name> --allow

  # To DISABLE public access (make private):
  admin bucket set-public-access --bucket <bucket_name>
  ```

## `user`

Manages administrative users for the Anvil system itself.

- **`create`**: Creates a new admin user.
  ```bash
  admin user create --username <username> --email <email> --password <password> --role <role>
  ```