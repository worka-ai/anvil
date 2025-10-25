---
slug: /anvil/operational-guide/admin-cli
title: 'Operational Guide: The Anvil Admin CLI'
description: A reference guide for using the `anvil admin` command-line interface to manage tenants, apps, policies, and regions.
tags: [operational-guide, admin, cli, tenants, apps, policies]
---

# Chapter 7: The Anvil Admin CLI

> **TL;DR:** Use the `anvil admin` CLI for core administrative tasks. It connects directly to the global database to manage tenants, regions, apps, and policies.

Anvil includes a powerful command-line interface (CLI) for performing essential administrative tasks. This tool is the primary way to bootstrap the system and manage high-level resources. It works by connecting directly to the global PostgreSQL database.

### Running the Admin CLI

When running Anvil via Docker Compose, you can execute the admin CLI using `docker-compose exec`.

```bash
docker-compose exec anvil1 anvil admin <COMMAND>
```

All admin commands require the `GLOBAL_DATABASE_URL` and `ANVIL_SECRET_ENCRYPTION_KEY` to be set, which is typically handled by the environment variables in the `docker-compose.yml` file.

### Command Reference

#### Managing Regions

Regions must be created before you can assign buckets to them.

**Create a Region**

This command is idempotent; it will do nothing if the region already exists.

```bash
anvil admin regions create --name <REGION_NAME>
```

*   `--name`: The name of the new region (e.g., `us-east-1`, `DOCKER_TEST`).

#### Managing Tenants

Tenants are the top-level organizational unit in Anvil.

**Create a Tenant**

```bash
anvil admin tenants create --name <TENANT_NAME>
```

*   `--name`: The unique name for the new tenant (e.g., `my-organization`).

#### Managing Apps

Apps are entities within a tenant that are granted API credentials.

**Create an App**

This command creates an app and outputs its `Client ID` and `Client Secret`, which are used for S3 and gRPC authentication.

```bash
anvil admin apps create --tenant-name <TENANT_NAME> --app-name <APP_NAME>
```

*   `--tenant-name`: The name of the tenant that will own the app.
*   `--app-name`: A descriptive name for the app (e.g., `backup-script`, `web-frontend`).

> **Security Note:** The `Client Secret` is only displayed once upon creation. You must save it in a secure location.

#### Managing Policies

Policies grant permissions to apps.

**Grant a Policy**

This command gives an app specific permissions for an action on a resource.

```bash
anvil admin policies grant \
    --app-name <APP_NAME> \
    --action <ACTION> \
    --resource <RESOURCE>
```

*   `--app-name`: The name of the app to grant the policy to.
*   `--action`: The permission to grant (e.g., `read`, `write`, `*`).
*   `--resource`: The resource the action applies to (e.g., `bucket:my-bucket/*`).

**Example:**

```bash
# Allow the 'web-frontend' app to read objects from the 'public-assets' bucket
anvil admin policies grant \
    --app-name web-frontend \
    --action "read" \
    --resource "bucket:public-assets/*"
```
