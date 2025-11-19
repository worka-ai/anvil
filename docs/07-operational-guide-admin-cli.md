--- 
slug: /anvil/operational-guide/admin-tool
title: 'Operational Guide: The Admin Tool'
description: A reference guide for using the `admin` tool to manage tenants, apps, policies, and regions.
tags: [operational-guide, admin, tenants, apps, policies]
---

# Chapter 9: The Admin Tool

> **TL;DR:** Use the `admin` tool for core administrative tasks. It connects directly to the global database to manage tenants, regions, apps, and policies.

Anvil includes a powerful command-line tool for performing essential administrative tasks. This tool is the primary way to bootstrap the system and manage high-level resources. It works by connecting directly to the global PostgreSQL database.

### Running the Admin Tool

When running Anvil via Docker Compose, you can execute the admin tool using `docker-compose exec`. The command to run is `admin`.

```bash
docker compose exec anvil1 admin <COMMAND>
```

All admin commands will automatically use the environment variables (`GLOBAL_DATABASE_URL`, etc.) set in your `docker-compose.yml` file.

### Command Reference

#### Managing Regions

Regions must be created before you can assign buckets to them.

**Create a Region**

This command is idempotent and uses a positional argument for the name.

```bash
# Usage: admin region create <NAME>
docker compose exec anvil1 admin region create us-east-1
```

#### Managing Tenants

Tenants are the top-level organizational unit in Anvil.

**Create a Tenant**

This command also uses a positional argument for the name.

```bash
# Usage: admin tenant create <NAME>
docker compose exec anvil1 admin tenant create my-organization
```

#### Managing Apps

Apps are entities within a tenant that are granted API credentials.

**Create an App**

This command uses named flags for the tenant and app names.

```bash
docker compose exec anvil1 admin app create --tenant-name <TENANT_NAME> --app-name <APP_NAME>
```

*   `--tenant-name`: The name of the tenant that will own the app.
*   `--app-name`: A descriptive name for the app (e.g., `backup-script`).

> **Security Note:** The `Client Secret` is only displayed once upon creation. You must save it in a secure location.

#### Managing Policies

Policies grant permissions to apps.

**Grant a Policy**

This command uses named flags.

```bash
docker compose exec anvil1 admin policy grant \
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
docker compose exec anvil1 admin policy grant \
    --app-name web-frontend \
    --action "read" \
    --resource "bucket:public-assets/*"
```
