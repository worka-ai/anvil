---
slug: /anvil/user-guide/authentication
title: 'User Guide: Authentication & Permissions'
description: Learn how to authenticate with Anvil and how its permission model works using Apps, Policies, and Scopes.
tags: [user-guide, authentication, permissions, security, jwt, sigv4]
---

# Chapter 3: Authentication & Permissions

> **TL;DR:** Authenticate with SigV4 for S3 or get a JWT for gRPC. Access is governed by policies attached to your App, defining what actions (`read`, `write`) you can perform on resources (`my-bucket/*`).

Anvil employs a robust, flexible security model designed to give you fine-grained control over your data. This chapter explains how authentication works and how you can manage permissions for your applications.

### 3.1. The Security Model: Apps, API Keys, and Scoped Tokens

Direct access using a master tenant credential is not permitted for data plane operations. Instead, all programmatic access is managed through **Apps**.

1.  **Create an App:** Within your tenant, you create an App which is assigned a permanent `Client ID` and `Client Secret`.
2.  **Grant Policies:** You attach **Policies** to this App, granting it specific permissions (e.g., the ability to write to a particular bucket).
3.  **Authenticate:** Your application uses its Client ID and Secret to authenticate against Anvil.
    *   For the **S3 Gateway**, these credentials are used directly as the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for SigV4 signing.
    *   For the **gRPC API**, these credentials are exchanged for a short-lived **JSON Web Token (JWT)**.

This model ensures that you can issue, rotate, and revoke credentials for different applications without affecting others, and you can grant each application only the minimum permissions it needs.

### 3.2. Creating an App and Getting Credentials

You create an App using the `admin` tool (as shown in the Getting Started guide) or via the administrative API.

```bash
# This command is run by an administrator
docker compose exec anvil1 admin app create \
    --tenant-name my-first-tenant \
    --app-name my-application
```

This will output the `Client ID` and `Client Secret` for the new app. Store these securely.

### 3.3. Understanding Policies: Actions and Resources

Permissions in Anvil are defined by policies that connect an App to an **action** and a **resource**. When you grant a policy, you are creating a **scope** that the system uses to authorize requests.

-   **Action:** A string representing the operation to be performed.
-   **Resource:** A string identifying the Anvil resource(s) the action applies to. Wildcards (`*`) are supported.

When granting a policy, you provide the action and resource separately. Internally, Anvil combines them into a single scope string in the format `action|resource` for evaluation.

**Common Actions:**

| Action          | Description                               |
| --------------- | ----------------------------------------- |
| `bucket:create` | Ability to create new buckets.            |
| `bucket:read`   | Ability to list objects in a bucket.      |
| `bucket:write`  | Ability to update a bucket's properties.  |
| `bucket:delete` | Ability to delete a bucket.               |
| `object:read`   | Ability to download an object.            |
| `object:write`  | Ability to upload or overwrite an object. |
| `object:delete` | Ability to delete an object.              |
| `*`             | A wildcard representing **any** action.   |

**Resource Examples:**

| Resource Pattern              | Description                               |
| ----------------------------- | ----------------------------------------- |
| `my-specific-bucket`          | A single, specific bucket.                |
| `my-company-bucket/*`         | All objects within `my-company-bucket`.   |
| `my-company-bucket/invoices/*`| All objects under the `invoices/` prefix. |
| `*`                           | A wildcard representing **all** resources.|

**Granting Policies:**

You grant policies using the `admin` tool. The tool takes the action and resource as separate flags.

```bash
# Grant the app permission to write objects in 'my-data-bucket'
docker compose exec anvil1 admin policy grant \
    --app-name my-application \
    --action "object:write" \
    --resource "my-data-bucket/*"

# Grant the app permission to read objects from the same bucket
docker compose exec anvil1 admin policy grant \
    --app-name my-application \
    --action "object:read" \
    --resource "my-data-bucket/*"

# Grant the app permission to create buckets
docker compose exec anvil1 admin policy grant \
    --app-name my-application \
    --action "bucket:create" \
    --resource "*"
```

### 3.4. Public vs. Private Buckets

By default, all buckets in Anvil are **private**. An attempt to access an object in a private bucket without valid credentials will be denied.

You can, however, choose to make a bucket **publicly readable**. This is useful for hosting websites, public datasets, or other content that needs to be accessible to anyone on the internet.

When a bucket is public:

-   `GetObject` and `HeadObject` operations are allowed for anonymous users (without any authentication).
-   All other operations (`PutObject`, `DeleteObject`, `ListObjects`) still require valid, authorized credentials.

You can set a bucket's public status using the `admin` tool or the gRPC API.

```bash
# Make a bucket public (requires 'grant' permission on the bucket)
docker compose exec anvil1 admin bucket set-public-access --bucket my-public-assets --allow
```