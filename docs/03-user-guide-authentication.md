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

You create an App using the `anvil admin` CLI (as shown in the Getting Started guide) or via the administrative API.

```bash
# This command is run by an administrator
docker compose exec anvil1 admin apps create \
    --tenant-name my-first-tenant \
    --app-name my-application
```

This will output the `Client ID` and `Client Secret` for the new app. Store these securely.

### 3.3. Understanding Policies: Resource and Action Scopes

Permissions in Anvil are defined by policies that connect an App to an **action** and a **resource**.

-   **Resource:** A string that identifies a set of Anvil resources. It follows the format `type:identifier`. Wildcards (`*`) are supported.
    *   `bucket:my-bucket`: A specific bucket.
    *   `bucket:my-bucket/invoices/*`: All objects inside the `invoices/` prefix in `my-bucket`.
    *   `bucket:*`: All buckets.
-   **Action:** A string representing an operation. Common actions include:
    *   `read`: Permission to get or list resources.
    *   `write`: Permission to create, update, or delete resources.
    *   `grant`: Permission to manage the permissions of other apps (a highly privileged action).

A policy is granted using the admin CLI:

```bash
# Grant the app permission to read and write objects in 'my-data-bucket'
docker compose exec anvil1 admin policies grant \
    --app-name my-application \
    --action "write" \
    --resource "bucket:my-data-bucket/*"

docker compose exec anvil1 admin policies grant \
    --app-name my-application \
    --action "read" \
    --resource "bucket:my-data-bucket/*"
```

### 3.4. Public vs. Private Buckets

By default, all buckets in Anvil are **private**. An attempt to access an object in a private bucket without valid credentials will be denied.

You can, however, choose to make a bucket **publicly readable**. This is useful for hosting websites, public datasets, or other content that needs to be accessible to anyone on the internet.

When a bucket is public:

-   `GetObject` and `HeadObject` operations are allowed for anonymous users (without any authentication).
-   All other operations (`PutObject`, `DeleteObject`, `ListObjects`) still require valid, authorized credentials.

You can set a bucket's public status using the `anvil admin` CLI or the gRPC API.

```bash
# Make a bucket public (requires 'grant' permission on the bucket)
docker compose exec anvil1 admin buckets set-public-access --bucket my-public-assets --allow
```
