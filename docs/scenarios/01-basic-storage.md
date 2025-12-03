---
slug: /scenarios/basic-storage
title: 'Scenario: Basic Storage Workflow'
description: A step-by-step guide showing a complete, realistic workflow for provisioning a tenant and managing private objects.
tags: [scenario, cli, workflow]
---

# Scenario: Basic Storage Workflow

This guide walks through the most common end-to-end workflow in Anvil:
1.  An **administrator** provisions a new tenant and an application with credentials.
2.  The administrator grants the application the necessary permissions.
3.  A **client** configures their CLI with the credentials.
4.  The client creates a bucket and manages objects within it.

### 1. Admin: Provision Tenant and App

First, the system administrator creates a new region, a tenant for the client (e.g., `acme-corp`), and an "App" which will generate the client's credentials.

```bash
# 1. Create the region where data will live
anvil-admin region create --name us-east-1

# 2. Create the tenant
anvil-admin tenant create --name acme-corp

# 3. Create the app and securely save the outputted credentials
anvil-admin app create --tenant-name acme-corp --app-name data-science-app
```
**Expected Output:**
```
Client ID: app_abc123...
Client Secret: xyz789...
```

### 2. Admin: Grant Initial Permissions

By default, the new app has no permissions. The administrator must grant it the ability to create buckets. The resource `"*"` is used here as a wildcard to allow bucket creation in any region.

```bash
# Grant the app permission to create buckets
anvil-admin policy grant \
  --app-name data-science-app \
  --action bucket:create \
  --resource "*"
```

### 3. Client: Configure CLI

The client receives the `Client ID` and `Client Secret` from their administrator and configures their local `anvil` CLI.

```bash
# Configure the client CLI with the provided credentials
anvil static-config \
  --name acme \
  --host "https://anvil.acme.com" \
  --client-id app_abc123... \
  --client-secret xyz789... \
  --default
```

### 4. Client: Create a Bucket

The client can now create a bucket. This operation succeeds because the administrator granted the `bucket:create` permission.

```bash
anvil bucket create project-x-data us-east-1
```

### 5. Admin: Grant Object Permissions

Now that the `project-x-data` bucket exists, the administrator grants the app fine-grained permissions to perform object operations *only within that specific bucket*.

```bash
# Grant write, read, list, and delete permissions on objects in the new bucket.
# The '/*' suffix is a prefix match for all objects within the bucket.
anvil policy grant --app-name data-science-app --action object:write --resource "project-x-data/*"
anvil policy grant --app-name data-science-app --action object:read --resource "project-x-data/*"
anvil policy grant --app-name data-science-app --action object:list --resource "project-x-data"
anvil policy grant --app-name data-science-app --action object:delete --resource "project-x-data/*"
```

### 6. Client: Manage Objects

The client can now perform a full range of object operations within their bucket.

```bash
# Upload a file
anvil object put ./report.pdf s3://project-x-data/quarterly/report.pdf

# List objects
anvil object ls s3://project-x-data/quarterly/

# View object metadata
anvil object head s3://project-x-data/quarterly/report.pdf

# Download the file
anvil object get s3://project-x-data/quarterly/report.pdf ./downloaded_report.pdf

# Delete the object
anvil object rm s3://project-x-data/quarterly/report.pdf
```
