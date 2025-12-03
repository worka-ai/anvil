---
slug: /scenarios/public-buckets
title: 'Scenario: Public Buckets & Permissions'
description: A guide covering how to host public files and manage app-to-app permissions.
tags: [scenario, cli, permissions, public-access]
---

# Scenario: Public Buckets & App-to-App Permissions

This guide covers two related advanced topics:
1.  Making a bucket's contents publicly readable over the internet.
2.  Delegating permissions from one application to another.

### 1. Admin: Configure a Public Bucket

By default, all buckets are private. To make a bucket public, an administrator must explicitly set its public access flag.

```bash
# 1. Create a bucket to hold public assets
anvil bucket create public-assets us-east-1

# 2. Grant your app ('data-science-app') permission to upload to this bucket
admin policy grant \
  --app-name data-science-app \
  --action object:write \
  --resource "public-assets/*"

# 3. Use the admin tool to set the bucket's public flag to true
admin bucket set-public-access --bucket public-assets --allow true
```

### 2. Client: Upload to the Public Bucket

The client can now upload files. Any object in this bucket will be accessible via its public URL without any authentication.

```bash
# Upload a company logo
anvil object put ./logo.png s3://public-assets/images/logo.png
```

This object is now publicly available at a URL like `https://anvil.acme.com/public-assets/images/logo.png`.

### 3. Delegating Permissions Between Apps

Anvil's security model allows an application to delegate its own permissions to another application. This is useful for creating workflows where a primary service needs to grant temporary or limited access to a secondary service.

**1. Admin: Create a Second App and Grant Delegation Rights**

First, an admin creates a second app (`reporting-app`). Then, they grant the primary `data-science-app` the special `policy:grant` permission, which allows it to delegate its own permissions.

```bash
# Create the second app
admin app create --tenant-name acme-corp --app-name reporting-app

# Grant the 'data-science-app' the ability to delegate its permissions
admin policy grant \
  --app-name data-science-app \
  --action policy:grant \
  --resource "*"
```

**2. Client: Delegate and Revoke Permissions**

Now, the user logged in as `data-science-app` can use the `anvil auth` commands to grant permissions to `reporting-app`.

```bash
# Delegate read access for 'project-x-data' to 'reporting-app'
anvil auth grant reporting-app object:read "project-x-data/*"

# Later, the client can revoke this access
anvil auth revoke reporting-app object:read "project-x-data/*"
```
