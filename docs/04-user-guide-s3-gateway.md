---
slug: /anvil/user-guide/s3-gateway
title: 'User Guide: Using the S3-Compatible Gateway'
description: Learn how to use standard S3 tools and SDKs to interact with your Anvil cluster.
tags: [user-guide, s3, aws-cli, rclone, sdk]
---

# Chapter 4: Using the S3-Compatible Gateway

> **TL;DR:** Point your existing S3 tools to the Anvil HTTP endpoint. Use your App's Client ID and Secret as the AWS Access Key and Secret Key. All standard operations like `put-object`, `get-object`, and `list-objects` are supported.

One of Anvil's most powerful features is its S3-compatible API gateway. This allows you to leverage the vast ecosystem of existing S3 tools, libraries, and SDKs to interact with your Anvil cluster without needing to write any custom code.

> **Note:** While this guide focuses on S3-compatible tools, the `anvil` is the recommended primary interface for most operations. See the [Getting Started](./getting-started) guide for `anvil` examples.

### 4.1. Configuring S3 Clients

To connect an S3 client to Anvil, you need to configure three things:

1.  **Endpoint URL:** The HTTP address of your Anvil node (e.g., `http://localhost:50051`).
2.  **Access Key ID:** Your Anvil App's **Client ID**.
3.  **Secret Access Key:** Your Anvil App's **Client Secret**.

#### Example: AWS CLI

The easiest way to configure the AWS CLI is by setting environment variables.

```bash
# Your App credentials
export AWS_ACCESS_KEY_ID="YOUR_CLIENT_ID"
export AWS_SECRET_ACCESS_KEY="YOUR_CLIENT_SECRET"

# The region your bucket is in
export AWS_DEFAULT_REGION="europe-west-1"

# The Anvil S3 endpoint
ANVIL_ENDPOINT="http://localhost:50051"
```

Alternatively, you can create a dedicated profile in your `~/.aws/config` and `~/.aws/credentials` files.

#### Example: rclone

When configuring `rclone`, choose `Amazon S3` as the storage type. Then, provide the following details:

-   **Provider:** `Other`
-   **Access Key ID:** Your Anvil Client ID
-   **Secret Access Key:** Your Anvil Client Secret
-   **Region:** The region your bucket is in
-   **Endpoint:** The HTTP address of your Anvil node

#### Example: AWS SDKs (e.g., Boto3 for Python)

When initializing the S3 client in your code, you must override the endpoint.

```python
import boto3

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:50051',
    aws_access_key_id='YOUR_CLIENT_ID',
    aws_secret_access_key='YOUR_CLIENT_SECRET',
    region_name='europe-west-1'
)
```

### 4.2. Common Operations

Once configured, you can use the standard S3 commands to manage your buckets and objects.

**Create a Bucket**

```bash
aws s3api create-bucket \
    --bucket my-s3-bucket \
    --region europe-west-1 \
    --endpoint-url $ANVIL_ENDPOINT
```

**Upload a File**

```bash
aws s3 cp local-file.txt s3://my-s3-bucket/remote-file.txt --endpoint-url $ANVIL_ENDPOINT
```

**List Objects**

```bash
aws s3 ls s3://my-s3-bucket/ --endpoint-url $ANVIL_ENDPOINT
```

**Download a File**

```bash
aws s3 cp s3://my-s3-bucket/remote-file.txt downloaded-file.txt --endpoint-url $ANVIL_ENDPOINT
```
