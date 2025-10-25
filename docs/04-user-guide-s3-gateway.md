---
slug: /anvil/user-guide/s3-gateway
title: 'User Guide: Using the S3-Compatible Gateway'
description: Learn how to use standard S3 tools and SDKs to interact with your Anvil cluster.
tags: [user-guide, s3, aws-cli, rclone, sdk]
---

# Chapter 4: Using the S3-Compatible Gateway

> **TL;DR:** Point your existing S3 tools to the Anvil HTTP endpoint. Use your App's Client ID and Secret as the AWS Access Key and Secret Key. All standard operations like `put-object`, `get-object`, and `list-objects` are supported.

One of Anvil's most powerful features is its S3-compatible API gateway. This allows you to leverage the vast ecosystem of existing S3 tools, libraries, and SDKs to interact with your Anvil cluster without needing to write any custom code.

### 4.1. Configuring S3 Clients

To connect an S3 client to Anvil, you need to configure three things:

1.  **Endpoint URL:** The HTTP address of your Anvil node (e.g., `http://localhost:9000`).
2.  **Access Key ID:** Your Anvil App's **Client ID**.
3.  **Secret Access Key:** Your Anvil App's **Client Secret**.

#### Example: AWS CLI

The easiest way to configure the AWS CLI is by setting environment variables.

```bash
# Your App credentials
export AWS_ACCESS_KEY_ID="YOUR_CLIENT_ID"
export AWS_SECRET_ACCESS_KEY="YOUR_CLIENT_SECRET"

# The region your bucket is in
export AWS_DEFAULT_REGION="DOCKER_TEST"

# The Anvil S3 endpoint
ANVIL_ENDPOINT="http://localhost:9000"
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
    endpoint_url='http://localhost:9000',
    aws_access_key_id='YOUR_CLIENT_ID',
    aws_secret_access_key='YOUR_CLIENT_SECRET',
    region_name='DOCKER_TEST'
)
```

### 4.2. Common Operations

Once configured, you can use the standard S3 commands to manage your buckets and objects.

**Create a Bucket**

```bash
aws s3api create-bucket \
    --bucket my-s3-bucket \
    --region DOCKER_TEST \
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

### 4.3. Generating Presigned URLs

Anvil's S3 gateway supports generating presigned URLs, which provide temporary, credential-less access to your objects. This is the most secure way to grant a user temporary access to download or upload a specific file.

**Generate a Presigned URL for Download (GET)**

```bash
aws s3 presign s3://my-s3-bucket/remote-file.txt --expires-in 300 --endpoint-url $ANVIL_ENDPOINT
```

This will return a long URL that can be used by anyone to download `remote-file.txt` for the next 5 minutes (300 seconds).

```bash
# Anyone can use this URL to download the file
curl "THE_PRESIGNED_URL"
```

**Generate a Presigned URL for Upload (PUT)**

```bash
aws s3 presign s3://my-s3-bucket/new-object.txt --expires-in 600 --endpoint-url $ANVIL_ENDPOINT
```

This URL can be used to upload a file to the specified key.

```bash
curl -T "local-upload.txt" "THE_PRESIGNED_URL"
```

