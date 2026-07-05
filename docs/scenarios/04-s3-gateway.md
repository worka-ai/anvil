---
slug: /scenarios/s3-gateway
title: 'Scenario: Using the S3 Gateway'
description: Learn how to use standard S3 tools and SDKs with Anvil's S3-compatible gateway.
tags: [scenario, s3, aws-cli, rclone, sdk]
---

# Scenario: Using the S3-Compatible Gateway

Anvil exposes an S3-compatible gateway for tools that already speak S3. The gateway is a protocol adapter over the Anvil model: requests still authenticate as Anvil principals, authorisation still applies, object writes still go through CoreStore, and `_anvil/` paths remain reserved.

Use the native API when you need Anvil-specific features such as index definitions, PersonalDB, relationship authorisation watches, or repair diagnostics. Use the S3 gateway when existing tools need to move object bytes.

## Configure an S3 client

You need:

1. the Anvil public API endpoint;
2. an Anvil application client id;
3. an Anvil application client secret;
4. the region used by the bucket.

```bash
export AWS_ACCESS_KEY_ID="$ANVIL_CLIENT_ID"
export AWS_SECRET_ACCESS_KEY="$ANVIL_CLIENT_SECRET"
export AWS_DEFAULT_REGION="eu-west-1"
export ANVIL_ENDPOINT="http://localhost:50051"
```

With `rclone`, choose `Amazon S3`, provider `Other`, then set the same access key, secret key, region, and endpoint.

With SDKs, override the endpoint:

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:50051",
    aws_access_key_id="YOUR_CLIENT_ID",
    aws_secret_access_key="YOUR_CLIENT_SECRET",
    region_name="eu-west-1",
)
```

## Common operations

```bash
aws s3api create-bucket \
  --bucket documents \
  --region eu-west-1 \
  --endpoint-url "$ANVIL_ENDPOINT"

aws s3 cp ./contract.txt s3://documents/contracts/contract-42.txt \
  --endpoint-url "$ANVIL_ENDPOINT"

aws s3api head-object \
  --bucket documents \
  --key contracts/contract-42.txt \
  --endpoint-url "$ANVIL_ENDPOINT"

aws s3 ls s3://documents/contracts/ \
  --endpoint-url "$ANVIL_ENDPOINT"

aws s3 cp s3://documents/contracts/contract-42.txt ./downloaded.txt \
  --endpoint-url "$ANVIL_ENDPOINT"
```

## Security rules that still apply

The gateway does not bypass Anvil.

- Credentials map to Anvil application principals.
- Bucket and object policies are still checked.
- Public buckets are explicit policy decisions.
- Reserved `_anvil/` paths are denied for GET, HEAD, PUT, DELETE, LIST, COPY, multipart, and range reads.
- Region routing and host aliases are resolved through Anvil mesh records.
- Object bytes, metadata, versions, and watches are persisted through CoreStore.

If an S3 client receives `UnauthorizedReservedNamespace`, the request targeted Anvil-owned internal state and must be changed.
