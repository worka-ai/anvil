---
title: S3 Compatibility
description: Use existing S3 tools with Anvil while understanding the boundary of the compatibility API.
---

# S3 Compatibility

**Goal:** connect existing S3 clients to Anvil and understand which Anvil features require the native API.

Anvil provides an S3-compatible gateway for common object workflows. This lets existing tools upload, download, list, copy, and delete objects without learning the native API first.

## What S3 means here

S3 is a widely adopted HTTP object API. Clients know how to sign requests, name buckets, put objects, list prefixes, read ranges, and attach metadata. Compatibility means Anvil accepts those requests and maps them to Anvil's object model.

Compatibility does not mean every Anvil feature can be expressed through S3. Index definitions, PersonalDB commits, relationship tuple writes, native watches, and administrative repair operations use the native API.

## Credentials

Use the Anvil application client id as the S3 access key id and the Anvil client secret as the S3 secret access key. Point the client endpoint to the Anvil S3 endpoint.

```bash
export AWS_ACCESS_KEY_ID="<anvil-client-id>"
export AWS_SECRET_ACCESS_KEY="<anvil-client-secret>"
export AWS_DEFAULT_REGION="local"
export ANVIL_S3_ENDPOINT="http://127.0.0.1:50051"
```

## Basic operations

Create a bucket:

```bash
aws s3api create-bucket \
  --endpoint-url "$ANVIL_S3_ENDPOINT" \
  --bucket documents
```

Upload an object:

```bash
aws s3api put-object \
  --endpoint-url "$ANVIL_S3_ENDPOINT" \
  --bucket documents \
  --key tenants/acme/contracts/contract-42.pdf \
  --body ./contract-42.pdf \
  --content-type application/pdf \
  --metadata customer=acme,document_type=contract
```

Read metadata:

```bash
aws s3api head-object \
  --endpoint-url "$ANVIL_S3_ENDPOINT" \
  --bucket documents \
  --key tenants/acme/contracts/contract-42.pdf
```

List a prefix:

```bash
aws s3api list-objects-v2 \
  --endpoint-url "$ANVIL_S3_ENDPOINT" \
  --bucket documents \
  --prefix tenants/acme/contracts/
```

## Precondition and range behavior

Anvil enforces ETag and date preconditions for compatibility reads and writes. Use them to avoid lost updates. Range reads are supported for standard single-range requests, which matters for media players, model readers, and resumable downloads.

## Reserved namespace behavior

The S3 gateway enforces the same reserved namespace rules as the native API. Keys under `_anvil/` are not readable, writable, listable, copyable, or deletable through S3. The gateway rejects them before exposing existence information.

## When to switch to native APIs

Switch to the native API when you need to:

- define or query full text and vector indexes;
- watch prefixes and indexes;
- write relationship tuples;
- use PersonalDB;
- inspect source or model ingestion metadata;
- access structured administrative diagnostics.

The intended pattern is not either/or. Many systems use S3 clients for bulk object transfer and the native API for Anvil-specific control planes.
