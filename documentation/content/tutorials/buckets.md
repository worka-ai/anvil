---
title: Buckets And Policies
description: Create buckets, list them, delete them, manage bucket policies, and watch bucket metadata.
---

# Buckets And Policies

**What this page gives you:** a tutorial for every operation in this area, with Rust examples for each operation.

A bucket is the administrative boundary around related objects. Create buckets around stable ownership or policy boundaries, not around every small folder. This tutorial starts with bucket creation, then covers listing, deletion, policy updates, and metadata watches. By the end, you can manage the container that every other Anvil operation uses.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artefact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Create a bucket

**Operation:** `BucketService.CreateBucket`

Creates the policy and placement boundary that will hold related objects.

```anvil-tabs
{
  "operation": "CreateBucket",
  "rust": "use anvil_storage::{AnvilClient, proto::CreateBucketRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.buckets().create_bucket(CreateBucketRequest { bucket_name: \"documents\".into(), region: \"eu-west-1\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List buckets

**Operation:** `BucketService.ListBuckets`

Returns bucket boundaries visible to the caller.

```anvil-tabs
{
  "operation": "ListBuckets",
  "rust": "use anvil_storage::{AnvilClient, proto::ListBucketsRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.buckets().list_buckets(Default::default()).await?;\nprintln!(\"{response:?}\");"
}
```

## Read bucket policy

**Operation:** `BucketService.GetBucketPolicy`

Reads the policy document currently attached to a bucket.

```anvil-tabs
{
  "operation": "GetBucketPolicy",
  "rust": "use anvil_storage::{AnvilClient, proto::GetBucketPolicyRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.buckets().get_bucket_policy(GetBucketPolicyRequest { bucket_name: \"documents\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Write bucket policy

**Operation:** `BucketService.PutBucketPolicy`

Replaces or creates a bucket policy using an explicit policy document.

```anvil-tabs
{
  "operation": "PutBucketPolicy",
  "rust": "use anvil_storage::{AnvilClient, proto::PutBucketPolicyRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.buckets().put_bucket_policy(PutBucketPolicyRequest { bucket_name: \"documents\".into(), policy_json: \"{\\\"version\\\":\\\"2026-06-29\\\",\\\"statements\\\":[]}\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Watch bucket metadata

**Operation:** `BucketService.WatchBucketMetadata`

Streams bucket metadata changes after a cursor.

```anvil-tabs
{
  "operation": "WatchBucketMetadata",
  "rust": "use anvil_storage::{AnvilClient, proto::WatchBucketMetadataRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.buckets().watch_bucket_metadata(WatchBucketMetadataRequest { bucket_name: \"documents\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Delete a bucket

**Operation:** `BucketService.DeleteBucket`

Deletes a bucket boundary after callers have decided how object lifecycle should end.

```anvil-tabs
{
  "operation": "DeleteBucket",
  "rust": "use anvil_storage::{AnvilClient, proto::DeleteBucketRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.buckets().delete_bucket(DeleteBucketRequest { bucket_name: \"documents\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
