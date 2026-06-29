---
title: Buckets And Policies
description: Create buckets, list them, delete them, manage bucket policies, and watch bucket metadata.
---

# Buckets And Policies

**What this page gives you:** a tutorial for every operation in this area, with Rust, Java, Node.js, and Python tabs for each operation.

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
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CreateBucketRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.create_bucket(CreateBucketRequest { bucket_name: \"documents\".into(), region: \"eu-west-1\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CreateBucketRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.createBucket(\n    CreateBucketRequest.builder()\n        .bucketName(\"documents\")\n        .region(\"eu-west-1\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.createBucket({ bucketName: 'documents', region: 'eu-west-1' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.create_bucket(bucket_name='documents', region='eu-west-1')\nprint(response)"
}
```

## List buckets

**Operation:** `BucketService.ListBuckets`

Returns bucket boundaries visible to the caller.

```anvil-tabs
{
  "operation": "ListBuckets",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListBucketsRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_buckets(Default::default()).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListBucketsRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listBuckets(\n    new ListBucketsRequest()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listBuckets({});\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_buckets()\nprint(response)"
}
```

## Read bucket policy

**Operation:** `BucketService.GetBucketPolicy`

Reads the policy document currently attached to a bucket.

```anvil-tabs
{
  "operation": "GetBucketPolicy",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetBucketPolicyRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_bucket_policy(GetBucketPolicyRequest { bucket_name: \"documents\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetBucketPolicyRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getBucketPolicy(\n    GetBucketPolicyRequest.builder()\n        .bucketName(\"documents\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getBucketPolicy({ bucketName: 'documents' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_bucket_policy(bucket_name='documents')\nprint(response)"
}
```

## Write bucket policy

**Operation:** `BucketService.PutBucketPolicy`

Replaces or creates a bucket policy using an explicit policy document.

```anvil-tabs
{
  "operation": "PutBucketPolicy",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::PutBucketPolicyRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.put_bucket_policy(PutBucketPolicyRequest { bucket_name: \"documents\".into(), policy_json: \"{\\\"version\\\":\\\"2026-06-29\\\",\\\"statements\\\":[]}\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.PutBucketPolicyRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.putBucketPolicy(\n    PutBucketPolicyRequest.builder()\n        .bucketName(\"documents\")\n        .policyJson(\"{\\\"version\\\":\\\"2026-06-29\\\",\\\"statements\\\":[]}\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.putBucketPolicy({ bucketName: 'documents', policyJson: '{\"version\":\"2026-06-29\",\"statements\":[]}' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.put_bucket_policy(bucket_name='documents', policy_json='{\"version\":\"2026-06-29\",\"statements\":[]}')\nprint(response)"
}
```

## Watch bucket metadata

**Operation:** `BucketService.WatchBucketMetadata`

Streams bucket metadata changes after a cursor.

```anvil-tabs
{
  "operation": "WatchBucketMetadata",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchBucketMetadataRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_bucket_metadata(WatchBucketMetadataRequest { bucket_name: \"documents\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchBucketMetadataRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchBucketMetadata(\n    WatchBucketMetadataRequest.builder()\n        .bucketName(\"documents\")\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchBucketMetadata({ bucketName: 'documents', afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_bucket_metadata(bucket_name='documents', after_cursor='lastCursor')\nprint(response)"
}
```

## Delete a bucket

**Operation:** `BucketService.DeleteBucket`

Deletes a bucket boundary after callers have decided how object lifecycle should end.

```anvil-tabs
{
  "operation": "DeleteBucket",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::DeleteBucketRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.delete_bucket(DeleteBucketRequest { bucket_name: \"documents\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.DeleteBucketRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.deleteBucket(\n    DeleteBucketRequest.builder()\n        .bucketName(\"documents\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.deleteBucket({ bucketName: 'documents' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.delete_bucket(bucket_name='documents')\nprint(response)"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
