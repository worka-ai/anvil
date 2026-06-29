---
title: Objects, Versions, Streams, And Multipart Uploads
description: Write, read, list, copy, compose, patch, stream, version, and upload large objects.
---

# Objects, Versions, Streams, And Multipart Uploads

**What this page gives you:** a tutorial for every operation in this area, with Rust, Java, Node.js, and Python tabs for each operation.

Objects are the durable source facts in Anvil. A write stores bytes, metadata, checksums, preconditions, and watch cursors. A read returns a specific object version. This tutorial covers the full object lifecycle: ordinary writes and reads, listings, versions, copy and compose operations, JSON patching, manifest compare-and-swap, append streams, and multipart uploads for large payloads.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artefact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Write an object

**Operation:** `ObjectService.PutObject`

Writes object bytes and metadata as a new durable object version.

```anvil-tabs
{
  "operation": "PutObject",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::PutObjectRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.put_object(PutObjectRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), body: \"bytes\".into(), metadata: \"metadata\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.PutObjectRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.putObject(\n    PutObjectRequest.builder()\n        .bucketName(\"documents\")\n        .objectKey(\"projects/acme/contract.pdf\")\n        .body(\"bytes\")\n        .metadata(\"metadata\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.putObject({ bucketName: 'documents', objectKey: 'projects/acme/contract.pdf', body: 'bytes', metadata: 'metadata' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.put_object(bucket_name='documents', object_key='projects/acme/contract.pdf', body='bytes', metadata='metadata')\nprint(response)"
}
```

## Read an object

**Operation:** `ObjectService.GetObject`

Reads object metadata and body bytes for a selected version.

```anvil-tabs
{
  "operation": "GetObject",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetObjectRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_object(GetObjectRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetObjectRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getObject(\n    GetObjectRequest.builder()\n        .bucketName(\"documents\")\n        .objectKey(\"projects/acme/contract.pdf\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getObject({ bucketName: 'documents', objectKey: 'projects/acme/contract.pdf' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_object(bucket_name='documents', object_key='projects/acme/contract.pdf')\nprint(response)"
}
```

## Read object metadata

**Operation:** `ObjectService.HeadObject`

Reads object metadata without streaming the body.

```anvil-tabs
{
  "operation": "HeadObject",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::HeadObjectRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.head_object(HeadObjectRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.HeadObjectRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.headObject(\n    HeadObjectRequest.builder()\n        .bucketName(\"documents\")\n        .objectKey(\"projects/acme/contract.pdf\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.headObject({ bucketName: 'documents', objectKey: 'projects/acme/contract.pdf' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.head_object(bucket_name='documents', object_key='projects/acme/contract.pdf')\nprint(response)"
}
```

## List objects

**Operation:** `ObjectService.ListObjects`

Lists objects by prefix and authorisation scope.

```anvil-tabs
{
  "operation": "ListObjects",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListObjectsRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_objects(ListObjectsRequest { bucket_name: \"documents\".into(), prefix: \"projects/acme/\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListObjectsRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listObjects(\n    ListObjectsRequest.builder()\n        .bucketName(\"documents\")\n        .prefix(\"projects/acme/\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listObjects({ bucketName: 'documents', prefix: 'projects/acme/' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_objects(bucket_name='documents', prefix='projects/acme/')\nprint(response)"
}
```

## List object versions

**Operation:** `ObjectService.ListObjectVersions`

Lists previous object states for audit, recovery, or explicit version reads.

```anvil-tabs
{
  "operation": "ListObjectVersions",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListObjectVersionsRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_object_versions(ListObjectVersionsRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListObjectVersionsRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listObjectVersions(\n    ListObjectVersionsRequest.builder()\n        .bucketName(\"documents\")\n        .objectKey(\"projects/acme/contract.pdf\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listObjectVersions({ bucketName: 'documents', objectKey: 'projects/acme/contract.pdf' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_object_versions(bucket_name='documents', object_key='projects/acme/contract.pdf')\nprint(response)"
}
```

## Copy an object

**Operation:** `ObjectService.CopyObject`

Copies one object version to another key without the client re-uploading bytes.

```anvil-tabs
{
  "operation": "CopyObject",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CopyObjectRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.copy_object(CopyObjectRequest { source_bucket: \"documents\".into(), source_key: \"a.txt\".into(), target_bucket: \"documents\".into(), target_key: \"b.txt\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CopyObjectRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.copyObject(\n    CopyObjectRequest.builder()\n        .sourceBucket(\"documents\")\n        .sourceKey(\"a.txt\")\n        .targetBucket(\"documents\")\n        .targetKey(\"b.txt\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.copyObject({ sourceBucket: 'documents', sourceKey: 'a.txt', targetBucket: 'documents', targetKey: 'b.txt' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.copy_object(source_bucket='documents', source_key='a.txt', target_bucket='documents', target_key='b.txt')\nprint(response)"
}
```

## Compose an object

**Operation:** `ObjectService.ComposeObject`

Creates one object from ordered source object ranges or parts.

```anvil-tabs
{
  "operation": "ComposeObject",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ComposeObjectRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.compose_object(ComposeObjectRequest { bucket_name: \"documents\".into(), target_key: \"combined.bin\".into(), sources: \"sources\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ComposeObjectRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.composeObject(\n    ComposeObjectRequest.builder()\n        .bucketName(\"documents\")\n        .targetKey(\"combined.bin\")\n        .sources(\"sources\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.composeObject({ bucketName: 'documents', targetKey: 'combined.bin', sources: 'sources' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.compose_object(bucket_name='documents', target_key='combined.bin', sources='sources')\nprint(response)"
}
```

## Patch a JSON object

**Operation:** `ObjectService.PatchJsonObject`

Applies a structured JSON patch with preconditions.

```anvil-tabs
{
  "operation": "PatchJsonObject",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::PatchJsonObjectRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.patch_json_object(PatchJsonObjectRequest { bucket_name: \"documents\".into(), object_key: \"record.json\".into(), patch: \"patch\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.PatchJsonObjectRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.patchJsonObject(\n    PatchJsonObjectRequest.builder()\n        .bucketName(\"documents\")\n        .objectKey(\"record.json\")\n        .patch(\"patch\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.patchJsonObject({ bucketName: 'documents', objectKey: 'record.json', patch: 'patch' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.patch_json_object(bucket_name='documents', object_key='record.json', patch='patch')\nprint(response)"
}
```

## Compare and swap a manifest

**Operation:** `ObjectService.CompareAndSwapManifest`

Updates a manifest only when its current version matches the expected value.

```anvil-tabs
{
  "operation": "CompareAndSwapManifest",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CompareAndSwapManifestRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.compare_and_swap_manifest(CompareAndSwapManifestRequest { bucket_name: \"documents\".into(), object_key: \"manifest.json\".into(), expected_version: \"v7\".into(), manifest: \"manifest\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CompareAndSwapManifestRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.compareAndSwapManifest(\n    CompareAndSwapManifestRequest.builder()\n        .bucketName(\"documents\")\n        .objectKey(\"manifest.json\")\n        .expectedVersion(\"v7\")\n        .manifest(\"manifest\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.compareAndSwapManifest({ bucketName: 'documents', objectKey: 'manifest.json', expectedVersion: 'v7', manifest: 'manifest' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.compare_and_swap_manifest(bucket_name='documents', object_key='manifest.json', expected_version='v7', manifest='manifest')\nprint(response)"
}
```

## Watch a prefix

**Operation:** `ObjectService.WatchPrefix`

Streams object mutations below a bucket/key prefix.

```anvil-tabs
{
  "operation": "WatchPrefix",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchPrefixRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_prefix(WatchPrefixRequest { bucket_name: \"documents\".into(), prefix: \"projects/acme/\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchPrefixRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchPrefix(\n    WatchPrefixRequest.builder()\n        .bucketName(\"documents\")\n        .prefix(\"projects/acme/\")\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchPrefix({ bucketName: 'documents', prefix: 'projects/acme/', afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_prefix(bucket_name='documents', prefix='projects/acme/', after_cursor='lastCursor')\nprint(response)"
}
```

## Create an append stream

**Operation:** `ObjectService.CreateAppendStream`

Creates an ordered append stream for records that should become sealed segments.

```anvil-tabs
{
  "operation": "CreateAppendStream",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CreateAppendStreamRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.create_append_stream(CreateAppendStreamRequest { bucket_name: \"events\".into(), stream_name: \"audit\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CreateAppendStreamRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.createAppendStream(\n    CreateAppendStreamRequest.builder()\n        .bucketName(\"events\")\n        .streamName(\"audit\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.createAppendStream({ bucketName: 'events', streamName: 'audit' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.create_append_stream(bucket_name='events', stream_name='audit')\nprint(response)"
}
```

## Append a stream record

**Operation:** `ObjectService.AppendStreamRecord`

Appends one record to an active stream.

```anvil-tabs
{
  "operation": "AppendStreamRecord",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::AppendStreamRecordRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.append_stream_record(AppendStreamRecordRequest { bucket_name: \"events\".into(), stream_name: \"audit\".into(), record: \"record\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.AppendStreamRecordRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.appendStreamRecord(\n    AppendStreamRecordRequest.builder()\n        .bucketName(\"events\")\n        .streamName(\"audit\")\n        .record(\"record\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.appendStreamRecord({ bucketName: 'events', streamName: 'audit', record: 'record' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.append_stream_record(bucket_name='events', stream_name='audit', record='record')\nprint(response)"
}
```

## Seal an append stream segment

**Operation:** `ObjectService.SealAppendStreamSegment`

Closes the active segment and makes it durable for readers and derived systems.

```anvil-tabs
{
  "operation": "SealAppendStreamSegment",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::SealAppendStreamSegmentRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.seal_append_stream_segment(SealAppendStreamSegmentRequest { bucket_name: \"events\".into(), stream_name: \"audit\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.SealAppendStreamSegmentRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.sealAppendStreamSegment(\n    SealAppendStreamSegmentRequest.builder()\n        .bucketName(\"events\")\n        .streamName(\"audit\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.sealAppendStreamSegment({ bucketName: 'events', streamName: 'audit' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.seal_append_stream_segment(bucket_name='events', stream_name='audit')\nprint(response)"
}
```

## Initiate multipart upload

**Operation:** `ObjectService.InitiateMultipartUpload`

Starts a large-object upload split into numbered parts.

```anvil-tabs
{
  "operation": "InitiateMultipartUpload",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::InitiateMultipartUploadRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.initiate_multipart_upload(InitiateMultipartUploadRequest { bucket_name: \"media\".into(), object_key: \"video.mov\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.InitiateMultipartUploadRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.initiateMultipartUpload(\n    InitiateMultipartUploadRequest.builder()\n        .bucketName(\"media\")\n        .objectKey(\"video.mov\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.initiateMultipartUpload({ bucketName: 'media', objectKey: 'video.mov' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.initiate_multipart_upload(bucket_name='media', object_key='video.mov')\nprint(response)"
}
```

## Upload multipart part

**Operation:** `ObjectService.UploadPart`

Uploads one numbered part of a multipart object.

```anvil-tabs
{
  "operation": "UploadPart",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::UploadPartRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.upload_part(UploadPartRequest { upload_id: \"uploadId\".into(), part_number: \"1\".into(), body: \"bytes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.UploadPartRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.uploadPart(\n    UploadPartRequest.builder()\n        .uploadId(\"uploadId\")\n        .partNumber(\"1\")\n        .body(\"bytes\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.uploadPart({ uploadId: 'uploadId', partNumber: '1', body: 'bytes' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.upload_part(upload_id='uploadId', part_number='1', body='bytes')\nprint(response)"
}
```

## Complete multipart upload

**Operation:** `ObjectService.CompleteMultipartUpload`

Assembles uploaded parts into the final object version.

```anvil-tabs
{
  "operation": "CompleteMultipartUpload",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CompleteMultipartUploadRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.complete_multipart_upload(CompleteMultipartUploadRequest { upload_id: \"uploadId\".into(), parts: \"parts\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CompleteMultipartUploadRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.completeMultipartUpload(\n    CompleteMultipartUploadRequest.builder()\n        .uploadId(\"uploadId\")\n        .parts(\"parts\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.completeMultipartUpload({ uploadId: 'uploadId', parts: 'parts' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.complete_multipart_upload(upload_id='uploadId', parts='parts')\nprint(response)"
}
```

## Abort multipart upload

**Operation:** `ObjectService.AbortMultipartUpload`

Cancels an incomplete multipart upload and releases temporary state.

```anvil-tabs
{
  "operation": "AbortMultipartUpload",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::AbortMultipartUploadRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.abort_multipart_upload(AbortMultipartUploadRequest { upload_id: \"uploadId\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.AbortMultipartUploadRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.abortMultipartUpload(\n    AbortMultipartUploadRequest.builder()\n        .uploadId(\"uploadId\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.abortMultipartUpload({ uploadId: 'uploadId' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.abort_multipart_upload(upload_id='uploadId')\nprint(response)"
}
```

## Delete an object

**Operation:** `ObjectService.DeleteObject`

Writes a delete marker or removes the current object state according to policy.

```anvil-tabs
{
  "operation": "DeleteObject",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::DeleteObjectRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.delete_object(DeleteObjectRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.DeleteObjectRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.deleteObject(\n    DeleteObjectRequest.builder()\n        .bucketName(\"documents\")\n        .objectKey(\"projects/acme/contract.pdf\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.deleteObject({ bucketName: 'documents', objectKey: 'projects/acme/contract.pdf' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.delete_object(bucket_name='documents', object_key='projects/acme/contract.pdf')\nprint(response)"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
