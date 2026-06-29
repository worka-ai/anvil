---
title: Objects, Versions, Streams, And Multipart Uploads
description: Write, read, list, copy, compose, patch, stream, version, and upload large objects.
---

# Objects, Versions, Streams, And Multipart Uploads

**What this page gives you:** a tutorial for every operation in this area, with Rust examples for each operation.

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
  "rust": "use anvil_storage::{AnvilClient, proto::PutObjectRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().put_object(PutObjectRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), body: \"bytes\".into(), metadata: \"metadata\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Read an object

**Operation:** `ObjectService.GetObject`

Reads object metadata and body bytes for a selected version.

```anvil-tabs
{
  "operation": "GetObject",
  "rust": "use anvil_storage::{AnvilClient, proto::GetObjectRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().get_object(GetObjectRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Read object metadata

**Operation:** `ObjectService.HeadObject`

Reads object metadata without streaming the body.

```anvil-tabs
{
  "operation": "HeadObject",
  "rust": "use anvil_storage::{AnvilClient, proto::HeadObjectRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().head_object(HeadObjectRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List objects

**Operation:** `ObjectService.ListObjects`

Lists objects by prefix and authorisation scope.

```anvil-tabs
{
  "operation": "ListObjects",
  "rust": "use anvil_storage::{AnvilClient, proto::ListObjectsRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().list_objects(ListObjectsRequest { bucket_name: \"documents\".into(), prefix: \"projects/acme/\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List object versions

**Operation:** `ObjectService.ListObjectVersions`

Lists previous object states for audit, recovery, or explicit version reads.

```anvil-tabs
{
  "operation": "ListObjectVersions",
  "rust": "use anvil_storage::{AnvilClient, proto::ListObjectVersionsRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().list_object_versions(ListObjectVersionsRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Copy an object

**Operation:** `ObjectService.CopyObject`

Copies one object version to another key without the client re-uploading bytes.

```anvil-tabs
{
  "operation": "CopyObject",
  "rust": "use anvil_storage::{AnvilClient, proto::CopyObjectRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().copy_object(CopyObjectRequest { source_bucket: \"documents\".into(), source_key: \"a.txt\".into(), target_bucket: \"documents\".into(), target_key: \"b.txt\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Compose an object

**Operation:** `ObjectService.ComposeObject`

Creates one object from ordered source object ranges or parts.

```anvil-tabs
{
  "operation": "ComposeObject",
  "rust": "use anvil_storage::{AnvilClient, proto::ComposeObjectRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().compose_object(ComposeObjectRequest { bucket_name: \"documents\".into(), target_key: \"combined.bin\".into(), sources: \"sources\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Patch a JSON object

**Operation:** `ObjectService.PatchJsonObject`

Applies a structured JSON patch with preconditions.

```anvil-tabs
{
  "operation": "PatchJsonObject",
  "rust": "use anvil_storage::{AnvilClient, proto::PatchJsonObjectRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().patch_json_object(PatchJsonObjectRequest { bucket_name: \"documents\".into(), object_key: \"record.json\".into(), patch: \"patch\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Compare and swap a manifest

**Operation:** `ObjectService.CompareAndSwapManifest`

Updates a manifest only when its current version matches the expected value.

```anvil-tabs
{
  "operation": "CompareAndSwapManifest",
  "rust": "use anvil_storage::{AnvilClient, proto::CompareAndSwapManifestRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().compare_and_swap_manifest(CompareAndSwapManifestRequest { bucket_name: \"documents\".into(), object_key: \"manifest.json\".into(), expected_version: \"v7\".into(), manifest: \"manifest\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Watch a prefix

**Operation:** `ObjectService.WatchPrefix`

Streams object mutations below a bucket/key prefix.

```anvil-tabs
{
  "operation": "WatchPrefix",
  "rust": "use anvil_storage::{AnvilClient, proto::WatchPrefixRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().watch_prefix(WatchPrefixRequest { bucket_name: \"documents\".into(), prefix: \"projects/acme/\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Create an append stream

**Operation:** `ObjectService.CreateAppendStream`

Creates an ordered append stream for records that should become sealed segments.

```anvil-tabs
{
  "operation": "CreateAppendStream",
  "rust": "use anvil_storage::{AnvilClient, proto::CreateAppendStreamRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().create_append_stream(CreateAppendStreamRequest { bucket_name: \"events\".into(), stream_name: \"audit\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Append a stream record

**Operation:** `ObjectService.AppendStreamRecord`

Appends one record to an active stream.

```anvil-tabs
{
  "operation": "AppendStreamRecord",
  "rust": "use anvil_storage::{AnvilClient, proto::AppendStreamRecordRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().append_stream_record(AppendStreamRecordRequest { bucket_name: \"events\".into(), stream_name: \"audit\".into(), record: \"record\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Seal an append stream segment

**Operation:** `ObjectService.SealAppendStreamSegment`

Closes the active segment and makes it durable for readers and derived systems.

```anvil-tabs
{
  "operation": "SealAppendStreamSegment",
  "rust": "use anvil_storage::{AnvilClient, proto::SealAppendStreamSegmentRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().seal_append_stream_segment(SealAppendStreamSegmentRequest { bucket_name: \"events\".into(), stream_name: \"audit\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Initiate multipart upload

**Operation:** `ObjectService.InitiateMultipartUpload`

Starts a large-object upload split into numbered parts.

```anvil-tabs
{
  "operation": "InitiateMultipartUpload",
  "rust": "use anvil_storage::{AnvilClient, proto::InitiateMultipartUploadRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().initiate_multipart_upload(InitiateMultipartUploadRequest { bucket_name: \"media\".into(), object_key: \"video.mov\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Upload multipart part

**Operation:** `ObjectService.UploadPart`

Uploads one numbered part of a multipart object.

```anvil-tabs
{
  "operation": "UploadPart",
  "rust": "use anvil_storage::{AnvilClient, proto::UploadPartRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().upload_part(UploadPartRequest { upload_id: \"uploadId\".into(), part_number: \"1\".into(), body: \"bytes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Complete multipart upload

**Operation:** `ObjectService.CompleteMultipartUpload`

Assembles uploaded parts into the final object version.

```anvil-tabs
{
  "operation": "CompleteMultipartUpload",
  "rust": "use anvil_storage::{AnvilClient, proto::CompleteMultipartUploadRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().complete_multipart_upload(CompleteMultipartUploadRequest { upload_id: \"uploadId\".into(), parts: \"parts\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Abort multipart upload

**Operation:** `ObjectService.AbortMultipartUpload`

Cancels an incomplete multipart upload and releases temporary state.

```anvil-tabs
{
  "operation": "AbortMultipartUpload",
  "rust": "use anvil_storage::{AnvilClient, proto::AbortMultipartUploadRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().abort_multipart_upload(AbortMultipartUploadRequest { upload_id: \"uploadId\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Delete an object

**Operation:** `ObjectService.DeleteObject`

Writes a delete marker or removes the current object state according to policy.

```anvil-tabs
{
  "operation": "DeleteObject",
  "rust": "use anvil_storage::{AnvilClient, proto::DeleteObjectRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.objects().delete_object(DeleteObjectRequest { bucket_name: \"documents\".into(), object_key: \"projects/acme/contract.pdf\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
