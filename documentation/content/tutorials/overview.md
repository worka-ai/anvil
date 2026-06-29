---
title: Tutorials
description: Operation-by-operation tutorials for Anvil in Rust, Java, Node.js, and Python.
---

# Tutorials

**What this page gives you:** a complete map of the task-oriented tutorials. The Learn section explains concepts. These pages show how to perform operations. Each operation includes tabs for Rust, Java, Node.js, and Python so a reader can translate the same Anvil model into the language they use.

An operation in Anvil is not just a remote procedure call. It is a storage action that passes through identity, authorization, validation, preconditions, durable state, indexes, watch cursors, and diagnostics. The tutorials repeat that model deliberately so the safe pattern becomes muscle memory.

## Before you start

You need four facts in every language:

1. an Anvil endpoint;
2. credentials or a bearer token;
3. a bucket, group, repository, model, or resource name appropriate to the operation;
4. a caller identity with the permission required by the operation.

The examples use production-shaped client calls. They are intentionally small, but they show where request context, keys, metadata, preconditions, and watch cursors belong.

## Tutorial map

| Area | Operations | Tutorial |
| --- | --- | --- |
| Buckets And Policies | `CreateBucket`, `ListBuckets`, `GetBucketPolicy`, `PutBucketPolicy`, `WatchBucketMetadata`, `DeleteBucket` | [Buckets And Policies](/tutorials/buckets/) |
| Objects, Versions, Streams, And Multipart Uploads | `PutObject`, `GetObject`, `HeadObject`, `ListObjects`, `ListObjectVersions`, `CopyObject`, `ComposeObject`, `PatchJsonObject`, `CompareAndSwapManifest`, `WatchPrefix`, `CreateAppendStream`, `AppendStreamRecord`, `SealAppendStreamSegment`, `InitiateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, `AbortMultipartUpload`, `DeleteObject` | [Objects, Versions, Streams, And Multipart Uploads](/tutorials/objects/) |
| Indexes And Search | `CreateIndex`, `UpdateIndex`, `ListIndexes`, `QueryIndex`, `WatchIndexDefinition`, `WatchIndexPartition`, `ListIndexDiagnostics`, `DisableIndex`, `DropIndex` | [Indexes And Search](/tutorials/search/) |
| Authentication And Relationship Authorization | `GetAccessToken`, `GrantAccess`, `RevokeAccess`, `SetPublicAccess`, `WriteAuthzTuple`, `CheckPermission`, `WatchAuthzTupleLog`, `WatchAuthzNamespace`, `WatchAuthzDerivedLag` | [Authentication And Relationship Authorization](/tutorials/authorization/) |
| PersonalDB Witnessing | `CreatePersonalDbGroup`, `GetPersonalDbGroup`, `CreatePersonalDbProjection`, `GetPersonalDbProjection`, `SubmitPersonalDbChangeset`, `CatchUpPersonalDb`, `WatchPersonalDbGroup`, `WatchPersonalDbProjection` | [PersonalDB Witnessing](/tutorials/personaldb/) |
| Source, Model, And Ingestion Artifacts | `PutGitPack`, `GetGitObject`, `GetGitBlobByPath`, `ListGitTree`, `WatchGitSource`, `PutModelManifest`, `ListTensors`, `GetTensor`, `GetTensors`, `CreateKey`, `ListKeys`, `DeleteKey`, `StartIngestion`, `GetIngestionStatus`, `CancelIngestion` | [Source, Model, And Ingestion Artifacts](/tutorials/artifacts/) |
| Repair And Operator Operations | `RepairIndex`, `RepairDirectoryIndex`, `RepairAuthzDerivedIndex`, `RepairPersonalDbLogChain`, `ListRepairFindings`, `PutShard`, `GetShard`, `CommitShard`, `DeleteShard` | [Repair And Operator Operations](/tutorials/operations/) |

## How to read the tabs

The four language tabs are equivalent. Do not treat one language as a different product surface. Rust, Java, Node.js, and Python clients all call the same Anvil API and should preserve the same invariants: explicit bucket and key names, idempotency for retries, preconditions for updates, authorization on every result, and watch cursors for derived work.

## What you can do after this page

Pick the tutorial that matches the operation you need. If a concept is unfamiliar, pause and read the corresponding Learn page first.
