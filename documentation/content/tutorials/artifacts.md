---
title: Source, Model, And Ingestion Artefacts
description: Store source packs, query source trees, manage ingestion keys and jobs, and serve model tensor artefacts.
---

# Source, Model, And Ingestion Artefacts

**What this page gives you:** a tutorial for every operation in this area, with Rust examples for each operation.

Anvil can store more than end-user uploads. Source packs, build outputs, model manifests, tensor shards, and imported model repositories all benefit from the same object identity, metadata, authorisation, watches, and search model. This tutorial covers Git source artefacts, model manifests, tensor reads, ingestion keys, and ingestion jobs.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artefact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Upload a Git pack

**Operation:** `GitSourceService.PutGitPack`

Stores a source pack and indexes commit, tree, and blob records.

```anvil-tabs
{
  "operation": "PutGitPack",
  "rust": "use anvil_storage::{AnvilClient, proto::PutGitPackRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.git_sources().put_git_pack(PutGitPackRequest { repository_id: \"repo-1\".into(), bucket_name: \"source-artifacts\".into(), pack: \"packBytes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Find Git object locations

**Operation:** `GitSourceService.GetGitObject`

Finds where a Git object is stored inside source artefact packs.

```anvil-tabs
{
  "operation": "GetGitObject",
  "rust": "use anvil_storage::{AnvilClient, proto::GetGitObjectRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.git_sources().get_git_object(GetGitObjectRequest { repository_id: \"repo-1\".into(), object_id: \"abc123\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Find Git blob by path

**Operation:** `GitSourceService.GetGitBlobByPath`

Resolves a repository commit and path to the stored blob location.

```anvil-tabs
{
  "operation": "GetGitBlobByPath",
  "rust": "use anvil_storage::{AnvilClient, proto::GetGitBlobByPathRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.git_sources().get_git_blob_by_path(GetGitBlobByPathRequest { repository_id: \"repo-1\".into(), commit_id: \"main\".into(), tree_path: \"src/lib.rs\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List Git tree

**Operation:** `GitSourceService.ListGitTree`

Lists source tree entries below a commit prefix.

```anvil-tabs
{
  "operation": "ListGitTree",
  "rust": "use anvil_storage::{AnvilClient, proto::ListGitTreeRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.git_sources().list_git_tree(ListGitTreeRequest { repository_id: \"repo-1\".into(), commit_id: \"main\".into(), prefix: \"src/\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Watch Git source artefacts

**Operation:** `GitSourceService.WatchGitSource`

Streams source artefact index events.

```anvil-tabs
{
  "operation": "WatchGitSource",
  "rust": "use anvil_storage::{AnvilClient, proto::WatchGitSourceRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.git_sources().watch_git_source(WatchGitSourceRequest { repository_id: \"repo-1\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Write model manifest

**Operation:** `ModelService.PutModelManifest`

Writes a structured model manifest that points at tensor artefacts.

```anvil-tabs
{
  "operation": "PutModelManifest",
  "rust": "use anvil_storage::{AnvilClient, proto::PutModelManifestRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.models().put_model_manifest(PutModelManifestRequest { model_id: \"model-1\".into(), manifest: \"manifest\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List tensors

**Operation:** `ModelService.ListTensors`

Lists tensors described by a model manifest.

```anvil-tabs
{
  "operation": "ListTensors",
  "rust": "use anvil_storage::{AnvilClient, proto::ListTensorsRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.models().list_tensors(ListTensorsRequest { model_id: \"model-1\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Read one tensor

**Operation:** `ModelService.GetTensor`

Streams one tensor payload.

```anvil-tabs
{
  "operation": "GetTensor",
  "rust": "use anvil_storage::{AnvilClient, proto::GetTensorRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.models().get_tensor(GetTensorRequest { model_id: \"model-1\".into(), tensor_name: \"encoder.weight\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Read multiple tensors

**Operation:** `ModelService.GetTensors`

Streams multiple tensor payloads.

```anvil-tabs
{
  "operation": "GetTensors",
  "rust": "use anvil_storage::{AnvilClient, proto::GetTensorsRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.models().get_tensors(GetTensorsRequest { model_id: \"model-1\".into(), tensor_names: \"[\\\"encoder.weight\\\",\\\"decoder.weight\\\"]\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Create ingestion key

**Operation:** `HuggingFaceKeyService.CreateKey`

Stores an external ingestion credential by name without returning the secret.

```anvil-tabs
{
  "operation": "CreateKey",
  "rust": "use anvil_storage::{AnvilClient, proto::CreateKeyRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.hugging_face_keys().create_key(CreateKeyRequest { name: \"hf-production\".into(), token: \"secret\".into(), note: \"release imports\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List ingestion keys

**Operation:** `HuggingFaceKeyService.ListKeys`

Lists stored ingestion credentials without exposing secret values.

```anvil-tabs
{
  "operation": "ListKeys",
  "rust": "use anvil_storage::{AnvilClient, proto::ListKeysRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.hugging_face_keys().list_keys(Default::default()).await?;\nprintln!(\"{response:?}\");"
}
```

## Delete ingestion key

**Operation:** `HuggingFaceKeyService.DeleteKey`

Deletes an external ingestion credential.

```anvil-tabs
{
  "operation": "DeleteKey",
  "rust": "use anvil_storage::{AnvilClient, proto::DeleteKeyRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.hugging_face_keys().delete_key(DeleteKeyRequest { name: \"hf-production\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Start ingestion

**Operation:** `HfIngestionService.StartIngestion`

Starts an import job into a target bucket and prefix.

```anvil-tabs
{
  "operation": "StartIngestion",
  "rust": "use anvil_storage::{AnvilClient, proto::StartIngestionRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.hf_ingestion().start_ingestion(StartIngestionRequest { key_name: \"hf-production\".into(), repo: \"org/model\".into(), target_bucket: \"models\".into(), target_prefix: \"imports/model/\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Read ingestion status

**Operation:** `HfIngestionService.GetIngestionStatus`

Reads import progress, completion, or failure state.

```anvil-tabs
{
  "operation": "GetIngestionStatus",
  "rust": "use anvil_storage::{AnvilClient, proto::GetIngestionStatusRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.hf_ingestion().get_ingestion_status(GetIngestionStatusRequest { ingestion_id: \"ingestionId\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Cancel ingestion

**Operation:** `HfIngestionService.CancelIngestion`

Requests cancellation of an active import job.

```anvil-tabs
{
  "operation": "CancelIngestion",
  "rust": "use anvil_storage::{AnvilClient, proto::CancelIngestionRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.hf_ingestion().cancel_ingestion(CancelIngestionRequest { ingestion_id: \"ingestionId\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
