---
title: Source, Model, And Ingestion Artefacts
description: Store source packs, query source trees, manage ingestion keys and jobs, and serve model tensor artefacts.
---

# Source, Model, And Ingestion Artefacts

**What this page gives you:** a tutorial for every operation in this area, with Rust, Java, Node.js, and Python tabs for each operation.

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
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::PutGitPackRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.put_git_pack(PutGitPackRequest { repository_id: \"repo-1\".into(), bucket_name: \"source-artifacts\".into(), pack: \"packBytes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.PutGitPackRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.putGitPack(\n    PutGitPackRequest.builder()\n        .repositoryId(\"repo-1\")\n        .bucketName(\"source-artifacts\")\n        .pack(\"packBytes\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.putGitPack({ repositoryId: 'repo-1', bucketName: 'source-artifacts', pack: 'packBytes' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.put_git_pack(repository_id='repo-1', bucket_name='source-artifacts', pack='packBytes')\nprint(response)"
}
```

## Find Git object locations

**Operation:** `GitSourceService.GetGitObject`

Finds where a Git object is stored inside source artefact packs.

```anvil-tabs
{
  "operation": "GetGitObject",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetGitObjectRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_git_object(GetGitObjectRequest { repository_id: \"repo-1\".into(), object_id: \"abc123\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetGitObjectRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getGitObject(\n    GetGitObjectRequest.builder()\n        .repositoryId(\"repo-1\")\n        .objectId(\"abc123\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getGitObject({ repositoryId: 'repo-1', objectId: 'abc123' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_git_object(repository_id='repo-1', object_id='abc123')\nprint(response)"
}
```

## Find Git blob by path

**Operation:** `GitSourceService.GetGitBlobByPath`

Resolves a repository commit and path to the stored blob location.

```anvil-tabs
{
  "operation": "GetGitBlobByPath",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetGitBlobByPathRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_git_blob_by_path(GetGitBlobByPathRequest { repository_id: \"repo-1\".into(), commit_id: \"main\".into(), tree_path: \"src/lib.rs\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetGitBlobByPathRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getGitBlobByPath(\n    GetGitBlobByPathRequest.builder()\n        .repositoryId(\"repo-1\")\n        .commitId(\"main\")\n        .treePath(\"src/lib.rs\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getGitBlobByPath({ repositoryId: 'repo-1', commitId: 'main', treePath: 'src/lib.rs' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_git_blob_by_path(repository_id='repo-1', commit_id='main', tree_path='src/lib.rs')\nprint(response)"
}
```

## List Git tree

**Operation:** `GitSourceService.ListGitTree`

Lists source tree entries below a commit prefix.

```anvil-tabs
{
  "operation": "ListGitTree",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListGitTreeRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_git_tree(ListGitTreeRequest { repository_id: \"repo-1\".into(), commit_id: \"main\".into(), prefix: \"src/\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListGitTreeRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listGitTree(\n    ListGitTreeRequest.builder()\n        .repositoryId(\"repo-1\")\n        .commitId(\"main\")\n        .prefix(\"src/\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listGitTree({ repositoryId: 'repo-1', commitId: 'main', prefix: 'src/' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_git_tree(repository_id='repo-1', commit_id='main', prefix='src/')\nprint(response)"
}
```

## Watch Git source artefacts

**Operation:** `GitSourceService.WatchGitSource`

Streams source artefact index events.

```anvil-tabs
{
  "operation": "WatchGitSource",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchGitSourceRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_git_source(WatchGitSourceRequest { repository_id: \"repo-1\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchGitSourceRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchGitSource(\n    WatchGitSourceRequest.builder()\n        .repositoryId(\"repo-1\")\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchGitSource({ repositoryId: 'repo-1', afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_git_source(repository_id='repo-1', after_cursor='lastCursor')\nprint(response)"
}
```

## Write model manifest

**Operation:** `ModelService.PutModelManifest`

Writes a structured model manifest that points at tensor artefacts.

```anvil-tabs
{
  "operation": "PutModelManifest",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::PutModelManifestRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.put_model_manifest(PutModelManifestRequest { model_id: \"model-1\".into(), manifest: \"manifest\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.PutModelManifestRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.putModelManifest(\n    PutModelManifestRequest.builder()\n        .modelId(\"model-1\")\n        .manifest(\"manifest\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.putModelManifest({ modelId: 'model-1', manifest: 'manifest' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.put_model_manifest(model_id='model-1', manifest='manifest')\nprint(response)"
}
```

## List tensors

**Operation:** `ModelService.ListTensors`

Lists tensors described by a model manifest.

```anvil-tabs
{
  "operation": "ListTensors",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListTensorsRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_tensors(ListTensorsRequest { model_id: \"model-1\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListTensorsRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listTensors(\n    ListTensorsRequest.builder()\n        .modelId(\"model-1\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listTensors({ modelId: 'model-1' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_tensors(model_id='model-1')\nprint(response)"
}
```

## Read one tensor

**Operation:** `ModelService.GetTensor`

Streams one tensor payload.

```anvil-tabs
{
  "operation": "GetTensor",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetTensorRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_tensor(GetTensorRequest { model_id: \"model-1\".into(), tensor_name: \"encoder.weight\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetTensorRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getTensor(\n    GetTensorRequest.builder()\n        .modelId(\"model-1\")\n        .tensorName(\"encoder.weight\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getTensor({ modelId: 'model-1', tensorName: 'encoder.weight' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_tensor(model_id='model-1', tensor_name='encoder.weight')\nprint(response)"
}
```

## Read multiple tensors

**Operation:** `ModelService.GetTensors`

Streams multiple tensor payloads.

```anvil-tabs
{
  "operation": "GetTensors",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetTensorsRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_tensors(GetTensorsRequest { model_id: \"model-1\".into(), tensor_names: \"[\\\"encoder.weight\\\",\\\"decoder.weight\\\"]\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetTensorsRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getTensors(\n    GetTensorsRequest.builder()\n        .modelId(\"model-1\")\n        .tensorNames(\"[\\\"encoder.weight\\\",\\\"decoder.weight\\\"]\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getTensors({ modelId: 'model-1', tensorNames: '[\"encoder.weight\",\"decoder.weight\"]' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_tensors(model_id='model-1', tensor_names='[\"encoder.weight\",\"decoder.weight\"]')\nprint(response)"
}
```

## Create ingestion key

**Operation:** `HuggingFaceKeyService.CreateKey`

Stores an external ingestion credential by name without returning the secret.

```anvil-tabs
{
  "operation": "CreateKey",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CreateKeyRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.create_key(CreateKeyRequest { name: \"hf-production\".into(), token: \"secret\".into(), note: \"release imports\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CreateKeyRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.createKey(\n    CreateKeyRequest.builder()\n        .name(\"hf-production\")\n        .token(\"secret\")\n        .note(\"release imports\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.createKey({ name: 'hf-production', token: 'secret', note: 'release imports' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.create_key(name='hf-production', token='secret', note='release imports')\nprint(response)"
}
```

## List ingestion keys

**Operation:** `HuggingFaceKeyService.ListKeys`

Lists stored ingestion credentials without exposing secret values.

```anvil-tabs
{
  "operation": "ListKeys",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListKeysRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_keys(Default::default()).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListKeysRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listKeys(\n    new ListKeysRequest()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listKeys({});\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_keys()\nprint(response)"
}
```

## Delete ingestion key

**Operation:** `HuggingFaceKeyService.DeleteKey`

Deletes an external ingestion credential.

```anvil-tabs
{
  "operation": "DeleteKey",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::DeleteKeyRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.delete_key(DeleteKeyRequest { name: \"hf-production\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.DeleteKeyRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.deleteKey(\n    DeleteKeyRequest.builder()\n        .name(\"hf-production\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.deleteKey({ name: 'hf-production' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.delete_key(name='hf-production')\nprint(response)"
}
```

## Start ingestion

**Operation:** `HfIngestionService.StartIngestion`

Starts an import job into a target bucket and prefix.

```anvil-tabs
{
  "operation": "StartIngestion",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::StartIngestionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.start_ingestion(StartIngestionRequest { key_name: \"hf-production\".into(), repo: \"org/model\".into(), target_bucket: \"models\".into(), target_prefix: \"imports/model/\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.StartIngestionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.startIngestion(\n    StartIngestionRequest.builder()\n        .keyName(\"hf-production\")\n        .repo(\"org/model\")\n        .targetBucket(\"models\")\n        .targetPrefix(\"imports/model/\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.startIngestion({ keyName: 'hf-production', repo: 'org/model', targetBucket: 'models', targetPrefix: 'imports/model/' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.start_ingestion(key_name='hf-production', repo='org/model', target_bucket='models', target_prefix='imports/model/')\nprint(response)"
}
```

## Read ingestion status

**Operation:** `HfIngestionService.GetIngestionStatus`

Reads import progress, completion, or failure state.

```anvil-tabs
{
  "operation": "GetIngestionStatus",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetIngestionStatusRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_ingestion_status(GetIngestionStatusRequest { ingestion_id: \"ingestionId\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetIngestionStatusRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getIngestionStatus(\n    GetIngestionStatusRequest.builder()\n        .ingestionId(\"ingestionId\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getIngestionStatus({ ingestionId: 'ingestionId' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_ingestion_status(ingestion_id='ingestionId')\nprint(response)"
}
```

## Cancel ingestion

**Operation:** `HfIngestionService.CancelIngestion`

Requests cancellation of an active import job.

```anvil-tabs
{
  "operation": "CancelIngestion",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CancelIngestionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.cancel_ingestion(CancelIngestionRequest { ingestion_id: \"ingestionId\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CancelIngestionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.cancelIngestion(\n    CancelIngestionRequest.builder()\n        .ingestionId(\"ingestionId\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.cancelIngestion({ ingestionId: 'ingestionId' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.cancel_ingestion(ingestion_id='ingestionId')\nprint(response)"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
