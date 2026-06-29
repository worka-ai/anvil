---
title: Indexes And Search
description: Create, update, query, disable, drop, watch, and diagnose Anvil indexes.
---

# Indexes And Search

**What this page gives you:** a tutorial for every operation in this area, with Rust, Java, Node.js, and Python tabs for each operation.

Indexes turn stored objects into fast product queries. Directory and metadata indexes answer structural questions. Full text indexes answer language questions. Vector indexes answer semantic similarity questions. Hybrid indexes combine signals. This tutorial shows the complete index lifecycle and the query path that keeps authorization attached to results.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artifact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Create an index

**Operation:** `IndexService.CreateIndex`

Creates a derived query structure for path, metadata, full text, vector, hybrid, source, or PersonalDB data.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "CreateIndex",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CreateIndexRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.create_index(CreateIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), kind: \"full_text\".into(), selector_json: \"selector\".into(), extractor_json: \"extractor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CreateIndexRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.createIndex(\n    CreateIndexRequest.builder()\n        .bucketName(\"documents\")\n        .name(\"documents_text\")\n        .kind(\"full_text\")\n        .selectorJson(\"selector\")\n        .extractorJson(\"extractor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.createIndex({ bucketName: 'documents', name: 'documents_text', kind: 'full_text', selectorJson: 'selector', extractorJson: 'extractor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.create_index(bucket_name='documents', name='documents_text', kind='full_text', selector_json='selector', extractor_json='extractor')\nprint(response)"
}
```

## Update an index

**Operation:** `IndexService.UpdateIndex`

Changes selector, extractor, authorization, or build policy while preserving the named index identity.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "UpdateIndex",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::UpdateIndexRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.update_index(UpdateIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), selector_json: \"selector\".into(), extractor_json: \"extractor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.UpdateIndexRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.updateIndex(\n    UpdateIndexRequest.builder()\n        .bucketName(\"documents\")\n        .name(\"documents_text\")\n        .selectorJson(\"selector\")\n        .extractorJson(\"extractor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.updateIndex({ bucketName: 'documents', name: 'documents_text', selectorJson: 'selector', extractorJson: 'extractor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.update_index(bucket_name='documents', name='documents_text', selector_json='selector', extractor_json='extractor')\nprint(response)"
}
```

## List indexes

**Operation:** `IndexService.ListIndexes`

Lists index definitions for a bucket.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "ListIndexes",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListIndexesRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_indexes(ListIndexesRequest { bucket_name: \"documents\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListIndexesRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listIndexes(\n    ListIndexesRequest.builder()\n        .bucketName(\"documents\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listIndexes({ bucketName: 'documents' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_indexes(bucket_name='documents')\nprint(response)"
}
```

## Query an index

**Operation:** `IndexService.QueryIndex`

Runs a path, metadata, full text, vector, or hybrid query and filters results through authorization.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "QueryIndex",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::QueryIndexRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.query_index(QueryIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), query: \"query\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.QueryIndexRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.queryIndex(\n    QueryIndexRequest.builder()\n        .bucketName(\"documents\")\n        .name(\"documents_text\")\n        .query(\"query\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.queryIndex({ bucketName: 'documents', name: 'documents_text', query: 'query' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.query_index(bucket_name='documents', name='documents_text', query='query')\nprint(response)"
}
```

## Watch index definitions

**Operation:** `IndexService.WatchIndexDefinition`

Streams index definition changes so clients and workers can react.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "WatchIndexDefinition",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchIndexDefinitionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_index_definition(WatchIndexDefinitionRequest { bucket_name: \"documents\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchIndexDefinitionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchIndexDefinition(\n    WatchIndexDefinitionRequest.builder()\n        .bucketName(\"documents\")\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchIndexDefinition({ bucketName: 'documents', afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_index_definition(bucket_name='documents', after_cursor='lastCursor')\nprint(response)"
}
```

## Watch index partitions

**Operation:** `IndexService.WatchIndexPartition`

Streams partition progress and generation changes for derived index work.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "WatchIndexPartition",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchIndexPartitionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_index_partition(WatchIndexPartitionRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), partition: \"0\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchIndexPartitionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchIndexPartition(\n    WatchIndexPartitionRequest.builder()\n        .bucketName(\"documents\")\n        .name(\"documents_text\")\n        .partition(\"0\")\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchIndexPartition({ bucketName: 'documents', name: 'documents_text', partition: '0', afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_index_partition(bucket_name='documents', name='documents_text', partition='0', after_cursor='lastCursor')\nprint(response)"
}
```

## List index diagnostics

**Operation:** `IndexService.ListIndexDiagnostics`

Returns index build and query findings that explain lag, failures, or repair state.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "ListIndexDiagnostics",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListIndexDiagnosticsRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_index_diagnostics(ListIndexDiagnosticsRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListIndexDiagnosticsRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listIndexDiagnostics(\n    ListIndexDiagnosticsRequest.builder()\n        .bucketName(\"documents\")\n        .name(\"documents_text\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listIndexDiagnostics({ bucketName: 'documents', name: 'documents_text' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_index_diagnostics(bucket_name='documents', name='documents_text')\nprint(response)"
}
```

## Disable an index

**Operation:** `IndexService.DisableIndex`

Stops an index from serving new queries without deleting its definition immediately.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "DisableIndex",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::DisableIndexRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.disable_index(DisableIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.DisableIndexRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.disableIndex(\n    DisableIndexRequest.builder()\n        .bucketName(\"documents\")\n        .name(\"documents_text\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.disableIndex({ bucketName: 'documents', name: 'documents_text' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.disable_index(bucket_name='documents', name='documents_text')\nprint(response)"
}
```

## Drop an index

**Operation:** `IndexService.DropIndex`

Removes the index definition and derived state when the query path is no longer used.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "DropIndex",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::DropIndexRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.drop_index(DropIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.DropIndexRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.dropIndex(\n    DropIndexRequest.builder()\n        .bucketName(\"documents\")\n        .name(\"documents_text\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.dropIndex({ bucketName: 'documents', name: 'documents_text' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.drop_index(bucket_name='documents', name='documents_text')\nprint(response)"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behavior.
