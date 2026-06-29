---
title: Indexes And Search
description: Create, update, query, disable, drop, watch, and diagnose Anvil indexes.
---

# Indexes And Search

**What this page gives you:** a tutorial for every operation in this area, with Rust examples for each operation.

Indexes turn stored objects into fast product queries. Directory and metadata indexes answer structural questions. Full text indexes answer language questions. Vector indexes answer semantic similarity questions. Hybrid indexes combine signals. This tutorial shows the complete index lifecycle and the query path that keeps authorisation attached to results.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artefact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Create an index

**Operation:** `IndexService.CreateIndex`

Creates a derived query structure for path, metadata, full text, vector, hybrid, source, or PersonalDB data.

```anvil-tabs
{
  "operation": "CreateIndex",
  "rust": "use anvil_storage_client::{AnvilClient, proto::CreateIndexRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().create_index(CreateIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), kind: \"full_text\".into(), selector_json: \"selector\".into(), extractor_json: \"extractor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Update an index

**Operation:** `IndexService.UpdateIndex`

Changes selector, extractor, authorisation, or build policy while preserving the named index identity.

```anvil-tabs
{
  "operation": "UpdateIndex",
  "rust": "use anvil_storage_client::{AnvilClient, proto::UpdateIndexRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().update_index(UpdateIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), selector_json: \"selector\".into(), extractor_json: \"extractor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List indexes

**Operation:** `IndexService.ListIndexes`

Lists index definitions for a bucket.

```anvil-tabs
{
  "operation": "ListIndexes",
  "rust": "use anvil_storage_client::{AnvilClient, proto::ListIndexesRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().list_indexes(ListIndexesRequest { bucket_name: \"documents\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Query an index

**Operation:** `IndexService.QueryIndex`

Runs a path, metadata, full text, vector, or hybrid query and filters results through authorisation.

```anvil-tabs
{
  "operation": "QueryIndex",
  "rust": "use anvil_storage_client::{AnvilClient, proto::QueryIndexRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().query_index(QueryIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), query: \"query\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Watch index definitions

**Operation:** `IndexService.WatchIndexDefinition`

Streams index definition changes so clients and workers can react.

```anvil-tabs
{
  "operation": "WatchIndexDefinition",
  "rust": "use anvil_storage_client::{AnvilClient, proto::WatchIndexDefinitionRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().watch_index_definition(WatchIndexDefinitionRequest { bucket_name: \"documents\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Watch index partitions

**Operation:** `IndexService.WatchIndexPartition`

Streams partition progress and generation changes for derived index work.

```anvil-tabs
{
  "operation": "WatchIndexPartition",
  "rust": "use anvil_storage_client::{AnvilClient, proto::WatchIndexPartitionRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().watch_index_partition(WatchIndexPartitionRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), partition: \"0\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List index diagnostics

**Operation:** `IndexService.ListIndexDiagnostics`

Returns index build and query findings that explain lag, failures, or repair state.

```anvil-tabs
{
  "operation": "ListIndexDiagnostics",
  "rust": "use anvil_storage_client::{AnvilClient, proto::ListIndexDiagnosticsRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().list_index_diagnostics(ListIndexDiagnosticsRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Disable an index

**Operation:** `IndexService.DisableIndex`

Stops an index from serving new queries without deleting its definition immediately.

```anvil-tabs
{
  "operation": "DisableIndex",
  "rust": "use anvil_storage_client::{AnvilClient, proto::DisableIndexRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().disable_index(DisableIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Drop an index

**Operation:** `IndexService.DropIndex`

Removes the index definition and derived state when the query path is no longer used.

```anvil-tabs
{
  "operation": "DropIndex",
  "rust": "use anvil_storage_client::{AnvilClient, proto::DropIndexRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.indexes().drop_index(DropIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
