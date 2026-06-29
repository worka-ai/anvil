---
title: Repair And Operator Operations
description: Repair indexes, directory indexes, authorisation-derived indexes, PersonalDB log chains, and inspect findings.
---

# Repair And Operator Operations

**What this page gives you:** a tutorial for every operation in this area, with Rust examples for each operation.

Operators need safe ways to prove derived state is correct and to repair it when checks fail. Repair operations do not invent source truth. They validate durable state, rebuild derived structures from source records, and write findings that can be reviewed. This tutorial covers the public repair APIs and explains the internal shard APIs that are reserved for Anvil nodes.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artefact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Repair an index

**Operation:** `RepairService.RepairIndex`

Rebuilds or validates index derived state from source object facts.

```anvil-tabs
{
  "operation": "RepairIndex",
  "rust": "use anvil_storage_client::{AnvilClient, proto::RepairIndexRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.repair().repair_index(RepairIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Repair a directory index

**Operation:** `RepairService.RepairDirectoryIndex`

Rebuilds or validates path listing structures.

```anvil-tabs
{
  "operation": "RepairDirectoryIndex",
  "rust": "use anvil_storage_client::{AnvilClient, proto::RepairDirectoryIndexRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.repair().repair_directory_index(RepairDirectoryIndexRequest { bucket_name: \"documents\".into(), prefix: \"projects/acme/\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Repair authorisation-derived indexes

**Operation:** `RepairService.RepairAuthzDerivedIndex`

Rebuilds authorisation-derived views from source tuple and namespace facts.

```anvil-tabs
{
  "operation": "RepairAuthzDerivedIndex",
  "rust": "use anvil_storage_client::{AnvilClient, proto::RepairAuthzDerivedIndexRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.repair().repair_authz_derived_index(RepairAuthzDerivedIndexRequest { namespace: \"document\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Repair a PersonalDB log chain

**Operation:** `RepairService.RepairPersonalDbLogChain`

Validates PersonalDB commit chains and reports required recovery work.

```anvil-tabs
{
  "operation": "RepairPersonalDbLogChain",
  "rust": "use anvil_storage_client::{AnvilClient, proto::RepairPersonalDbLogChainRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.repair().repair_personal_db_log_chain(RepairPersonalDbLogChainRequest { group_id: \"notes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## List repair findings

**Operation:** `RepairService.ListRepairFindings`

Reads repair diagnostics produced by repair jobs.

```anvil-tabs
{
  "operation": "ListRepairFindings",
  "rust": "use anvil_storage_client::{AnvilClient, proto::ListRepairFindingsRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.repair().list_repair_findings(ListRepairFindingsRequest { scope: \"documents\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Internal put shard

**Operation:** `InternalAnvilService.PutShard`

Node-to-node storage write used by Anvil internals, not public application code.

> This is an internal node-to-node operation. Application clients do not call it directly; it is documented here so operators understand the complete Anvil surface.


```anvil-tabs
{
  "operation": "PutShard",
  "rust": "use anvil_storage_client::{AnvilClient, proto::PutShardRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.internal().put_shard(PutShardRequest { shard_id: \"shard\".into(), body: \"bytes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Internal get shard

**Operation:** `InternalAnvilService.GetShard`

Node-to-node storage read used by Anvil internals, not public application code.

> This is an internal node-to-node operation. Application clients do not call it directly; it is documented here so operators understand the complete Anvil surface.


```anvil-tabs
{
  "operation": "GetShard",
  "rust": "use anvil_storage_client::{AnvilClient, proto::GetShardRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.internal().get_shard(GetShardRequest { shard_id: \"shard\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Internal commit shard

**Operation:** `InternalAnvilService.CommitShard`

Node-to-node shard commit used by Anvil internals, not public application code.

> This is an internal node-to-node operation. Application clients do not call it directly; it is documented here so operators understand the complete Anvil surface.


```anvil-tabs
{
  "operation": "CommitShard",
  "rust": "use anvil_storage_client::{AnvilClient, proto::CommitShardRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.internal().commit_shard(CommitShardRequest { shard_id: \"shard\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Internal delete shard

**Operation:** `InternalAnvilService.DeleteShard`

Node-to-node shard deletion used by Anvil internals, not public application code.

> This is an internal node-to-node operation. Application clients do not call it directly; it is documented here so operators understand the complete Anvil surface.


```anvil-tabs
{
  "operation": "DeleteShard",
  "rust": "use anvil_storage_client::{AnvilClient, proto::DeleteShardRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.internal().delete_shard(DeleteShardRequest { shard_id: \"shard\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
