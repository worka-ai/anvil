---
title: Repair And Operator Operations
description: Repair indexes, directory indexes, authorisation-derived indexes, PersonalDB log chains, and inspect findings.
---

# Repair And Operator Operations

**What this page gives you:** a tutorial for every operation in this area, with Rust, Java, Node.js, and Python tabs for each operation.

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
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::RepairIndexRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.repair_index(RepairIndexRequest { bucket_name: \"documents\".into(), name: \"documents_text\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.RepairIndexRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.repairIndex(\n    RepairIndexRequest.builder()\n        .bucketName(\"documents\")\n        .name(\"documents_text\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.repairIndex({ bucketName: 'documents', name: 'documents_text' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.repair_index(bucket_name='documents', name='documents_text')\nprint(response)"
}
```

## Repair a directory index

**Operation:** `RepairService.RepairDirectoryIndex`

Rebuilds or validates path listing structures.

```anvil-tabs
{
  "operation": "RepairDirectoryIndex",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::RepairDirectoryIndexRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.repair_directory_index(RepairDirectoryIndexRequest { bucket_name: \"documents\".into(), prefix: \"projects/acme/\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.RepairDirectoryIndexRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.repairDirectoryIndex(\n    RepairDirectoryIndexRequest.builder()\n        .bucketName(\"documents\")\n        .prefix(\"projects/acme/\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.repairDirectoryIndex({ bucketName: 'documents', prefix: 'projects/acme/' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.repair_directory_index(bucket_name='documents', prefix='projects/acme/')\nprint(response)"
}
```

## Repair authorisation-derived indexes

**Operation:** `RepairService.RepairAuthzDerivedIndex`

Rebuilds authorisation-derived views from source tuple and namespace facts.

```anvil-tabs
{
  "operation": "RepairAuthzDerivedIndex",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::RepairAuthzDerivedIndexRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.repair_authz_derived_index(RepairAuthzDerivedIndexRequest { namespace: \"document\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.RepairAuthzDerivedIndexRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.repairAuthzDerivedIndex(\n    RepairAuthzDerivedIndexRequest.builder()\n        .namespace(\"document\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.repairAuthzDerivedIndex({ namespace: 'document' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.repair_authz_derived_index(namespace='document')\nprint(response)"
}
```

## Repair a PersonalDB log chain

**Operation:** `RepairService.RepairPersonalDbLogChain`

Validates PersonalDB commit chains and reports required recovery work.

```anvil-tabs
{
  "operation": "RepairPersonalDbLogChain",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::RepairPersonalDbLogChainRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.repair_personal_db_log_chain(RepairPersonalDbLogChainRequest { group_id: \"notes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.RepairPersonalDbLogChainRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.repairPersonalDbLogChain(\n    RepairPersonalDbLogChainRequest.builder()\n        .groupId(\"notes\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.repairPersonalDbLogChain({ groupId: 'notes' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.repair_personal_db_log_chain(group_id='notes')\nprint(response)"
}
```

## List repair findings

**Operation:** `RepairService.ListRepairFindings`

Reads repair diagnostics produced by repair jobs.

```anvil-tabs
{
  "operation": "ListRepairFindings",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::ListRepairFindingsRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.list_repair_findings(ListRepairFindingsRequest { scope: \"documents\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.ListRepairFindingsRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.listRepairFindings(\n    ListRepairFindingsRequest.builder()\n        .scope(\"documents\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.listRepairFindings({ scope: 'documents' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.list_repair_findings(scope='documents')\nprint(response)"
}
```

## Internal put shard

**Operation:** `InternalAnvilService.PutShard`

Node-to-node storage write used by Anvil internals, not public application code.

> This is an internal node-to-node operation. Application clients do not call it directly; it is documented here so operators understand the complete Anvil surface.


```anvil-tabs
{
  "operation": "PutShard",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::PutShardRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.put_shard(PutShardRequest { shard_id: \"shard\".into(), body: \"bytes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.PutShardRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.putShard(\n    PutShardRequest.builder()\n        .shardId(\"shard\")\n        .body(\"bytes\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.putShard({ shardId: 'shard', body: 'bytes' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.put_shard(shard_id='shard', body='bytes')\nprint(response)"
}
```

## Internal get shard

**Operation:** `InternalAnvilService.GetShard`

Node-to-node storage read used by Anvil internals, not public application code.

> This is an internal node-to-node operation. Application clients do not call it directly; it is documented here so operators understand the complete Anvil surface.


```anvil-tabs
{
  "operation": "GetShard",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetShardRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_shard(GetShardRequest { shard_id: \"shard\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetShardRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getShard(\n    GetShardRequest.builder()\n        .shardId(\"shard\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getShard({ shardId: 'shard' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_shard(shard_id='shard')\nprint(response)"
}
```

## Internal commit shard

**Operation:** `InternalAnvilService.CommitShard`

Node-to-node shard commit used by Anvil internals, not public application code.

> This is an internal node-to-node operation. Application clients do not call it directly; it is documented here so operators understand the complete Anvil surface.


```anvil-tabs
{
  "operation": "CommitShard",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CommitShardRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.commit_shard(CommitShardRequest { shard_id: \"shard\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CommitShardRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.commitShard(\n    CommitShardRequest.builder()\n        .shardId(\"shard\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.commitShard({ shardId: 'shard' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.commit_shard(shard_id='shard')\nprint(response)"
}
```

## Internal delete shard

**Operation:** `InternalAnvilService.DeleteShard`

Node-to-node shard deletion used by Anvil internals, not public application code.

> This is an internal node-to-node operation. Application clients do not call it directly; it is documented here so operators understand the complete Anvil surface.


```anvil-tabs
{
  "operation": "DeleteShard",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::DeleteShardRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.delete_shard(DeleteShardRequest { shard_id: \"shard\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.DeleteShardRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.deleteShard(\n    DeleteShardRequest.builder()\n        .shardId(\"shard\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.deleteShard({ shardId: 'shard' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.delete_shard(shard_id='shard')\nprint(response)"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
