---
title: PersonalDB Witnessing
description: Create groups and projections, submit SQLite changesets, catch up replicas, and watch PersonalDB state.
---

# PersonalDB Witnessing

**What this page gives you:** a tutorial for every operation in this area, with Rust, Java, Node.js, and Python tabs for each operation.

PersonalDB lets local-first applications keep SQLite as their fast local database while Anvil acts as the witness for shared truth. The witness validates changesets, records commit certificates, maintains snapshots, and exposes authorized projections. This tutorial walks through creating a group, defining projections, submitting changesets, catching up another device, and watching group or projection activity.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artifact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Create PersonalDB group

**Operation:** `PersonalDbService.CreatePersonalDbGroup`

Creates a local-first database coordination group.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "CreatePersonalDbGroup",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CreatePersonalDbGroupRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.create_personal_db_group(CreatePersonalDbGroupRequest { group_id: \"notes\".into(), schema_hash: \"schemaHash\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CreatePersonalDbGroupRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.createPersonalDbGroup(\n    CreatePersonalDbGroupRequest.builder()\n        .groupId(\"notes\")\n        .schemaHash(\"schemaHash\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.createPersonalDbGroup({ groupId: 'notes', schemaHash: 'schemaHash' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.create_personal_db_group(group_id='notes', schema_hash='schemaHash')\nprint(response)"
}
```

## Read PersonalDB group

**Operation:** `PersonalDbService.GetPersonalDbGroup`

Reads group manifest and state.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "GetPersonalDbGroup",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetPersonalDbGroupRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_personal_db_group(GetPersonalDbGroupRequest { group_id: \"notes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetPersonalDbGroupRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getPersonalDbGroup(\n    GetPersonalDbGroupRequest.builder()\n        .groupId(\"notes\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getPersonalDbGroup({ groupId: 'notes' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_personal_db_group(group_id='notes')\nprint(response)"
}
```

## Create PersonalDB projection

**Operation:** `PersonalDbService.CreatePersonalDbProjection`

Defines a server-side projection over witnessed SQLite changesets.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "CreatePersonalDbProjection",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CreatePersonalDbProjectionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.create_personal_db_projection(CreatePersonalDbProjectionRequest { group_id: \"notes\".into(), projection_id: \"assigned_tasks\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CreatePersonalDbProjectionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.createPersonalDbProjection(\n    CreatePersonalDbProjectionRequest.builder()\n        .groupId(\"notes\")\n        .projectionId(\"assigned_tasks\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.createPersonalDbProjection({ groupId: 'notes', projectionId: 'assigned_tasks' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.create_personal_db_projection(group_id='notes', projection_id='assigned_tasks')\nprint(response)"
}
```

## Read PersonalDB projection

**Operation:** `PersonalDbService.GetPersonalDbProjection`

Reads projection definition and status.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "GetPersonalDbProjection",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetPersonalDbProjectionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_personal_db_projection(GetPersonalDbProjectionRequest { group_id: \"notes\".into(), projection_id: \"assigned_tasks\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetPersonalDbProjectionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getPersonalDbProjection(\n    GetPersonalDbProjectionRequest.builder()\n        .groupId(\"notes\")\n        .projectionId(\"assigned_tasks\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getPersonalDbProjection({ groupId: 'notes', projectionId: 'assigned_tasks' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_personal_db_projection(group_id='notes', projection_id='assigned_tasks')\nprint(response)"
}
```

## Submit PersonalDB changeset

**Operation:** `PersonalDbService.SubmitPersonalDbChangeset`

Submits a SQLite changeset for validation, certification, and replication.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "SubmitPersonalDbChangeset",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::SubmitPersonalDbChangesetRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.submit_personal_db_changeset(SubmitPersonalDbChangesetRequest { group_id: \"notes\".into(), changeset: \"sqliteChangeset\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.SubmitPersonalDbChangesetRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.submitPersonalDbChangeset(\n    SubmitPersonalDbChangesetRequest.builder()\n        .groupId(\"notes\")\n        .changeset(\"sqliteChangeset\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.submitPersonalDbChangeset({ groupId: 'notes', changeset: 'sqliteChangeset' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.submit_personal_db_changeset(group_id='notes', changeset='sqliteChangeset')\nprint(response)"
}
```

## Catch up PersonalDB replica

**Operation:** `PersonalDbService.CatchUpPersonalDb`

Returns missing commits or a snapshot instruction for a replica.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "CatchUpPersonalDb",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CatchUpPersonalDbRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.catch_up_personal_db(CatchUpPersonalDbRequest { group_id: \"notes\".into(), after_commit: \"lastCommit\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CatchUpPersonalDbRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.catchUpPersonalDb(\n    CatchUpPersonalDbRequest.builder()\n        .groupId(\"notes\")\n        .afterCommit(\"lastCommit\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.catchUpPersonalDb({ groupId: 'notes', afterCommit: 'lastCommit' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.catch_up_personal_db(group_id='notes', after_commit='lastCommit')\nprint(response)"
}
```

## Watch PersonalDB group

**Operation:** `PersonalDbService.WatchPersonalDbGroup`

Streams group commits, snapshots, and state changes.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "WatchPersonalDbGroup",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchPersonalDbGroupRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_personal_db_group(WatchPersonalDbGroupRequest { group_id: \"notes\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchPersonalDbGroupRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchPersonalDbGroup(\n    WatchPersonalDbGroupRequest.builder()\n        .groupId(\"notes\")\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchPersonalDbGroup({ groupId: 'notes', afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_personal_db_group(group_id='notes', after_cursor='lastCursor')\nprint(response)"
}
```

## Watch PersonalDB projection

**Operation:** `PersonalDbService.WatchPersonalDbProjection`

Streams projection build or update events.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "WatchPersonalDbProjection",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchPersonalDbProjectionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_personal_db_projection(WatchPersonalDbProjectionRequest { group_id: \"notes\".into(), projection_id: \"assigned_tasks\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchPersonalDbProjectionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchPersonalDbProjection(\n    WatchPersonalDbProjectionRequest.builder()\n        .groupId(\"notes\")\n        .projectionId(\"assigned_tasks\")\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchPersonalDbProjection({ groupId: 'notes', projectionId: 'assigned_tasks', afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_personal_db_projection(group_id='notes', projection_id='assigned_tasks', after_cursor='lastCursor')\nprint(response)"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behavior.
