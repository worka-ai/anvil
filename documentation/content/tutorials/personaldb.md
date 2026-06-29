---
title: PersonalDB Witnessing
description: Create groups and projections, submit SQLite changesets, catch up replicas, and watch PersonalDB state.
---

# PersonalDB Witnessing

**What this page gives you:** a tutorial for every operation in this area, with Rust examples for each operation.

PersonalDB lets local-first applications keep SQLite as their fast local database while Anvil acts as the witness for shared truth. The witness validates changesets, records commit certificates, maintains snapshots, and exposes authorised projections. This tutorial walks through creating a group, defining projections, submitting changesets, catching up another device, and watching group or projection activity.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artefact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Create PersonalDB group

**Operation:** `PersonalDbService.CreatePersonalDbGroup`

Creates a local-first database coordination group.

```anvil-tabs
{
  "operation": "CreatePersonalDbGroup",
  "rust": "use anvil_storage_client::{AnvilClient, proto::CreatePersonalDbGroupRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.personaldb().create_personal_db_group(CreatePersonalDbGroupRequest { group_id: \"notes\".into(), schema_hash: \"schemaHash\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Read PersonalDB group

**Operation:** `PersonalDbService.GetPersonalDbGroup`

Reads group manifest and state.

```anvil-tabs
{
  "operation": "GetPersonalDbGroup",
  "rust": "use anvil_storage_client::{AnvilClient, proto::GetPersonalDbGroupRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.personaldb().get_personal_db_group(GetPersonalDbGroupRequest { group_id: \"notes\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Create PersonalDB projection

**Operation:** `PersonalDbService.CreatePersonalDbProjection`

Defines a server-side projection over witnessed SQLite changesets.

```anvil-tabs
{
  "operation": "CreatePersonalDbProjection",
  "rust": "use anvil_storage_client::{AnvilClient, proto::CreatePersonalDbProjectionRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.personaldb().create_personal_db_projection(CreatePersonalDbProjectionRequest { group_id: \"notes\".into(), projection_id: \"assigned_tasks\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Read PersonalDB projection

**Operation:** `PersonalDbService.GetPersonalDbProjection`

Reads projection definition and status.

```anvil-tabs
{
  "operation": "GetPersonalDbProjection",
  "rust": "use anvil_storage_client::{AnvilClient, proto::GetPersonalDbProjectionRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.personaldb().get_personal_db_projection(GetPersonalDbProjectionRequest { group_id: \"notes\".into(), projection_id: \"assigned_tasks\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Submit PersonalDB changeset

**Operation:** `PersonalDbService.SubmitPersonalDbChangeset`

Submits a SQLite changeset for validation, certification, and replication.

```anvil-tabs
{
  "operation": "SubmitPersonalDbChangeset",
  "rust": "use anvil_storage_client::{AnvilClient, proto::SubmitPersonalDbChangesetRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.personaldb().submit_personal_db_changeset(SubmitPersonalDbChangesetRequest { group_id: \"notes\".into(), changeset: \"sqliteChangeset\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Catch up PersonalDB replica

**Operation:** `PersonalDbService.CatchUpPersonalDb`

Returns missing commits or a snapshot instruction for a replica.

```anvil-tabs
{
  "operation": "CatchUpPersonalDb",
  "rust": "use anvil_storage_client::{AnvilClient, proto::CatchUpPersonalDbRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.personaldb().catch_up_personal_db(CatchUpPersonalDbRequest { group_id: \"notes\".into(), after_commit: \"lastCommit\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Watch PersonalDB group

**Operation:** `PersonalDbService.WatchPersonalDbGroup`

Streams group commits, snapshots, and state changes.

```anvil-tabs
{
  "operation": "WatchPersonalDbGroup",
  "rust": "use anvil_storage_client::{AnvilClient, proto::WatchPersonalDbGroupRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.personaldb().watch_personal_db_group(WatchPersonalDbGroupRequest { group_id: \"notes\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## Watch PersonalDB projection

**Operation:** `PersonalDbService.WatchPersonalDbProjection`

Streams projection build or update events.

```anvil-tabs
{
  "operation": "WatchPersonalDbProjection",
  "rust": "use anvil_storage_client::{AnvilClient, proto::WatchPersonalDbProjectionRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, token).await?;\nlet response = anvil.personaldb().watch_personal_db_projection(WatchPersonalDbProjectionRequest { group_id: \"notes\".into(), projection_id: \"assigned_tasks\".into(), after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behaviour.
