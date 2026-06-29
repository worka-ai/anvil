---
title: Authentication And Relationship Authorisation
description: Get tokens, grant and revoke scoped access, manage public access, write relationship tuples, check permissions, and watch authorisation state.
---

# Authentication And Relationship Authorisation

**What this page gives you:** operation-by-operation examples for the Anvil authorisation surface. It shows who is expected to call each operation, what authority is required, and how the request fields map to the authorisation model from the learning guide.

## Scenario

The examples use one tenant with two applications:

- `admin-api` already has bootstrap-granted policy authority for the tenant.
- `reader-api` is a tenant application that should read objects and check document relationships.

The tenant also has an external identity system. That system maps a signed-in person to the relationship subject `user:amy`. Anvil does not create Amy's login account here; Anvil stores and checks the relationship facts that mention `user:amy`.

The sequence is:

```text
bootstrap creates tenant and applications
  -> admin-api requests a token for its assigned scopes
  -> admin-api grants reader-api object and authorisation scopes
  -> admin-api writes document:contract-42#viewer@user:amy
  -> application code checks whether user:amy can view contract-42
  -> watch consumers observe tuple, namespace, or derived-index movement
```

## Before you call these APIs

A public API caller must already have a bearer token, except when it calls `GetAccessToken` with application credentials. Tenant creation, application creation, secret creation, initial policy grants, namespace schema registration, and caveat registration are bootstrap/admin concerns.

A tenant application can delegate only authority it already has. If `admin-api` does not have `policy:grant` on `bucket:documents/*`, it cannot grant that scope to `reader-api`.

## Get an access token

**Operation:** `AuthService.GetAccessToken`

**Who calls it:** a tenant application or service job that has a `client_id` and `client_secret` created by bootstrap/admin tooling.

**What it does:** verifies the application secret, intersects requested scopes with assigned policy, and returns a short-lived bearer token. Empty scopes or `*` request all scopes already assigned to the application; they do not create new authority.

```anvil-tabs
{
  "operation": "GetAccessToken",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetAccessTokenRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, bootstrap_token).await?;\nlet response = anvil.get_access_token(GetAccessTokenRequest {\n    client_id: \"admin-client-id\".into(),\n    client_secret: \"admin-client-secret\".into(),\n    scopes: vec![\n        \"policy:grant|bucket:documents/*\".into(),\n        \"authz:tuple_write|document/contract-42#viewer\".into(),\n    ],\n}).await?;\nlet admin_token = response.access_token;",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetAccessTokenRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, bootstrapToken);\nvar response = anvil.getAccessToken(\n    GetAccessTokenRequest.builder()\n        .clientId(\"admin-client-id\")\n        .clientSecret(\"admin-client-secret\")\n        .addScopes(\"policy:grant|bucket:documents/*\")\n        .addScopes(\"authz:tuple_write|document/contract-42#viewer\")\n        .build()\n);\nString adminToken = response.getAccessToken();",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: bootstrapToken });\nconst response = await anvil.getAccessToken({\n  clientId: \"admin-client-id\",\n  clientSecret: \"admin-client-secret\",\n  scopes: [\n    \"policy:grant|bucket:documents/*\",\n    \"authz:tuple_write|document/contract-42#viewer\",\n  ],\n});\nconst adminToken = response.accessToken;",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=bootstrap_token)\nresponse = anvil.get_access_token(\n    client_id=\"admin-client-id\",\n    client_secret=\"admin-client-secret\",\n    scopes=[\n        \"policy:grant|bucket:documents/*\",\n        \"authz:tuple_write|document/contract-42#viewer\",\n    ],\n)\nadmin_token = response.access_token"
}
```

## Grant scoped access to another application

**Operation:** `AuthService.GrantAccess`

**Who calls it:** an application whose token has `policy:grant` on the requested resource.

**What it does:** adds one action/resource policy to another tenant application. The grantee is named by application id/name as `grantee_app_id`; it is not a relationship subject such as `user:amy`.

```anvil-tabs
{
  "operation": "GrantAccess",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GrantAccessRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, admin_token).await?;\nanvil.grant_access(GrantAccessRequest {\n    grantee_app_id: \"reader-api\".into(),\n    resource: \"bucket:documents/*\".into(),\n    action: \"object:read\".into(),\n}).await?;",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GrantAccessRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, adminToken);\nanvil.grantAccess(\n    GrantAccessRequest.builder()\n        .granteeAppId(\"reader-api\")\n        .resource(\"bucket:documents/*\")\n        .action(\"object:read\")\n        .build()\n);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: adminToken });\nawait anvil.grantAccess({\n  granteeAppId: \"reader-api\",\n  resource: \"bucket:documents/*\",\n  action: \"object:read\",\n});",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=admin_token)\nanvil.grant_access(\n    grantee_app_id=\"reader-api\",\n    resource=\"bucket:documents/*\",\n    action=\"object:read\",\n)"
}
```

## Revoke scoped access

**Operation:** `AuthService.RevokeAccess`

**Who calls it:** an application whose token has `policy:revoke` on the requested resource.

**What it does:** removes one action/resource policy from another tenant application. Revocation affects future token issuance and future checks using the changed policy set.

```anvil-tabs
{
  "operation": "RevokeAccess",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::RevokeAccessRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, admin_token).await?;\nanvil.revoke_access(RevokeAccessRequest {\n    grantee_app_id: \"reader-api\".into(),\n    resource: \"bucket:documents/*\".into(),\n    action: \"object:read\".into(),\n}).await?;",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.RevokeAccessRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, adminToken);\nanvil.revokeAccess(\n    RevokeAccessRequest.builder()\n        .granteeAppId(\"reader-api\")\n        .resource(\"bucket:documents/*\")\n        .action(\"object:read\")\n        .build()\n);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: adminToken });\nawait anvil.revokeAccess({\n  granteeAppId: \"reader-api\",\n  resource: \"bucket:documents/*\",\n  action: \"object:read\",\n});",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=admin_token)\nanvil.revoke_access(\n    grantee_app_id=\"reader-api\",\n    resource=\"bucket:documents/*\",\n    action=\"object:read\",\n)"
}
```

## Set public bucket access

**Operation:** `AuthService.SetPublicAccess`

**Who calls it:** an application whose token has `policy:grant` on `bucket:<bucket>`.

**What it does:** toggles public read policy for a bucket. Public access is a policy decision; it should be reserved for buckets that are deliberately designed for public distribution.

```anvil-tabs
{
  "operation": "SetPublicAccess",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::SetPublicAccessRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, admin_token).await?;\nanvil.set_public_access(SetPublicAccessRequest {\n    bucket: \"public-assets\".into(),\n    allow_public_read: true,\n}).await?;",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.SetPublicAccessRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, adminToken);\nanvil.setPublicAccess(\n    SetPublicAccessRequest.builder()\n        .bucket(\"public-assets\")\n        .allowPublicRead(true)\n        .build()\n);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: adminToken });\nawait anvil.setPublicAccess({\n  bucket: \"public-assets\",\n  allowPublicRead: true,\n});",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=admin_token)\nanvil.set_public_access(bucket=\"public-assets\", allow_public_read=True)"
}
```

## Write a relationship tuple

**Operation:** `AuthService.WriteAuthzTuple`

**Who calls it:** an application whose token has `authz:tuple_write` for the tuple resource envelope.

**What it does:** adds or removes one source relationship fact. Tuple writes are security mutations and should include a reason.

```anvil-tabs
{
  "operation": "WriteAuthzTuple",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WriteAuthzTupleRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, admin_token).await?;\nlet response = anvil.write_authz_tuple(WriteAuthzTupleRequest {\n    namespace: \"document\".into(),\n    object_id: \"contract-42\".into(),\n    relation: \"viewer\".into(),\n    subject_kind: \"user\".into(),\n    subject_id: \"amy\".into(),\n    caveat_hash: String::new(),\n    operation: \"add\".into(),\n    reason: \"grant contract visibility\".into(),\n}).await?;\nlet zookie = response.zookie;",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WriteAuthzTupleRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, adminToken);\nvar response = anvil.writeAuthzTuple(\n    WriteAuthzTupleRequest.builder()\n        .namespace(\"document\")\n        .objectId(\"contract-42\")\n        .relation(\"viewer\")\n        .subjectKind(\"user\")\n        .subjectId(\"amy\")\n        .caveatHash(\"\")\n        .operation(\"add\")\n        .reason(\"grant contract visibility\")\n        .build()\n);\nString zookie = response.getZookie();",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: adminToken });\nconst response = await anvil.writeAuthzTuple({\n  namespace: \"document\",\n  objectId: \"contract-42\",\n  relation: \"viewer\",\n  subjectKind: \"user\",\n  subjectId: \"amy\",\n  caveatHash: \"\",\n  operation: \"add\",\n  reason: \"grant contract visibility\",\n});\nconst zookie = response.zookie;",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=admin_token)\nresponse = anvil.write_authz_tuple(\n    namespace=\"document\",\n    object_id=\"contract-42\",\n    relation=\"viewer\",\n    subject_kind=\"user\",\n    subject_id=\"amy\",\n    caveat_hash=\"\",\n    operation=\"add\",\n    reason=\"grant contract visibility\",\n)\nzookie = response.zookie"
}
```

## Check permission

**Operation:** `AuthService.CheckPermission`

**Who calls it:** an application whose token has `authz:check` for the tuple resource envelope.

**What it does:** evaluates whether a subject has a direct or computed relationship on an object. Use `zookie` when the check must be at least as fresh as a tuple write you already observed.

```anvil-tabs
{
  "operation": "CheckPermission",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CheckPermissionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, reader_token).await?;\nlet response = anvil.check_permission(CheckPermissionRequest {\n    namespace: \"document\".into(),\n    object_id: \"contract-42\".into(),\n    relation: \"viewer\".into(),\n    subject_kind: \"user\".into(),\n    subject_id: \"amy\".into(),\n    caveat_hash: String::new(),\n    consistency: \"at_least\".into(),\n    zookie,\n}).await?;\nassert!(response.allowed);",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CheckPermissionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, readerToken);\nvar response = anvil.checkPermission(\n    CheckPermissionRequest.builder()\n        .namespace(\"document\")\n        .objectId(\"contract-42\")\n        .relation(\"viewer\")\n        .subjectKind(\"user\")\n        .subjectId(\"amy\")\n        .caveatHash(\"\")\n        .consistency(\"at_least\")\n        .zookie(zookie)\n        .build()\n);\nassert response.getAllowed();",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: readerToken });\nconst response = await anvil.checkPermission({\n  namespace: \"document\",\n  objectId: \"contract-42\",\n  relation: \"viewer\",\n  subjectKind: \"user\",\n  subjectId: \"amy\",\n  caveatHash: \"\",\n  consistency: \"at_least\",\n  zookie,\n});\nif (!response.allowed) throw new Error(\"permission denied\");",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=reader_token)\nresponse = anvil.check_permission(\n    namespace=\"document\",\n    object_id=\"contract-42\",\n    relation=\"viewer\",\n    subject_kind=\"user\",\n    subject_id=\"amy\",\n    caveat_hash=\"\",\n    consistency=\"at_least\",\n    zookie=zookie,\n)\nassert response.allowed"
}
```

## Watch tuple changes

**Operation:** `AuthService.WatchAuthzTupleLog`

**Who calls it:** an application whose token has `authz:watch` for the namespace or `*`.

**What it does:** streams tuple log records after a revision. Consumers use this to keep caches, projections, and audit displays current.

```anvil-tabs
{
  "operation": "WatchAuthzTupleLog",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchAuthzTupleLogRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, admin_token).await?;\nlet mut stream = anvil.watch_authz_tuple_log(WatchAuthzTupleLogRequest {\n    namespace: \"document\".into(),\n    after_revision: last_revision,\n}).await?;\nwhile let Some(event) = stream.message().await? {\n    println!(\"{} {}#{}\", event.revision, event.object_id, event.relation);\n}",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchAuthzTupleLogRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, adminToken);\nvar stream = anvil.watchAuthzTupleLog(\n    WatchAuthzTupleLogRequest.builder()\n        .namespace(\"document\")\n        .afterRevision(lastRevision)\n        .build()\n);\nfor (var event : stream) {\n    System.out.println(event.getRevision());\n}",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: adminToken });\nfor await (const event of anvil.watchAuthzTupleLog({ namespace: \"document\", afterRevision: lastRevision })) {\n  console.log(event.revision, event.objectId, event.relation);\n}",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=admin_token)\nfor event in anvil.watch_authz_tuple_log(namespace=\"document\", after_revision=last_revision):\n    print(event.revision, event.object_id, event.relation)"
}
```

## Watch namespace schema changes

**Operation:** `AuthService.WatchAuthzNamespace`

**Who calls it:** an authorised operator or service watching privileged namespace-policy changes.

**What it does:** streams namespace schema events after a cursor. This is an observation API. It is not a tenant self-service API for creating or changing namespace schemas.

```anvil-tabs
{
  "operation": "WatchAuthzNamespace",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchAuthzNamespaceRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, admin_token).await?;\nlet mut stream = anvil.watch_authz_namespace(WatchAuthzNamespaceRequest {\n    namespace: \"document\".into(),\n    after_cursor_low: last_cursor_low,\n    after_cursor_high: last_cursor_high,\n}).await?;\nwhile let Some(event) = stream.message().await? {\n    println!(\"{} {}\", event.namespace, event.schema_hash);\n}",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchAuthzNamespaceRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, adminToken);\nvar stream = anvil.watchAuthzNamespace(\n    WatchAuthzNamespaceRequest.builder()\n        .namespace(\"document\")\n        .afterCursorLow(lastCursorLow)\n        .afterCursorHigh(lastCursorHigh)\n        .build()\n);\nfor (var event : stream) {\n    System.out.println(event.getSchemaHash());\n}",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: adminToken });\nfor await (const event of anvil.watchAuthzNamespace({ namespace: \"document\", afterCursorLow: lastCursorLow, afterCursorHigh: lastCursorHigh })) {\n  console.log(event.namespace, event.schemaHash);\n}",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=admin_token)\nfor event in anvil.watch_authz_namespace(\n    namespace=\"document\",\n    after_cursor_low=last_cursor_low,\n    after_cursor_high=last_cursor_high,\n):\n    print(event.namespace, event.schema_hash)"
}
```

## Watch derived authorisation lag

**Operation:** `AuthService.WatchAuthzDerivedLag`

**Who calls it:** an operator or service that needs to know whether derived userset indexes have caught up with tuple writes.

**What it does:** streams lag records for a derived authorisation index. Query paths that require strict consistency should wait or fail closed when derived state is behind the required revision.

```anvil-tabs
{
  "operation": "WatchAuthzDerivedLag",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchAuthzDerivedLagRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, admin_token).await?;\nlet mut stream = anvil.watch_authz_derived_lag(WatchAuthzDerivedLagRequest {\n    derived_index_id: \"userset-default\".into(),\n    after_cursor_low: last_cursor_low,\n    after_cursor_high: last_cursor_high,\n}).await?;\nwhile let Some(event) = stream.message().await? {\n    println!(\"lag={} latest={}\", event.revision_lag, event.latest_revision);\n}",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchAuthzDerivedLagRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, adminToken);\nvar stream = anvil.watchAuthzDerivedLag(\n    WatchAuthzDerivedLagRequest.builder()\n        .derivedIndexId(\"userset-default\")\n        .afterCursorLow(lastCursorLow)\n        .afterCursorHigh(lastCursorHigh)\n        .build()\n);\nfor (var event : stream) {\n    System.out.println(event.getRevisionLag());\n}",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token: adminToken });\nfor await (const event of anvil.watchAuthzDerivedLag({ derivedIndexId: \"userset-default\", afterCursorLow: lastCursorLow, afterCursorHigh: lastCursorHigh })) {\n  console.log(event.revisionLag, event.latestRevision);\n}",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=admin_token)\nfor event in anvil.watch_authz_derived_lag(\n    derived_index_id=\"userset-default\",\n    after_cursor_low=last_cursor_low,\n    after_cursor_high=last_cursor_high,\n):\n    print(event.revision_lag, event.latest_revision)"
}
```

## What this proves

After these operations you have exercised both authorisation layers:

- application credentials produce tenant-scoped tokens;
- scoped policy grants control which applications can call broad API areas;
- relationship tuples control object-level decisions;
- permission checks can request revision freshness through a zookie;
- watches expose ordered changes for caches, projections, audits, and derived state without giving callers direct access to `_anvil/` internals.

Next, apply the same model to object reads, search queries, PersonalDB projections, and watch consumers: every exposure path needs both the right token scope and the right relationship decision for the data it returns.
