---
title: Authentication And Relationship Authorization
description: Get tokens, grant access, revoke access, manage public access, write tuples, check permissions, and watch authz state.
---

# Authentication And Relationship Authorization

**What this page gives you:** a tutorial for every operation in this area, with Rust, Java, Node.js, and Python tabs for each operation.

Authentication proves who is calling. Authorization decides what that caller may see or change. Anvil supports token scopes for direct policy and Zanzibar-style relationship tuples for product permissions. This tutorial shows how to issue credentials, grant or revoke access, set public access policy, write relationship facts, check permissions, and watch authorization changes.

## Workflow

1. Connect a client with an endpoint and token.
2. Send a request that names the bucket, object, index, group, resource, or artifact explicitly.
3. Preserve the returned version, cursor, generation, certificate, or diagnostic id when the response includes one.
4. Use that returned value for preconditions, watch resume, catch-up, or repair verification.

## Get an access token

**Operation:** `AuthService.GetAccessToken`

Exchanges application credentials for a bearer token.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "GetAccessToken",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GetAccessTokenRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.get_access_token(GetAccessTokenRequest { client_id: \"clientId\".into(), client_secret: \"clientSecret\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GetAccessTokenRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.getAccessToken(\n    GetAccessTokenRequest.builder()\n        .clientId(\"clientId\")\n        .clientSecret(\"clientSecret\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.getAccessToken({ clientId: 'clientId', clientSecret: 'clientSecret' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.get_access_token(client_id='clientId', client_secret='clientSecret')\nprint(response)"
}
```

## Grant scoped access

**Operation:** `AuthService.GrantAccess`

Adds an access grant according to policy.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "GrantAccess",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::GrantAccessRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.grant_access(GrantAccessRequest { subject: \"app:reader\".into(), action: \"object:read\".into(), resource: \"documents/projects/acme/*\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.GrantAccessRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.grantAccess(\n    GrantAccessRequest.builder()\n        .subject(\"app:reader\")\n        .action(\"object:read\")\n        .resource(\"documents/projects/acme/*\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.grantAccess({ subject: 'app:reader', action: 'object:read', resource: 'documents/projects/acme/*' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.grant_access(subject='app:reader', action='object:read', resource='documents/projects/acme/*')\nprint(response)"
}
```

## Revoke scoped access

**Operation:** `AuthService.RevokeAccess`

Removes an access grant.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "RevokeAccess",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::RevokeAccessRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.revoke_access(RevokeAccessRequest { subject: \"app:reader\".into(), action: \"object:read\".into(), resource: \"documents/projects/acme/*\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.RevokeAccessRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.revokeAccess(\n    RevokeAccessRequest.builder()\n        .subject(\"app:reader\")\n        .action(\"object:read\")\n        .resource(\"documents/projects/acme/*\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.revokeAccess({ subject: 'app:reader', action: 'object:read', resource: 'documents/projects/acme/*' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.revoke_access(subject='app:reader', action='object:read', resource='documents/projects/acme/*')\nprint(response)"
}
```

## Set public access

**Operation:** `AuthService.SetPublicAccess`

Changes whether a bucket or resource exposes public read behavior.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "SetPublicAccess",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::SetPublicAccessRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.set_public_access(SetPublicAccessRequest { bucket_name: \"public-assets\".into(), enabled: true, ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.SetPublicAccessRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.setPublicAccess(\n    SetPublicAccessRequest.builder()\n        .bucketName(\"public-assets\")\n        .enabled(true)\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.setPublicAccess({ bucketName: 'public-assets', enabled: 'true' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.set_public_access(bucket_name='public-assets', enabled='true')\nprint(response)"
}
```

## Write authorization tuple

**Operation:** `AuthService.WriteAuthzTuple`

Writes a relationship fact such as a viewer, editor, owner, or group membership.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "WriteAuthzTuple",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WriteAuthzTupleRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.write_authz_tuple(WriteAuthzTupleRequest { tuple: \"document:contract-42#viewer@user:amy\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WriteAuthzTupleRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.writeAuthzTuple(\n    WriteAuthzTupleRequest.builder()\n        .tuple(\"document:contract-42#viewer@user:amy\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.writeAuthzTuple({ tuple: 'document:contract-42#viewer@user:amy' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.write_authz_tuple(tuple='document:contract-42#viewer@user:amy')\nprint(response)"
}
```

## Check permission

**Operation:** `AuthService.CheckPermission`

Evaluates whether a subject has a relation or permission on a resource.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "CheckPermission",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::CheckPermissionRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.check_permission(CheckPermissionRequest { resource: \"document:contract-42\".into(), permission: \"view\".into(), subject: \"user:amy\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.CheckPermissionRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.checkPermission(\n    CheckPermissionRequest.builder()\n        .resource(\"document:contract-42\")\n        .permission(\"view\")\n        .subject(\"user:amy\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.checkPermission({ resource: 'document:contract-42', permission: 'view', subject: 'user:amy' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.check_permission(resource='document:contract-42', permission='view', subject='user:amy')\nprint(response)"
}
```

## Watch authz tuple log

**Operation:** `AuthService.WatchAuthzTupleLog`

Streams relationship tuple changes.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "WatchAuthzTupleLog",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchAuthzTupleLogRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_authz_tuple_log(WatchAuthzTupleLogRequest { after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchAuthzTupleLogRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchAuthzTupleLog(\n    WatchAuthzTupleLogRequest.builder()\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchAuthzTupleLog({ afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_authz_tuple_log(after_cursor='lastCursor')\nprint(response)"
}
```

## Watch authz namespace

**Operation:** `AuthService.WatchAuthzNamespace`

Streams schema or namespace changes.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "WatchAuthzNamespace",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchAuthzNamespaceRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_authz_namespace(WatchAuthzNamespaceRequest { after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchAuthzNamespaceRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchAuthzNamespace(\n    WatchAuthzNamespaceRequest.builder()\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchAuthzNamespace({ afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_authz_namespace(after_cursor='lastCursor')\nprint(response)"
}
```

## Watch authz derived lag

**Operation:** `AuthService.WatchAuthzDerivedLag`

Streams lag information for derived authorization views.

The important rule is to pass the caller identity and request context through the client instead of bypassing Anvil with out-of-band credentials. That keeps object state, indexes, search results, watch streams, and authorization decisions aligned.


```anvil-tabs
{
  "operation": "WatchAuthzDerivedLag",
  "rust": "use anvil_storage::client::AnvilClient;\nuse anvil_storage::proto::WatchAuthzDerivedLagRequest;\n\nlet mut anvil = AnvilClient::connect(endpoint, token).await?;\nlet response = anvil.watch_authz_derived_lag(WatchAuthzDerivedLagRequest { after_cursor: \"lastCursor\".into(), ..Default::default() }).await?;\nprintln!(\"{response:?}\");",
  "java": "import dev.anvil.AnvilClient;\nimport dev.anvil.proto.WatchAuthzDerivedLagRequest;\n\nAnvilClient anvil = AnvilClient.connect(endpoint, token);\nvar response = anvil.watchAuthzDerivedLag(\n    WatchAuthzDerivedLagRequest.builder()\n        .afterCursor(\"lastCursor\")\n        .build()\n);\nSystem.out.println(response);",
  "node": "import { AnvilClient } from \"@anvil/storage\";\n\nconst anvil = await AnvilClient.connect({ endpoint, token });\nconst response = await anvil.watchAuthzDerivedLag({ afterCursor: 'lastCursor' });\nconsole.log(response);",
  "python": "from anvil_storage import AnvilClient\n\nanvil = AnvilClient.connect(endpoint=endpoint, token=token)\nresponse = anvil.watch_authz_derived_lag(after_cursor='lastCursor')\nprint(response)"
}
```

## What you can do after this page

You should now be able to perform every operation in this area and understand why the request shape matters. Continue to another tutorial area or use the reference pages when you need exact configuration and error behavior.
