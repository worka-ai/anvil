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


## CLI flow: grant, prove access, revoke, prove denial

This is the operational flow most teams use first. The admin CLI creates authority. The user CLI proves the result from an ordinary application credential.

Create a tenant, application credential, and bucket through the admin API:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"

admin --host http://127.0.0.1:50052 tenant create \
  --name docs \
  --home-region eu-west-1 \
  --audit-reason "create docs tenant"

admin --host http://127.0.0.1:50052 app create \
  --tenant-id docs \
  --app-name docs-reader \
  --audit-reason "create docs reader app"

admin --host http://127.0.0.1:50052 bucket create \
  --tenant-id docs \
  --bucket-name documents \
  --region eu-west-1 \
  --audit-reason "create documents bucket"
```

The `app create` command prints a client id and client secret once. Store them securely, then configure the user CLI:

```bash
anvil-cli static-config \
  --name docs-reader \
  --host http://127.0.0.1:50051 \
  --client-id "$ANVIL_CLIENT_ID" \
  --client-secret "$ANVIL_CLIENT_SECRET" \
  --default
```

Before the grant, ordinary object access fails because the application has no object scope:

```bash
anvil-cli object ls s3://documents/
# expected: permission denied
```

Grant read and write access from the admin plane:

```bash
admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id docs \
  --app-name docs-reader \
  --action object:write \
  --resource 'documents/*' \
  --audit-reason "allow docs reader uploads"

admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id docs \
  --app-name docs-reader \
  --action object:read \
  --resource 'documents/*' \
  --audit-reason "allow docs reader downloads"
```

Now the same user CLI can write, list, head, and read through the public API:

```bash
printf 'hello from Anvil\n' > /tmp/anvil-doc.txt
anvil-cli object put /tmp/anvil-doc.txt s3://documents/guides/hello.txt
anvil-cli object ls s3://documents/guides/
anvil-cli object head s3://documents/guides/hello.txt
anvil-cli object get s3://documents/guides/hello.txt /tmp/anvil-doc-copy.txt
```

Revoke write access and prove the change from the public API:

```bash
admin --host http://127.0.0.1:50052 policy revoke \
  --tenant-id docs \
  --app-name docs-reader \
  --action object:write \
  --resource 'documents/*' \
  --audit-reason "remove docs reader upload access"

anvil-cli object put /tmp/anvil-doc.txt s3://documents/guides/after-revoke.txt
# expected: permission denied
```

The important point is the boundary. `admin` changes tenant, credential, and policy state on the internal admin API. `anvil-cli` proves the access decision through the public data API. At no point does either CLI write `_anvil/` paths or edit storage files directly.

## Get an access token

**Operation:** `AuthService.GetAccessToken`

**Who calls it:** a tenant application or service job that has a `client_id` and `client_secret` created by bootstrap/admin tooling.

**What it does:** verifies the application secret, intersects requested scopes with assigned policy, and returns a short-lived bearer token. Empty scopes or `*` request all scopes already assigned to the application; they do not create new authority.

```anvil-tabs
{
  "operation": "GetAccessToken",
  "rust": "use anvil_storage::{AnvilClient, proto::GetAccessTokenRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, bootstrap_token).await?;\nlet response = anvil.auth().get_access_token(GetAccessTokenRequest {\n    client_id: \"admin-client-id\".into(),\n    client_secret: \"admin-client-secret\".into(),\n    scopes: vec![\n        \"policy:grant|bucket:documents/*\".into(),\n        \"authz:tuple_write|document/contract-42#viewer\".into(),\n    ],\n}).await?;\nlet admin_token = response.access_token;"
}
```

## Grant scoped access to another application

**Operation:** `AuthService.GrantAccess`

**Who calls it:** an application whose token has `policy:grant` on the requested resource.

**What it does:** adds one action/resource policy to another tenant application. The grantee is named by application id/name as `grantee_app_id`; it is not a relationship subject such as `user:amy`.

```anvil-tabs
{
  "operation": "GrantAccess",
  "rust": "use anvil_storage::{AnvilClient, proto::GrantAccessRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, admin_token).await?;\nanvil.grant_access(GrantAccessRequest {\n    grantee_app_id: \"reader-api\".into(),\n    resource: \"bucket:documents/*\".into(),\n    action: \"object:read\".into(),\n}).await?;"
}
```

## Revoke scoped access

**Operation:** `AuthService.RevokeAccess`

**Who calls it:** an application whose token has `policy:revoke` on the requested resource.

**What it does:** removes one action/resource policy from another tenant application. Revocation affects future token issuance and future checks using the changed policy set.

```anvil-tabs
{
  "operation": "RevokeAccess",
  "rust": "use anvil_storage::{AnvilClient, proto::RevokeAccessRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, admin_token).await?;\nanvil.revoke_access(RevokeAccessRequest {\n    grantee_app_id: \"reader-api\".into(),\n    resource: \"bucket:documents/*\".into(),\n    action: \"object:read\".into(),\n}).await?;"
}
```

## Set public bucket access

**Operation:** `AuthService.SetPublicAccess`

**Who calls it:** an application whose token has `policy:grant` on `bucket:<bucket>`.

**What it does:** toggles public read policy for a bucket. Public access is a policy decision; it should be reserved for buckets that are deliberately designed for public distribution.

```anvil-tabs
{
  "operation": "SetPublicAccess",
  "rust": "use anvil_storage::{AnvilClient, proto::SetPublicAccessRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, admin_token).await?;\nanvil.set_public_access(SetPublicAccessRequest {\n    bucket: \"public-assets\".into(),\n    allow_public_read: true,\n}).await?;"
}
```

## Write a relationship tuple

**Operation:** `AuthService.WriteAuthzTuple`

**Who calls it:** an application whose token has `authz:tuple_write` for the tuple resource envelope.

**What it does:** adds or removes one source relationship fact. Tuple writes are security mutations and should include a reason.

```anvil-tabs
{
  "operation": "WriteAuthzTuple",
  "rust": "use anvil_storage::{AnvilClient, proto::WriteAuthzTupleRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, admin_token).await?;\nlet response = anvil.auth().write_authz_tuple(WriteAuthzTupleRequest {\n    namespace: \"document\".into(),\n    object_id: \"contract-42\".into(),\n    relation: \"viewer\".into(),\n    subject_kind: \"user\".into(),\n    subject_id: \"amy\".into(),\n    caveat_hash: String::new(),\n    operation: \"add\".into(),\n    reason: \"grant contract visibility\".into(),\n}).await?;\nlet zookie = response.zookie;"
}
```

## Check permission

**Operation:** `AuthService.CheckPermission`

**Who calls it:** an application whose token has `authz:check` for the tuple resource envelope.

**What it does:** evaluates whether a subject has a direct or computed relationship on an object. Use `zookie` when the check must be at least as fresh as a tuple write you already observed.

```anvil-tabs
{
  "operation": "CheckPermission",
  "rust": "use anvil_storage::{AnvilClient, proto::CheckPermissionRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, reader_token).await?;\nlet response = anvil.auth().check_permission(CheckPermissionRequest {\n    namespace: \"document\".into(),\n    object_id: \"contract-42\".into(),\n    relation: \"viewer\".into(),\n    subject_kind: \"user\".into(),\n    subject_id: \"amy\".into(),\n    caveat_hash: String::new(),\n    consistency: \"at_least\".into(),\n    zookie,\n}).await?;\nassert!(response.allowed);"
}
```

## Watch tuple changes

**Operation:** `AuthService.WatchAuthzTupleLog`

**Who calls it:** an application whose token has `authz:watch` for the namespace or `*`.

**What it does:** streams tuple log records after a revision. Consumers use this to keep caches, projections, and audit displays current.

```anvil-tabs
{
  "operation": "WatchAuthzTupleLog",
  "rust": "use anvil_storage::{AnvilClient, proto::WatchAuthzTupleLogRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, admin_token).await?;\nlet mut stream = anvil.auth().watch_authz_tuple_log(WatchAuthzTupleLogRequest {\n    namespace: \"document\".into(),\n    after_revision: last_revision,\n}).await?;\nwhile let Some(event) = stream.message().await? {\n    println!(\"{} {}#{}\", event.revision, event.object_id, event.relation);\n}"
}
```

## Watch namespace schema changes

**Operation:** `AuthService.WatchAuthzNamespace`

**Who calls it:** an authorised operator or service watching privileged namespace-policy changes.

**What it does:** streams namespace schema events after a cursor. This is an observation API. It is not a tenant self-service API for creating or changing namespace schemas.

```anvil-tabs
{
  "operation": "WatchAuthzNamespace",
  "rust": "use anvil_storage::{AnvilClient, proto::WatchAuthzNamespaceRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, admin_token).await?;\nlet mut stream = anvil.auth().watch_authz_namespace(WatchAuthzNamespaceRequest {\n    namespace: \"document\".into(),\n    after_cursor_low: last_cursor_low,\n    after_cursor_high: last_cursor_high,\n}).await?;\nwhile let Some(event) = stream.message().await? {\n    println!(\"{} {}\", event.namespace, event.schema_hash);\n}"
}
```

## Watch derived authorisation lag

**Operation:** `AuthService.WatchAuthzDerivedLag`

**Who calls it:** an operator or service that needs to know whether derived userset indexes have caught up with tuple writes.

**What it does:** streams lag records for a derived authorisation index. Query paths that require strict consistency should wait or fail closed when derived state is behind the required revision.

```anvil-tabs
{
  "operation": "WatchAuthzDerivedLag",
  "rust": "use anvil_storage::{AnvilClient, proto::WatchAuthzDerivedLagRequest};\n\nlet anvil = AnvilClient::connect_with_bearer(endpoint, admin_token).await?;\nlet mut stream = anvil.auth().watch_authz_derived_lag(WatchAuthzDerivedLagRequest {\n    derived_index_id: \"userset-default\".into(),\n    after_cursor_low: last_cursor_low,\n    after_cursor_high: last_cursor_high,\n}).await?;\nwhile let Some(event) = stream.message().await? {\n    println!(\"lag={} latest={}\", event.revision_lag, event.latest_revision);\n}"
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
