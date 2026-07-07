---
title: Tenants, Apps, and Credentials
description: Create a tenant, hand over application credentials, mint bearer tokens, and delegate narrow public API scopes.
---

# Tenants, Apps, and Credentials

This tutorial continues from [Run Anvil Locally](/tutorials/setup-local-anvil/), [Bootstrap Administration](/tutorials/admin-bootstrap/), and [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/). It assumes the `anvil-local` container is running and your shell has an `ANVIL_AUTH_TOKEN` for the bootstrap-created system administrator.

This page has a deliberate handoff. Creating the storage tenant and its first app is operator work through the private admin API. After that, tenant-owned app management and public policy delegation move to the public API. Do not keep using `anvil-admin` for tenant-owned operations just because the first command on this page uses it. The split is part of Anvil's security model.

The CLIs are manual helpers over APIs. The private operator CLI is documented in [Admin CLI](/reference/admin-cli/). The tenant-facing CLI is documented in [Public CLI](/reference/public-cli/). The scope and relation model is documented in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) and [Authorisation](/learn/authorisation/).

By the end of the walkthrough, an operator has created one storage tenant and handed it to a tenant-owned app. The tenant then creates a narrower service principal for document work. Bearer tokens carry only the public scopes the app is allowed to use.

## Prerequisites and safety checks

Start by confirming that your token is the system-admin token from the bootstrap pages. The admin API checks the system realm, so a tenant token cannot create the first tenant.

```bash
printf 'current bearer token has %s characters\n' "${#ANVIL_AUTH_TOKEN}"

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin audit list --limit 1
```

A successful audit read proves the token can reach the private admin plane and has at least the audit-read relation. It does not prove it can create tenants, but it catches the common mistake of starting this page with no token or an expired one.

The examples use `jq` to capture JSON output. If you do not have `jq`, run the command without the assignment, read the printed JSON, and export the value manually.

## Understand the moving parts

An **Anvil storage tenant** is the isolation record Anvil enforces for stored data, credentials, public policies, indexes, watches, PersonalDB groups, and tenant-owned relationship authorisation. It may represent one customer, one environment, one workspace, or another boundary chosen by your product. It is not automatically the same thing as an end-user account.

An **app** is an application identity or service principal inside a tenant. It receives a client id and client secret. Use separate apps for separate services because they give you independent credentials, audit identities, rotation schedules, revocation paths, and least-privilege scopes.

A **client id and client secret** are long-lived credential material. Store them in a secret manager. They are used to ask the public authentication service for a bearer token. They are not meant to be sent on every object, index, or authz request.

A **bearer token** is the short-lived request credential that API calls send. If a bearer token leaks, its lifetime is limited. If a client secret leaks, rotate the app secret.

A **public policy grant** is an action/resource pair such as `object:write|documents/tutorial/welcome.txt`. It authorises a tenant app to call a public API operation on a public-plane resource. It is not a system-realm admin relation.

A **system-realm admin relation** authorises private admin API calls such as tenant creation, first app provisioning, topology changes, secret-envelope rotation, repair, and admin audit reads. Tenants cannot grant system-realm authority to themselves through public APIs.

## Create the storage tenant through the admin API

The first tenant must be created by an operator because there is no tenant principal yet. In the local Docker setup, the admin API is private to the container, so the command runs through `docker exec` and passes the short-lived admin token explicitly.

```bash
ACME_TENANT_ID="$(
  docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
    anvil-admin tenant create \
      --name acme \
      --home-region local \
      --audit-reason 'create acme tutorial tenant' \
    | jq -r '.resource.tenant_id'
)"

printf 'created tenant acme with id %s\n' "$ACME_TENANT_ID"
```

This calls the private `CreateTenant` admin operation. Success proves that the caller authenticated to the admin API, the system realm authorised tenant management, the tenant name was accepted, and Anvil committed a tenant record. The numeric `tenant_id` matters because later app-management scopes use resources such as `tenant:1`, not the human-readable tenant name.

The `home-region` value records placement intent. It does not make bucket placement safe by itself. Bucket creation still depends on a writable active region, which the mesh tutorials discuss separately.

Common failures have different meanings:

- `AlreadyExists` means the tenant name is already taken in this deployment. Use the existing tenant id if this is a rerun, or choose a new tutorial name.
- `PermissionDenied` means the token is not authorised for tenant creation in the system realm.
- A connection failure means the admin endpoint inside the container is wrong or the server is not running.

## Create the first tenant app through the admin handoff

The tenant needs an initial app so routine work can move to the public API. This is a handoff credential: the operator creates it once, stores it securely, and gives it only the public scopes needed for the tenant to continue.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin app create \
    --tenant-id "$ACME_TENANT_ID" \
    --app-name acme-owner \
    --audit-reason 'create first acme owner app' \
  > /tmp/acme-owner-app.json

chmod 600 /tmp/acme-owner-app.json
export ACME_CLIENT_ID="$(jq -r '.resource.client_id' /tmp/acme-owner-app.json)"
export ACME_CLIENT_SECRET="$(jq -r '.resource.client_secret' /tmp/acme-owner-app.json)"
```

A successful response contains a client id and client secret for `acme-owner`. It proves Anvil created credential material for an app inside the `acme` tenant. It does not prove `acme-owner` can do anything useful yet. An app can mint only the scopes that have been granted to it.

Keep `/tmp/acme-owner-app.json` private. In production, move those values into the secret manager owned by the tenant or platform automation and remove local copies.

## Grant narrow public API scopes for handoff

The admin API can grant public/data-plane scopes during bootstrap. That is still an operator action because the tenant cannot yet delegate anything. These grants do not create admin authority. They only let `acme-owner` call selected public APIs.

For this tutorial, the owner app needs to create and list tenant apps, create one `documents` bucket later when placement is ready, exercise one tutorial object path, and delegate that exact object path to another tenant app. Each grant names one action and one resource.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin policy grant \
    --tenant-id "$ACME_TENANT_ID" \
    --app-name acme-owner \
    --action app:create \
    --resource "tenant:$ACME_TENANT_ID" \
    --audit-reason 'allow acme owner to create tenant apps'

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin policy grant \
    --tenant-id "$ACME_TENANT_ID" \
    --app-name acme-owner \
    --action app:read \
    --resource "tenant:$ACME_TENANT_ID" \
    --audit-reason 'allow acme owner to list tenant apps'

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin policy grant \
    --tenant-id "$ACME_TENANT_ID" \
    --app-name acme-owner \
    --action bucket:create \
    --resource documents \
    --audit-reason 'allow acme owner to create the tutorial documents bucket'
```

Those three grants let `acme-owner` manage tenant app identities and create one named bucket. They do not let it read or write arbitrary objects. To delegate a future writer for one exact object, the owner also needs the object authority it will delegate and policy-grant authority on that same resource.

```bash
docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin policy grant \
    --tenant-id "$ACME_TENANT_ID" \
    --app-name acme-owner \
    --action object:write \
    --resource documents/tutorial/welcome.txt \
    --audit-reason 'allow acme owner to write the tutorial object'

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin policy grant \
    --tenant-id "$ACME_TENANT_ID" \
    --app-name acme-owner \
    --action object:read \
    --resource documents/tutorial/welcome.txt \
    --audit-reason 'allow acme owner to read the tutorial object'

docker exec -e ANVIL_AUTH_TOKEN="$ANVIL_AUTH_TOKEN" anvil-local \
  anvil-admin policy grant \
    --tenant-id "$ACME_TENANT_ID" \
    --app-name acme-owner \
    --action policy:grant \
    --resource documents/tutorial/welcome.txt \
    --audit-reason 'allow acme owner to delegate the tutorial object path'
```

A successful policy grant proves the admin API mutated public policy for a tenant app and recorded an audit reason. It does not prove a token minted earlier already contains the new scope; mint a fresh token after changing grants.

## Configure a public CLI profile for the tenant app

Now move to the public plane. Create an `acme` profile for the tenant app. The host is the public endpoint because token exchange and tenant commands happen on the public API.

```bash
anvil static-config \
  --name acme \
  --host http://127.0.0.1:50051 \
  --client-id "$ACME_CLIENT_ID" \
  --client-secret "$ACME_CLIENT_SECRET" \
  --default
```

Switch your shell from the system-admin token to an `acme-owner` token. Both `anvil` and `anvil-admin` honour `ANVIL_AUTH_TOKEN`; leaving an admin token in the environment can make public commands run as the wrong principal.

```bash
export ANVIL_AUTH_TOKEN="$(anvil --profile acme auth get-token)"
printf 'received acme bearer token with %s characters\n' "${#ANVIL_AUTH_TOKEN}"
```

Success proves the public authentication service accepted `acme-owner` credentials and minted a token containing the app's approved public policy scopes. If token exchange fails, debug the public endpoint, client id, and client secret. Do not switch back to the admin CLI for tenant-owned app operations.

## Create a tenant-owned service principal on the public plane

Tenant teams usually create separate apps for separate services. Here, create `docs-writer` to represent a job that writes and reads one tutorial object. This is now public-plane work because `acme-owner` has been delegated tenant app authority.

```bash
anvil --profile acme app create docs-writer > /tmp/docs-writer-app.txt
chmod 600 /tmp/docs-writer-app.txt
```

A successful response contains the new app's client id, client secret, and app identity. It proves `acme-owner` has `app:create` on `tenant:$ACME_TENANT_ID`. The new app still has no useful object authority until the owner delegates it.

Delegate only the actions the writer needs:

```bash
anvil --profile acme auth grant docs-writer object:write documents/tutorial/welcome.txt
anvil --profile acme auth grant docs-writer object:read documents/tutorial/welcome.txt
```

These commands use the public policy delegation path. Public delegation is non-escalating: the caller must have `policy:grant` for the target resource and must already hold the action being delegated. That is why the admin handoff granted `acme-owner` `object:write`, `object:read`, and `policy:grant` on the same exact object path.

If delegation fails, do not add a wildcard grant first. Check which half is missing: the owner may lack `policy:grant`, or it may be trying to delegate an action/resource it does not hold. If you try to grant `documents/tutorial/other.txt`, the denial is correct because this tutorial did not grant that path.

## Verify the tenant boundary

List tenant apps through the public plane:

```bash
anvil --profile acme app list
```

The list should include `acme-owner` and `docs-writer`. This proves the public app-management grant works for this tenant. It does not prove the caller has object, index, repair, or authz administration authority.

You can also inspect the grants for the writer:

```bash
anvil --profile acme auth list-grants docs-writer
```

The expected useful grants are `object:write` and `object:read` on `documents/tutorial/welcome.txt`. If the output contains broad wildcard grants, fix the provisioning flow before using the credential in an application.

## What you have built

You now have an Anvil storage tenant named `acme`, an owner app with narrow public API scopes, and a separate writer app for one object path. The operator used the private admin API only for tenant bootstrap and initial public-policy handoff. The tenant used the public API for day-to-day app creation and delegation.

This is the production pattern: operators establish tenant boundaries and first credentials; tenant automation uses public APIs with scoped service principals. Keep admin relations, public policy grants, client secrets, and bearer tokens separate in runbooks and code.

## Success and failure cues

The expected handoff has four durable facts: `ACME_TENANT_ID` names a tenant, `/tmp/acme-owner-app.json` contains protected owner credential material, `anvil --profile acme auth get-token` mints a token, and `docs-writer` has only the object grants you delegated.

If a public command unexpectedly succeeds with authority the tenant should not have, check whether `ANVIL_AUTH_TOKEN` still contains a system-admin token. If a public command fails with permission denied, read the required action/resource pair in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) before widening grants. If a bucket or object command fails later with placement errors, return to topology; credentials cannot fix a region lifecycle precondition.

## Where to go next

Move to [Buckets and Objects](/tutorials/buckets-and-objects/) to use tenant credentials on the public object path. Keep [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/) open when adding new grants; an app that can create credentials should not automatically become a bucket writer, index reader, relationship-authorisation administrator, or system administrator.
