---
title: Tenants, Apps, and Credentials
description: Create a tenant, hand over application credentials, mint bearer tokens, and delegate narrow public API scopes.
---

# Tenants, Apps, and Credentials

This tutorial continues from [Run Anvil Locally](/tutorials/setup-local-anvil/), [Bootstrap Administration](/tutorials/admin-bootstrap/), and [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/). It assumes the `anvil-local` container is running and your shell still has an `ANVIL_AUTH_TOKEN` for the bootstrap-created system administrator.

Anvil has two different administration stories on this page. Creating the storage tenant and its first app is an operator action through the private admin API. Using that app to mint tokens, create tenant-owned apps, and grant public API scopes is tenant work through the public API. The CLI commands are manual helpers over those APIs; application code should normally call the public API or Rust client directly. Keep [Admin CLI](/reference/admin-cli/), [Public CLI](/reference/public-cli/), and [CLI Workflows](/reference/cli-workflows/) nearby while you read.

For the authorisation model behind these commands, see [Authorisation](/learn/authorisation/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and [Tenant and Bucket Provisioning](/operators/tenant-and-bucket-provisioning/).

## Understand the moving parts

An **Anvil storage tenant** is the isolation record Anvil enforces for stored data, credentials, policies, indexes, watches, and tenant-owned authorisation. It may map to one customer, one environment, one product workspace, or a larger internal boundary. Do not assume it is the same thing as your application's user account model.

An **app** is an application identity or service principal inside a tenant. It is the thing that receives a client id and client secret. Use separate apps for separate automation because they give you independent credentials, audit identity, rotation, revocation, and least-privilege scopes. A document importer, read-only reporting job, index maintenance worker, and web front end should not need to share one secret.

A **client id and client secret** are long-lived credential material. Store them in a secret manager, print them as little as possible, and rotate them when ownership or risk changes. They are used to ask Anvil for a token; they are not meant to be attached to every API request forever.

A **bearer token** is a short-lived request credential. `anvil auth get-token` exists so a CLI, service, or Rust client can exchange long-lived credential material for a token that individual public API requests can carry. If the token leaks, its lifetime is limited; if the client secret leaks, rotate the app secret.

A **policy grant** or **scope** is public/data-plane authorisation. It says an app may perform a public API action such as `app:create`, `object:write`, or `policy:grant` over a specific resource pattern. It is not a system-realm admin relation.

A **system-realm admin relation** is private admin-plane authorisation. It decides whether a principal can call admin API operations such as tenant creation, app provisioning, topology changes, repair, audit export, or secret-envelope rotation. Public policy grants do not create system-realm admin authority, and the public API cannot perform mesh or tenant provisioning.

## Create the storage tenant through the admin API

The first tenant must be created by an operator because there is no tenant principal yet. In the local Docker setup, the admin API is private to the container, so use `docker exec` and pass the short-lived admin bearer token from the previous page.

This command creates an `acme` storage tenant and captures the numeric tenant id from the admin CLI JSON output. The numeric id matters because public app-management scopes are checked against resources such as `tenant:1`, not just the human-friendly tenant name.

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

After this command, Anvil has a tenant record named `acme`. The `home-region` value records operator intent for placement, but bucket creation still depends on an active writable region. In the current local tutorial chain, region activation is intentionally left as a documented checkpoint-workflow gap, so this page focuses on identity and scoped public API access rather than bucket placement.

If you do not have `jq`, run the `docker exec ... anvil-admin tenant create ...` command by itself and copy the `tenant_id` value from the JSON response into `ACME_TENANT_ID`.

## Create the first tenant app

The tenant needs an initial app so tenant-side work can move to the public API. This is a handover credential: the operator creates it once, stores the output securely, and then the tenant uses it to configure public API clients.

This command creates an app called `acme-owner` in the `acme` tenant and writes the one-time credential output to a local file. The file contains long-lived credential material, so keep its permissions narrow and move the values into a secret manager for anything beyond this tutorial.

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

After this command, `acme-owner` has a client id and client secret, but it is not all-powerful. It can only mint tokens containing scopes that Anvil has granted to that app. The next step grants a small set of public API scopes for this tutorial.

## Grant narrow public API scopes for handover

The admin API can grant public/data-plane scopes to the first tenant app so the tenant can take over routine work. These are not system-realm admin relations. They do not let `acme-owner` call the private admin API, create regions, rotate server encryption keys, or bypass tenant authorisation.

For this tutorial, `acme-owner` needs to create and list tenant-owned apps, delegate access to one example object path, and later create the `documents` bucket when the local region activation workflow is available. Each grant names one action and one resource. There is no wildcard grant.

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

Those three grants let the owner manage tenant app identities and, later, create one named bucket. They still do not let it read or write arbitrary objects. To delegate a writer app for one exact tutorial object, the owner must also hold both the action it wants to delegate and permission to grant that action.

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

After these grants, the operator handover is complete for this narrow tutorial path. Further tenant work should use the public API as `acme-owner`, not the private admin API.

## Configure the public CLI for the tenant app

Create a public CLI profile for `acme-owner`. A profile stores the public endpoint plus the long-lived client id and client secret. It is a convenience for token exchange, not a permission grant.

```bash
anvil static-config \
  --name acme \
  --host http://127.0.0.1:50051 \
  --client-id "$ACME_CLIENT_ID" \
  --client-secret "$ACME_CLIENT_SECRET" \
  --default
```

Now switch your shell from the admin token to an `acme-owner` public API token. This is important because both CLIs honour `ANVIL_AUTH_TOKEN` when it is set. If you leave the system-admin token in the environment, public tenant commands may run as the wrong principal.

```bash
export ANVIL_AUTH_TOKEN="$(anvil --profile acme auth get-token)"
printf 'received acme bearer token with %s characters\n' "${#ANVIL_AUTH_TOKEN}"
```

The token exchange proves that `acme-owner` has valid long-lived credential material and at least one granted scope. From this point in the tutorial, ordinary tenant commands should use the public `anvil` CLI or the public API. If you need to return to admin work later, mint a fresh system-admin token from the `local-system` profile instead of reusing this tenant token.

## Create a tenant-owned service principal

Tenant teams usually create separate apps for separate services. Here, create `docs-writer` to represent a job that writes and reads one tutorial object. It gets its own client secret, can be rotated without touching `acme-owner`, and can be revoked without deleting the tenant.

```bash
anvil --profile acme app create docs-writer > /tmp/docs-writer-app.txt
chmod 600 /tmp/docs-writer-app.txt
```

The file contains the `docs-writer` client id and client secret. Store them in the secret manager for the service that will use them. Do not copy `acme-owner` credentials into application workers just because they already exist.

Now delegate only the object actions this writer needs. These grants use the public API as `acme-owner`; they do not involve the private admin API and they do not create admin-plane authority.

```bash
anvil --profile acme auth grant docs-writer object:write documents/tutorial/welcome.txt
anvil --profile acme auth grant docs-writer object:read documents/tutorial/welcome.txt
```

After these commands, `docs-writer` can mint a bearer token that contains the delegated object scopes. It still cannot create tenants, change topology, grant unrelated paths, rotate server keys, or act as a system administrator.

## What you have built

You now have an Anvil storage tenant named `acme`, an owner app with narrow public API scopes, and a separate writer app for one object path. The operator used the private admin API only for tenant bootstrap and initial policy handover. The tenant used the public API for day-to-day app creation and delegation.

This split is the production pattern: operators establish the tenant boundary and first credential, then tenant automation uses public APIs with scoped service principals. Keep admin relations, public policy grants, client secrets, and bearer tokens separate in your runbooks and in your code.
