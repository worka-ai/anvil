---
title: Tenant and Bucket Provisioning
description: Create storage tenants, first application credentials, policy grants, and buckets with a clean operator-to-tenant handover.
---

# Tenant and Bucket Provisioning

Provisioning is a handover, not a permanent operating mode. The operator creates the storage tenant and the first application credential on the private admin plane. After that, tenant-owned work should move to the public API: creating buckets where delegated, managing tenant application credentials, writing objects, defining relationship authorisation, building indexes, and publishing tenant-owned links or host aliases where supported.

That split prevents a common confusion. A storage tenant is Anvil's isolation boundary; it is not necessarily the same as a customer account, organisation, workspace, or end user inside your product. The first app exists because a brand-new tenant needs one authenticated principal before it can call public APIs or delegate to narrower service principals. It is a bootstrap bridge into tenant ownership, not a reason for operators to keep publishing tenant data with admin credentials.

The admin API remains private and system-authorised. Public API credentials remain tenant-scoped and policy-authorised. If an operation is normal tenant data work, prefer the public API even when an operator could technically do something similar through `anvil-admin`. For the plane split, read [Admin Plane](/operators/admin-plane/), [Authorisation](/learn/authorisation/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## Storage tenants and first apps

A storage tenant owns buckets, object metadata, object versions, indexes, relationship-authorisation records, watches, task leases, append streams, and tenant audit events. Anvil enforces that boundary with the tenant id in bearer tokens and with resource checks inside each service. Your application can still model many product users or organisations inside one storage tenant by using object layout, metadata, and relationship authorisation; do not create a storage tenant for every end user unless that is genuinely the isolation unit you want to operate.

A first app is the first service principal inside that storage tenant. It has a client id and client secret. The app exchanges those credentials for short-lived bearer tokens, and those tokens contain the public policy scopes granted to the app. The first app is useful when the tenant needs to create its initial bucket, create narrower application credentials, or delegate a controlled set of public policy scopes without waiting for a product-specific user system to exist.

The first app should not become a forever super-user. Give it enough authority to complete the handover, then use it to create narrower apps such as `docs-ingest`, `search-indexer`, or `public-site-publisher`. Rotate or retire the bootstrap credential after your tenant-owned automation is in place.

## Create the storage tenant

The operator starts on the admin plane:

```bash
anvil-admin --host http://10.10.0.12:50052 tenant create \
  --name acme \
  --home-region eu-west-1 \
  --audit-reason 'create acme storage tenant for onboarding ticket ACME-42'
```

This command asks the private admin API to create a storage tenant named `acme`. It proves that the admin endpoint is reachable, the caller has the system-realm `manage_tenants` relation, the tenant name is available, and an admin audit event can be written. It does not create a product user, grant any public API scopes, create a bucket, or prove that object writes will work.

`--home-region` expresses the intended home region in the admin request and response. Current source has an implementation mismatch to account for: the admin request accepts `home_region`, but the durable tenant locator is written from the serving node's configured region. Treat the flag as API intent and audit evidence rather than as an independently verified placement override until that gap is closed. Bucket placement is more concrete today: each bucket create request carries a region, and the bucket locator records that bucket home region.

Tenant creation can fail if the name already exists, if the admin caller lacks the relation, or if the server cannot write control-plane state. It can also succeed while later bucket creation fails because the target region, cell, or node lifecycle is not active for new writable placement. See [Topology Planning](/operators/topology-planning/) and [Mesh Routing and Lifecycle](/learn/mesh-routing-and-lifecycle/) before provisioning production regions.

## Create the first application credential

Next, create the first app inside the tenant:

```bash
anvil-admin --host http://10.10.0.12:50052 app create \
  --tenant-id acme \
  --app-name acme-owner \
  --audit-reason 'create first acme app credential for secure tenant handover'
```

The command returns a client id and client secret. Store the secret immediately in the tenant's secret manager or handover vault; it is a credential, not a log message. This command proves that the admin caller has the system-realm `manage_apps` relation and that Anvil can create an application identity for the tenant. It does not by itself let the app do anything useful. An app with no public policy grants cannot mint a data-plane token unless it is a system admin app in the system realm, which this tenant app is not.

This is often the moment operators ask why the tenant needs an app before any bucket exists. The answer is that the app is the tenant's way to authenticate to the public API. Without it, the operator would have to keep using admin credentials to create tenant data, and that would blur audit evidence and ownership. With it, the operator can grant a narrow starting policy, hand over the secret, and let tenant automation take over.

## Grant only the bootstrap authority needed

Public policy grants are not system-realm admin relations. They are scoped permissions stored on an app and minted into the app's bearer token. They should describe the public API work the app is expected to perform. Avoid grants such as global wildcard action plus global wildcard resource; those make compromise and audit review much worse. If a wildcard resource is unavoidable for a current implementation gap, treat it as a controlled temporary bootstrap exception with a removal step.

A narrow first handover for a document system might let `acme-owner` create exactly one bucket named `documents`, create tenant-owned apps, and delegate a write prefix to an ingestion app:

```bash
anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action bucket:create \
  --resource documents \
  --audit-reason 'allow acme owner to create the documents bucket'

anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action app:create \
  --resource "tenant:$ACME_TENANT_ID" \
  --audit-reason 'allow acme owner to create tenant service apps'

anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action policy:grant \
  --resource documents/incoming/* \
  --audit-reason 'allow acme owner to delegate the incoming document prefix'

anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name acme-owner \
  --action object:write \
  --resource documents/incoming/* \
  --audit-reason 'allow acme owner to delegate writes it already holds'
```

These commands grant separate capabilities instead of one broad owner scope. `bucket:create` on `documents` is enough for the public bucket service to create that bucket name. `app:create` on `tenant:$ACME_TENANT_ID` lets the first app create narrower tenant apps through the public API. `policy:grant` plus `object:write` on `documents/incoming/*` lets it delegate write access to the same prefix; public delegation also checks that the caller already holds the action it is trying to delegate.

Use the tenant id returned by `tenant create` for `$ACME_TENANT_ID`, not the tenant name. App-management checks currently use `tenant:<tenant_id>`. If you also want the first app to list tenant apps, rotate app secrets, revoke grants, read objects, or set public-read on a bucket, grant those actions deliberately and separately. Do not add them because they might be convenient later.

## Hand over to the public API

The tenant or tenant-owned automation then configures the public CLI profile with the first app credential:

```bash
anvil static-config \
  --name acme-owner \
  --host https://storage.example.com \
  --client-id "$ACME_CLIENT_ID" \
  --client-secret "$ACME_CLIENT_SECRET" \
  --default
```

This writes a CLI profile for the public endpoint. It does not contact the server and does not prove that the secret is valid. The first server-side proof comes when the CLI exchanges the client id and secret for a bearer token during a public API command.

Create the initial bucket from the public plane where the tenant has `bucket:create`:

```bash
anvil bucket create documents eu-west-1
```

This command proves that the public endpoint is reachable, the first app can mint a token, the token contains `bucket:create` for `documents`, and the server can create a bucket locator for `eu-west-1`. It does not prove that object upload works, that bucket listing is allowed, that public-read is enabled, or that relationship authorisation permits any product user to see objects.

Bucket region is a placement and routing decision. Reads and writes for a bucket should go to the bucket's home region or follow the server's cross-region behaviour where supported. Creating a bucket in a region can fail if the region, cell, or node is not active for new writable placement. That failure is an operator topology issue, not a reason to fall back to admin object writes.

## Create narrower tenant apps

After the first bucket exists, the owner app can create a narrower ingestion credential through the public API if it has the app-management grant:

```bash
anvil app create docs-ingest
```

The returned secret belongs to the ingestion service, not to the operator. Store it where that service can read it and rotate it independently. The command proves that the caller has `app:create` for the tenant; it does not grant the new app any object access.

Delegate only the prefix the ingestion service needs:

```bash
anvil auth grant docs-ingest object:write documents/incoming/*
```

This public delegation proves three things: the caller is authenticated as the tenant, the caller has `policy:grant` on `documents/incoming/*`, and the caller already holds `object:write` on that same resource pattern. It does not allow the caller to grant wildcard authority, system-realm authority, cross-tenant authority, or reserved `_anvil/` access. Public delegation rejects those shapes.

For a read-only search service, a separate grant such as `object:read documents/published/*` is usually better than reusing the ingestion app. For an indexer, grant only the index and object read scopes the current index APIs require. Current index read scopes are still coarser than ideal in some paths; see [Indexes and Query](/learn/indexes-and-query/) before designing long-lived grants.

## CLI upload smoke tests and current gaps

The public object API is the primary production surface for writes. The CLI is a manual helper. A smoke-test upload looks simple:

```bash
anvil static-config \
  --name docs-ingest \
  --host https://storage.example.com \
  --client-id "$DOCS_INGEST_CLIENT_ID" \
  --client-secret "$DOCS_INGEST_CLIENT_SECRET" \
  --default

anvil object put ./welcome.txt s3://documents/incoming/welcome.txt
```

The `static-config` command only changes the local CLI profile; it does not prove the ingestion secret is valid. The upload command is the first server-side proof. Today that command does more than call `PutObject`. The public CLI first calls `ListBuckets` to discover the numeric `bucket_id` needed for the native mutation context, then sends the object stream. The object write itself checks `object:write` against `documents/incoming/welcome.txt`, so the prefix grant above is enough for the write. The bucket discovery step currently requires `bucket:list` on the global resource `*`, because `ListBuckets` checks that exact resource.

That creates a least-privilege gap for CLI uploads: a service that should only write `documents/incoming/*` also needs a tenant-wide bucket-name listing grant if it uses the current CLI helper. Public delegation cannot grant that wildcard resource, so an operator would need to add it through the admin API if a CLI smoke test is required:

```bash
anvil-admin --host http://10.10.0.12:50052 policy grant \
  --tenant-id acme \
  --app-name docs-ingest \
  --action bucket:list \
  --resource '*' \
  --audit-reason 'temporary CLI bucket id discovery for docs-ingest smoke test'
```

Use that as a temporary temporary path, not as a normal service grant. Revoke it after the test if the service does not need tenant-wide bucket listing:

```bash
anvil-admin --host http://10.10.0.12:50052 policy revoke \
  --tenant-id acme \
  --app-name docs-ingest \
  --action bucket:list \
  --resource '*' \
  --audit-reason 'remove temporary CLI bucket id discovery grant after smoke test'
```

Production clients can avoid this particular CLI limitation by using the API directly and storing the bucket id returned by bucket creation or another authorised discovery path. The current public CLI also does not expose `content_type` or `user_metadata_json` on `object put`, even though the API metadata message supports those fields. If your application depends on content type or user metadata for static hosting, indexing, or downstream processing, use the API or another current gateway path that can set the metadata you need.

Other current scope checks are coarser than the ideal model. Object prefix listing checks `object:list` at the bucket level, not at a prefix. App-management checks are tenant-wide through `tenant:<tenant_id>`, not per app. Some index read and query paths use bucket-level `index:read`. Model those limitations explicitly in the tenant's access design rather than hiding them behind broad grants.

## Public access and bucket policy

Public-read is a bucket/object visibility decision on the public surface, not an admin bypass. If the tenant intentionally serves static or public data, prefer tenant-owned public API operations where the current surface supports them:

```bash
anvil bucket set-public documents --allow true
```

This command uses the public bucket service and requires `bucket:write` on `documents`. It proves that the caller can change the bucket public-read flag. It does not make the admin API public, publish every tenant bucket, create DNS, activate a host alias, or override object-level relationship authorisation where that filtering is applied. Public means anyone who can reach the public surface may read matching data, so keep public-read changes deliberate and auditable. Read [Public Access](/tutorials/public-access/) and [Gateway Operations](/operators/gateway-operations/) before exposing data.

Admin bucket public-access commands exist for operator-controlled provisioning, repair, or migration. They should not be the normal publishing path for tenant applications.

## Deprovisioning and evidence

Deprovisioning should be as deliberate as provisioning. Start by identifying which app credentials and grants are still in use. Revoke public policy grants that are no longer needed, rotate or delete tenant app credentials through the public app APIs where possible, and remove temporary wildcard-resource temporary paths. If the tenant still has public-read buckets, host aliases, object links, indexes, watches, task leases, append streams, or PersonalDB groups, decide whether each must be drained, archived, retained, or deleted under your retention policy.

For object and bucket removal, use tenant-owned APIs when the tenant is responsible for the data:

```bash
anvil bucket rm documents
```

This can fail if the bucket is not empty or if retained uploads and objects still exist. That failure is useful: it prevents an operator or tenant automation from deleting a namespace while data remains. Remove or archive objects according to policy, then retry. Do not use direct storage edits to make the bucket disappear.

Keep audit evidence from both planes. Admin provisioning, admin policy grants, admin policy revokes, and admin repairs write admin audit events with the supplied audit reason. Tenant-owned public operations write tenant audit events where the service records them. During offboarding, collect the tenant id, app names, grant list, public-read state, bucket list, host aliases, and final deletion or retention decision. [Repair and Diagnostics](/operators/repair-and-diagnostics/), [Security Hardening](/operators/security-hardening/), and [Public CLI](/reference/public-cli/) describe the supporting surfaces.

## Current gaps to plan around

The current implementation is usable for handover, but not every least-privilege shape is exposed cleanly through the public CLI. The most visible gap is public object mutation: the CLI discovers `bucket_id` through `ListBuckets`, and `ListBuckets` currently requires `bucket:list` on `*`. That blocks a clean CLI-only upload profile for one bucket or one prefix. Direct API clients can work around it by retaining the bucket id, while the product should eventually expose a narrower bucket lookup or accept a safer mutation context flow.

The public CLI upload path also lacks flags for `content_type` and `user_metadata_json`. Some public scopes are broader than ideal, especially bucket listing, object listing, tenant app management, and some index read/query paths. Tenant `--home-region` handling has an implementation mismatch as described earlier, and region activation/placement workflows still need operator care. Treat these as documented constraints when writing runbooks, not as reasons to use global wildcard grants or direct storage writes.

## Handover evidence

A complete handover gives the tenant owner a client id, the one-time client secret, the public endpoint, the initial grants, and the bucket or region assumptions. It should also record which admin principal created the tenant and app, which audit reasons were used, and which wildcard grants are temporary.

After handover, prove that the tenant owner can mint a token and perform only the intended public operations. A good first check is `anvil auth get-token`, `anvil bucket ls`, and one narrow object write/read under the prefix the app is supposed to own.
