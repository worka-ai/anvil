---
title: Static Hosting and Aliases
description: Serve object-backed sites and downloads through host aliases while keeping routing, public access, and movable object links explicit.
---

# Static Hosting and Aliases

This tutorial continues from [Buckets and Objects](/tutorials/buckets-and-objects/), [Public Access](/tutorials/public-access/), [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/), and [S3-Compatible Gateway](/tutorials/s3-gateway/). It assumes you understand the `documents` bucket, object keys, bucket public-read, and object links as aliases rather than copies.

Static hosting in Anvil is object delivery through a gateway route. A request arrives with a host and a path, Anvil resolves that host and path to a tenant, bucket, region, and object key, then the ordinary object read rules decide whether bytes can be returned. That means static hosting is convenient, but it is not a bypass around tenants, bucket policy, object authorisation, reserved prefixes, or the private admin API.

The API is the primary product surface. Application deployment tools should normally call the public Object API to upload assets, create links, and manage tenant-owned host aliases. The `anvil` CLI commands in this page are supporting manual helpers over that public API. Operators use the private admin CLI only for system-side routing, host-alias lifecycle, repair, and mesh administration. For background, read [Gateways](/learn/gateways/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## Understand host routing before creating records

Anvil has three related URL shapes for object delivery.

A **path-style regional route** puts the region in the host and the tenant and bucket in the path:

```text
https://local.anvil.test/acme/documents/sites/www/index.html
  -> tenant: acme
  -> bucket: documents
  -> region: local
  -> key: sites/www/index.html
```

A **virtual-host regional route** puts the bucket, tenant, and region in the host:

```text
https://documents.acme.local.anvil.test/sites/www/index.html
  -> tenant: acme
  -> bucket: documents
  -> region: local
  -> key: sites/www/index.html
```

A **custom host alias** stores a hostname, tenant id, bucket, region, and key prefix. A request to that hostname joins the alias prefix with the request path:

```text
https://docs.example.test/assets/site.css
  host alias: docs.example.test -> tenant acme, bucket documents, prefix sites/www/
  -> key: sites/www/assets/site.css
```

These routes are only routing decisions. They do not make a private bucket public, do not create DNS records, do not issue TLS certificates, do not configure a CDN, and do not grant write access. The backing object still has to be readable under Anvil's rules. For a browser-facing public site, that usually means a dedicated public-read bucket or another deliberately public delivery policy. For private sites, clients still need an authenticated surface that can present credentials.

The current S3/static HTTP gateway uses `PUBLIC_REGION_BASE_DOMAIN` to enable host-based routing at serve time. If this setting is empty, the gateway still handles ordinary path-style S3 requests, but host alias routing is not enabled. When Anvil sits behind a reverse proxy or load balancer, configure `TRUSTED_PROXY_SOURCE_RANGES` so Anvil only trusts forwarded host and scheme metadata from known proxies.

## Know what tenant and operator ownership mean

Tenant-owned host aliases are public API records exposed through `ObjectService.CreateHostAlias`, `VerifyHostAlias`, `ReadHostAlias`, `ListHostAliases`, and `DeleteHostAlias`. The public CLI exposes those calls as `anvil host-alias ...`. A tenant principal can create, verify, read, list, and delete aliases for buckets in its own tenant when it has the right bucket scopes.

Operator host-alias commands exist too, but they are for system responsibilities: migration, repair, suspension, lifecycle intervention, and routing projection work. They run through the private admin API and require system-realm authority such as host-alias management. Do not use the admin API to update a tenant's `latest` link, upload site assets, or avoid a missing tenant scope. Keep tenant content and tenant-owned aliases on the public API.

Object links are tenant data, not routing records. A link is a symlink-like object alias inside a bucket:

```text
sites/www -> sites/www/index.html
releases/latest.tar.gz -> releases/app-1.0.1.tar.gz
```

Links are useful with static hosting because the host alias can stay stable while tenants move default pages, latest downloads, or release channels. Updating a link does not copy the target payload. Deleting a link removes the alias, not the target object.

## Check the prerequisites honestly

The earlier local tutorial chain deliberately leaves some operations illustrative. Region activation may still be blocked by the current activation-checkpoint documentation gap, and host alias creation requires an active region. The local container start command also does not set `PUBLIC_REGION_BASE_DOMAIN`, so serving custom hostnames requires starting Anvil with that configuration or running a deployment that already has it.

For the public API examples below, the tenant principal needs narrow scopes rather than wildcard grants:

| Operation | Public policy scope checked today |
| --- | --- |
| Create, verify, or delete a host alias for `documents` | `bucket:write` on `documents` |
| Read or list visible host aliases for `documents` | `bucket:read` on `documents` |
| Upload static asset objects | `object:write` on each key or a narrow site prefix such as `documents/sites/www/*` |
| Read private static assets through a signed request | `object:read` on each key or a narrow site prefix |
| Create or update object links | `object:write` on the link key |
| Read link descriptors | `object:read` on the link key |
| Delete object links | `object:delete` on the link key |

A browser-facing public site also needs public-read on the backing bucket or another delivery path that can authenticate readers. Public-read is bucket-wide in the current public CLI, so use a dedicated bucket for production public sites whenever possible. This tutorial keeps the `documents` bucket only to continue the earlier pages.

## Write site assets as ordinary objects

A static site asset is just an object body at a predictable key. Application code should use `ObjectService.PutObject` or a matching client library so it can set content type, metadata, idempotency, and preconditions deliberately. The current public CLI `object put` is a simple helper: it uploads bytes, but it does not expose content-type flags and may still hit the least-privilege bucket lookup gap described in [Buckets and Objects](/tutorials/buckets-and-objects/).

Create a tiny site locally:

```bash
mkdir -p site/assets
cat > site/index.html <<'HTML'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Anvil static site</title>
    <link rel="stylesheet" href="https://docs.example.test/assets/site.css">
  </head>
  <body>
    <h1>Hello from Anvil</h1>
    <p>This page is stored as Anvil objects and served through host routing.</p>
  </body>
</html>
HTML

cat > site/assets/site.css <<'CSS'
body { font-family: sans-serif; margin: 3rem; }
h1 { color: #17324d; }
CSS
```

Those commands do not contact Anvil. They only prepare files that will become object bodies.

Upload them with the public CLI when your bucket, region, and grants are ready:

```bash
anvil --profile acme object put site/index.html s3://documents/sites/www/index.html
anvil --profile acme object put site/assets/site.css s3://documents/sites/www/assets/site.css
```

A successful upload proves the caller can write those object keys and that Anvil committed current object versions. It does not prove the files have browser-friendly MIME types, that the bucket is public, that a host alias exists, or that an index/default-page route is configured. For production static sites, use the public API or S3 gateway upload path to set `text/html`, `text/css`, cache metadata, and any deployment metadata your operators inspect.

## Use a link for the root page

The current gateway does not implement a web-server-style automatic `index.html` fallback. If a host alias has prefix `sites/www/`, a request for `/assets/site.css` maps to `sites/www/assets/site.css`, and a request for `/` maps to `sites/www`. To serve the homepage at `/`, create a link at that key that points to the actual page object.

```bash
anvil --profile acme object link create \
  s3://documents/sites/www \
  s3://documents/sites/www/index.html \
  --resolution follow
```

A successful command proves the caller can write the link key `sites/www`, the target exists unless you explicitly allowed a dangling link, and Anvil created a link descriptor with generation `1`. It does not copy `index.html`. Reads through `sites/www` follow the link to the target body, and the followed response ETag is link-aware so caches can notice when the alias view changes.

Use the link read helper to inspect the descriptor rather than downloading the page body:

```bash
anvil --profile acme object link read s3://documents/sites/www
```

This proves the caller can read the link metadata. It does not prove the caller can read the target body; followed reads of private buckets also check target-side read visibility.

## Use latest-file links for downloads

Static hosting is not only for HTML. A common pattern is a stable download path whose target moves when a release is promoted.

```bash
printf 'release-v1\n' > app-1.0.0.tar.gz
printf 'release-v2\n' > app-1.0.1.tar.gz

anvil --profile acme object put app-1.0.0.tar.gz s3://documents/releases/app-1.0.0.tar.gz
anvil --profile acme object put app-1.0.1.tar.gz s3://documents/releases/app-1.0.1.tar.gz

anvil --profile acme object link create \
  s3://documents/releases/latest.tar.gz \
  s3://documents/releases/app-1.0.0.tar.gz \
  --resolution follow
```

The link starts at generation `1`. When you promote the next version, update the link with the generation you read. That generation is a compare-and-swap token for the alias itself; it prevents two release jobs from silently moving `latest.tar.gz` over each other.

```bash
anvil --profile acme object link update \
  s3://documents/releases/latest.tar.gz \
  s3://documents/releases/app-1.0.1.tar.gz \
  --expected-generation 1 \
  --resolution follow
```

If the generation has changed, the update fails and the safe response is to read the link again and decide whether your release should still win. That failure is useful race detection, not a generic storage outage.

The public CLI can create live links to the current target key. It does not currently expose a `--target-version` flag for pinned links. Use the API or Rust client when a public download must always mean a specific historical object version.

## Decide between follow, metadata, and redirect

For static hosting, use `--resolution follow` today. A followed link lets a normal object `GET` or `HEAD` return the target bytes through the alias. The S3/static gateway also adds link headers such as `x-anvil-object-kind`, `x-anvil-link-key`, `x-anvil-link-generation`, and `x-anvil-link-target-version` when it follows a link.

Metadata reads are a diagnostic mode. The native public CLI exposes them with `anvil object link read`. The S3/static gateway also has an Anvil-specific `x-anvil-link-mode: metadata` request header that returns the link descriptor JSON instead of following the link.

Redirect links are modelled in the API and the public CLI accepts `--resolution redirect`, but current native object reads and the current S3/static gateway do not turn that into an HTTP `3xx` redirect. They operate in follow mode and reject redirect links with a precondition error. Do not rely on redirect links for public downloads or browser static hosting until the delivery surface you use explicitly implements redirect behaviour.

## Create a tenant host alias

A host alias maps one hostname to one tenant bucket and optional prefix. In API terms, the tenant calls `CreateHostAliasRequest` with `hostname`, `bucket_name`, `region`, `prefix`, and a `PublicMutationContext`. In CLI form the current command shape is positional:

```bash
anvil --profile acme host-alias create docs.example.test documents \
  --region local \
  --prefix sites/www/
```

A successful create prints a descriptor like `docs.example.test -> documents/sites/www/ (pending_verification, generation 1)` plus a `verification_challenge=...` line. It proves the caller authenticated, the bucket belongs to the caller's tenant, the caller has `bucket:write` on `documents`, the selected region exists and is active, the region has a usable virtual-host suffix, and the hostname does not overlap Anvil's native regional host forms.

It does not prove DNS exists, TLS is configured, the alias is active, the bucket is public, or the current server process has `PUBLIC_REGION_BASE_DOMAIN` set for serving host-routed requests.

## Verify and inspect the alias

New public host aliases start in `pending_verification`. The descriptor includes a deterministic challenge string. In a real custom-domain workflow, your automation should publish or observe that challenge through the domain-control mechanism your platform requires, such as DNS or edge configuration, before asking Anvil to activate the alias.

The current public API does not fetch DNS by itself. `VerifyHostAliasRequest` accepts an `observed_challenge` string and compares it with the expected challenge. That means the caller or surrounding automation is responsible for honestly observing the domain-control value before passing it to Anvil.

```bash
anvil --profile acme host-alias read docs.example.test

anvil --profile acme host-alias verify docs.example.test \
  "<verification_challenge from create/read output>" \
  --expected-generation 1
```

The read command proves the caller can see alias metadata for its tenant and has `bucket:read` for the backing bucket. The verify command proves the supplied challenge matched, the expected generation was still current, and Anvil transitioned the alias to `active`. It does not create a DNS record or certificate.

List aliases when you need an operator or tenant administration view:

```bash
anvil --profile acme host-alias list --region local --limit 50
```

The list is tenant-scoped and filtered by bucket read authority. It is not a global routing dump. Operators who need system-wide routing records should use the admin routing and host-alias commands on the private admin API.

## Test delivery with the actual host

Once the alias is active, a request must arrive at the public gateway with the alias hostname preserved in the `Host` header. For a local smoke test, `curl --resolve` can send `docs.example.test` to the local public listener without changing public DNS:

```bash
curl -i --resolve docs.example.test:50051:127.0.0.1 \
  http://docs.example.test:50051/

curl -i --resolve docs.example.test:50051:127.0.0.1 \
  http://docs.example.test:50051/assets/site.css
```

If the bucket is public-read and the gateway has host routing enabled, the first request should read the `sites/www` link and return the `index.html` body, and the second should return `sites/www/assets/site.css`. That proves host alias routing, prefix joining, object reads, and public-read delivery are working through the same public listener.

If the bucket is private, an unsigned browser-style request should fail. That is correct. A host alias is a route, not an authorisation grant. If the request fails unexpectedly, check the alias state, `PUBLIC_REGION_BASE_DOMAIN`, the incoming `Host` header, trusted proxy settings, bucket public-read, object existence, link target health, reserved-prefix rejection, and region routing before changing credentials.

## Delete aliases deliberately

Tenant principals can delete their own host aliases through the public API when they have `bucket:write` on the backing bucket. Deletion is generation-checked.

```bash
anvil --profile acme host-alias delete docs.example.test --expected-generation 2
```

A successful delete proves the caller can mutate that alias and that the generation still matched. It stops Anvil from treating the alias as active once requests observe the updated routing state. It does not remove DNS records, revoke certificates, purge CDN caches, delete site objects, or delete object links. Clean those up through the systems that own them.

The public CLI does not expose suspend/reactivate operations. Suspension is an operator lifecycle action in the current admin CLI, useful for incident response, migration, or policy intervention without deleting the alias descriptor.

## Current limitations to design around

Current static hosting is exact object delivery through host routing; it is not a full web server. There is no automatic `index.html` fallback, directory redirect, SPA fallback, cache-control policy engine, DNS automation, TLS certificate automation, CDN invalidation, or MIME-type inference in the public CLI. Store default pages with explicit links, set content types through the API or S3 upload path, and run cache/TLS/DNS automation outside Anvil.

Host alias serving requires both durable alias state and runtime routing configuration. A region descriptor's `virtual_host_suffix` is used when creating aliases, while the server needs `PUBLIC_REGION_BASE_DOMAIN` for host-route parsing at the gateway. The earlier local Docker tutorial does not set that variable, and the mesh tutorial currently stops short of region activation, so the commands here are illustrative until those deployment prerequisites are satisfied.

Verification is also deliberately narrow today: Anvil compares the challenge value it is given, but it does not independently query DNS. Treat that as an implementation gap in automated domain onboarding. Do not mark an alias active until your deployment has actually proved domain control.

Finally, keep public and private boundaries visible. Use public APIs for tenant assets, links, and tenant-owned aliases. Use the private admin API only for system routing lifecycle and repair. Do not expose the admin listener to make a website work, and do not put private and public objects in the same bucket unless the whole bucket is safe to publish.
