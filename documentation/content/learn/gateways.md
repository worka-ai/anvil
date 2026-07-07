---
title: Gateways
description: Understand Anvil gateways as protocol adapters over the native API, object model, authorisation, routing, regions, and CoreStore-backed source records.
---

# Gateways

A gateway is a translator at the edge of Anvil. It lets an outside protocol speak to Anvil without making that protocol the source of truth. An S3 client can upload an object. A browser can fetch a static asset by host and path. A future package registry can speak the language expected by a package manager. In each case, the durable decision inside Anvil is still made in Anvil terms: storage tenant, bucket, object key, version, metadata, link, principal, public policy scope, relationship authorisation, region, and CoreStore-backed state.

That distinction is the centre of this page. Gateways are useful because ecosystems already have tools, SDKs, deployment systems, caches, and package managers. They are risky if you treat them as a second storage model or a weaker security boundary. The native public API remains the source contract. Gateways adapt requests into that contract when the protocol can express the operation safely.

Read this page with [Object Model](/learn/object-model/), [Authorisation](/learn/authorisation/), [Reads, Listing, and Links](/learn/reads-listing-and-links/), [Regions, Cells, and Nodes](/learn/regions-cells-and-nodes/), [S3-Compatible Gateway](/tutorials/s3-gateway/), [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/), [Package Gateway Foundations](/tutorials/package-gateway-foundations/), [Public CLI](/reference/public-cli/), [Admin CLI](/reference/admin-cli/), [Gateway Operations](/operators/gateway-operations/), and [Network and Ports](/operators/network-and-ports/).

## Native API first

Anvil's native API is the contract that exposes the whole model. It has first-class fields for object versions, compare-and-swap preconditions, idempotency, metadata, links, watches, index definitions, query catch-up, relationship-authorisation tuples, PersonalDB commits, repair, and administrative lifecycle. A gateway protocol may only have a subset of those ideas. S3 has buckets, keys, headers, ETags, version ids, and multipart uploads. HTTP static delivery has hosts, paths, methods, response headers, and cache behaviour. Package managers have their own names for repositories, packages, versions, tags, tokens, manifests, and blobs.

The safe design is to ask what the outside request means in native Anvil terms before deciding whether a gateway should handle it. A successful gateway request should be explainable as a normal Anvil operation:

```text
outside protocol request
  -> resolve the public listener, host, region, tenant, bucket, or gateway mount
  -> authenticate a protocol credential or identify an intentional public-read path
  -> map the request to an Anvil principal and resource
  -> run normal public policy and relationship-authorisation checks
  -> read or mutate CoreStore-backed source records
  -> translate the result back to the outside protocol
```

If the protocol cannot express the correctness condition, use the native API for that part of the workflow. For example, an S3 `PUT Object` can use ETag preconditions for simple overwrite protection, but it is not the same as a native fenced mutation tied to a task lease. A static `GET` can follow an object link, but it cannot create a new link generation with a compare-and-swap check. A package download URL can fetch bytes, but it cannot by itself prove a package ecosystem's version-solving, provenance, or yanking rules.

## The public listener is not the admin plane

The current server exposes the native public gRPC API and the S3/static HTTP gateway on the same public listener. Locally that is usually `API_LISTEN_ADDR`, such as `127.0.0.1:50051`. The server routes requests with a gRPC content type to the public gRPC services and other HTTP requests to the S3 gateway. That shared listener is convenient for operators because one application-facing endpoint can serve native clients, S3-compatible tools, and browser/static object reads.

The private admin API is separate. It is configured with `ADMIN_LISTEN_ADDR`, defaults to loopback, and is authorised through Anvil's system realm. Operators use it for tenant bootstrap, mesh and region lifecycle, routing repair, system diagnostics, and other control-plane work. Do not expose the admin listener merely because you want S3, static hosting, or package downloads to be reachable. If an operator deliberately binds the admin listener off loopback, the server requires `ALLOW_PUBLIC_ADMIN_LISTENER=true`, and that setting still assumes private networking and operator-only controls.

This split is not just a deployment preference. It is a security boundary. Tenant content publishing, S3 uploads, public downloads, host aliases, package artefacts, and object links belong on the public data plane. System routing repair, region activation, node lifecycle, and built-in admin relations belong on the private admin plane. A gateway should never become a shortcut around missing tenant scopes or relationship tuples.

## What S3 maps to today

The implemented S3-compatible gateway is the clearest example of an Anvil gateway. S3 access keys map to Anvil tenant app credentials: the app `client_id` is the S3 access key id, and the app `client_secret` is the S3 secret key. The gateway verifies AWS Signature Version 4 for signed requests, checks clock freshness, resolves the app to an Anvil principal, and then runs the normal Anvil authorisation path for the requested bucket or object. Unsigned `GET` and `HEAD` requests are allowed to reach the object handlers so deliberate public-read buckets can be served; unsigned writes and management operations are rejected.

The object-shaped parts of S3 map well:

| S3 concept | Anvil concept |
| --- | --- |
| Bucket | A tenant-scoped Anvil bucket. |
| Key | An Anvil object key. Slashes are naming convention, not real directories. |
| Object body | The payload of an Anvil object version. |
| `PUT Object` | A write that creates a new object version and moves the current pointer. |
| `GET` or `HEAD` | A read of the current version, or a pinned version when a version id is supplied. |
| ETag | Version content identity used by S3-style preconditions. |
| VersionId | Anvil object version id surfaced as S3 version metadata. |
| Prefix listing | Anvil object listing with Anvil visibility and reserved-prefix checks. |
| User metadata | Simple `x-amz-meta-*` string metadata and content type where supported. |
| Multipart upload | A gateway-managed upload session that commits an Anvil object version when completed. |

S3 does not define Anvil's full control plane. AWS IAM policies, S3 ACLs, bucket-policy JSON, lifecycle rules, notification configuration, object tags, CORS management, and S3 website configuration are not the source of truth for Anvil security or operations. Native-only features also stay native: relationship schemas and tuples, zookies, watches and cursors, rich typed metadata, typed/full-text/vector/hybrid indexes, PersonalDB, task leases, fenced mutations, repair, diagnostics, and mesh lifecycle.

That is why the S3 tutorial keeps repeating what each command proves. A successful S3 upload proves the gateway can verify the app credential, authorise the write, and commit an ordinary Anvil object version. It does not prove an index has caught up, a watch consumer has processed the change, a public-read policy exists, or a native fenced mutation occurred.

## Static hosting is object delivery with routing

Static hosting in Anvil is not a separate filesystem. It is HTTP object delivery through gateway routing. A request arrives with a host and path, Anvil resolves them to a tenant, bucket, region, and key, and the ordinary object read rules decide whether bytes or metadata can be returned. The current implementation uses the S3/static HTTP gateway path for this delivery, including object links and public-read checks.

When `PUBLIC_REGION_BASE_DOMAIN` is configured, Anvil can parse regional host shapes. A path-style regional route puts the region in the host and the tenant and bucket in the path:

```text
https://local.anvil.example/acme/documents/sites/www/index.html
  -> region local, tenant acme, bucket documents, key sites/www/index.html
```

A virtual-host regional route puts bucket, tenant, and region in the host:

```text
https://documents.acme.local.anvil.example/sites/www/index.html
  -> region local, tenant acme, bucket documents, key sites/www/index.html
```

A custom host alias stores a hostname, tenant id, bucket, region, and optional key prefix. If `docs.example.com` is an active alias for tenant `acme`, bucket `documents`, prefix `sites/www/`, then `/assets/site.css` maps to `sites/www/assets/site.css`. Host aliases have lifecycle state and generation: public tenant APIs create pending aliases, verify them with an observed challenge, read and list visible aliases, and delete them with generation checks. Operator/admin host-alias commands exist too, but they are for private routing lifecycle, repair, suspension, and recovery.

Routing does not grant data access. A custom domain does not make private data public, create object versions, write DNS records, issue certificates, or bypass reserved prefixes. A browser-facing site normally needs a dedicated public-read bucket or another deliberate authenticated delivery design. Public-read means anyone who can reach the public surface can read matching data; it is not an admin API exposure and it is not write authority.

## Links make names move without copying bytes

Gateways also expose the difference between an object and a name for an object. An Anvil object link is a symlink-like alias inside a bucket. It can make a stable path such as `releases/latest.tar.gz` follow a target such as `releases/app-1.4.2.tar.gz` without copying the target bytes. The link descriptor has its own generation so publishers can move the alias with a compare-and-swap check.

The S3/static gateway follows object links by default for `GET` and `HEAD`. When it follows a link, it can include Anvil link headers such as `x-anvil-object-kind`, `x-anvil-link-key`, `x-anvil-link-generation`, and `x-anvil-link-target-version`. It also supports the Anvil-specific request header `x-anvil-link-mode: metadata`, which returns the link descriptor instead of the target body.

This is native Anvil behaviour surfaced through a gateway. S3 copy operations are not links, and links are not copies. Deleting or updating a link changes the alias record, not the target object. A dangling link is possible only where the link API allows it; followed reads of a dangling link fail because there is no current target body to return.

## Regions and cross-region routing

A bucket has a home region. Gateway routing has to respect that because an HTTP request may arrive at a listener in the wrong region. The host and path identify where the caller wanted to go; the bucket locator tells Anvil where the bucket actually lives. The `CROSS_REGION_ROUTING_POLICY` setting chooses the local behaviour for remote buckets:

| Policy | Meaning at a high level |
| --- | --- |
| `redirect_preferred` | Prefer returning a redirect to the bucket's home region. |
| `proxy_preferred` | Proxy when the internal proxy path is available, otherwise redirect. |
| `proxy_required` | Proxy only; fail when proxying is unavailable. |
| `local_only` | Reject requests that should be served by another region. |

This is an operator routing decision, not a tenant permission decision. A redirect does not grant object access. A proxy does not skip authorisation. A local-only rejection does not mean the object is missing. It means the request reached a region that the current policy refuses to use for that bucket.

Reverse proxies introduce another subtlety. S3 signatures include host and scheme information, and static routing also depends on the effective host. Anvil therefore only trusts forwarded host and scheme metadata from peers configured in `TRUSTED_PROXY_SOURCE_RANGES`. Without that, a proxy can make requests fail signature verification or route as the wrong host.

## Gateway records and mounts

The current codebase contains internal gateway-store records for registry-shaped gateways. These records are CoreStore-backed foundations, not a public package registry product by themselves. They model repositories, blobs, mutable tags, upload sessions, credentials, gateway mounts, short-lived access tokens, and audit events. Gateway access tokens are capped by the internal maximum token TTL, currently 900 seconds, and credentials can be revoked by changing the credential record generation or revocation state.

A `GatewayMountRecord` is the internal routing shape for package/registry-style gateways. It contains the gateway family, hosts, path prefixes, mesh id, region, Anvil storage tenant id, relationship-authorisation scope, tenant id, registry instance id, default bucket, repository prefix, state, generation, and record hash. Mount resolution can match exact host aliases, virtual-host regional names, or path-style regional prefixes. Mount state can be active, disabled, or draining.

Do not confuse those internal records with a tenant-facing package-registry API. The S3/static gateway currently uses the object routing and host-alias model described earlier. Package gateway mount and credential management is not exposed through a public CLI or public package service today. The records are important because they show how future gateways should be made durable and auditable through Anvil primitives rather than through side databases.

## Package gateways are foundations today

Package and registry gateways are a product direction, but current support is foundational. Anvil can already store package-shaped data well: artefact bytes as objects, immutable version manifests as JSON objects, checksums in metadata or manifests, movable channels as object links, typed catalogue indexes as derived data, and public/private download policy through normal authorisation. The [Package Gateway Foundations](/tutorials/package-gateway-foundations/) tutorial shows that modelling approach.

What is not implemented today is equally important. The repository does not expose Docker Registry v2, npm, PyPI, Maven, Cargo, or other package-manager protocol endpoints as a tenant-facing gateway. There is no public `anvil package` or `anvil registry` CLI, no public gateway-mount management workflow, no public registry token challenge endpoint, and no package-manager-specific upload session exposed to clients. S3 is an implemented gateway; package registries should not be described as implemented until their protocol handlers and public surfaces exist.

This does not make the foundations useless. It means application teams can build honest package-like workflows on native Anvil objects today, while future gateway handlers should translate package protocol calls into the same object, link, metadata, index, credential, and audit primitives.

## What stays native

The boundary between a gateway and the native API is easiest to see by asking whether the outside protocol can carry the evidence Anvil needs. If it cannot, keep that operation native.

| Need | Prefer the native surface |
| --- | --- |
| Write with idempotency, object-version preconditions, or a fenced mutation context | Object API, task lease, and fenced mutation APIs. |
| Create or update object links with generation checks | Object link API or public CLI link helpers. |
| Store rich metadata or typed fields for query | Native object API and index definitions. |
| Maintain derived data from cursors | Watch APIs and index/query APIs. |
| Check relationship tuples, usersets, zookies, or schema bindings | Authz APIs and public authz CLI helpers. |
| Query typed, full-text, vector, or hybrid indexes | Native Index API. |
| Work with PersonalDB groups, commits, heads, replicas, and projections | PersonalDB APIs and their current CLI helpers. |
| Diagnose or repair system state | Public repair APIs where available, or private admin APIs for operator-only work. |
| Manage mesh regions, cells, nodes, routing records, and bucket placement | Private admin API and admin CLI. |

A gateway can still participate in those workflows as a byte-moving or read-serving surface. For example, an S3 client may upload a PDF, a native worker may set typed metadata and publish an index-aware object record, a watch consumer may update a derived catalogue, and a static host alias may serve a public rendition. Each step should use the surface that can express its correctness requirements.

## Security and operational expectations

A gateway must preserve Anvil's security model. It must reject reserved internal object prefixes such as `_anvil/`, map credentials to normal principals, check public policy scopes and relationship visibility, and keep tenant data-plane operations separate from private admin operations. Public-read must be deliberate and auditable, because public means public to anyone who can reach the relevant public surface.

Operators should also treat gateway observability as part of the boundary. Logs and metrics should identify the request id, gateway family, mapped tenant, bucket or repository, operation, result, latency, and authorisation outcome. They should not log secrets, signatures, bearer tokens, or object bodies. Gateway diagnostics are evidence about the adapter path; they do not prove that every derived view, index, CDN cache, or downstream consumer has processed the same event.

Repair follows the same source/derived split as the rest of Anvil. Source records live in Anvil's durable model. Derived gateway state, indexes, static route projections, and caches should be rebuildable or repairable from those records. If a workflow requires an operator to mutate state, it belongs in an explicit repair or admin path, not in an undocumented protocol side effect.

## Current protocol support and gaps

Today, the reliable public gateway story is object-focused. The native public API is implemented. The S3-compatible gateway is implemented on the public listener and supports common object operations such as signed upload, read, head, list, delete, copy, range reads, multipart upload, simple user metadata, version ids, and public-read `GET`/`HEAD` paths. Static object delivery and custom host aliases build on the same HTTP gateway and routing model. Tenant host-alias APIs and CLI helpers exist, and private admin host-alias/routing commands exist for operators.

The current gaps are not cosmetic. S3 compatibility is partial rather than full AWS S3: AWS IAM policy documents, ACLs, lifecycle rules, notifications, tags, CORS, and website configuration are not Anvil's control plane. Static hosting is direct object delivery by route; richer web-server behaviours such as automatic index fallback or redirect-link delivery should be checked against the current static-hosting tutorial and implementation before relying on them. Package registry gateways are not currently exposed as implemented protocol surfaces. Public gateway mount and package credential management are not exposed. Some routing and cross-region behaviour depends on deployment configuration and internal proxy availability.

Design with those limits visible. Use gateways where they help existing tools reach Anvil's object model. Use the native API when the application needs Anvil-specific state, consistency, authorisation, watches, query, or repair. Keep the admin plane private. Treat unsupported protocols as future work, not as hidden features.

## What to take forward

Gateways make Anvil easier to adopt without changing what Anvil is. They translate familiar protocol requests into the native storage, identity, authorisation, routing, and CoreStore model. S3 and static object delivery are implemented adapters. Package/registry support is currently a foundations and modelling story, not a set of production package-manager endpoints. The safest architecture keeps gateway traffic on the public data plane, operator lifecycle on the private admin plane, and durable truth in Anvil's source records rather than in protocol-specific side stores.
