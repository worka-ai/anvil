---
title: Reads, Listing, and Links
description: Understand Anvil current reads, pinned reads, metadata, prefix listings, pagination, public visibility, object links, and gateway routing effects.
---

# Reads, Listing, and Links

Reading an object store sounds simple: ask for a key and get bytes back. In Anvil, that simple request still passes through several pieces of state. The server must resolve the tenant, bucket, route, current object pointer, optional version id, delete marker state, object metadata, link behaviour, public-read setting, and authorisation decision before it can return data.

That work is deliberate. Reads are where users notice lost updates, stale links, accidental public exposure, and confusing gateway routes. A developer building on Anvil should know what a read proves. An operator debugging a failed download should know whether the failure is a missing object, a private bucket, a deleted current version, a dangling link, a remote-region route, or a gateway limitation.

This page builds on [Object Model](/learn/object-model/) and pairs with [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/). For hands-on use, read [Buckets and Objects](/tutorials/buckets-and-objects/), [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/), [Public Access](/tutorials/public-access/), [S3-Compatible Gateway](/tutorials/s3-gateway/), and [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/). Command syntax is in [Public CLI](/reference/public-cli/), and permission/resource strings are in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

## The read path in one picture

A current object read follows roughly this shape:

```text
request arrives
  -> resolve route to tenant, bucket, region, and key
  -> reject reserved internal namespaces
  -> find the bucket and check public-read or caller authorisation
  -> read the current pointer for the key
  -> reject current delete markers as not found
  -> if the current entry is a link, apply link mode
  -> load the target object payload and metadata
  -> stream metadata, then bytes
```

A pinned version read changes one step: instead of reading the current pointer, it asks for a specific `version_id`. That version id names a committed state of the key. It does not bypass authorisation, region routing, reserved namespace checks, or delete-marker handling.

## Current reads and pinned reads

A **current read** asks, "what is visible at this key now?" In the native API that is `ObjectService.GetObject` with no `version_id`. In the S3-compatible gateway it is a `GET` without `versionId`. If the current pointer names a normal blob version, Anvil returns the blob. If the current pointer names a delete marker, Anvil behaves as though the object is not found.

A **pinned read** asks, "give me this exact committed version." In the native API, set `GetObjectRequest.version_id`. In the S3 gateway, use the S3-style `versionId` query parameter. Pinned reads are useful for package manifests, audit evidence, reproducible deployments, and user workflows that need to show the same file later even if the key moves on.

Pinned reads still fail when the caller cannot read the object, the bucket is in another region and the current surface cannot serve it, the version id is invalid, the version does not exist, or the requested version is a delete marker. A version id is an address inside Anvil's object history, not an access token.

There is an important link-specific caveat. Current reads can follow current link objects. Pinned reads do not generally turn an old link version into a target-body read in the native path. If you need link descriptor history, use link metadata APIs or version-listing diagnostics. If you need reproducible target bytes, create a link that records `target_version` through the API, or store the target object's version id in your own manifest.

## HEAD and GET are different tools

`GET` returns the object body. In the native API, the first streamed message is `ObjectInfo`, which includes content type, content length, version id, and user metadata JSON. Later messages contain byte chunks. In the S3 gateway, `GET` returns ordinary HTTP object bytes and headers such as `ETag`, `Content-Length`, `Content-Type`, and `x-amz-version-id`.

`HEAD` returns metadata without the body. It is the safer tool when a client wants to check the current version id, ETag, size, content type, or last-modified time before deciding what to do next. Native `HeadObject` returns more Anvil-specific evidence than `GetObject` metadata currently does: ETag, size, last-modified time, version id, mutation id, record hash, authorisation revision, index policy snapshot, content type, and user metadata JSON.

Metadata is still data. A key name, content type, custom metadata field, ETag, object size, or link target can reveal customer names, workflow state, release timing, or internal structure. Public-read and listing decisions must treat metadata exposure as real exposure, not as harmless decoration.

The current public CLI exposes small helpers:

```bash
anvil --profile acme object head s3://documents/tutorial/welcome.txt
anvil --profile acme object get s3://documents/tutorial/welcome.txt welcome.txt
```

These commands prove authenticated metadata and body reads through the public API. They do not prove anonymous public access, and the current CLI does not expose pinned-version flags. For version-aware application logic, use the API or a client library.

## Range and conditional reads at the gateway

Large downloads often need byte ranges: media playback, resumed downloads, package installers, and browser caches all use them. The S3/static HTTP gateway supports range-shaped reads over the object version it is serving and returns `Accept-Ranges: bytes` for ordinary object responses. The native gRPC `GetObject` streams chunks but does not expose a separate range request field today.

The gateway also evaluates HTTP-style read preconditions such as ETag and date conditions for supported S3 operations. Those checks happen after route resolution and object lookup. They are cache and transfer controls, not a replacement for Anvil write preconditions. For write-side race control, use the version and CAS mechanisms described in [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/).

## Prefix listing is string listing, not a filesystem

Anvil object keys are strings. A prefix such as `customers/acme/contracts/` is a convention chosen by the application. There is no parent directory object unless you create an object at that key.

`ObjectService.ListObjects` lists current, non-deleted entries whose keys start with a prefix. It accepts:

| Field | Meaning today |
| --- | --- |
| `bucket_name` | Bucket to list in the authenticated tenant or routed public context. |
| `prefix` | Only keys beginning with this string are considered. Reserved internal prefixes are rejected. |
| `delimiter` | Optional grouping string, commonly `/`, used to return `common_prefixes`. |
| `start_after` | Marker-style starting point; only keys lexicographically greater than this value are considered. |
| `max_keys` | Maximum number of returned entries, defaulting in service code when omitted or non-positive. |

With no delimiter, listing returns object summaries. With delimiter `/`, keys below the next slash are grouped into `common_prefixes`, similar to an S3 folder view. That view is still derived from object keys. Renaming a "directory" means writing new object keys and deleting old ones; there is no directory inode to move.

Current listings exclude current delete markers. If `docs/a.txt` was deleted by writing a delete marker, ordinary current listing does not show `docs/a.txt`. Version listing can still show the delete marker and earlier versions.

## Listing visibility and authorisation

Private-bucket listing has two layers of checks. First, the caller needs list authority for the bucket: current ordinary object listing checks `object:list` on the bucket name, not a narrow prefix resource. Secondly, Anvil filters returned objects by object-read visibility. The object-read decision can come from public policy scopes or relationship authorisation for the `object` resource and `reader` relation.

That means a caller can be allowed to list a bucket but still see only the objects it may read. It also means a caller without list authority cannot use prefix listing as a discovery tool even if it can read one known object.

Public-read buckets are different. Current public-aware object-manager paths bypass ordinary per-object read/list checks for public buckets while still rejecting reserved internal namespaces. If a bucket is public-read, assume object names, common prefixes, sizes, ETags, content types, user metadata, and version-listing information may be exposed by the public or gateway surfaces you enable. Use separate public buckets unless the whole bucket is safe to publish.

Object-link metadata has one current wrinkle. The tenant-facing `ReadObjectLink` and `ListObjectLinks` service methods check public policy scopes for the link key and list prefix. Ordinary object reads that follow a link use the normal object-read visibility path for the link and, in private buckets, the target. Keep link-management grants narrow, and do not assume link metadata checks are identical to followed target reads.

## Pagination and markers

Pagination in Anvil is currently marker based in several read surfaces. It is useful, but it is not a snapshot-isolation token.

Native `ListObjects` has `start_after` and `max_keys`, but the response does not include `is_truncated` or a next-page token. A simple caller can request a page, remember the last returned key, and pass it as `start_after` for the next page. That is adequate for manual tools and many batch jobs. It is not a durable opaque cursor bound to a consistent listing snapshot. If objects are added or deleted while you page, design the caller to tolerate movement.

Native `ListObjectVersions` is more explicit. It returns `is_truncated`, `next_key_marker`, and `next_version_id_marker`. The next request passes both markers back. This is the API to use when diagnosing version history, delete markers, and exact version ordering.

The S3 gateway maps S3 listing parameters onto these primitives. ListObjectsV2 uses `continuation-token` or `start-after`, and the gateway returns `NextContinuationToken` when a response is truncated. Version listing uses `key-marker` and `version-id-marker`. These gateway tokens are simple markers derived from keys and versions; they are not signed opaque tokens that prove the request shape, principal, index generation, or snapshot. Avoid documenting them as stronger than they are.

Tenant-owned link and host-alias list APIs accept a `PageRequest`, but current implementations truncate to `limit`, set `has_more`, and return an empty `next_cursor`. The public CLI also does not expose full pagination control for ordinary object listing. Treat that as a current API/CLI gap for large administration views.

## Version listing and delete markers

`ListObjectVersions` is the history inspection tool. It returns entries with the key, version id, ETag, size, last-modified time, content type, user metadata, whether the entry is the latest version, and whether it is a delete marker.

This distinction matters when a user says "the file disappeared". There are several different cases:

| Observation | What it usually means |
| --- | --- |
| Current `GET` returns not found and version listing has no key | The key has no visible history in that bucket. |
| Current `GET` returns not found and the latest version is a delete marker | The key was deleted without deleting all historical versions. |
| Pinned `GET` for an old version succeeds | The old version still exists and the caller can read it. |
| Pinned `GET` for a delete marker fails as not found | The version id names deletion state, not a readable blob. |
| Version listing is permission denied | The caller does not have the list authority needed for that bucket. |

There is no current public CLI command for `ListObjectVersions`. Use the API, an S3-compatible version listing request, or a client wrapper when history matters.

## Public reads are still Anvil reads

Public access is a bucket read policy, not a shortcut around Anvil. A public-read bucket can be read by anyone who can reach the relevant public surface, but reserved internal keys remain blocked, writes still require authenticated write authority, and the private admin API remains private.

The surface matters. The native gRPC `GetObject` path can serve anonymous object bodies for public buckets because the service accepts missing claims and the object manager checks public bucket state. Native `HeadObject`, `ListObjects`, and `ListObjectVersions` service methods currently require claims at the gRPC service boundary, although the object manager and S3/static gateway have public-aware read-side paths. The S3/static gateway can therefore expose public `GET`, `HEAD`, listing, and version-listing behaviour depending on the route and request.

Do not use the CLI as an anonymous public-read test. The CLI is profile-oriented and sends bearer tokens for its normal commands. To verify public access, send a request through the actual exposed surface without an Anvil bearer token, S3 signing key, client secret, or admin credential.

## Object links are aliases, not copies

An object link is an object-like metadata record inside a bucket. It has its own key, version history, content type, ETag, generation, creator, timestamps, target key, optional target version, and resolution mode. It does not duplicate the target payload.

This is the usual pattern:

```text
releases/latest.tar.gz  ->  releases/app-3.2.0.tar.gz
```

Reading `releases/latest.tar.gz` in follow mode returns the target bytes. Updating the link later moves the alias to a different target. Deleting the link deletes the alias, not `releases/app-3.2.0.tar.gz`.

A link can be **live** or **pinned**. A live link has no `target_version`, so it follows the target key's current version at read time. A pinned link records a specific target version, so it keeps resolving to that version even if the target key later changes. The public API supports `target_version`; the current public CLI `object link create` and `object link update` do not expose a target-version flag, so CLI-created links are live links today.

Links are same-bucket aliases in the current public API shape. The public CLI enforces this by requiring the link path and target path to use the same bucket.

## Follow, metadata, redirect, and dangling links

Link reads have different modes:

| Mode | Current behaviour |
| --- | --- |
| Follow | Ordinary current object `GET`/`HEAD` follows a current link when the link resolution is `follow`. Private buckets also check target read visibility before returning target bytes. |
| Metadata | `ObjectService.ReadObjectLink`, `anvil object link read`, and the S3/static header `x-anvil-link-mode: metadata` return the link descriptor instead of target bytes. |
| Redirect | The API model and CLI accept `resolution: redirect`, but current native object reads and the S3/static gateway do not emit HTTP `3xx` redirects for redirect links. They reject such reads with a precondition-style error. |
| Dangling | Create/update can allow a link whose target is absent. Following it fails later with `DanglingObjectLink` until the target exists. |

Followed link responses use a link-aware ETag derived from both the link generation and target version. The S3/static gateway also adds headers such as `x-anvil-object-kind: link`, `x-anvil-link-key`, `x-anvil-link-generation`, and `x-anvil-link-target-version` when it follows or returns link metadata. These headers help caches and diagnostics distinguish "I read the target directly" from "I read the target through an alias".

Link loops and excessive link chains are rejected. The current maximum resolution depth is eight. Normal link creation without `allow_dangling` requires the target to exist and be a blob, not another link. If you explicitly allow dangling links, you are taking responsibility for later resolution failures, loops, or non-blob targets until the target graph becomes healthy.

## Link generations are CAS tokens

A link generation is the compare-and-swap token for the alias. Creating a link starts at generation `1`. Updating the link writes a new link version and increments the generation. Deleting the link is also generation-checked and writes a delete marker for the link.

The public CLI exposes that guard for updates and deletes:

```bash
anvil --profile acme object link update \
  s3://documents/releases/latest.tar.gz \
  s3://documents/releases/app-3.2.1.tar.gz \
  --expected-generation 1

anvil --profile acme object link delete \
  s3://documents/releases/latest.tar.gz \
  --expected-generation 2
```

These commands prove that the caller can mutate the link key and that the generation still matches. They do not prove the target is immutable, that readers have target access, or that every gateway cache has forgotten the previous alias response.

## Host aliases and static delivery

A host alias is a routing record, not an object link. It maps a hostname to a tenant, bucket, region, and key prefix. When a request arrives at the S3/static gateway with that host, Anvil joins the alias prefix with the request path and then performs an ordinary object read.

For example:

```text
host alias: docs.example.test -> tenant acme, bucket documents, prefix sites/www/
request:    https://docs.example.test/assets/site.css
object key: sites/www/assets/site.css
```

The host alias does not make the bucket public. It does not grant read authority. It does not create DNS records, TLS certificates, CDN invalidations, or an `index.html` fallback. If the request is anonymous, the object must be readable through public-read behaviour. If the bucket is private, the request must be authenticated through a surface that supports the required credentials.

Object links and host aliases often work together for static delivery. If the gateway maps `/` to `sites/www` and there is no automatic directory index, a tenant can create a link at `sites/www` pointing to `sites/www/index.html`. Reads through the host then follow the link to the page body. That remains an object-model alias, not a web-server rewrite rule.

Host alias serving requires runtime configuration as well as records. The gateway needs host-route parsing configured, such as `PUBLIC_REGION_BASE_DOMAIN`, and reverse-proxy deployments must preserve trusted host and scheme information. Tenant-owned alias lifecycle is exposed through public `host-alias` commands and Object API methods; operator lifecycle and repair belong on the private admin API.

## Gateway differences to remember

Anvil's native API, S3 gateway, and static host routing share the object model, but they do not expose identical controls.

| Surface | Read-side strengths | Current gaps to remember |
| --- | --- | --- |
| Native Object API | Clear current and pinned reads, rich `HeadObject` evidence, structured version/list APIs. | CLI does not expose pinned reads or version listing; some anonymous read-side methods require claims at the service boundary. |
| S3 gateway | Familiar `GET`, `HEAD`, range reads, S3-style listing/version listing, host-shaped routing, public HTTP delivery. | S3 tokens are marker based; Anvil-specific link metadata uses `x-anvil-link-mode`; redirect links are not HTTP redirects today. |
| Public CLI | Useful manual helpers for head/get/list, links, public bucket policy, and host aliases. | Helpers are not full API coverage; object `head` omits version id in output, `get` cannot request a version, `ls` has no pagination flags, link commands cannot pin `target_version`. |
| Static host alias | Convenient public or authenticated object delivery under custom hosts. | Routing is not authorisation; no automatic index fallback, DNS/TLS automation, cache policy engine, or CDN purge. |

For production applications, prefer the API surface that exposes the evidence your workflow needs. Use CLI commands to smoke test and inspect, not as proof that every API precondition or version-aware path is covered.

## What to take forward

A current read follows the current pointer. A pinned read asks for a specific version. `HEAD` is the efficient way to inspect metadata and validators. Prefix listing is string listing over current, non-deleted entries, with marker-based paging rather than a universal snapshot token. Version listing is how you see delete markers and history. Public-read makes read-side data public through configured surfaces; it is not an admin bypass. Links are generation-checked aliases, not copies. Host aliases route requests to keys; they do not grant access.

If a read fails, keep those distinctions visible. "Not found" might mean no current object, a current delete marker, an absent pinned version, a dangling link, an unauthenticated private bucket, or a remote-region route. The fix depends on which layer made the decision.

## Diagnosing read/list/link disagreements

When `object get` succeeds but `object ls` omits the key, the object source record is probably present and the directory-derived view or list permission is the next place to inspect. When `object link read` shows a target but data reads through the link fail, separate target existence, target visibility, and link resolution mode. When S3 reads fail but native reads succeed, focus on gateway signing, host routing, metadata translation, or proxy configuration rather than object durability.

For support tickets, capture the exact bucket, key, optional version, link generation, caller app, region reached by the request, and whether the failing request was native public API, S3, or static HTTP. That evidence tells the next reader whether to use tenant diagnostics, index/directory repair, gateway operations, or admin routing diagnostics.
