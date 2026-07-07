---
title: S3-Compatible Gateway
description: Use existing S3 tools while keeping Anvil as the authoritative object, identity, and authorisation model.
---

# S3-Compatible Gateway

This tutorial continues from [Buckets and Objects](/tutorials/buckets-and-objects/) and [Public Access](/tutorials/public-access/). It assumes you understand the `documents` bucket, object keys such as `tutorial/welcome.txt`, public policy scopes, and the current local tutorial limits around region activation and least-privilege CLI uploads.

The S3-compatible gateway lets existing S3 tools talk to Anvil. It is useful for import jobs, backup tools, migration scripts, and SDKs that already know how to upload, download, list, copy, range-read, and multipart-upload S3 objects. It is not a second storage system and it is not the security model. A gateway request is still translated into an Anvil tenant, bucket, object key, principal, public policy scope, relationship check, object version, and CoreStore-backed write or read.

Use the native public API when you control the client and need Anvil-specific features such as rich metadata, indexes, watches, PersonalDB, relationship schemas and tuples, task leases, fenced mutations, repair, or diagnostics. Use the S3 gateway when the client is already S3-shaped and the operation is object movement or S3-style object inspection. For the surrounding model, read [Gateways](/learn/gateways/), [Authorisation](/learn/authorisation/), [Public CLI](/reference/public-cli/), [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/), and [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/).

## Understand what is being exposed

The current Anvil server serves the native public gRPC API and the S3 gateway from the same public listener. In the local tutorials that listener is `http://127.0.0.1:50051`; there is not a separate local S3 port such as `9000`. Requests with a gRPC content type go to the native public API. Other HTTP requests are routed to the S3 gateway.

That shared public listener is deliberate. It means an operator can expose the application-facing surface once and let native API clients, S3-compatible tools, and static delivery routes use it according to their protocol. It does not mean the private admin API becomes public. The admin listener is configured separately with `ADMIN_LISTEN_ADDR`, defaults to loopback, and the server rejects a non-loopback admin bind unless `ALLOW_PUBLIC_ADMIN_LISTENER=true` is set. Do not set that flag merely to make S3 work; if you ever use it, the admin port must still be protected by private networking and operator-only controls.

In a multi-region or host-routed deployment, additional configuration can affect how S3 requests arrive. `PUBLIC_REGION_BASE_DOMAIN` enables region-aware virtual-host routing, `TRUSTED_PROXY_SOURCE_RANGES` controls which reverse proxies may supply forwarded host and scheme metadata, and `CROSS_REGION_ROUTING_POLICY` decides whether a request for a bucket homed in another region is redirected, proxied, or rejected locally. Those settings are gateway routing and operator concerns. They do not replace tenant authorisation.

## How S3 requests become Anvil requests

An S3 request carries an access key id, a secret key signature, a bucket name, an object key, headers, and query parameters. The Anvil gateway verifies AWS Signature Version 4 for signed requests. The access key id is the Anvil app `client_id`; the secret key is that app's `client_secret`. After signature verification, Anvil builds the same kind of principal context used by public API calls and checks the operation against the app's public policy scopes and any relationship-authorisation paths involved in object visibility.

Unsigned S3 `GET` and `HEAD` requests are allowed to reach the handlers so public-read buckets can be served. Other unsigned S3 operations, such as `PUT`, `DELETE`, multipart upload, and bucket management, are rejected before they can mutate state. Public-read is therefore a read policy, not a write bypass. A private bucket still requires a signed request and a successful authorisation decision before data is returned.

The mapping is intentionally object-focused:

| S3 idea | Anvil model |
| --- | --- |
| Bucket | Tenant-scoped Anvil bucket. |
| Key | Anvil object key. Slashes are part of the key, not real directories. |
| PUT Object | New Anvil object version and current-pointer update. |
| GET or HEAD Object | Read the current version, or a requested version id. |
| ETag | Version content identity used for S3-style preconditions. |
| VersionId | Anvil object version id returned as `x-amz-version-id`. |
| Prefix listing | Anvil prefix listing with Anvil authorisation and reserved-prefix checks. |
| S3 user metadata | Simple string values from `x-amz-meta-*` headers. |
| Multipart upload | Anvil multipart upload session, parts, completion, and abort. |

What does not map cleanly is just as important. S3 ACLs, AWS IAM policies, S3 bucket-policy documents, lifecycle rules, notification configuration, object tags, and CORS management are not Anvil's core control plane. Do not design production security around those AWS-specific features unless the current Anvil gateway explicitly exposes the operation you need. In Anvil, use public policy scopes and relationship authorisation as the source of truth.

## Prepare an app credential for S3 signing

Production applications normally create and rotate app credentials through the public API. The CLI command below is a manual helper for the same Auth API. It creates a tenant-owned app credential that can be used as an S3 access key pair after you grant it narrow public policy scopes.

```bash
anvil --profile acme app create s3-uploader
```

A successful command prints `app_id`, `app_name`, `client_id`, and `client_secret`. The `client_id` becomes `AWS_ACCESS_KEY_ID`, and the `client_secret` becomes `AWS_SECRET_ACCESS_KEY`. This proves that the `acme` profile can create a tenant app credential. It does not grant that app access to any bucket or object by itself.

If the command fails with permission denied, the current profile lacks `app:create` for the tenant resource. Have an already-authorised tenant owner or provisioning workflow create the credential. Do not use the bootstrap system-admin credential as a general S3 credential.

The S3 app then needs the exact public policy scopes for the operations it will perform. The examples in this tutorial use one object key, `documents/s3/hello.txt`, plus bucket-level listing where needed. The active profile that runs these grant commands must itself be allowed to delegate the same actions; public delegation is non-escalating.

```bash
export S3_APP_ID="<app_id from anvil app create>"

anvil --profile acme auth grant "$S3_APP_ID" bucket:read documents
anvil --profile acme auth grant "$S3_APP_ID" object:write documents/s3/hello.txt
anvil --profile acme auth grant "$S3_APP_ID" object:read documents/s3/hello.txt
anvil --profile acme auth grant "$S3_APP_ID" object:list documents
```

These commands are real public CLI commands, but they are illustrative until your tutorial tenant, bucket, region placement, and delegation scopes are ready. `bucket:read` lets S3 tools perform bucket metadata checks such as `HeadBucket`, `GetBucketLocation`, and `GetBucketVersioning`. `object:write` allows the upload to the exact key. `object:read` allows `GET` and `HEAD` for that key. `object:list` is currently checked against the bucket name for prefix listings, so it cannot be narrowed to only `s3/` through the ordinary object-list path today.

The commands do not grant delete access, write neighbouring keys, manage bucket policy, create indexes, read authz tuples, or call the admin API. Add those only when the app genuinely needs them.

## Point an S3 client at the Anvil public listener

Most S3 clients need four pieces of configuration: endpoint, access key id, secret key, and signing region. The local tutorials use the public Anvil listener as the S3 endpoint.

```bash
export ANVIL_S3_ENDPOINT="http://127.0.0.1:50051"
export AWS_ACCESS_KEY_ID="<client_id from anvil app create>"
export AWS_SECRET_ACCESS_KEY="<client_secret from anvil app create>"
export AWS_DEFAULT_REGION="local"
```

This configuration does not contact Anvil. It only prepares your shell so `aws` can sign S3 requests. The region value is part of the SigV4 signature that the client and gateway verify; bucket placement remains Anvil placement, not an AWS region selection.

If a signed request fails with an authorisation or signature error, check the obvious causes first: wrong endpoint, wrong access key, wrong secret, clock skew outside the SigV4 freshness window, a reverse proxy changing the effective host without being listed in `TRUSTED_PROXY_SOURCE_RANGES`, or missing public policy scopes for the requested bucket/key.

## Upload an object through S3

Create a small local file and upload it with an S3-compatible client. The `aws s3 cp` command is convenient here, but any client that emits supported S3 requests and SigV4 headers should exercise the same gateway path.

```bash
printf 'hello from the Anvil S3 gateway\n' > s3-hello.txt
aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3 cp s3-hello.txt s3://documents/s3/hello.txt
```

A successful upload proves that the S3 client could sign the request, the gateway could verify the app credential, the app had `object:write` for `documents/s3/hello.txt`, the bucket existed in the caller's tenant, and Anvil committed a new object version. The response also includes an ETag and an Anvil version id translated into S3 headers.

It does not prove that typed metadata was written, that an index has caught up, that a watch consumer has processed the change, or that the object is public. It is a write through a compatibility adapter into the normal Anvil object model.

You can inspect the same object through the native public CLI if your profile has read access:

```bash
anvil --profile acme object head s3://documents/s3/hello.txt
```

That `head` command calls the native object API, not S3. A successful result proves the S3 upload produced an ordinary Anvil object that native clients can see. If the command fails because the profile lacks read scope, that does not invalidate the S3 upload; it only means the profile you used for inspection is not authorised to read that key.

## Download, head, and range-read

S3 `GET Object` and `HEAD Object` map to Anvil object reads. Reads return the stored content type, content length, ETag, `x-amz-version-id`, and any simple S3 user metadata. Range reads are supported with the standard `Range` header.

```bash
aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3 cp s3://documents/s3/hello.txt s3-download.txt

aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api head-object --bucket documents --key s3/hello.txt

aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api get-object --bucket documents --key s3/hello.txt \
  --range bytes=0-4 s3-range.txt
```

The first command proves a signed S3 download can read the object body. The second proves a metadata-only read can observe the current version without downloading bytes. The third proves the gateway can serve a byte range from the stored object. These commands do not prove anonymous access; they still carry the app credential from the environment.

S3 read preconditions such as `If-Match`, `If-None-Match`, `If-Unmodified-Since`, and `If-Modified-Since` are evaluated against the object ETag and last-modified time. Write-side S3 ETag preconditions are currently supported for `PUT Object` with `If-Match` and `If-None-Match`, and copy-source preconditions are supported for `CopyObject`. Use the native API when you need Anvil's richer mutation context, idempotency key, or fenced write semantics.

## Understand keys, prefixes, and listing

S3 keys often look like paths, but Anvil stores them as object keys. `s3/hello.txt` is one key containing a slash. There is no real directory named `s3` unless your application treats the prefix that way. S3's `prefix` and `delimiter` options are listing conventions layered on top of key ordering.

```bash
aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api list-objects-v2 --bucket documents --prefix s3/ --delimiter /
```

A successful listing proves the app has `object:list` on `documents` and that the gateway can translate an S3 prefix listing into Anvil's object listing. For private buckets, Anvil still filters visibility according to object read authority and relationship checks. For public-read buckets, current gateway/object-manager behaviour allows unauthenticated read-side listing paths as well, so object names and metadata in a public bucket should be treated as public data.

Listing does not prove that every returned object body can be downloaded by every possible principal. It proves the listing principal can enumerate visible entries for that listing operation. If the result is empty, check the prefix, bucket name, tenant, object read visibility, and whether the object is hidden behind a delete marker.

Reserved internal prefixes remain blocked. Requests that try to read, write, copy from, or list Anvil-reserved internal keys are rejected by the S3 gateway before object lookup. Do not use `_anvil/` or other reserved internal namespaces for application data.

## Store simple S3 metadata

S3 user metadata is header-based. The current gateway stores `Content-Type` and string-valued `x-amz-meta-*` headers as Anvil object metadata. That is enough for many tools that attach source, checksum label, or importer information to an uploaded object.

```bash
aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api put-object \
  --bucket documents \
  --key s3/hello.txt \
  --body s3-hello.txt \
  --content-type text/plain \
  --metadata '{"source":"s3-tutorial","owner":"docs"}'

aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api head-object --bucket documents --key s3/hello.txt
```

The `put-object` command overwrites the current key with a new version, because S3 writes are still Anvil object writes. The `head-object` command should show the content type and metadata fields as S3 headers. This proves simple S3 metadata round-trips through the gateway.

It does not prove support for Anvil's richer typed metadata model. If an application needs structured JSON metadata, typed fields for query, native mutation preconditions, or carefully controlled idempotency, use the native public API for the write and use the indexing tutorials to design the derived query surface.

## Work with versions deliberately

Anvil objects are versioned. The S3 gateway exposes that through S3 version ids. When you overwrite `documents/s3/hello.txt`, the current pointer moves to the new version, but older versions remain part of the object's history until lifecycle or delete/version deletion logic removes them.

```bash
aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api get-bucket-versioning --bucket documents

aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api list-object-versions --bucket documents --prefix s3/hello.txt
```

`get-bucket-versioning` currently reports versioning as `Enabled` for an existing bucket. `list-object-versions` proves the gateway can expose the version history and delete markers visible to the caller.

There is also a real S3 `put-bucket-versioning` path, but it requires `bucket:write` on the bucket and currently only accepts an empty configuration or `Status=Enabled`. Suspending or disabling bucket versioning is not implemented by the current gateway because Anvil's object model is versioned. If you grant that bucket-write authority deliberately, the helper command is:

```bash
anvil --profile acme auth grant "$S3_APP_ID" bucket:write documents

aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api put-bucket-versioning \
  --bucket documents \
  --versioning-configuration Status=Enabled
```

That command proves the gateway recognises the S3 versioning-management call. It does not turn versioning off, and it should not be used as a reason to give a general S3 uploader bucket-policy authority.

If you need to read an older version, pass the returned `VersionId` to an S3 `get-object` or use the native API's version id field. If you delete the key without a version id, the gateway creates a delete marker and returns it in `x-amz-version-id`; deleting a specific version uses the S3 `versionId` query parameter.

Grant delete authority only when the tool genuinely needs it. The earlier grants did not include delete, so add it explicitly before running a delete example:

```bash
anvil --profile acme auth grant "$S3_APP_ID" object:delete documents/s3/hello.txt

aws --endpoint-url "$ANVIL_S3_ENDPOINT" \
  s3api delete-object --bucket documents --key s3/hello.txt
```

A successful delete proves the app can create a delete marker for that exact key. It does not erase every older version by default, and it does not close the bucket or prevent a later authorised writer from creating a new current version for the same key.

## Copy and multipart upload

The current gateway supports `CopyObject` using `x-amz-copy-source`, including source version ids and the common source ETag/date preconditions. It also supports the S3 multipart lifecycle: initiate an upload, upload parts, list parts, complete the upload, abort the upload, and list active multipart uploads for a bucket.

For ordinary users, the easiest multipart test is usually a normal high-level S3 upload of a large enough file with a client that automatically chooses multipart. The exact threshold is client-side, not Anvil-side, so this tutorial does not pretend that a small fixed command proves multipart. At the protocol level, successful multipart completion proves that Anvil accepted the upload session and parts and committed the completed object as another Anvil object version.

Multipart uploads are still authorised writes. Listing active multipart uploads is an authenticated operation, not public-read. Deleting a bucket with retained object versions or active multipart uploads is rejected, so operators should treat incomplete multipart state as part of bucket cleanup and diagnostics.

## Public-read through S3 is still Anvil public access

A public-read bucket can be read without S3 credentials through supported read-side paths. For S3, unsigned `GET` and `HEAD` are allowed to reach the object handlers; current listing and version-listing paths also use public-aware object-manager behaviour for public buckets. Writes, deletes, bucket management, and multipart operations still require signed requests.

If you deliberately make `documents` public for a local test, use the public bucket policy command from the public-access tutorial and turn it off afterwards:

```bash
anvil --profile acme bucket set-public documents --allow true
curl -i "$ANVIL_S3_ENDPOINT/documents/s3/hello.txt"
anvil --profile acme bucket set-public documents --allow false
```

The `bucket set-public` commands call the native public Bucket API and require `bucket:write` on `documents`. The `curl` command is an unsigned HTTP request to the S3 path-style route. If it succeeds while the bucket is public, it proves that anyone who can reach that public listener can read that object through the S3 surface. If it fails after public-read is disabled, that proves only that this endpoint now denies this unsigned read; caches, CDNs, logs, and earlier downloads may still contain data that was public before.

Do not make a mixed private/public bucket public just to test S3. Public means object names, read-side metadata, versions exposed through listing paths, and object bodies may become visible through the public surface. Use a dedicated bucket for public assets in production, and review names and metadata as carefully as bodies.

## Object links and host routing

Anvil object links are not S3 copies. A link is an Anvil object-model alias that resolves to a target key or version according to its link metadata. The S3 gateway reads links in follow mode by default, so a `GET` through S3 can return the target body. The gateway also supports an Anvil-specific `x-anvil-link-mode: metadata` request header for reading link descriptor metadata instead of following the link.

Create, update, delete, and inspect links with the native public API or `anvil object link ...` commands, not with invented S3 management operations. That keeps link generation checks, dangling-link policy, and follow/redirect semantics in the Anvil model where they belong.

Host routing is also an Anvil gateway feature rather than an AWS bucket feature. With `PUBLIC_REGION_BASE_DOMAIN` and active host aliases, Anvil can route host-shaped requests to a tenant, bucket, and key prefix. That is useful for S3 virtual-host style requests, static hosting, and custom download domains. The host only chooses a route; it does not make private data public and it does not grant write authority.

## Know when S3 is the wrong surface

Use S3 for object compatibility. Switch to the native API when the operation depends on Anvil semantics that S3 cannot express:

| Need | Use instead |
| --- | --- |
| Relationship schemas, tuples, usersets, and checks | Native authz API and `anvil authz ...`. |
| Index definitions, full-text/vector/hybrid query, diagnostics, and catch-up checks | Native index API and `anvil index ...`. |
| Watches and durable derived-data checkpoints | Native watch API and `anvil watch ...`. |
| PersonalDB groups, replicas, changesets, heads, and watches | PersonalDB API/CLI surfaces. |
| Task leases, fence tokens, and fenced mutations | Coordination/native mutation APIs. |
| Repair, mesh routing, lifecycle, system audit, tenants, regions, and server secret envelopes | The appropriate public repair APIs or the private admin API, depending on the operation. |
| Rich typed metadata and native idempotency context | Native object API. |

This distinction is a correctness boundary. For example, an S3 overwrite can create a new object version, and ETag preconditions can prevent a stale overwrite in simple cases. That is not the same as a native fenced mutation tied to a task lease, a zookie/revision-bound authz check, or an index query that requires catch-up to a watch cursor.

## Troubleshoot from the boundary inward

When an S3 command fails, keep the layers separate.

First, prove the public listener is reachable:

```bash
curl -fsS "$ANVIL_S3_ENDPOINT/ready"
```

This only proves the listener is up. It does not prove credentials, bucket existence, or authorisation.

Next, check the client-side request shape. A `Missing Authorization` response on a write means the S3 client did not sign the request. A signature error usually means the wrong `AWS_ACCESS_KEY_ID`, wrong `AWS_SECRET_ACCESS_KEY`, clock skew, endpoint/host mismatch, or proxy host rewriting problem. A `NoSuchBucket` or region redirect points to bucket placement and routing. `AccessDenied` means the request reached Anvil but the app, public-read state, or relationship checks did not allow the operation.

Finally, use native inspection where possible. `anvil bucket ls`, `anvil object head`, `anvil auth list-grants`, and index/watch diagnostics can tell you what Anvil believes. They are public API helpers and still require the active profile to be authorised. The private admin API should stay private and should be used only for operator-plane questions, not as a shortcut around tenant data-plane authorisation.

## Current limitations to keep in mind

The gateway is useful, but it is intentionally not all of S3 and not all of Anvil.

Versioning is always exposed as enabled for existing buckets, and suspending or disabling it through S3 is not supported. S3 metadata is limited to content type and string `x-amz-meta-*` values. Prefix-specific list grants are coarser than the ideal model because ordinary object listing currently checks `object:list` on the bucket name. Public-read bucket listing and version listing can expose names and metadata, so public buckets must be curated deliberately. S3 management surfaces such as AWS IAM policy documents, ACLs, lifecycle rules, tags, notifications, CORS configuration, and website configuration are not the core Anvil control plane. Native-only features remain native-only.

Those gaps are not reasons to avoid the gateway. They are reasons to use it for what it does well: move and read object bytes with existing S3 tooling while Anvil remains the authoritative model for tenants, credentials, authorisation, versions, links, derived data, and operations.
