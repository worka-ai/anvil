---
title: Public Access
description: Expose bucket content deliberately while keeping tenant, object, and admin boundaries intact.
---

# Public Access

This tutorial continues from [Buckets and Objects](/tutorials/buckets-and-objects/). It assumes you understand the `documents` bucket, object keys such as `tutorial/welcome.txt`, and the current local tutorial limits around region activation and least-privilege CLI uploads.

Public access in Anvil is not a bypass. It is a bucket policy choice that changes how read requests are authorised for data that you have deliberately chosen to publish. The private admin API, tenant credentials, bucket policy mutation, object writes, deletes, indexes, relationship tuples, and internal `_anvil/` records remain protected.

Use the public API or a client library for automation. The `anvil bucket set-public` CLI command in this page is a manual helper over the public Bucket API. For the surrounding model, read [Authorisation](/learn/authorisation/), [Gateways](/learn/gateways/), [Public CLI](/reference/public-cli/), and [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/). Public object delivery through protocol adapters is covered in [S3-Compatible Gateway](/tutorials/s3-gateway/) and [Static Hosting and Aliases](/tutorials/static-hosting-and-aliases/).

## What public means

A private bucket requires an authenticated principal and a successful authorisation decision before object data is returned. That decision can come from public policy scopes, relationship authorisation, or another allowed path in the service.

A public-read bucket changes read behaviour for object data. Anyone who can reach the relevant public surface can read matching objects without presenting an Anvil tenant credential. In the current implementation, the setting is bucket-wide: `is_public_read = true` on the bucket. There is not a separate public flag for one object key in the public CLI.

That has two practical consequences. First, do not mix private and public data in the same bucket unless every current and future object in that bucket is safe to expose through public-read behaviour. Secondly, prefer a dedicated bucket such as `public-assets`, `downloads`, or `static-site` for production public content. This tutorial uses `documents` only because the earlier pages already introduced it.

Public read is still constrained by Anvil's model. Reserved `_anvil/` paths are rejected before object lookup. A public bucket does not grant write access, delete access, bucket-policy access, app-management access, authz tuple access, index-definition access, or private admin API access. It also does not create DNS, TLS, CDN cache invalidation, or host-alias activation for you.

## Know which surfaces can serve public data

The **native public API** has an unauthenticated `ObjectService.GetObject` path for public buckets. Current unauthenticated native gRPC support is narrow: it is for object body reads. Other native public API calls, such as bucket policy changes, object writes, and most metadata/list operations, still require a bearer token.

The **S3-compatible gateway** can serve object reads through S3-style `GET` and `HEAD` requests and can map S3 listing requests to Anvil prefix listing. In the current object manager, public buckets bypass the ordinary per-object read/list checks for these read-side paths, while reserved internal prefixes remain blocked.

**Static hosting and host aliases** map a hostname and path prefix to an Anvil bucket and key prefix. A host alias is routing; it does not make private data public by itself. The object still has to be readable under Anvil's rules. For a public static site, the backing bucket or delivery policy must allow public reads, and the edge still needs correct DNS, TLS, and cache policy.

Authenticated tenant access is different. A signed request carries a tenant app identity, token scopes, and relationship context. Public access has no tenant app identity for the reader, so you cannot use it to distinguish Alice from Bob. If your product needs per-user decisions, keep the bucket private and require authenticated reads.

## Prerequisites for the tutorial command

The previous tutorial did not grant `acme-owner` `bucket:write` on `documents`. That is the exact public policy scope the current `anvil bucket set-public` command needs, because the command calls `BucketService.PutBucketPolicy` for that bucket. A bootstrap operator or already-authorised tenant owner must grant that scope deliberately before you run the command.

Do not grant a wildcard just to complete a tutorial. The narrow prerequisite is:

```text
bucket:write on documents
```

If you also need to upload a new public example object through the API, the writer needs `object:write` on that exact key, such as:

```text
object:write on documents/tutorial/public-hello.txt
```

Those are public policy scopes. They let a tenant principal call public/data-plane APIs. They do not make the principal a system administrator and do not let it expose the private admin listener.

## Enable public reads on the bucket

In application code, enable public-read by calling `BucketService.PutBucketPolicy` for the bucket with `policy_json` containing `{"is_public_read": true}`. The public CLI exposes the same operation through `bucket set-public`:

```bash
anvil --profile acme bucket set-public documents --allow true
```

This sends `PutBucketPolicy` with `policy_json` equivalent to `{"is_public_read": true}`. A successful response proves four things: the `acme` profile can authenticate, the caller has `bucket:write` for `documents`, the bucket exists in the caller's tenant, and Anvil has committed the bucket policy update.

It does not prove that every gateway route is reachable from the internet. It does not prove that DNS or TLS is configured. It does not prove that downstream caches have the latest object. It only changes Anvil's bucket read policy.

In the local tutorial chain, this command is illustrative until the `documents` bucket exists and the required `bucket:write` scope has been granted. If it fails with permission denied, fix the tenant grant. If it fails with bucket not found or region-placement errors, return to the bucket and mesh setup rather than opening the admin API.

## Publish only data that can be public

If you already uploaded `tutorial/welcome.txt` in the buckets tutorial, enabling public read on `documents` makes that object readable through public delivery paths. If you want a separate small example object and have exact write authority for it, upload it with the current object CLI:

```bash
printf 'hello public world\n' > public-hello.txt
anvil --profile acme object put public-hello.txt s3://documents/tutorial/public-hello.txt
```

This command is still an authenticated write. Public-read does not allow anonymous uploads. A successful upload proves the caller can write that exact object key and that Anvil committed a current object version. As described in [Buckets and Objects](/tutorials/buckets-and-objects/), the current CLI upload path may still hit the least-privilege bucket lookup gap because it discovers bucket ids through `ListBuckets`. If you are avoiding that broader helper permission, use an API/client path that supplies the mutation context directly, or reuse an object you uploaded earlier.

Before making a real bucket public, review the whole bucket, not only the object you plan to advertise. Prefixes are a naming convention, not a hard public/private boundary in the current bucket-level setting.

## Verify with the right tool

The current `anvil` CLI is built around profiles and bearer tokens. Even `anvil object get` calls the public API with credentials from the selected profile or `ANVIL_AUTH_TOKEN`. It is therefore useful for proving authenticated reads, but it is not a reliable anonymous-read test.

```bash
anvil --profile acme object get s3://documents/tutorial/public-hello.txt public-download.txt
```

A successful CLI download proves the object exists and the authenticated `acme` profile can read it. It does not prove that an unauthenticated browser, S3 client, CDN, or static-host request can read it.

To prove public access, use the delivery surface you actually expose: native `GetObject` without an `authorization` metadata header, an unsigned S3/static HTTP request if your gateway allows that path, or a browser/CDN request to the configured host alias. The exact command depends on your deployment endpoint, so this page does not invent one. The important test is that no tenant bearer token, client id, client secret, S3 signing key, or admin credential is present in the request.

If the unauthenticated request succeeds, you have proved that anyone who can reach that surface can fetch the object. If it fails, inspect the bucket policy, object key, route/host alias, gateway listener, region routing, and reserved-prefix checks before changing credentials.

## Understand listing and versions

The setting is named public-read, but current gateway/object-manager behaviour also permits unauthenticated read-side listing for public buckets through paths that call public-aware listing. That means a public bucket can expose object names, prefixes, sizes, ETags, content types, user metadata, and version-listing information depending on the gateway operation used.

Treat names and metadata as data. A key such as `customers/acme/legal-settlement.pdf` can leak sensitive information even if the body is never downloaded. User metadata can reveal workflow state, retention class, customer identifiers, or internal labels. Put only public-safe names and metadata in a public-read bucket.

The native gRPC surface is narrower than the gateway surface today: anonymous native access is currently routed for `ObjectService.GetObject`, while S3/static delivery can exercise `HEAD` and listing paths through gateway handlers. Document the surface you expose in your deployment runbook so operators know what public-read means for that endpoint.

## Keep the admin API private

Do not publish the admin API to make public access work. Public object delivery belongs on the public API, S3 gateway, or static-hosting surface. The admin API is for system operators and must stay on loopback, a private management network, or an equivalent operator-only path.

Making a bucket public does not require an admin listener. It requires a tenant principal with the right public policy scope. If a public download does not work, debug routing, bucket policy, object existence, and gateway configuration. Do not expose `50052`, do not hand out the bootstrap system-admin credential, and do not serve files by reading Anvil's storage directory directly.

## Turn public access off

Disable public reads through the same public CLI command:

```bash
anvil --profile acme bucket set-public documents --allow false
```

A successful response proves the caller still has `bucket:write` for `documents` and that Anvil committed a new bucket policy state. Future unauthenticated object reads should fail once requests reach Anvil and observe the updated bucket metadata.

Turning public-read off is not the same as erasing every copy. Browser caches, CDNs, package mirrors, object download clients, and logs may still hold data that was public before the change. For sensitive incidents, combine the Anvil policy change with cache invalidation, key rotation where credentials were exposed, and an incident review of what was published.

## What to take forward

Use public-read only for buckets whose contents, names, metadata, and versions are safe for anonymous readers. Keep private and public data in separate buckets where possible. Remember that public access is a data-plane read policy, not an admin bypass. Verify public delivery with a request that truly has no credentials. Keep the admin API private. Record and review public-read changes as security-relevant configuration changes, because the effect is simple: anyone who can reach the public surface can read the matching data.
