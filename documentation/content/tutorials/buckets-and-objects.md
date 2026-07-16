---
title: Buckets and Objects
description: Understand tenant bucket boundaries, object keys, object bodies, metadata, and the current public CLI limits for a least-privilege local flow.
---

# Buckets and Objects

This tutorial continues from [Tenants, Apps, and Credentials](/tutorials/tenants-apps-and-credentials/). It assumes you have `ACME_TENANT_ID`, `ACME_CLIENT_ID`, `ACME_CLIENT_SECRET`, an `acme` public CLI profile, and the narrow tutorial grants from that page.

Buckets and objects are tenant/public API work. Do not use the private admin CLI here to create buckets or write objects. That binary is the private control-plane helper for operators; bucket and object operations should be performed by tenant principals through the public API. The `anvil` CLI commands in this page are manual helpers over that API. Application code should normally call the public API or Rust client directly.

For the model behind this page, read [Object Model](/learn/object-model/) and [Reads, Listing, and Links](/learn/reads-listing-and-links/). Versions, compare-and-swap writes, and links are introduced here only briefly; the detailed flow belongs in [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/). The CLI reference is [Public CLI](/reference/public-cli/), and the permission strings used by the examples are detailed in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

The page deliberately separates the data model from CLI convenience. You will learn what a bucket protects, what an object key means, which command proves placement and authorisation, why list access is different from read access, and where the public API is more precise than the current helper CLI.

## Prerequisites and current authority

Before running commands, check which principal your shell will use. Both public and admin CLIs honour `ANVIL_AUTH_TOKEN`, so a leftover system-admin token can hide missing tenant grants. For this page, mint a tenant token from the `acme` profile:

```bash
export ANVIL_AUTH_TOKEN="$(anvil --profile acme auth get-token)"
printf 'using acme token with %s characters\n' "${#ANVIL_AUTH_TOKEN}"
```

This proves token exchange for the tenant app, not bucket readiness. Bucket creation can still fail if topology has not made `local` writable. Object upload can still fail if the current CLI needs a broader bucket lookup than the exact object grant. Keep those failures separate while reading the rest of the page.

## Understand buckets and object keys

A **bucket** is a tenant-scoped namespace. The `acme` tenant can have a bucket named `documents`, and another tenant can also have a bucket named `documents`, because the tenant boundary is part of every authorisation and routing decision. Use buckets for durable operational boundaries: placement, lifecycle, policy shape, gateway exposure, indexing strategy, and recovery scope.

An **object key** is the name of an object inside a bucket. Keys often look like paths, such as `tutorial/welcome.txt`, but they are not local filesystem paths. There are no real directories unless your application chooses to model them. Prefixes still matter because Anvil can list, watch, route, authorise, and repair by prefix-shaped key ranges.

An **object body** is the stored bytes for one version of a key. Anvil does not care whether those bytes are text, JSON, a PDF, a model file, or a package archive. Object metadata is the small descriptive record around the body: content type, user metadata, size, ETag, version id, mutation id, and related indexing or authorisation revision data. The public CLI currently sends simple uploads; the public API exposes richer metadata fields for application clients.

Each successful write creates a new object version and moves the current pointer for that key. Ordinary reads fetch the current version. Later tutorials cover pinned version reads, compare-and-swap preconditions, and object links.

## Check the placement precondition

The previous tutorial granted `acme-owner` permission to create only one bucket: `documents`. That is intentionally narrow. The command below is the public API command a tenant should use to create the bucket once the target region is active and writable.

```bash
anvil --profile acme bucket create documents local
```

If your local `local` region has been activated, the command creates the `documents` bucket and prints a success message. That proves tenant-owned bucket creation works through the public API without the private admin plane.

In the current tutorial chain, the mesh page deliberately stopped before region activation because `region activate` requires a documented activation-checkpoint workflow. In that state, this bucket command is expected to fail with a placement or lifecycle precondition error. That failure is useful: it proves Anvil is not silently placing new bucket data into a region that is still joining. Do not work around it through the private admin plane; fix or complete the region activation workflow first.

## Prepare a concrete object body

Use a small text file as the tutorial object. The key we will use for it is `tutorial/welcome.txt` inside the `documents` bucket. That key puts the tutorial scope first, so later prefix listing or watch examples can target `tutorial/` without scanning unrelated data.

```bash
cat > tutorial-welcome.txt <<'TEXT'
Welcome to the Acme document bucket.
This object is intentionally small so reads, metadata checks, and later version updates are easy to inspect.
TEXT
```

This command only creates a local file. It does not contact Anvil. The file body is concrete tutorial data that a later public API upload can store as `s3://documents/tutorial/welcome.txt`.

## Upload and read once the public path is ready

The existing public CLI object commands are `anvil object put`, `anvil object head`, `anvil object get`, `anvil object ls`, and `anvil object rm`. They all talk to the public API. The intended upload command for this tutorial object is:

```bash
anvil --profile acme object put tutorial-welcome.txt s3://documents/tutorial/welcome.txt
```

That command uploads the local file body to the `documents` bucket under the key `tutorial/welcome.txt`. A successful upload proves three things: the bucket exists in an active writable region, the caller has `object:write` for that exact key, and the public object API can commit a new current version.

There is an important current CLI limitation in the least-privilege local chain. The public CLI builds its object mutation context by calling `ListBuckets` to discover the bucket id. Today, `ListBuckets` is authorised with `bucket:list` on the resource `*`. This tutorial does not grant wildcard resources, so the least-privilege `acme-owner` profile from the previous page may be unable to use `anvil object put` even though it has `object:write` for the exact key. Do not add a broad list grant just to make the tutorial pass; the safer fix is for the CLI/API flow to support a narrow bucket lookup or to reuse the `bucket_id` returned by bucket creation.

When the bucket exists and the object has been uploaded, these read commands inspect it through the public API:

```bash
anvil --profile acme object head s3://documents/tutorial/welcome.txt
anvil --profile acme object get s3://documents/tutorial/welcome.txt downloaded-welcome.txt
```

`head` returns metadata such as ETag, size, last-modified time, and version id without downloading the body. `get` downloads the body. Together they prove that metadata reads and body reads are authorised like object reads; metadata is not a free side channel.

## Listing requires a separate scope

Prefix listing is not the same operation as reading one object. A list result may reveal names, customer identifiers, document structure, or workflow state even when the bodies are small or unread. That is why `object:list` is a separate scope.

The previous page did not grant `object:list`, so this tutorial does not run a listing command as part of the least-privilege flow. If your product needs a service to list keys in `documents`, grant the narrow list scope your application needs before running:

```bash
anvil --profile acme object ls s3://documents/tutorial/
```

A successful list would prove the caller can enumerate visible keys under the `tutorial/` prefix. Without an appropriate `object:list` grant, the expected result is permission denied. That denial is correct behaviour, not a broken bucket.

## Prove unrelated paths stay protected

The previous page delegated access only for `documents/tutorial/welcome.txt`. The owner should not be able to delegate or exercise unrelated object paths unless it already holds authority for those paths.

This command attempts to grant the writer access to a different key. It should fail because `acme-owner` was not granted `policy:grant` or `object:write` for `documents/tutorial/other.txt`.

```bash
if anvil --profile acme auth grant docs-writer object:write documents/tutorial/other.txt; then
  echo 'unexpected: unrelated object grant succeeded'
else
  echo 'expected: acme-owner cannot delegate an unrelated object key'
fi
```

The denial proves the handover grants are scoped to the tutorial object rather than the whole bucket. That same least-privilege rule should apply to service credentials in production: a writer for one prefix should not automatically receive access to neighbouring prefixes.

## API shape for application code

At the public API level, the same flow is explicit rather than hidden behind CLI convenience. A tenant app calls `BucketService.CreateBucket` with `bucket_name = "documents"` and `region = "local"`; the response contains `bucket_id`. An object-writing app then streams `ObjectService.PutObject` with an `ObjectMetadata` frame followed by body chunks. The metadata frame includes `bucket_name`, `object_key`, and a `NativeMutationContext` containing the tenant id, bucket id, principal, request id, precondition, and idempotency key.

That API shape is why application clients can be more precise than the current CLI helper: they can keep the `bucket_id` from bucket creation or from a narrow application lookup instead of listing every bucket. The Rust client wraps the generated public API surface; use the client version that matches your Anvil release for exact constructors and streaming helpers.

## What you should take forward

Use buckets as tenant-scoped operational boundaries, not as tiny folders. Put stable scope early in object keys. Treat object metadata as protected data. Grant reads, writes, deletes, and listings separately. Do not use the private admin API for tenant object work. And when a command is denied, read the denial as a useful signal: either the region is not ready for placement, the bucket does not exist, or the current principal does not have the specific public API scope required for that operation.

## Success and failure cues

A successful bucket create proves public-plane tenant authority and a writable placement target. A successful object upload proves the bucket exists, the exact key is writable, and the public object path can commit a current version. Permission denied on listing is expected in the least-privilege path unless `object:list` was granted, and placement errors are expected while the region remains unactivated. Keep those failure classes separate; broadening grants cannot fix a region lifecycle precondition.

## Where to go next

After you understand bucket placement and one-object access, read [Object Versions, CAS, and Links](/tutorials/object-versions-cas-and-links/) for safe updates and aliases, then [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/) before adding queryable structure. If you are still blocked on placement, return to [Mesh Regions, Cells, and Nodes](/tutorials/mesh-regions-cells-and-nodes/) instead of widening tenant credentials.
