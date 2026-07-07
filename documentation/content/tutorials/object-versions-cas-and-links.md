---
title: Object Versions, CAS, and Links
description: Use object versions, current-pointer preconditions, and object links without hiding races or copying payloads.
---

# Object Versions, CAS, and Links

This tutorial continues from [Buckets and Objects](/tutorials/buckets-and-objects/) and [Metadata and Typed Fields](/tutorials/metadata-and-typed-fields/). Those pages introduced the `documents` bucket, the `tutorial/welcome.txt` object, object metadata, and the current limits in the local tutorial chain.

The limits still matter here. Region activation may still block bucket placement, and the public CLI upload helper currently discovers bucket ids through a broad `ListBuckets` call. The CLI is therefore a useful manual helper, not the primary contract for correctness-sensitive writes. Application code should use the public API or Rust client so it can keep bucket ids, carry version ids, set metadata, send idempotency keys, and express preconditions deliberately. The exact command surface is in [Public CLI](/reference/public-cli/), and the permission strings behind these operations are in [Authorisation Actions and Resources](/reference/authorisation-actions-and-resources/).

For the read-side model behind this page, keep [Reads, Listing, and Links](/learn/reads-listing-and-links/) open. For the write-side model, especially stale writer rejection and fences, read [Writes, Consistency, and Fences](/learn/writes-consistency-and-fences/).

## Why versions exist

An object key is a stable name. A version is one committed state of that name.

When a writer stores new bytes at `documents/tutorial/welcome.txt`, Anvil creates a new object version record and moves the key's **current pointer** to that version. Ordinary reads follow the current pointer. A pinned read asks for one saved `version_id` instead. A delete without a version id writes a delete marker and moves the current pointer to that marker, so current reads behave as not found even though older version records may still be referenced while they exist.

This distinction is what prevents distributed clients from pretending that races did not happen. If a user opens an invoice, a worker updates the same invoice, and the user later saves an older edit, the save should not silently overwrite the worker. The client should say, in the request, which current version it believes it is replacing.

## Capture the current validator through the API

The public API exposes the values an application needs for safe updates. `ObjectService.HeadObject` returns metadata without downloading the body:

```text
HeadObjectRequest {
  bucket_name: "documents"
  object_key: "tutorial/welcome.txt"
  version_id: none
}

HeadObjectResponse {
  etag: "..."
  version_id: "..."
  size: ...
  last_modified: "..."
  content_type: "..."
  user_metadata_json: "..."
}
```

The `version_id` names the exact committed version. Use it when the application needs to prove that the current pointer has not moved. The `etag` is an opaque validator for the visible representation; it is useful for HTTP-style conditional behaviour and can also be used as a native mutation precondition. Do not parse either value or infer ordering from it.

The current public CLI has a supporting `head` command:

```bash
anvil --profile acme object head s3://documents/tutorial/welcome.txt
```

If the object exists and the caller has `object:read`, this proves that metadata reads are authorised and routed through the public API. In the current CLI implementation, the command prints ETag, size, and last-modified time but does not print `version_id`, even though the API response contains it. Use the API or Rust client for workflows that need to carry the version id into a later save.

`ObjectService.GetObject` accepts the same optional `version_id` field. With no version id, it reads the current pointer. With a version id, it reads that pinned version or fails if that version is not found, is a delete marker, or is not visible to the caller. The current CLI `object get` always sends no version id, so it is a current read helper rather than a pinned-version tool.

## List versions when you are diagnosing history

An application normally keeps the version id it received from `PutObject`, `HeadObject`, or `GetObject`. Operators and repair tools sometimes need to inspect a key's visible history. The public API has `ObjectService.ListObjectVersions` for that:

```text
ListObjectVersionsRequest {
  bucket_name: "documents"
  prefix: "tutorial/"
  key_marker: ""
  version_id_marker: ""
  max_keys: 100
}
```

Each returned `ObjectVersionSummary` includes the key, version id, ETag, size, content type, user metadata, whether the entry is the latest version, and whether it is a delete marker. That tells you whether a current not-found result is caused by no object, a delete marker, or a caller looking at the wrong key.

There is not currently a public CLI command for listing object versions. Do not invent one in runbooks. Use the API or a client wrapper until the CLI exposes that call.

## Write with a current-pointer precondition

The simplest safe update is: replace this object only if the current pointer is still the version I read.

At the API level, `PutObject` starts with an `ObjectMetadata` frame. That frame carries a `NativeMutationContext`. The context includes identity and retry fields, and its `precondition` string is where the current-pointer check goes:

```text
NativeMutationContext {
  tenant_id: <authenticated tenant id>
  bucket_id: <documents bucket id>
  principal: <authenticated principal>
  request_id: "save-invoice-1001-..."
  precondition: "version:<version_id returned by HeadObject>"
  authz_zookie_optional: ""
  idempotency_key: "save-invoice-1001-<stable operation id>"
}
```

The body stream then sends the new bytes. If the current pointer is still that version, Anvil writes a new version and moves the pointer. If another writer won first, the request fails with a precondition failure. That failure is not a storage outage. It is the system telling the client to reload, merge, ask the user, or abandon the stale write.

The native mutation precondition supports these current-pointer forms:

| Precondition | Meaning |
| --- | --- |
| `none` | Write without checking the current pointer. Use this only when last-writer-wins is acceptable. |
| `exists` | Write only if the key currently has a non-deleted object. |
| `not_exists`, `not-exists`, or `absent` | Create only if the key currently has no non-deleted object. |
| `version:<uuid>` | Write only if the current object version id matches the supplied version. |
| `etag:<etag>` | Write only if the current object ETag matches the supplied ETag. Quotes around the ETag are ignored for comparison. |

Invalid precondition syntax is an invalid request. A syntactically valid precondition that no longer matches is a failed precondition. Missing `object:write`, a bucket mismatch, or a principal mismatch is an authorisation or validation failure instead. Keep those cases separate in application error handling because they require different action.

The current public CLI `object put` always builds a mutation context with `precondition: "none"` and does not expose a flag for version or ETag preconditions. It is useful for a manual upload once the local path is ready:

```bash
anvil --profile acme object put tutorial-welcome.txt s3://documents/tutorial/welcome.txt
```

That command proves the bucket exists, placement is writable, the caller can write that key, and the current CLI upload path can commit a version. It does not prove your application handles concurrent edits safely.

## Use explicit write preconditions for richer mutations

Some object operations expose a structured `WritePrecondition` in addition to the native mutation context. `PatchJsonObject`, `CompareAndSwapManifest`, append-stream writes, stream sealing, and mutation batches can all carry object-version and lease-fence checks.

For an invoice from the metadata tutorial, a structured precondition can say that the invoice is still at the version your worker read:

```text
WritePrecondition {
  object_versions: [
    {
      bucket_name: "documents"
      object_key: "accounting/invoices/inv-1001.json"
      expected_version_id: "<saved version id>"
      must_not_exist: false
    }
  ]
}
```

For a first-writer-wins create, set `must_not_exist: true` and omit `expected_version_id`. If the object already exists, Anvil rejects the mutation before publishing a partial result.

`CompareAndSwapManifest` adds another common pattern: a JSON manifest with a numeric revision. A release controller might read revision `7` of `releases/manifest.json`, decide that `latest` should move to a new artefact, and submit:

```text
CompareAndSwapManifestRequest {
  bucket_name: "documents"
  manifest_key: "releases/manifest.json"
  expected_revision: 7
  manifest_json: "{...new manifest...}"
  mutation_context: <NativeMutationContext>
  precondition: <optional WritePrecondition>
}
```

The request succeeds only if the manifest revision is still `7` and any supplied write preconditions also hold. If the revision changed, another writer published first. The safe response is to reload the manifest and decide whether your intended change still makes sense.

There is not currently a public CLI helper for `CompareAndSwapManifest`, structured `WritePrecondition`, or mutation batches. Treat these as application API features.

## Understand links before creating one

An object link is a small object-like metadata record whose key points at another key in the same bucket. It is useful when readers need a stable name but writers need to move the target over time:

```text
releases/latest.bin -> releases/app-1.0.0.bin
```

A link is not a copy. The link record has link metadata and a generation counter; it does not duplicate the target payload. Many links can point at the same target. Deleting a link removes the alias, not the target object. Moving a link writes a new link version and increments the link generation, but the target object remains the same object it was before.

Links are still object data. Creating or updating a link requires `object:write` on the link key. Reading link metadata requires `object:read` on the link key. Deleting a link requires `object:delete` on the link key. Link listing requires list authority for the listed link prefix and filters results by per-link read authority. A followed read of a private target must also be authorised to return the target data.

## Create a live link with the public CLI

The following commands are illustrative unless your local tutorial environment has an active writable region, a real `documents` bucket, uploaded release artefacts, and grants for the `documents/releases/` keys. They use only the existing public CLI commands.

First upload a concrete artefact. As described earlier, `object put` is a manual helper and may hit the current least-privilege bucket lookup gap.

```bash
printf 'binary-v1' > app-1.0.0.bin
anvil --profile acme object put app-1.0.0.bin s3://documents/releases/app-1.0.0.bin
```

If this succeeds, Anvil has a current object at `releases/app-1.0.0.bin`. Now create a link named `releases/latest.bin`:

```bash
anvil --profile acme object link create \
  s3://documents/releases/latest.bin \
  s3://documents/releases/app-1.0.0.bin
```

The default link resolution is `follow`, and the current CLI sends no target version, so this creates a **live** link to the target key's current version. The create command proves that the caller can write the link key, the link key did not already exist, and the target currently exists as a blob unless `--allow-dangling` is used. A successful create prints the link and its generation, normally generation `1`.

Read the link descriptor after creation:

```bash
anvil --profile acme object link read s3://documents/releases/latest.bin
```

This reads link metadata, not the target bytes. The API descriptor includes `link_key`, `target_key`, optional `target_version`, `resolution`, timestamps, creator, and `generation`. The current CLI output is deliberately compact and prints the link, optional pinned target version, and generation.

## Move a link with a generation check

A link generation is a compare-and-swap token for the alias itself. It is not the target object's version id. Creation starts at generation `1`; each successful update writes a new link version with the next generation.

```bash
printf 'binary-v2' > app-1.0.1.bin
anvil --profile acme object put app-1.0.1.bin s3://documents/releases/app-1.0.1.bin

anvil --profile acme object link update \
  s3://documents/releases/latest.bin \
  s3://documents/releases/app-1.0.1.bin \
  --expected-generation 1
```

The update says: move `latest.bin` only if it is still at generation `1`. If another release job already moved it to generation `2`, Anvil rejects the update with a generation conflict. That is the link equivalent of a CAS failure. Reload the link with `object link read`, inspect the target, and decide whether to publish a newer move.

Without the generation check, two release jobs could both believe they moved `latest.bin` from the same old target. With it, one update wins and the other must make an explicit decision.

## Delete the alias, not the object

Deleting a link is also generation checked:

```bash
anvil --profile acme object link delete \
  s3://documents/releases/latest.bin \
  --expected-generation 2
```

If generation `2` is still current, Anvil writes a delete marker for the link and returns the deletion generation. The target object, `releases/app-1.0.1.bin`, is not deleted. Keep this distinction visible in product UI: "remove alias" and "delete object" are different operations and require different authority.

If the link is missing, points at a non-link object, or has a different generation, the delete fails. Do not convert those failures into a blind object delete.

## Use pinned links when the target version must not move

The API can store `target_version` on a link. A link with no `target_version` is live: it follows the current version of `target_key`. A link with `target_version` is pinned: it resolves to that exact version even if the target key later receives a newer current version.

Pinned links are useful for reproducible downloads, audit evidence, package manifests, and any external reference that must mean "the artefact that was approved" rather than "whatever is current under this key today".

The current public CLI does not expose a `--target-version` option for link create or update, and the current `object head` command does not print version ids. Use the API or Rust client when you need pinned links.

## Decide between follow and redirect

Object links carry a `resolution` field. The public CLI accepts it on create and update:

```bash
anvil --profile acme object link create \
  s3://documents/releases/download.bin \
  s3://documents/releases/app-1.0.1.bin \
  --resolution follow
```

`follow` means a normal object read can resolve the link and serve the target bytes. For followed links, the response ETag is link-aware: it changes when the link generation, target key, pinned target version, target version, or target ETag changes. That helps caches notice that the alias view changed; it is not necessarily the bare ETag of the target blob.

`redirect` means the link descriptor says the caller should be redirected to the target rather than served through the alias. The enum and CLI flag exist, but current native object `GetObject` and `HeadObject` operate in follow mode. If they encounter a redirect link, they fail with an `ObjectLinkRedirectRequired` precondition error instead of inventing an HTTP redirect. Use `ReadObjectLink` to inspect the descriptor, and verify the gateway or delivery surface you are using actually implements redirect semantics before relying on it for public downloads or static hosting.

## Handle dangling links deliberately

By default, link create and update validate that the target exists, is not a delete marker, and is a blob. If the target is missing, Anvil rejects the mutation with a dangling-link precondition failure. That default is safest for most release aliases because it prevents publishing a broken `latest` pointer.

There are controlled cases where a dangling link is useful. An importer might create a set of aliases before replicated artefacts arrive, or a static-site deployment might prepare route aliases before all objects have been uploaded. The public CLI exposes that choice:

```bash
anvil --profile acme object link create \
  s3://documents/releases/next.bin \
  s3://documents/releases/app-1.0.2.bin \
  --allow-dangling
```

A dangling link can be created or updated, but it cannot be followed successfully until the target resolves. Readers should surface that as a broken alias or not-ready state, not as an empty file. Link resolution also detects loops and enforces a depth limit, so chains such as `a -> b -> c` should be short and intentional rather than a hidden routing system.

## List links for inspection

Link listing is useful for operator checks and tenant administration screens:

```bash
anvil --profile acme object link list s3://documents/releases/ --limit 50
```

A successful list proves the caller can list that link prefix and read the returned link descriptors. It does not prove the caller can read every target body. Followed reads still perform the target-side checks needed to return bytes.

The response is a descriptor listing, not a version listing and not a body listing. Use it to answer "which aliases exist under this prefix?" rather than "which artefacts occupy storage?".

## What to take forward

Use object versions as concurrency evidence. Carry `version_id` or ETag from reads into writes that must not lose updates. Treat precondition failures and link generation conflicts as successful race detection, not generic outages. Use API calls, not current CLI upload helpers, for CAS, pinned reads, pinned links, structured preconditions, metadata-rich writes, and mutation batches. Use links for aliases, not copies, and make the difference between following, redirecting, moving, dangling, and deleting explicit in both application code and operator runbooks.
