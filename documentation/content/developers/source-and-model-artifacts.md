---
title: Source And Model Artifacts
description: Store source packs, generated artifacts, model files, and ingestion manifests in Anvil.
---

# Source And Model Artifacts

**Goal:** use Anvil for artifacts that must be durable, indexed, authorized, and reproducible.

Anvil can store ordinary files, but source and model artifacts benefit from more structure. A build system needs to know which source pack produced which result. A model registry needs manifests, tensor metadata, large files, and ingestion status. Anvil stores the bytes and maintains indexes over the metadata that makes those bytes useful.

## Source artifacts

Store git pack files under predictable keys:

```text
tenants/acme/workspaces/ws-1/source/revisions/00000042/repo.pack
tenants/acme/workspaces/ws-1/source/revisions/00000042/manifest.json
```

The pack is the durable source artifact. The manifest records revision id, commit identity, tree hash, author, generated assets, and build inputs.

Anvil source indexes are derived from the stored pack bytes through watch streams. If a source index is missing or invalid, Anvil rebuilds it from the pack.

## Generated artifacts

Store generated outputs with content hashes and source references:

```text
tenants/acme/workspaces/ws-1/builds/build-77/artifacts/app-linux.run
tenants/acme/workspaces/ws-1/builds/build-77/logs/cargo.txt
tenants/acme/workspaces/ws-1/builds/build-77/screenshots/home.png
```

Each artifact should include metadata linking it to source revision, build id, platform, content type, and integrity hash.

## Model files

Large model files are normal objects, but model ingestion adds structure:

- external source identity;
- revision;
- included and excluded file globs;
- destination bucket and prefix;
- ingestion status;
- generated `anvil-index.json` manifest;
- tensor metadata index where applicable.

This lets applications query model artifacts without scanning every model file.

## Authorization

Source and model artifacts often contain sensitive data. Use relationship authorization for repository, workspace, model, and tenant boundaries. Search and index APIs must apply those same checks before exposing artifact metadata.

## Verification pattern

For every artifact family, verify:

1. the object exists at the expected key;
2. the content hash matches metadata;
3. the manifest references the same hash;
4. the source revision or ingestion id is present;
5. unauthorized callers cannot list, head, search, or download it.
