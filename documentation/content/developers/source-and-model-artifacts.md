---
title: Source And Model Artifacts
description: Store source packs, generated files, model artifacts, logs, screenshots, and manifests in Anvil.
---

# Source And Model Artifacts

**What this page achieves:** you will learn how to store build inputs, generated outputs, model files, logs, and visual artifacts as first-class Anvil objects with metadata, hashes, indexes, and authorization.

Many platforms need to store more than user documents. They store source packs, build logs, screenshots, generated bundles, test reports, model manifests, media derivatives, and provenance records. These artifacts are not throwaway files. They need durable names, hashes, metadata, search, access control, and lifecycle management.

Anvil treats artifacts as ordinary objects plus stronger conventions.

## Artifact families

Common families include:

| Family | Example |
| --- | --- |
| Source | Git pack files, archive snapshots, dependency manifests |
| Build output | Binaries, bundles, packages, checksums |
| Logs | Compiler output, test logs, agent transcripts, task events |
| Visual evidence | Screenshots, videos, rendered previews |
| Models | Model files, tokenizer files, config, manifest indexes |
| Derived media | Transcripts, thumbnails, embeddings, extracted text |

Each family should have a predictable key layout and metadata schema.

## Source packs

A source pack captures repository state. Store it with revision metadata and a content hash:

```text
bucket: source-artifacts
key: tenants/acme/repos/app/revisions/sha256-{hash}/repo.pack
metadata:
  revision = abc123
  branch = main
  source_kind = git-pack
  produced_by = ingestion-service
```

The pack file is the durable object. Metadata and manifests make it discoverable and verifiable.

## Build logs and evidence

Build and test systems should upload logs and evidence as objects rather than embedding large blobs in a database row. Use predictable paths:

```text
tenants/acme/projects/p-123/runs/run-42/logs/cargo-build.txt
tenants/acme/projects/p-123/runs/run-42/screenshots/home.png
tenants/acme/projects/p-123/runs/run-42/reports/qa.json
```

The UI can list the run prefix, fetch small summaries, and open large assets only when needed.

## Model artifacts

Model artifacts can be large and multi-file. Store a manifest object that records every file, size, hash, media type, and source revision. Search and indexing can then operate over the manifest rather than guessing file relationships from loose object names.

A model manifest should answer:

- which files belong to the model;
- which source repository and revision produced them;
- which embedding dimensions and tokenizer apply;
- which hashes verify each file;
- which authorization rules apply.

## Search and authorization

Artifact search must respect the same authorization rules as ordinary objects. A user should not discover a private source pack through metadata search or a log snippet.

Index artifact metadata such as run id, revision, language, package name, model id, media type, and status. Use full text indexing for logs and extracted text. Use vector indexing for semantic search over documents, images, audio, or video when useful.

## What you can build after this page

You should be able to design artifact storage where files, logs, screenshots, model assets, and provenance records are durable, searchable, and authorized. Move to the Operator guides when you are ready to deploy and run Anvil.
