---
title: Source And Model Artefacts
description: Store source archives, build outputs, model files, logs, media, manifests, and derived records as indexed Anvil artefacts.
---

# Source And Model Artefacts

**What this page gives you:** a model for storing reproducible technical artefacts in Anvil. You will learn how to represent source packs, build outputs, model files, logs, screenshots, media, and manifests with object storage, metadata, search, and authorisation.

Modern systems produce many artefacts that are not ordinary user uploads: source archives, build logs, test screenshots, generated reports, model weights, tokenizer files, media extracts, embeddings, provenance records, and release bundles. These artefacts need durability, searchability, permissions, and operational traceability.

Anvil treats them as first-class object data.

## Artefact families

Common artefact families include:

- source packs and repository snapshots;
- build logs and compiler diagnostics;
- generated binaries or packages;
- test reports and screenshots;
- model weights and configuration;
- tokenizer and vocabulary files;
- media transcodes and transcripts;
- embedding batches;
- release manifests and checksums.

Each family should have clear keys, metadata, retention, and authorisation.

## Manifests

A manifest records the relationship between artefacts. For a build, it might include:

```json
{
  "kind": "build.manifest",
  "build_id": "1842",
  "source_revision": "abc123",
  "artifacts": [
    { "key": "builds/1842/log.txt", "sha256": "..." },
    { "key": "builds/1842/package.tar.zst", "sha256": "..." }
  ],
  "status": "passed"
}
```

The manifest is the stable record that links source, generated outputs, checksums, and status.

## Metadata and search

Index metadata such as:

- run id;
- revision;
- package name;
- model id;
- media type;
- language;
- status;
- producer;
- created time.

Use full text indexing for logs, reports, transcripts, and extracted document text. Use vector indexing for semantic search over documents, images, audio, or video when useful.

## Authorisation

Artefacts can be sensitive. A build log may include environment details. A model file may be proprietary. A source pack may contain private code. Apply relationship authorisation to artefact prefixes and search results, not only direct downloads.

## What you can build after this page

You should be able to model technical artefacts as objects with manifests, metadata, search, and authorisation so teams can find, verify, and recover them later.
