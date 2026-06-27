---
title: Metadata Architecture
description: Anvil's native metadata architecture.
tags: [architecture, deep-dive, metadata, indexing]
---

# Metadata Architecture

Anvil owns its metadata store. Object records, bucket state, task state, authorization tuple state, index definitions, index events, manifests, and model artifact metadata are persisted below each node's `STORAGE_PATH`.

The current native store keeps control-plane records in Anvil-managed state files while object version state is read from the metadata journal. The target architecture is a partitioned native metadata engine where append-only mutation records, sealed manifests, and derived indexes are maintained by Anvil itself.

## Metadata Responsibilities

Anvil metadata tracks:

- tenants, applications, encrypted app secrets, and app policies;
- buckets, bucket metadata events, public access state, and deletion state;
- object versions, delete markers, mutation ids, content hashes, user metadata, shard maps, and inline payload metadata;
- multipart upload sessions and parts;
- task queues and background work state;
- authorization tuples and tuple indexes;
- index definitions, index events, diagnostics, and search/vector index materialization state.

## Indexing

Indexes are native Anvil data structures maintained from object and metadata watch streams. Index definitions describe what each bucket indexes. Watch events are durable inputs to the indexing pipeline, allowing index workers selected inside the Anvil process to catch up deterministically.

## Reserved Namespace

Paths under Anvil's reserved internal namespace are owned by Anvil. Public object APIs must reject direct reads and writes to those paths. Internal components may mutate them only through private Anvil code paths with server-minted authority.
