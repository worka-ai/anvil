---
title: Backup And Recovery
description: Back up and restore the full Anvil state, including objects, metadata, indexes, authz, watches, and PersonalDB.
---

# Backup And Recovery

**What this page gives you:** a complete recovery model for Anvil. You will learn why backing up object bytes alone is incomplete and how to verify a restored deployment.

Anvil's durable state lives under `STORAGE_PATH`. That state includes object bytes, but it also includes the metadata and proofs that make the bytes useful and safe. A backup that captures only object bodies is not an Anvil backup.

## What must be recoverable

A complete backup strategy covers:

- object bodies and multipart state;
- object metadata journals and compaction outputs;
- bucket and placement metadata;
- path and directory indexes;
- metadata index definitions and generations;
- full text index segments and tokenizer metadata;
- vector index segments and HNSW graph material;
- authorisation schemas, tuples, caveats, and derived indexes;
- watch logs, cursors, and checkpoints;
- PersonalDB commits, snapshots, projections, and certificates;
- source and model artefact manifests;
- control-plane secrets where recoverable by policy;
- diagnostic and repair findings.

## Backup principles

A backup should be:

- consistent enough to restore source facts and derived structures;
- independently stored from the failed deployment;
- encrypted when it contains sensitive data;
- regularly tested through restore drills;
- versioned so corrupted latest state does not destroy recovery options.

## Restore sequence

A restore should be deliberate:

```text
stop affected writers or isolate target
  -> restore durable state
  -> start Anvil in recovery mode or controlled environment
  -> validate object manifests and hashes
  -> validate authorisation state
  -> validate watch cursors
  -> validate index generations
  -> rebuild invalid derived structures
  -> run S3 and native API smoke tests
  -> open PersonalDB groups and verify heads
  -> resume normal traffic
```

Do not skip validation. A deployment that starts after restore may still have stale indexes or invalid authorisation-derived state.

## Recovery testing

A release-quality recovery drill should prove:

1. objects can be read by key and version;
2. checksums match;
3. prefix listings match expected counts;
4. metadata filters return expected results;
5. full text and vector indexes either validate or rebuild;
6. relationship authorisation decisions match expected fixtures;
7. reserved namespaces remain inaccessible through public APIs;
8. watches resume without event loss;
9. PersonalDB groups open and accepted heads match certificates;
10. clients can authenticate and perform ordinary operations.

## What you can do after this page

You should be able to design and test backups that preserve the full Anvil system, not only object bytes.
