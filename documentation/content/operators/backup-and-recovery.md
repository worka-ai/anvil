---
title: Backup And Recovery
description: Back up Anvil state and recover nodes without violating consistency.
---

# Backup And Recovery

**Goal:** understand what must be backed up, how recovery works, and how to prove restored data is usable.

Anvil's durable state lives under `STORAGE_PATH`. Backups must preserve object bytes, journals, manifests, segments, indexes, authorization state, PersonalDB logs, snapshots, source artifacts, and control-plane records.

## What to back up

Back up the whole storage path consistently. Include:

- content chunks and inline payload journals;
- metadata journals and sealed segments;
- directory segments;
- manifests;
- full text and vector index segments;
- authorization tuple segments and derived proofs;
- PersonalDB logs, snapshots, schemas, projections, and certificates;
- source and model artifact manifests;
- task queue and control-plane journals.

## Restore principle

A restored node must validate manifests, hashes, journal chains, and segment envelopes before serving data. If a derived index cannot prove its source cursor and manifest, rebuild it from base data.

## Recovery sequence

1. Stop the affected node.
2. Restore its storage path from backup.
3. Start the node with the same identity and secrets where required.
4. Let Anvil validate journals, manifests, and segments.
5. Confirm health endpoints.
6. Run object PUT/GET/LIST smoke tests.
7. Run authorization checks.
8. Run search queries against known data.
9. Open PersonalDB groups and verify latest heads.
10. Confirm watch streams resume or rebuild from manifest checkpoints.

## Disaster recovery drills

Run restore drills before you need them. A backup that has never been restored is an assumption, not a recovery plan.

A useful drill restores into an isolated environment, runs checks, verifies search and PersonalDB state, and records timings.

## Data-loss boundaries

Anvil acknowledges writes only after required durable records are written. If storage below Anvil lies about durability, no software layer can fully compensate. Use storage systems and volumes with honest fsync semantics for production.
