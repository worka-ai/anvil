---
title: Backup And Recovery
description: Back up Anvil state, restore nodes, and prove recovered data is correct.
---

# Backup And Recovery

**What this page achieves:** you will know what to back up, how recovery proceeds, and how to prove restored Anvil data is safe to serve.

Anvil's durable state lives under `STORAGE_PATH`. A backup strategy that captures only object bytes is incomplete. You must preserve metadata journals, manifests, indexes, authorization state, PersonalDB logs, source artifacts, and control-plane records.

## What must be backed up

Back up the full storage path consistently. It contains:

- object content chunks and inline payload records;
- metadata journals and compacted metadata segments;
- directory/path index segments;
- manifests and source cursor proofs;
- full text index segments and tokenizer metadata;
- vector index segments and HNSW graph material;
- authorization tuple logs, schemas, caveats, and derived indexes;
- PersonalDB commit logs, snapshots, projections, and certificates;
- source and model artifact manifests;
- task leases, task journals, and diagnostic records.

These pieces work together. Restoring only some of them can produce invalid derived state.

## Backup consistency

A backup must capture a coherent point or enough journal material for Anvil to recover to a coherent point. Do not copy random files while writes are active unless the storage layer provides snapshot semantics.

Recommended pattern:

1. Use volume snapshot capabilities or Anvil-supported backup coordination.
2. Record node identity, region, software version, and configuration hash.
3. Store backup metadata outside the same failure domain.
4. Periodically restore into an isolated environment and verify it.

## Restore sequence

A restore should be deliberate:

1. Stop the affected node.
2. Restore `STORAGE_PATH` from a known backup.
3. Restore required secrets and node identity.
4. Start Anvil in recovery mode or normal startup with validation enabled.
5. Validate manifests, hashes, journal chains, and segment envelopes.
6. Rebuild any derived output that cannot prove its source.
7. Confirm health and membership.
8. Run native object smoke tests.
9. Run S3 compatibility smoke tests.
10. Run authorization checks.
11. Run index/search checks.
12. Open PersonalDB groups and verify current heads.
13. Confirm watch streams resume from valid cursors.

## Disaster recovery drills

A backup that has never been restored is only a theory. Run drills. Measure:

- time to locate a backup;
- time to restore storage;
- time for validation and rebuilds;
- time for indexes to reach target cursors;
- application-visible downtime;
- operator steps that were unclear.

Turn drill findings into runbook updates.

## Data loss boundaries

Anvil acknowledges writes only after required durable records are written. If the underlying storage lies about durability, loses acknowledged fsyncs, or corrupts blocks without detection, no software layer can fully compensate. Use reliable volumes and monitor storage errors.

## What you can do after this page

You should be able to design backups that include the full Anvil state and run restores that prove object, index, authorization, watch, and PersonalDB correctness.
