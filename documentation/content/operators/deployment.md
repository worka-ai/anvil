---
title: Deployment
description: Deploy Anvil nodes, configure durable storage, and understand operational topology.
---

# Deployment

**Goal:** deploy Anvil as a production service and understand what each node needs to be correct.

Anvil is one server process. Every node runs the same binary and can serve APIs, own partitions, execute background task leases, maintain indexes, evaluate authorization, and witness PersonalDB commits. There are no special worker-only nodes in the architecture. Background work is a leased responsibility inside the Anvil process.

## Minimal deployment unit

A node needs:

- the Anvil server binary;
- durable `STORAGE_PATH`;
- network access for gRPC/S3 API traffic;
- cluster listen and advertise addresses for distributed deployments;
- shared authentication and cluster secrets where nodes must trust each other;
- backup and monitoring around the storage path.

A one-node deployment uses the same implementation paths as a multi-node deployment. It is not a simplified storage engine.

## Docker example

```yaml
services:
  anvil:
    image: ghcr.io/anvil-storage/anvil:latest
    command: ["anvil", "--init-cluster", "true"]
    environment:
      REGION: local
      JWT_SECRET: change-me
      ANVIL_SECRET_ENCRYPTION_KEY: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
      ANVIL_CLUSTER_SECRET: local-cluster-secret
      API_LISTEN_ADDR: 0.0.0.0:50051
      PUBLIC_API_ADDR: http://localhost:50051
      STORAGE_PATH: /var/lib/anvil
    ports:
      - "50051:50051"
    volumes:
      - anvil_data:/var/lib/anvil
volumes:
  anvil_data:
```

Use stronger secrets and dedicated volumes in production.

## Bootstrap sequence

1. Start the first node with cluster initialization enabled.
2. Create the first region and tenant.
3. Create administrative applications and policies.
4. Start additional nodes with bootstrap addresses.
5. Confirm each node reports health and membership.
6. Run S3 PUT/GET/LIST smoke tests.
7. Run native auth, index, watch, and PersonalDB smoke tests.

## Storage path

`STORAGE_PATH` contains object bytes, metadata journals, manifests, indexes, authorization tuples, control-plane records, PersonalDB data, source indexes, task logs, and derived proof files. Treat it as the node's durable state. Do not mount it on ephemeral disk in production.

## Capacity planning

Plan separately for:

- object bytes;
- metadata and directory segments;
- full text postings;
- vector segments;
- PersonalDB logs and snapshots;
- source and model artifacts;
- temporary ingest and compaction space.

Vector and media-heavy workloads need more CPU and memory during indexing. PersonalDB-heavy workloads need low-latency durable writes for commit logs.
