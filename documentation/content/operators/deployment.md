---
title: Deployment
description: Deploy Anvil nodes, configure durable storage, identity, clustering, and post-deployment smoke checks.
---

# Deployment

**What this page gives you:** an operator's introduction to running Anvil in production. You will learn what a node is, what state must be durable, which network addresses matter, and what to verify before users depend on the deployment.

Anvil runs as one server process per node. Every node runs the same binary. A node can serve native API traffic, serve S3-compatible traffic, participate in cluster coordination, own partitions, evaluate authorisation, maintain indexes, run leased background responsibilities, and witness PersonalDB commits. There are no separate worker-only binaries; background responsibilities are selected inside Anvil processes.

## Deployment model

A production deployment needs:

| Component | Purpose |
| --- | --- |
| Anvil server process | Serves native API, S3-compatible gateway, cluster traffic, and background leases. |
| Durable `STORAGE_PATH` | Stores objects, journals, indexes, manifests, authz state, PersonalDB state, and control records. |
| Public API address | Endpoint clients use for native and S3-compatible requests. |
| Cluster address | Endpoint nodes use to communicate with one another. |
| Secrets | Token validation, encrypted control data, and cluster trust. |
| Backup target | Independent location for recoverable durable state. |
| Monitoring | Metrics, logs, lag, repair findings, and capacity signals. |

The storage path is not a cache. Losing it means losing durable Anvil state unless backup and replication can restore it.

## Single binary, distributed responsibilities

Anvil assumes a distributed environment. Nodes are peers, but responsibilities are leased so the cluster can decide which process performs background work. A node may be serving client requests while also maintaining a vector index generation or processing PersonalDB projections.

This design avoids a split between "API nodes" and "worker nodes". Operators should still plan capacity for both request traffic and background work.

## Addressing

Distinguish listen addresses from public addresses.

- A listen address is where the process binds locally.
- A public address is what clients or peers use to reach it.

In containers or behind proxies, these are often different. If Anvil starts but clients cannot reach it, inspect public addresses. If Anvil cannot start, inspect listen addresses, port conflicts, and permissions.

## Bootstrap sequence

A controlled deployment follows this shape:

```text
prepare durable volumes
  -> configure secrets and addresses
  -> start first node with cluster initialization
  -> register additional nodes with bootstrap addresses
  -> create initial tenant and application credentials
  -> create buckets and policies
  -> run S3 and native API smoke tests
  -> create indexes and verify watch-driven maintenance
  -> verify authorisation, search, and PersonalDB paths
```

Do not treat process start as deployment success. A node that starts but cannot authorize requests, persist objects, build indexes, or resume watches is not ready.

## Capacity planning

Plan for several resource families:

- object body storage and multipart temporary space;
- metadata and path indexes;
- full text postings, token data, and snippets;
- vector segments and HNSW graph memory;
- authorisation tuples and derived userset indexes;
- PersonalDB commits, snapshots, and projections;
- watch logs and cursor retention;
- background rebuild and repair headroom.

Vector and media indexing are CPU and memory heavy. Large object ingestion is IO heavy. PersonalDB commit witnessing is latency-sensitive. Measure them separately.

## Post-deployment smoke checks

Before opening the deployment to users, prove:

1. health endpoint reports ready;
2. tenant and application credentials can be created;
3. token acquisition works;
4. S3-compatible bucket create/list/delete works where supported;
5. S3 PUT/GET/HEAD/LIST/DELETE works;
6. signed streaming upload works;
7. reserved `_anvil/` namespace is rejected;
8. native object API writes and reads work;
9. metadata index query returns expected results;
10. full text and vector index definitions can be created;
11. authorisation tuple writes affect access decisions;
12. watches deliver object events and resume from cursors;
13. PersonalDB group open and commit returns a certificate;
14. backup snapshot can be created or scheduled;
15. metrics and logs include request ids and lag signals.

## What you can do after this page

You should be able to describe the moving parts of an Anvil deployment and run meaningful readiness checks. Next, learn identity and access operations.
