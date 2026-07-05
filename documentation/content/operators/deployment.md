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
| Admin API address | Internal endpoint used by privileged operators and provisioners. Do not expose it publicly. |
| Cluster address | Endpoint nodes use to communicate with one another. |
| Secrets | Token validation, encrypted control data, server-side secret encryption, bootstrap administration, and cluster trust. |
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

Run public API traffic and admin API traffic on separate listeners. `API_LISTEN_ADDR` serves native API and S3-compatible traffic. `ADMIN_LISTEN_ADDR` serves administrative gRPC operations such as tenant creation, application provisioning, policy changes, lifecycle operations, diagnostics, repair, and secret key rotation. Bind the admin listener to loopback or an internal network; do not publish it next to public object traffic.

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

Do not treat process start as deployment success. A node that starts but cannot authorise requests, persist objects, build indexes, or resume watches is not ready.


## Secrets and First Bootstrap

Generate the server-side secret-encryption key before starting a persistent deployment:

```bash
admin key generate-secret-encryption-key
```

Store the printed value in a secret manager and inject it as `ANVIL_SECRET_ENCRYPTION_KEY` on Anvil server processes. The `admin` CLI does not use this key. It is server-only material for encrypted records such as application secrets, ingestion tokens, and encrypted shard files.

Set `ANVIL_BOOTSTRAP_ADMIN_TOKEN` only for first setup. Then create a tenant, create an administrative application, grant explicit admin scopes, and remove the bootstrap token:

```bash
export ANVIL_AUTH_TOKEN="$ANVIL_BOOTSTRAP_ADMIN_TOKEN"

admin --host http://127.0.0.1:50052 tenant create \
  --name default \
  --home-region eu-west-1 \
  --audit-reason "initial tenant"

admin --host http://127.0.0.1:50052 app create \
  --tenant-id default \
  --app-name ops-admin \
  --audit-reason "initial admin app"

admin --host http://127.0.0.1:50052 policy grant \
  --tenant-id default \
  --app-name ops-admin \
  --action 'anvil_admin:*' \
  --resource 'anvil_admin:cluster:default' \
  --audit-reason "grant admin access"
```

Rotate `ANVIL_SECRET_ENCRYPTION_KEY` by restarting with a new active key and the old key in `ANVIL_SECRET_ENCRYPTION_PREVIOUS_KEYS`, running `admin secret-encryption-key rotate`, then removing the old key from configuration after verification.

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
