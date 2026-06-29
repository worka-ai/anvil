---
title: Deployment
description: Deploy Anvil as a production service and understand the operational topology.
---

# Deployment

**What this page achieves:** you will understand what an Anvil deployment is, what every node needs, how distributed operation is organized, and how to prove a deployment is ready to receive production traffic.

Anvil runs as one server process. Each node runs the same binary. A node can serve gRPC and S3-compatible requests, own partitions, maintain indexes, evaluate authorization, execute leased background work, and witness PersonalDB commits. There are no separate worker-only binaries. Background responsibilities are selected inside the Anvil process through leases.

## Deployment model

A deployment contains:

- one or more Anvil nodes;
- durable storage for each node;
- a region identity;
- cluster authentication material;
- API endpoints for native gRPC and S3-compatible traffic;
- credentials for tenants, applications, and administrators;
- monitoring, backup, and recovery automation.

A single-node deployment uses the same storage and API model as a multi-node deployment. It is useful for development, small installations, and local testing. Distributed deployment is the default operational assumption for production capacity and resilience.

## What a node needs

Each node needs:

| Requirement | Why it matters |
| --- | --- |
| Anvil server binary | Runs native API, S3 gateway, cluster logic, and background leases. |
| Durable `STORAGE_PATH` | Stores objects, journals, indexes, manifests, authz state, PersonalDB state, and control records. |
| API listen address | Receives client traffic. |
| Public API address | Tells clients and peers how to reach the node. |
| Cluster secret | Authenticates internal node communication. |
| Token/credential secrets | Validates client identity and protects stored secrets. |
| Monitoring | Detects lag, rejection rates, repair findings, and storage pressure. |
| Backups | Allows recovery of durable state. |

Do not run production Anvil on ephemeral storage. If the storage path disappears, the node's durable records disappear with it.

## Bootstrap sequence

A production bootstrap should be repeatable:

1. Prepare durable storage and secrets.
2. Start the first node with initialization enabled.
3. Create the first region, tenant, and administrative application.
4. Grant only the permissions required for bootstrap automation.
5. Start additional nodes with cluster addresses and shared cluster trust.
6. Confirm health and membership.
7. Run native object PUT/GET/LIST checks.
8. Run S3 compatibility checks.
9. Run authorization tuple and reserved namespace checks.
10. Create a metadata index and verify query results.
11. Start a watch and verify it receives a write event.
12. Open a PersonalDB group and verify a commit certificate.

The last four steps matter because a deployment that only serves object GET is not yet proving the integrated Anvil surface.

## Container deployment

A minimal container shape is:

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

This example shows shape, not final security posture. Production deployments should use managed secret injection, explicit image versions, durable volumes, TLS termination or direct TLS support, and monitored health checks.

## Capacity planning

Plan capacity by workload family:

- object bytes and multipart temporary space;
- metadata journals and compacted metadata segments;
- directory/path indexes;
- full text postings and stored snippets;
- vector segments and HNSW graph memory;
- authorization tuple logs and derived userset indexes;
- PersonalDB commit logs, snapshots, and projections;
- source and model artifacts;
- temporary ingestion, extraction, and repair workspace.

Vector and media indexing are CPU and memory heavy. PersonalDB commit witnessing is latency sensitive. Large object ingestion needs IO bandwidth and temporary space. Plan them separately rather than using only total object bytes as the capacity metric.

## Readiness checks

A node is ready for production only when it can prove:

- API health endpoints respond;
- object write/read/list works through native API;
- S3 PUT/GET/HEAD/LIST/DELETE works;
- reserved namespaces are denied through public APIs;
- token scope authorization works;
- relationship tuple checks work;
- metadata index queries work;
- watch streams deliver events;
- PersonalDB commits return certificates;
- metrics and logs include request ids;
- backup and restore drill has been performed.

## What you can do after this page

You should be able to deploy Anvil deliberately and verify that the deployment is more than a basic object endpoint. Next, learn how to operate identity and access safely.
