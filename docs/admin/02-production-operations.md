---
title: Production Operations
description: Operating Anvil in production.
---

# Production Operations

Back up each node's `STORAGE_PATH` according to the durability policy for the deployment. The storage path contains object bytes, metadata journals, indexes, manifests, and control-plane state. Recovery consists of restoring the storage path for each node and restarting the Anvil process with the same secrets and advertised addresses.

Operational checks should cover:

- disk capacity and write latency for `STORAGE_PATH`;
- node readiness endpoints;
- cluster peer visibility;
- object watch and index lag;
- authorization tuple ingestion and reserved namespace rejection;
- backup restore drills using restored storage paths.
