---
slug: /anvil/operational-guide/monitoring
title: 'Operational Guide: Monitoring and Maintenance'
description: Learn how to monitor the health of your Anvil cluster and understand its maintenance processes.
tags: [operational-guide, monitoring, health, metrics, maintenance, prometheus]
---

# Chapter 9: Monitoring and Maintenance

> **TL;DR:** Anvil exposes a `/ready` endpoint for health checks and is designed to be monitored with standard tools like Prometheus. Background workers handle tasks like garbage collection and shard repair.

Running a distributed system in production requires robust monitoring and a clear understanding of its maintenance processes. Anvil is designed to be observable and resilient.

### 9.1. Health Checks and Readiness Probes

Each Anvil node exposes two HTTP endpoints for health monitoring:

-   **`/` (Health Check):** This is a simple endpoint that returns `200 OK` as long as the Anvil server process is running. It can be used for basic liveness probes.

-   **`/ready` (Readiness Check):** This is a more comprehensive check that should be used to determine if a node is ready to accept traffic. It returns `200 OK` only if:
    1.  The node can successfully connect to its databases (global and regional).
    2.  The node is part of a cluster and has discovered at least one peer (itself included).

In an orchestrator like Kubernetes, you should use the `/ready` endpoint for your readiness probes to ensure traffic is only routed to fully initialized nodes.

### 9.2. Key Metrics for Monitoring (Prometheus)

Anvil is designed to expose metrics in a Prometheus-compatible format. While the specific metrics will evolve, you should monitor the following key areas of the system:

-   **API Latency and Error Rates:**
    *   Latency for S3 and gRPC API calls (`PutObject`, `GetObject`).
    *   Rate of `4xx` and `5xx` errors.
-   **Cluster Membership:**
    *   Number of active peers in the cluster.
    *   Rate of peer churn (nodes joining or leaving).
-   **Storage Utilization:**
    *   Total storage capacity and usage across the cluster.
    *   Storage usage per tenant and per bucket.
-   **Task Queue:**
    *   Number of pending tasks in the queue.
    *   Rate of failed tasks.
-   **Shard Health:**
    *   Number of missing or corrupted shards.
    *   Rate of shard repair and rebalancing operations.

### 9.3. Backup and Recovery Strategy

Anvil's durability model is designed to withstand node failures, but a comprehensive backup strategy must also account for database failure.

-   **Database Backup:** Your primary backup responsibility is the **PostgreSQL databases**. You should use standard Postgres tools like `pg_dump` or continuous archiving (PITR) to back up both the **global** and all **regional** databases.
-   **Data Recovery:** In the event of a catastrophic failure where a sufficient number of nodes are lost to prevent erasure code reconstruction, you would:
    1.  Restore the PostgreSQL databases from your backup.
    2.  Restore the object data itself from your off-site backups (if you have them).
    3.  Launch a new Anvil cluster connected to the restored databases.

### 9.4. The Task Queue and Background Workers

Anvil uses a task queue within the global database to manage asynchronous, long-running, or deferrable operations. This ensures that the main API remains fast and responsive.

Each Anvil node runs a **background worker** that polls this queue for pending tasks.

**Key Tasks Handled by the Worker:**

-   **Garbage Collection:** When a user deletes an object or a bucket, it is initially "soft-deleted" (marked as deleted in the database). A `DeleteObject` or `DeleteBucket` task is enqueued. The background worker picks up this task and performs the actual physical deletion of the object shards from the storage nodes.
-   **Shard Repair:** The worker will eventually be responsible for periodically scanning for missing or corrupted shards and enqueuing tasks to reconstruct them from the remaining erasure-coded data.

Monitoring the health and depth of the task queue is a critical part of operating Anvil at scale.
