---
slug: /anvil/developer-guide/deep-dive/compute
title: 'Deep Dive: Compute Capabilities'
description: An exploration of Anvil's vision for unifying storage and compute, allowing peers to execute jobs directly on the data fabric.
tags: [developer-guide, architecture, compute, jobs, scheduling]
---

# Chapter 15: Deep Dive: Compute Capabilities

> **TL;DR:** Peers can register compute capabilities. A scheduler uses HRW hashing to dispatch jobs, turning the storage fabric into a compute fabric.

Worka Anvil is designed to be more than just a distributed storage system; it is a foundation for a unified **storage and compute fabric**. The architecture anticipates a world where the system not only stores data but also performs computations directly on that data, minimizing data movement and maximizing efficiency.

While this feature is still in its early stages, the architectural groundwork and API definitions are already in place.

### The Vision: Data-Local Compute

The core idea is to bring the computation to the data. Instead of a client downloading a massive dataset, running a computation, and then uploading the result, the client can submit a **Job** to Anvil. Anvil then schedules this job to run on a peer that is best suited for the task, ideally one that already holds some or all of the required data.

This is particularly powerful for workloads like:

-   **Machine Learning Inference:** A peer with a GPU can register an `inference` capability. A client can then submit a job with an image and a model name, and the peer will run the inference and return the result.
-   **Data Transformation:** A job could be submitted to resize an image, transcode a video, or convert a CSV file to Parquet format.
-   **Complex Queries:** A job could perform a complex analysis over a large dataset stored in Anvil, returning only the final, aggregated result.

### Architectural Components

The implementation of this vision relies on a few key components defined in the database schema and gRPC API.

#### 1. Capability Registry

The `compute_capabilities` table in the global PostgreSQL database acts as a registry for the compute resources available in the cluster.

```sql
CREATE TABLE compute_capabilities (
    peer_id UUID NOT NULL,
    region TEXT NOT NULL,
    capability TEXT NOT NULL, -- e.g., "inference:llama3", "video:transcode:h264"
    resources JSONB,          -- e.g., {"gpu": "true", "memory_gb": 16}
    max_concurrency INT,
    PRIMARY KEY(peer_id, capability)
);
```

When a peer with compute resources comes online, it registers its capabilities in this table. This allows the scheduler to discover which nodes can perform which tasks.

#### 2. The `ComputeService` gRPC API

The `anvil.proto` file defines the `ComputeService`, which is the user-facing entry point for submitting and managing jobs.

```proto
service ComputeService {
  rpc RegisterCapability(RegisterCapabilityRequest) returns (RegisterCapabilityResponse);
  rpc SubmitJob(SubmitJobRequest) returns (SubmitJobResponse);
  rpc GetJobStatus(GetJobStatusRequest) returns (GetJobStatusResponse);
}
```

-   `RegisterCapability`: Called by a compute peer on startup to advertise its capabilities.
-   `SubmitJob`: Called by a client to request a computation.
-   `GetJobStatus`: Called by a client to check on the progress of a submitted job.

#### 3. Job Scheduling

When a client calls `SubmitJob`, the Anvil scheduler performs the following steps:

1.  **Filter Peers:** It queries the `compute_capabilities` table to find all peers that have registered the required capability (e.g., `video:transcode:h264`) and are in the desired region.
2.  **Select a Peer:** It uses **Rendezvous Hashing (HRW)**, similar to shard placement, to select a peer from the filtered list. This provides load balancing and deterministic scheduling. The hashing could be influenced by the job's input data key, promoting data locality.
3.  **Dispatch Job:** The scheduler sends the job to the selected peer.

#### 4. Job Execution

The peer that receives the job is responsible for executing it. The execution environment is designed to be pluggable, but the primary methods would be:

-   **Containers (Podman/Docker):** For complex, non-sandboxed workloads, the peer could use a container runtime to execute the job.
-   **WASM Runtimes:** For secure, sandboxed, and portable compute, WebAssembly is an ideal choice.

If the job requires a model or other large assets, the peer can fetch them directly from Anvil's storage layer, benefiting from the distributed and data-local nature of the system.
