---
slug: /anvil/developer-guide/request-lifecycle
title: 'Developer Guide: The Lifecycle of a Request'
description: A step-by-step walkthrough of how Anvil handles a PutObject (write) and GetObject (read) request.
tags: [developer-guide, architecture, write-path, read-path, sharding, placement]
---

# Chapter 11: The Lifecycle of a Request

> **TL;DR:** A `PutObject` request is received, erasure coded into shards, and distributed to peers determined by Rendezvous Hashing. A `GetObject` request reconstructs the object from a subset of those shards, transparently handling node failures.

Understanding the path a request takes through the system is key to understanding Anvil's architecture. This chapter follows a `PutObject` (write) and a `GetObject` (read) request from start to finish.

### 11.1. Write Path: Ingest, Sharding, and Placement

When a client initiates a `PutObject` operation, either via the S3 gateway or the gRPC API, the following sequence of events occurs on the coordinating node that receives the request:

1.  **Authentication and Authorization:** The request is first processed by the authentication middleware. For S3, this involves verifying the SigV4 signature. For gRPC, it involves validating the JWT. The system then checks if the authenticated App has `write` permission for the target bucket and key.

2.  **Placement Calculation:** The `ObjectManager` calls the `PlacementManager` to determine which nodes in the cluster should store the object's shards. It uses **Rendezvous Hashing** (also known as Highest Random Weight hashing) for this. For a given object key, it hashes the key with each peer's ID to calculate a score. The peers with the highest scores are selected as storage targets. This is a deterministic and decentralized process.

3.  **Graceful Fallback:** If the number of available peers is less than the required number for erasure coding, Anvil gracefully falls back to a single-node storage model. It streams the entire object to a temporary file on the local disk, calculates its hash, and then moves it to its final content-addressed location.

4.  **Streaming and Erasure Coding (Distributed Case):** If enough peers are available, the `ObjectManager` begins streaming the object data from the client. It does not store the whole object in memory. Instead, it processes the data in "stripes":
    a.  It reads a chunk of data from the stream (e.g., 256KB).
    b.  This stripe is divided into `k` data shards (e.g., 4 shards of 64KB each).
    c.  The `ShardManager` is called to perform Reed-Solomon **erasure coding**, which generates an additional `m` parity shards (e.g., 2 parity shards of 64KB each).

5.  **Shard Distribution:** The coordinating node establishes gRPC connections to the `InternalAnvilService` on each of the target peers selected in Step 2. It then streams each data and parity shard to its designated peer. The peers store these shards as temporary files.

6.  **Final Commit:** After the entire object has been streamed and all its shards have been sent, the coordinating node calculates the final BLAKE3 hash of the complete object. It then sends a `CommitShard` RPC call to each peer, telling them to move the temporary shard files to their final, permanent location, named after the final object hash and shard index (e.g., `<hash>-00`, `<hash>-01`).

7.  **Metadata Insertion:** Finally, the coordinating node writes the object's metadata (bucket ID, key, size, content hash, shard map, etc.) into the regional PostgreSQL database. The request is now complete, and a success response is returned to the client.

### 11.2. Read Path: Discovery, Reconstruction, and Streaming

When a client requests to `GetObject`:

1.  **Authentication and Authorization:** The request is authenticated and authorized, checking for `read` permission on the object.

2.  **Metadata Lookup:** The coordinating node queries the regional PostgreSQL database to retrieve the object's metadata, including its `content_hash` and, crucially, its `shard_map`.

3.  **Single-Node Fallback:** If the `shard_map` is empty, it means the object was stored as a whole file. The node simply reads the file from its local disk and streams it back to the client.

4.  **Shard Discovery (Distributed Case):** If a `shard_map` exists, the node knows which peers are supposed to hold which shards. It then attempts to read the shards it holds locally from its own disk.

5.  **Remote Fetching:** For any shards that are not available locally (or if the coordinating node doesn't hold any shards itself), it makes a `GetShard` gRPC call to the appropriate peer(s) identified in the shard map to fetch the missing shard data.

6.  **On-the-Fly Reconstruction:** The `ShardManager` is given the collection of shards (some local, some fetched remotely). Reed-Solomon coding only requires *any* `k` of the `k+m` total shards to reconstruct the original data. The `reconstruct` method is called, which rebuilds the original data stripes from the available shards. This happens transparently, even if some nodes are offline.

7.  **Streaming to Client:** As the data is reconstructed, it is streamed immediately back to the client, without ever buffering the entire object in memory on the coordinating node.
