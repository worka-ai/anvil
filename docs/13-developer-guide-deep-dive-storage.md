---
slug: /anvil/developer-guide/deep-dive/storage
title: 'Deep Dive: Distributed Storage'
description: A detailed look at how Anvil achieves durability and efficiency using erasure coding, content addressing, and shard placement.
tags: [developer-guide, architecture, storage, erasure-coding, sharding, placement]
---

# Chapter 13: Deep Dive: Distributed Storage

> **TL;DR:** Objects are made durable by splitting them into erasure-coded shards and distributing them across peers using Rendezvous Hashing.

Anvil's storage engine is designed for durability, efficiency, and scalability. It achieves this by combining three core concepts: content-addressable storage, Reed-Solomon erasure coding, and Rendezvous Hashing for placement.

### Content-Addressable Storage

Internally, Anvil does not store objects by their key. Instead, it stores them by the hash of their content. When an object is uploaded, the `ObjectManager` calculates its **BLAKE3 hash**. This hash becomes the object's unique, immutable identifier at the storage layer.

This approach has several advantages:

-   **Automatic Deduplication:** If two different users upload the exact same file (e.g., a popular video or a common library), Anvil will calculate the same hash and store the data only once. The metadata layer simply creates two separate entries in the `objects` table that both point to the same `content_hash`.
-   **Data Integrity:** The content hash acts as a built-in checksum. After retrieving an object, its hash can be recalculated and compared against the stored hash to guarantee that the data has not been corrupted at rest or in transit.

### Erasure Coding with Reed-Solomon

To provide durability without the high storage cost of full replication, Anvil uses **Reed-Solomon erasure coding**. The `ShardManager` (`src/sharding.rs`) is responsible for this process.

-   **Configuration:** Anvil is configured with a `k+m` scheme. The current implementation uses a `4+2` scheme:
    *   `k = 4` data shards
    *   `m = 2` parity shards
-   **Encoding:** When an object is written, it is processed in stripes. Each stripe is split into 4 data shards. The Reed-Solomon algorithm is then used to calculate 2 additional parity shards from the data shards.
-   **Distribution:** All 6 shards (4 data + 2 parity) are then distributed to 6 different peers in the cluster.
-   **Reconstruction:** The key property of this `4+2` scheme is that the original data can be reconstructed from **any 4** of the 6 shards. This means the cluster can tolerate the complete failure of any 2 nodes holding shards for a given object without any data loss.

This provides the same durability as 3x replication (which can tolerate 2 failures) but with only 1.5x storage overhead (6 shards stored for 4 shards of data) instead of 3x.

### Shard Placement with Rendezvous Hashing

Once an object has been erasure-coded into a set of shards, the system must decide which peers will store them. Anvil uses **Rendezvous Hashing** (also known as Highest Random Weight, or HRW, hashing) for this, implemented in the `PlacementManager` (`src/placement.rs`).

**The Algorithm:**

1.  To find the `N` peers for an object's `N` shards, the `PlacementManager` iterates through all known peers in the `ClusterState`.
2.  For each peer, it calculates a score by hashing the object's key with the peer's unique ID.
    ```rust
    // Simplified example
    let mut hasher = AHasher::default();
    object_key.hash(&mut hasher);
    peer_id.hash(&mut hasher);
    let score = hasher.finish();
    ```
3.  It sorts the peers by this score in descending order.
4.  The top `N` peers from this sorted list are chosen as the storage targets for the `N` shards.

**Advantages of Rendezvous Hashing:**

-   **Decentralized and Deterministic:** Any node can independently calculate the correct placement for any object, without needing to consult a central authority. The placement is consistent as long as the cluster membership doesn't change.
-   **Minimal Disruption:** When a node is added to or removed from the cluster, only a small fraction of objects (`1/n`, where `n` is the number of nodes) need to be rebalanced. This provides much greater stability than traditional consistent hashing.
