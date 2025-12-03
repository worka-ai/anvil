---
slug: /architecture/storage
title: 'Deep Dive: Distributed Storage'
description: A detailed look at how Anvil achieves durability and efficiency using encryption, erasure coding, and content addressing.
tags: [architecture, deep-dive, storage, erasure-coding, encryption, sharding, placement]
---

# Deep Dive: Distributed Storage

> **TL;DR:** Objects are encrypted, erasure-coded into shards, and distributed across peers using Rendezvous Hashing.

Anvil's storage engine is designed for durability, efficiency, and scalability. It achieves this by combining four core concepts: at-rest encryption, content-addressable storage, Reed-Solomon erasure coding, and Rendezvous Hashing for placement.

### Content-Addressable Storage

Internally, Anvil does not store objects by their key. Instead, it stores them by the hash of their content. When an object is uploaded, the `ObjectManager` calculates its **BLAKE3 hash**. This hash becomes the object's unique, immutable identifier at the storage layer.

This approach has several advantages:

-   **Automatic Deduplication:** If two different users upload the exact same file, Anvil will calculate the same hash and store the data only once.
-   **Data Integrity:** The content hash acts as a built-in checksum, guaranteeing that data has not been corrupted at rest or in transit.

### At-Rest Encryption and Erasure Coding

To provide durability and security, Anvil uses **Reed-Solomon erasure coding** combined with strong encryption, managed by the `ShardManager` (`src/sharding.rs`).

#### Encryption

A critical, verified detail from the implementation is that Anvil performs **encryption at rest**. Before an object is processed for sharding, its raw data is first encrypted using the `ANVIL_SECRET_ENCRYPTION_KEY`. This means the data shards stored on disk are always encrypted, and are decrypted on-the-fly during a read request. This provides a powerful layer of security for all stored data.

#### Erasure Coding

Instead of making full, expensive copies (replication), Anvil splits the encrypted data into smaller pieces and then calculates a few extra "parity" pieces that can be used to rebuild the original data if any of the other pieces are lost.

-   **The `4+2` Scheme:** The current implementation uses a `4+2` scheme:
    *   `k = 4` data shards
    *   `m = 2` parity shards
-   **Encoding:** When an object is written, each chunk of it is encrypted, then split into 4 data shards. The Reed-Solomon algorithm calculates 2 additional parity shards.
-   **Distribution:** All 6 shards (4 data + 2 parity) are then distributed to 6 different peers in the cluster.
-   **Reconstruction:** The key property is that the original data can be reconstructed from **any 4** of the 6 shards. This means the cluster can tolerate the complete failure of any 2 nodes holding shards for a given object without any data loss.

This provides the same durability as 3x replication but with only **1.5x** storage overhead instead of 3x.

### Shard Placement with Rendezvous Hashing

Once an object has been erasure-coded into a set of shards, the system must decide which peers will store them. Anvil uses **Rendezvous Hashing** (also known as Highest Random Weight, or HRW, hashing) for this, implemented in the `PlacementManager` (`src/placement.rs`).

**The Algorithm:**

1.  To find the `N` peers for an object's `N` shards, the `PlacementManager` iterates through all known peers in the cluster.
2.  For each peer, it calculates a score by creating a BLAKE3 hash of the object's key combined with the peer's unique ID.
3.  It sorts the peers by this score in descending order.
4.  The top `N` peers from this sorted list are chosen as the storage targets for the `N` shards.

**Advantages of Rendezvous Hashing:**

-   **Decentralized and Deterministic:** Any node can independently calculate the correct placement for any object without a central authority.
-   **Minimal Disruption:** When a node is added to or removed from the cluster, only a small fraction of objects need to be rebalanced, providing greater stability than traditional consistent hashing.
