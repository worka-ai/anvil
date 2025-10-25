---
slug: /anvil/appendices/erasure-coding
title: 'Appendix D: Erasure Coding Explained'
description: A simplified explanation of how Anvil uses Reed-Solomon erasure coding to provide high durability with low storage overhead.
tags: [appendices, architecture, erasure-coding, durability, sharding]
---

# Appendix D: Erasure Coding Explained

Erasure coding is a mathematical method for turning data into a larger, redundant form that can withstand the loss of some of its parts. It is the core technology that Anvil uses to provide high durability for your data without the high storage cost of simple replication.

### The Problem with Replication

The simplest way to make data durable is **replication**. If you want to survive the loss of a disk, you can simply store two copies of your file on two different disks. If you want to survive the loss of two disks, you store three copies. 

-   **Pro:** It's very simple to understand and implement.
-   **Con:** It's very expensive. To tolerate `m` failures, you need `m+1` copies of your data, leading to a `3x` storage cost to survive 2 failures, for example.

### How Erasure Coding Works

Anvil uses a specific type of erasure coding called **Reed-Solomon**. Instead of making full copies, it splits the data into smaller pieces and then calculates a few extra, special pieces that can be used to rebuild the original data if any of the other pieces are lost.

Think of it like the equation `x + y = z`.

-   If you know `x=2` and `y=3`, you can calculate that `z=5`.
-   But if you only know `x=2` and `z=5`, you can still figure out that `y=3`.

Erasure coding applies this concept on a much larger scale.

**The `k+m` Scheme**

Anvil uses a `k+m` scheme, which is currently set to `4+2`:

-   `k = 4`: The original data is split into **4 data shards**.
-   `m = 2`: The algorithm calculates **2 parity shards** from the data shards.

This results in a total of `k+m = 6` shards for each chunk of data.

**The Magic of Reconstruction**

These 6 shards are then stored on 6 different nodes in the Anvil cluster. The key property of the Reed-Solomon algorithm is that you can reconstruct the original 4 data shards from **any 4 of the 6 total shards**.

This means the cluster can suffer the loss of **any 2 nodes** holding shards for that data, and Anvil can still rebuild the original data perfectly.

### Durability vs. Storage Cost

| Method              | To Tolerate 2 Failures | Storage Overhead |
| ------------------- | ---------------------- | ---------------- |
| **3x Replication**  | Store 3 full copies    | **300%**         |
| **Anvil (4+2 EC)**  | Store 6 shards (4 data, 2 parity) | **150%**         |

As you can see, erasure coding provides the same level of fault tolerance as 3x replication but with half the storage cost. This makes it a much more efficient and scalable solution for a large-scale storage system.
