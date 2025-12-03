---
slug: /
title: Introduction to Anvil
description: An overview of the Anvil architecture, its guiding principles, and the core technologies it is built on.
tags: [introduction, architecture, principles]
---

# Introduction to Anvil

Worka Anvil is an open-source distributed storage and compute system designed for simplicity, scalability, and future extensibility. The goal is to provide a system that can start small—a single-node instance that fits into a Docker Compose file—and scale seamlessly to thousands of peers and millions of clients.

What makes Anvil unique is that it isn't just about storage. It is also designed for a world where peers can register **compute capabilities**. Whether that's machine learning inference, data processing, or other compute-heavy tasks, Anvil treats compute as a first-class citizen alongside storage.

### Guiding Principles

Anvil's design is guided by a few core principles:

1.  **Operational Simplicity:** The system has as few moving parts as possible. Beyond the Anvil binary itself, PostgreSQL is the only external dependency.

2.  **Scalability from One to Many:** Anvil is designed to run just as well on a developer's laptop as it does in a multi-node, multi-region cluster. The architecture for a single node is the same as for a thousand nodes.

3.  **Durability and Security:** Data durability is achieved via Reed-Solomon erasure coding. As part of this process, data is also encrypted at rest, providing an essential layer of security.

4.  **Performance-First:** We prioritize performance by using modern, efficient technologies like Rust, Tokio, QUIC, and a two-phase commit process for safe, atomic writes.

### Core Technologies

-   **Rust (2024 Edition):** Provides memory safety, fearless concurrency, and zero-cost abstractions for high-performance, low-level networking and storage code.

-   **QUIC (via `libp2p`):** All peer-to-peer communication, for both cluster gossip and data transfer, happens over QUIC, a modern, secure, and high-performance transport protocol that runs over UDP.

-   **PostgreSQL (v17+):** Postgres is used as the metadata and indexing layer, split into two distinct roles:
    *   A **Global** database stores low-volume, high-importance data like tenants, buckets, and security policies.
    *   **Regional** databases store the high-volume object metadata for each region, enabling massive horizontal scaling.

-   **Tonic and Axum:** The API layer is built using `tonic` for the gRPC API and `axum` for the S3-compatible HTTP gateway, served from a single port for simplicity.
