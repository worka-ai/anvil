---
slug: /anvil/developer-guide/architecture
title: 'Developer Guide: Architectural Overview'
description: A high-level overview of the Anvil architecture, its guiding principles, and the core technologies it is built on.
tags: [developer-guide, architecture, principles, rust, quic, postgres]
---

# Chapter 10: Architectural Overview

> **TL;DR:** Anvil is a distributed system built on Rust, QUIC, and Postgres. It prioritizes operational simplicity and multi-tenancy, using erasure coding for durability and a gossip protocol for membership.

This guide is for developers who want to understand Anvil's internal workings. We begin with a high-level view of the system's architecture and the design decisions that shape it.

### 10.1. Guiding Principles

Anvil's design is guided by a few core principles:

1.  **Operational Simplicity:** The system should have as few moving parts as possible. Beyond the Anvil binary itself, PostgreSQL is the only external dependency. We deliberately avoid complex components like Zookeeper, etcd, or Kafka.

2.  **Scalability from One to Many:** Anvil is designed to run just as well on a developer's laptop in a single container as it does in a multi-node, multi-region cluster. The architecture for a single node is the same as for a thousand nodes, allowing for seamless scaling.

3.  **Durability through Redundancy:** Data durability is achieved via Reed-Solomon erasure coding, which provides a much higher level of durability for the same storage overhead compared to simple replication.

4.  **Performance-First:** We prioritize performance by using modern, efficient technologies. This includes zero-copy I/O where possible, a fully asynchronous Rust codebase built on Tokio, and a high-performance QUIC-based network protocol.

### 10.2. Core Technologies

The choice of technology is critical to achieving Anvil's design goals.

-   **Rust (2024 Edition):** Rust provides memory safety, fearless concurrency, and zero-cost abstractions. This allows us to write high-performance, low-level networking and storage code without sacrificing safety or stability. The asynchronous ecosystem, centered around `tokio`, is fundamental to the entire design.

-   **QUIC (via `quinn` and `libp2p`):** All peer-to-peer communication, for both cluster gossip and data transfer, happens over QUIC. This modern transport protocol, which runs over UDP, provides several advantages over traditional TCP:
    *   Built-in TLS 1.3 for security.
    *   Stream multiplexing without head-of-line blocking.
    *   Connection migration, improving resilience in dynamic network environments.

-   **PostgreSQL (v17+):** Postgres is used as the metadata and indexing layer. We made a key architectural decision to split the database into two distinct roles:
    *   A **Global** database stores low-volume, high-importance data like tenants, buckets, and security policies.
    *   **Regional** databases store the high-volume object metadata for each region. This allows the most frequent queries (listing objects) to remain local to a region, enabling massive horizontal scaling.

-   **Tonic and Axum:** The API layer is built using the `tonic` framework for the gRPC API and the `axum` framework for the S3-compatible HTTP gateway. They are integrated into a single server process, allowing Anvil to serve both protocols from one application.