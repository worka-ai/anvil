---
slug: /category/worka-anvil
title: Worka Anvil
---

# Worka Anvil

Worka Anvil is an open-source distributed storage and compute system
designed for simplicity, scalability, and future extensibility.

The goal is to provide a system that can start small --- a single-node
instance that fits into a Docker Compose file --- and scale seamlessly
to thousands of peers and millions of clients. Anvil is built in **Rust
2024 edition**, using **Postgres 17** for metadata indexing, and
**QUIC** for high-performance peer-to-peer networking.

What makes Anvil unique is that it isn't just about storage. From the
beginning, we also design for a world where peers can register **compute
capabilities**. Whether that's machine learning inference, data
processing, or other compute-heavy tasks, Anvil treats compute as a
first-class citizen alongside storage.

### How Anvil differs from others

-   **Operational simplicity:** only two moving parts --- Postgres and
    the Anvil binary.
-   **Scale from one node to many:** a single-node deployment grows
    naturally into a distributed cluster without re-architecture.
-   **Compute + storage unified:** peers can be storage-only,
    compute-only, or both.
-   **Modern networking:** QUIC is the foundation, avoiding TCP/TLS
    complexities.
-   **Postgres as metadata index:** rich queries via `ltree`, `pg_trgm`,
    and JSONB enable more than prefix-based lookups.
