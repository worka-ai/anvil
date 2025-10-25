---
slug: /anvil/developer-guide/deep-dive/metadata
title: 'Deep Dive: Metadata and Indexing'
description: A detailed look at Anvil's metadata architecture, including the global vs. regional database split and the use of advanced PostgreSQL features.
tags: [developer-guide, architecture, metadata, postgres, ltree, pg_trgm]
---

# Chapter 14: Deep Dive: Metadata and Indexing

> **TL;DR:** A global Postgres stores tenants/buckets, while regional Postgres instances use `ltree` and `pg_trgm` for powerful, scalable object indexing.

Anvil's approach to metadata is one of its most critical architectural decisions, designed for massive scale and query flexibility. Instead of using a simple key-value store or a single monolithic database, Anvil splits metadata storage into two distinct roles: a **global database** and one or more **regional databases**.

### The Global Database

The global database is the single source of truth for data that is low-volume but has high importance and global relevance. All nodes in an Anvil deployment, regardless of their region, connect to this single database.

**Schema (`migrations_global/`):**

-   `tenants`: Stores tenant information, including their names and API keys.
-   `buckets`: Defines each bucket, its owner (`tenant_id`), and, crucially, which `region` it belongs to.
-   `apps`: Contains the `client_id` and encrypted `client_secret` for each application.
-   `policies`: Maps apps to the actions and resources they are permitted to access.
-   `regions`: A simple table that registers all available regions in the deployment.

This centralized approach for top-level resources simplifies management and ensures consistency across the entire system.

### The Regional Database

The regional database is where Anvil handles the massive scale of object metadata. Each region in your deployment has its own, completely independent PostgreSQL database. This design is key to Anvil's scalability.

**Schema (`migrations_regional/`):**

The `objects` table is the centerpiece of the regional schema. It contains columns for:

-   `bucket_id` and `tenant_id`: To associate the object with its owner.
-   `key`: The user-visible name of the object.
-   `content_hash`: The BLAKE3 hash of the object's content, used for content-addressing.
-   `size`, `etag`, `content_type`, etc.: Standard object metadata.
-   `shard_map`: A JSONB column that stores the list of peer IDs responsible for holding the object's shards.

### Advanced Indexing with PostgreSQL Extensions

Anvil leverages powerful PostgreSQL extensions to provide query capabilities far beyond what a simple key-value store could offer.

#### `ltree` for Hierarchical Listing

To efficiently handle S3-style listing with prefixes and delimiters (which simulates a directory structure), Anvil uses the `ltree` extension.

-   **How it Works:** When an object is created, a trigger automatically converts its `key` (e.g., `"path/to/my/file.txt"`) into a special `ltree` format (e.g., `path.to.my.file_txt`).
-   **Querying:** This `ltree` representation allows for extremely fast hierarchical queries. A query for all objects under the `path/to/` prefix can use the `ltree` ancestor operator (`<@`), which is indexed using a GIST index for high performance.
-   **Benefit:** This avoids slow and inefficient `LIKE 'path/to/%'` queries, which do not perform well on large datasets.

#### `pg_trgm` for Flexible Searching

The `pg_trgm` (trigram) extension is used to provide more flexible, non-prefix-based search capabilities in the future.

-   **How it Works:** This extension breaks down text into three-character chunks (trigrams). It creates an index of these trigrams, allowing for very fast similarity matching and pattern searching.
-   **Potential Use Cases:** While not fully exposed in the current API, this lays the groundwork for features like:
    *   Finding objects with keys that *contain* a certain substring.
    *   Fuzzy searching for object keys.

By offloading complex indexing and querying to PostgreSQL, Anvil's application layer can remain simpler and more focused on the core logic of storage and retrieval, while still providing powerful and flexible metadata operations.
