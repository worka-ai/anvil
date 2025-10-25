---
slug: /anvil/appendices/postgres-schema
title: 'Appendix C: Postgres Schema DDL'
description: A reference copy of the SQL Data Definition Language (DDL) for Anvil's global and regional databases.
tags: [appendices, postgres, schema, sql, ddl]
---

# Appendix C: Postgres Schema DDL

This appendix provides the complete SQL schema for both the global and regional PostgreSQL databases used by Anvil. This is useful for understanding the underlying data model and for setting up databases manually.

### Global Database Schema

This schema defines the tables for globally relevant data, such as tenants, buckets, apps, and policies. All nodes in a deployment connect to this single database.

```sql
-- From migrations_global/V1__initial_global_schema.sql

CREATE TABLE regions (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tenants (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    api_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE buckets (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL UNIQUE,
    region TEXT NOT NULL REFERENCES regions(name) ON DELETE CASCADE,
    is_public_read BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE apps (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    client_id TEXT UNIQUE NOT NULL,
    client_secret_encrypted BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(tenant_id, name)
);

CREATE TABLE policies (
    id BIGSERIAL PRIMARY KEY,
    app_id BIGINT NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    resource TEXT NOT NULL, -- e.g., "my-bucket/folder/*"
    action TEXT NOT NULL,   -- e.g., "read", "write", "grant"
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(app_id, resource, action)
);

CREATE TYPE task_status AS ENUM ('pending', 'running', 'completed', 'failed');
CREATE TYPE task_type AS ENUM ('DELETE_OBJECT', 'DELETE_BUCKET', 'REBALANCE_SHARD');

CREATE TABLE tasks (
    id BIGSERIAL PRIMARY KEY,
    task_type task_type NOT NULL,
    payload JSONB NOT NULL,

    -- Scheduling and Execution
    priority INT NOT NULL DEFAULT 100, -- Lower is higher priority
    status task_status NOT NULL DEFAULT 'pending',
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Error Handling & Retries
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for efficient polling
CREATE INDEX idx_tasks_fetch_pending ON tasks (priority, scheduled_at)
    WHERE status = 'pending';
```

### Regional Database Schema

This schema defines the tables for object metadata. Each region in a deployment has its own independent database with this schema.

```sql
-- From migrations_regional/V1__initial_regional_schema.sql

-- For text search capabilities
CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- For hierarchical path queries
CREATE EXTENSION IF NOT EXISTS ltree;
-- For UUID generation
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE objects (
    id BIGSERIAL PRIMARY KEY,
    -- This is a reference to a bucket in the global database.
    -- There is no foreign key constraint as it crosses databases.
    bucket_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL,
    key TEXT NOT NULL,
    key_ltree LTREE,

    -- The BLAKE3 hash of the object's content, used for content-addressing
    content_hash TEXT NOT NULL,

    size BIGINT NOT NULL,
    etag TEXT NOT NULL,
    content_type TEXT,

    -- For versioning, though we won't implement versioning logic in Phase 1
    version_id UUID NOT NULL DEFAULT gen_random_uuid(),

    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- For future use
    storage_class SMALLINT,
    user_meta JSONB,

    -- In a distributed setup, this would map shards to peers
    shard_map JSONB,

    checksum BYTEA,
    deleted_at TIMESTAMPTZ,
    -- An object is uniquely identified by its bucket, key, and version
    UNIQUE(bucket_id, key, version_id)
);

-- One-time helper to turn a TEXT key into a safe ltree
CREATE OR REPLACE FUNCTION make_key_ltree(p_key text)
    RETURNS ltree
    LANGUAGE sql
    IMMUTABLE
AS $$
WITH cleaned AS (
    SELECT regexp_replace(trim(both '/' from coalesce(p_key, '')), '/+', '/', 'g') AS k
),
     segs AS (
         SELECT unnest(string_to_array(k, '/')) AS seg
         FROM cleaned
     ),
     norm AS (
         SELECT
             -- keep only letters/digits/underscore, lowercased
             lower(regexp_replace(seg, '[^A-Za-z0-9_]', '_', 'g')) AS s
         FROM segs
         WHERE seg <> ''                         -- drop empties
     ),
     head_fixed AS (
         SELECT
             CASE
                 WHEN s ~ '^[a-z]' THEN s            -- starts with a letter already
                 WHEN s = '' THEN 'x'                -- degenerate -> x
                 ELSE 'x' || s                       -- make it start with a letter
                 END AS lbl
         FROM norm
     ),
     joined AS (
         SELECT array_to_string(array_agg(lbl), '.') AS dot
         FROM head_fixed
     )
SELECT CASE
           WHEN dot IS NULL OR dot = '' THEN NULL
           ELSE text2ltree(dot)
           END
FROM joined;
$$;

-- Use the helper in your trigger
CREATE OR REPLACE FUNCTION update_key_ltree()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.key_ltree := make_key_ltree(NEW.key);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS objects_update_key_ltree_trigger ON objects;
CREATE TRIGGER objects_update_key_ltree_trigger
    BEFORE INSERT OR UPDATE ON objects
    FOR EACH ROW
EXECUTE FUNCTION update_key_ltree();

-- Indexes for efficient querying
CREATE INDEX idx_objects_bucket_key ON objects(bucket_id, key);
CREATE INDEX idx_objects_ltree ON objects USING GIST(key_ltree);
CREATE INDEX idx_objects_trgm ON objects USING GIN(key gin_trgm_ops);
CREATE INDEX idx_objects_created_at ON objects USING BRIN(created_at);

CREATE INDEX idx_objects_not_deleted ON objects (bucket_id, key) WHERE deleted_at IS NULL;
```
