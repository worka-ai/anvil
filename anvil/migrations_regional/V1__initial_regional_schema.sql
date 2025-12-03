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

CREATE TABLE model_artifacts (
                                 artifact_id TEXT PRIMARY KEY, -- blake3
                                 bucket_id   BIGINT NOT NULL,
                                 key         TEXT   NOT NULL,
                                 manifest    JSONB  NOT NULL,
                                 created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE model_tensors (
                               artifact_id TEXT      NOT NULL REFERENCES model_artifacts (artifact_id) ON DELETE CASCADE,
                               tensor_name TEXT      NOT NULL,
                               file_path   TEXT      NOT NULL,
                               file_offset BIGINT    NOT NULL,
                               byte_length BIGINT    NOT NULL,
                               dtype       TEXT      NOT NULL,
                               shape       INTEGER[] NOT NULL,
                               layout      TEXT      NOT NULL,
                               block_bytes INTEGER,
                               blocks      JSONB,
                               PRIMARY KEY (artifact_id, tensor_name)
);
CREATE INDEX idx_model_tensors_name ON model_tensors (artifact_id, tensor_name);
CREATE INDEX idx_model_tensors_file ON model_tensors (artifact_id, file_path, file_offset);
