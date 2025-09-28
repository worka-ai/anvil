-- For text search capabilities
CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- For hierarchical path queries
CREATE EXTENSION IF NOT EXISTS ltree;

CREATE TABLE objects (
    id BIGSERIAL PRIMARY KEY,
    -- This is a reference to a bucket in the global database.
    -- There is no foreign key constraint as it crosses databases.
    bucket_id BIGINT NOT NULL,
    
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

    -- An object is uniquely identified by its bucket, key, and version
    UNIQUE(bucket_id, key, version_id)
);

-- Create a trigger to automatically generate the ltree path from the key
CREATE OR REPLACE FUNCTION update_key_ltree()
RETURNS TRIGGER AS $$
BEGIN
    -- Replace slashes with dots for ltree compatibility
    NEW.key_ltree = text2ltree(REPLACE(NEW.key, '/', '.'));
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER objects_update_key_ltree_trigger
BEFORE INSERT OR UPDATE ON objects
FOR EACH ROW
EXECUTE FUNCTION update_key_ltree();


-- Indexes for efficient querying
CREATE INDEX idx_objects_bucket_key ON objects(bucket_id, key);
CREATE INDEX idx_objects_ltree ON objects USING GIST(key_ltree);
CREATE INDEX idx_objects_trgm ON objects USING GIN(key gin_trgm_ops);
CREATE INDEX idx_objects_created_at ON objects USING BRIN(created_at);