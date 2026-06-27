CREATE TABLE object_manifests (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bucket_id BIGINT NOT NULL,
    bucket_name TEXT NOT NULL,
    manifest_key TEXT NOT NULL,
    revision BIGINT NOT NULL,
    manifest_json JSONB NOT NULL,
    manifest_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(bucket_id, manifest_key)
);

CREATE INDEX idx_object_manifests_bucket_key
    ON object_manifests(bucket_id, manifest_key);
