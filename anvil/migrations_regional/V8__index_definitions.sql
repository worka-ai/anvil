CREATE TABLE index_definitions (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bucket_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    selector JSONB NOT NULL,
    extractor JSONB NOT NULL,
    authorization_mode TEXT NOT NULL,
    build_policy JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(bucket_id, name)
);

CREATE INDEX idx_index_definitions_bucket_enabled
    ON index_definitions(bucket_id, enabled, kind);
