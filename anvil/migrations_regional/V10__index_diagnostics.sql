CREATE TABLE index_diagnostics (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bucket_id BIGINT NOT NULL,
    bucket_name TEXT NOT NULL,
    index_id BIGINT,
    index_name TEXT NOT NULL,
    object_key TEXT NOT NULL,
    version_id UUID,
    severity TEXT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    details JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_index_diagnostics_bucket_cursor
    ON index_diagnostics(tenant_id, bucket_id, id);

CREATE INDEX idx_index_diagnostics_index_cursor
    ON index_diagnostics(tenant_id, bucket_id, index_name, id);
