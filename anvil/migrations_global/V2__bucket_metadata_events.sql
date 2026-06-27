CREATE TABLE bucket_metadata_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bucket_id BIGINT NOT NULL,
    bucket_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    bucket_metadata JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_bucket_metadata_events_tenant_cursor
    ON bucket_metadata_events(tenant_id, id);

CREATE INDEX idx_bucket_metadata_events_bucket_cursor
    ON bucket_metadata_events(tenant_id, bucket_name, id);
