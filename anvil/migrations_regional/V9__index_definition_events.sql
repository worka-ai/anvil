CREATE TABLE index_definition_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bucket_id BIGINT NOT NULL,
    bucket_name TEXT NOT NULL,
    index_id BIGINT NOT NULL,
    index_name TEXT NOT NULL,
    event_type TEXT NOT NULL,
    index_version BIGINT NOT NULL,
    definition JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_index_definition_events_bucket_cursor
    ON index_definition_events(tenant_id, bucket_id, id);

CREATE INDEX idx_index_definition_events_index
    ON index_definition_events(tenant_id, bucket_id, index_name, id);
