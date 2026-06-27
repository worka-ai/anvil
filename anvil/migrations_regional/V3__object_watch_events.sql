CREATE TABLE object_watch_events (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bucket_id BIGINT NOT NULL,
    bucket_name TEXT NOT NULL,
    key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    version_id UUID,
    etag TEXT,
    size BIGINT NOT NULL DEFAULT 0,
    is_delete_marker BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_object_watch_events_bucket_prefix
    ON object_watch_events(bucket_id, key, id);

CREATE INDEX idx_object_watch_events_tenant_cursor
    ON object_watch_events(tenant_id, id);
