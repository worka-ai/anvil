CREATE TABLE append_streams (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bucket_id BIGINT NOT NULL,
    bucket_name TEXT NOT NULL,
    stream_key TEXT NOT NULL,
    stream_id UUID NOT NULL DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sealed_at TIMESTAMPTZ,
    segment_hash TEXT,
    UNIQUE(bucket_id, stream_key, stream_id)
);

CREATE INDEX idx_append_streams_active
    ON append_streams(bucket_id, stream_key, stream_id)
    WHERE sealed_at IS NULL;

CREATE TABLE append_stream_records (
    id BIGSERIAL PRIMARY KEY,
    stream_id BIGINT NOT NULL REFERENCES append_streams(id) ON DELETE CASCADE,
    record_sequence BIGINT NOT NULL,
    payload_hash TEXT NOT NULL,
    payload_size BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(stream_id, record_sequence)
);

CREATE INDEX idx_append_stream_records_ordered
    ON append_stream_records(stream_id, record_sequence);
