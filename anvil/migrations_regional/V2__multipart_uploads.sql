CREATE TABLE multipart_uploads (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    bucket_id BIGINT NOT NULL,
    key TEXT NOT NULL,
    upload_id UUID NOT NULL DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    aborted_at TIMESTAMPTZ,
    UNIQUE(bucket_id, key, upload_id)
);

CREATE INDEX idx_multipart_uploads_active
    ON multipart_uploads(bucket_id, key, upload_id)
    WHERE completed_at IS NULL AND aborted_at IS NULL;

CREATE TABLE multipart_upload_parts (
    id BIGSERIAL PRIMARY KEY,
    upload_id BIGINT NOT NULL REFERENCES multipart_uploads(id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    content_hash TEXT NOT NULL,
    size BIGINT NOT NULL,
    etag TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(upload_id, part_number)
);

CREATE INDEX idx_multipart_upload_parts_ordered
    ON multipart_upload_parts(upload_id, part_number);
