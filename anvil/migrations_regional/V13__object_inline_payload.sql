ALTER TABLE objects
    ADD COLUMN inline_payload BYTEA;

CREATE INDEX idx_objects_inline_payload_present ON objects(bucket_id, key) WHERE inline_payload IS NOT NULL;
