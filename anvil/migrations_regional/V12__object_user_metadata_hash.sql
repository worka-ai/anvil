ALTER TABLE objects
    ADD COLUMN user_metadata_hash TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_objects_user_metadata_hash ON objects(bucket_id, user_metadata_hash);
