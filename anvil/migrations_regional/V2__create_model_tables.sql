CREATE TABLE model_artifacts (
    artifact_id TEXT PRIMARY KEY, -- blake3
    bucket_id   BIGINT NOT NULL,
    key         TEXT   NOT NULL,
    manifest    JSONB  NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE model_tensors (
    artifact_id TEXT      NOT NULL REFERENCES model_artifacts (artifact_id) ON DELETE CASCADE,
    tensor_name TEXT      NOT NULL,
    file_path   TEXT      NOT NULL,
    file_offset BIGINT    NOT NULL,
    byte_length BIGINT    NOT NULL,
    dtype       TEXT      NOT NULL,
    shape       INTEGER[] NOT NULL,
    layout      TEXT      NOT NULL,
    block_bytes INTEGER,
    blocks      JSONB,
    PRIMARY KEY (artifact_id, tensor_name)
);
CREATE INDEX idx_model_tensors_name ON model_tensors (artifact_id, tensor_name);
CREATE INDEX idx_model_tensors_file ON model_tensors (artifact_id, file_path, file_offset);
