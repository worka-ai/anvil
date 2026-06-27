ALTER TABLE objects
    ADD COLUMN mutation_id UUID NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN index_policy_snapshot TEXT NOT NULL DEFAULT '',
    ADD COLUMN authz_revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN record_hash TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_objects_mutation_id ON objects(mutation_id);
CREATE INDEX idx_objects_index_policy_snapshot ON objects(bucket_id, index_policy_snapshot);
