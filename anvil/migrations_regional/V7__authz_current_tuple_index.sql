CREATE TABLE authz_current_tuples (
    tenant_id BIGINT NOT NULL,
    namespace TEXT NOT NULL,
    object_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    caveat_hash TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL CHECK (operation IN ('add', 'remove')),
    revision BIGINT NOT NULL,
    written_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    written_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, namespace, object_id, relation, subject_kind, subject_id, caveat_hash)
);

CREATE INDEX idx_authz_current_by_subject
    ON authz_current_tuples(tenant_id, subject_kind, subject_id, namespace, relation, operation);

CREATE INDEX idx_authz_current_by_object_relation
    ON authz_current_tuples(tenant_id, namespace, object_id, relation, operation);
