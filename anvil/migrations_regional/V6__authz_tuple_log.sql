CREATE TABLE authz_tuple_log (
    revision BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    namespace TEXT NOT NULL,
    object_id TEXT NOT NULL,
    relation TEXT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    caveat_hash TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL CHECK (operation IN ('add', 'remove')),
    written_by TEXT NOT NULL,
    reason TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    written_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_authz_tuple_log_key_revision
    ON authz_tuple_log(tenant_id, namespace, object_id, relation, subject_kind, subject_id, caveat_hash, revision DESC);

CREATE INDEX idx_authz_tuple_log_revision
    ON authz_tuple_log(tenant_id, revision);
