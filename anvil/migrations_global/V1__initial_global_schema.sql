CREATE TABLE regions (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE tenants (
    id BIGSERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    api_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE buckets (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name TEXT NOT NULL UNIQUE,
    region TEXT NOT NULL REFERENCES regions(name) ON DELETE CASCADE,
    is_public_read BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE TABLE apps (
                      id BIGSERIAL PRIMARY KEY,
                      tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                      name TEXT NOT NULL,
                          client_id TEXT UNIQUE NOT NULL,
                          client_secret_encrypted BYTEA NOT NULL,                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                      UNIQUE(tenant_id, name)
);

CREATE TABLE policies (
                          id BIGSERIAL PRIMARY KEY,
                          app_id BIGINT NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
                          resource TEXT NOT NULL, -- e.g., "my-bucket/folder/*"
                          action TEXT NOT NULL,   -- e.g., "read", "write", "grant"
                          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                          UNIQUE(app_id, resource, action)
);

-- In a new migration file (e.g., V3__add_tasks_table.sql)
CREATE TYPE task_status AS ENUM ('pending', 'running', 'completed', 'failed');
CREATE TYPE task_type AS ENUM ('DELETE_OBJECT', 'DELETE_BUCKET', 'REBALANCE_SHARD', 'HF_INGESTION');

CREATE TABLE tasks (
                       id BIGSERIAL PRIMARY KEY,
                       task_type task_type NOT NULL,
                       payload JSONB NOT NULL,

    -- Scheduling and Execution
                       priority INT NOT NULL DEFAULT 100, -- Lower is higher priority
                       status task_status NOT NULL DEFAULT 'pending',
                       scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Error Handling & Retries
                       attempts INT NOT NULL DEFAULT 0,
                       last_error TEXT,

    -- Timestamps
                       created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                       updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for efficient polling
CREATE INDEX idx_tasks_fetch_pending ON tasks (priority, scheduled_at)
    WHERE status = 'pending';

-- Hugging Face integration tables
-- Stores named HF API keys (token encrypted at rest by application layer)
CREATE TABLE huggingface_keys (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    token_encrypted BYTEA NOT NULL,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ
);

-- Top-level ingestion jobs
CREATE TYPE hf_ingestion_state AS ENUM ('queued','running','completed','failed','canceled');
CREATE TABLE hf_ingestions (
    id BIGSERIAL PRIMARY KEY,
    key_id BIGINT NOT NULL REFERENCES huggingface_keys(id) ON DELETE RESTRICT,
    tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    requester_app_id BIGINT NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
    repo TEXT NOT NULL,
    revision TEXT,
    target_bucket TEXT NOT NULL,
    target_region TEXT NOT NULL,
    target_prefix TEXT,
    include_globs TEXT[],
    exclude_globs TEXT[],
    state hf_ingestion_state NOT NULL DEFAULT 'queued',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);
CREATE INDEX idx_hf_ingestions_state ON hf_ingestions(state);

-- Per-file progress
CREATE TYPE hf_item_state AS ENUM ('queued','downloading','stored','failed','skipped');
CREATE TABLE hf_ingestion_items (
    id BIGSERIAL PRIMARY KEY,
    ingestion_id BIGINT NOT NULL REFERENCES hf_ingestions(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    size BIGINT,
    etag TEXT,
    state hf_item_state NOT NULL DEFAULT 'queued',
    retries INT NOT NULL DEFAULT 0,
    error TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    UNIQUE(ingestion_id, path)
);
CREATE INDEX idx_hf_ingestion_items_ingest ON hf_ingestion_items(ingestion_id);
