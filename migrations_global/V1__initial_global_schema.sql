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
    name TEXT NOT NULL,
    region TEXT NOT NULL REFERENCES regions(name) ON DELETE CASCADE,
    is_public_read BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    UNIQUE(tenant_id, name)
);

CREATE TABLE apps (
                      id BIGSERIAL PRIMARY KEY,
                      tenant_id BIGINT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                      name TEXT NOT NULL,
                      client_id TEXT UNIQUE NOT NULL,
                      client_secret_hash TEXT NOT NULL,
                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
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
CREATE TYPE task_type AS ENUM ('DELETE_OBJECT', 'DELETE_BUCKET', 'REBALANCE_SHARD');

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
