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
