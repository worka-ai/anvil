-- Admin Auth Tables

CREATE TABLE admin_roles (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL -- e.g., 'SuperAdmin', 'ReadOnlyViewer'
);

CREATE TABLE admin_users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE admin_user_roles (
    user_id BIGINT NOT NULL REFERENCES admin_users(id) ON DELETE CASCADE,
    role_id INTEGER NOT NULL REFERENCES admin_roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE admin_role_permissions (
    id SERIAL PRIMARY KEY,
    role_id INTEGER NOT NULL REFERENCES admin_roles(id) ON DELETE CASCADE,
    resource TEXT NOT NULL, -- e.g., 'cluster', 'tenants', 'nodes'
    action TEXT NOT NULL, -- e.g., 'read', 'write', 'create', 'delete'
    UNIQUE (role_id, resource, action)
);

-- Seed the initial roles
INSERT INTO admin_roles (name) VALUES ('SuperAdmin'), ('ReadOnlyViewer');

-- Grant permissions to ReadOnlyViewer
-- This role can only perform GET requests
INSERT INTO admin_role_permissions (role_id, resource, action)
SELECT id, 'cluster', 'read' FROM admin_roles WHERE name = 'ReadOnlyViewer';

INSERT INTO admin_role_permissions (role_id, resource, action)
SELECT id, 'regions', 'read' FROM admin_roles WHERE name = 'ReadOnlyViewer';

INSERT INTO admin_role_permissions (role_id, resource, action)
SELECT id, 'tenants', 'read' FROM admin_roles WHERE name = 'ReadOnlyViewer';

INSERT INTO admin_role_permissions (role_id, resource, action)
SELECT id, 'apps', 'read' FROM admin_roles WHERE name = 'ReadOnlyViewer';

INSERT INTO admin_role_permissions (role_id, resource, action)
SELECT id, 'hf', 'read' FROM admin_roles WHERE name = 'ReadOnlyViewer';

-- Grant all permissions to SuperAdmin
INSERT INTO admin_role_permissions (role_id, resource, action)
SELECT id, '*', '*' FROM admin_roles WHERE name = 'SuperAdmin';

