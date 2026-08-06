-- Chapter 01: Core tenants table
-- The multi-tenancy primitive that every subsequent table references.

CREATE TABLE IF NOT EXISTS tenants (
    id         UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    name       TEXT        NOT NULL,
    plan       TEXT        NOT NULL DEFAULT 'free'
                           CHECK (plan IN ('free', 'pro', 'enterprise')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed data so the app is usable immediately after migrate-up.
INSERT INTO tenants (name, plan) VALUES
    ('Acme Corp',          'pro'),
    ('Globex Industries',  'enterprise'),
    ('Initech',            'free')
ON CONFLICT DO NOTHING;
