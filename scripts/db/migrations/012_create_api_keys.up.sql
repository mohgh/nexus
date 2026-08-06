-- Production-hardening: API key credential store.
--
-- Authentication for Nexus. Each row is one credential. We never store the
-- raw secret — only its SHA-256 hash — so a database compromise does not
-- leak usable keys. Lookups are by key_hash, which is UNIQUE and indexed.
--
-- scope:
--   'tenant' — authenticates as exactly one tenant; tenant_id is set.
--   'admin'  — operational / cross-tenant access; tenant_id is NULL.
--
-- A NULL revoked_at means the key is active. Revocation is a soft delete
-- (set revoked_at = NOW()) so the audit trail of who-held-access survives.

CREATE TABLE IF NOT EXISTS api_keys (
    id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id    UUID        REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash     BYTEA       NOT NULL UNIQUE,
    prefix       TEXT        NOT NULL,
    scope        TEXT        NOT NULL DEFAULT 'tenant'
                             CHECK (scope IN ('tenant', 'admin')),
    name         TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ,

    -- A tenant key must name its tenant; an admin key must not carry one.
    CONSTRAINT api_keys_scope_tenant CHECK (
        (scope = 'tenant' AND tenant_id IS NOT NULL) OR
        (scope = 'admin'  AND tenant_id IS NULL)
    )
);

-- Active-key lookups by tenant for the admin list/revoke surface.
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant
    ON api_keys (tenant_id) WHERE revoked_at IS NULL;
