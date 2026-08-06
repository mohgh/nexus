-- Chapter 14: Audit log and consent tracking for GDPR compliance.

-- ─── Audit Log ────────────────────────────────────────────────────────────
-- Append-only: never UPDATE or DELETE audit entries.
-- Retained even after tenant data erasure (legal requirement).
CREATE TABLE IF NOT EXISTS audit_log (
    id              BIGSERIAL   PRIMARY KEY,
    tenant_id       TEXT        NOT NULL,  -- TEXT not UUID FK: survives tenant deletion
    actor           TEXT        NOT NULL,  -- API key, user ID, or "system"
    action          TEXT        NOT NULL,  -- create, read, update, delete, export, erasure
    resource_type   TEXT        NOT NULL,
    resource_id     TEXT        NOT NULL,
    details         JSONB       NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS audit_log_tenant
    ON audit_log (tenant_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS audit_log_action
    ON audit_log (action, occurred_at DESC);

-- ─── Consent Records ─────────────────────────────────────────────────────
-- Current state only. History is in audit_log.
CREATE TABLE IF NOT EXISTS consent_records (
    tenant_id   UUID        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    purpose     TEXT        NOT NULL,  -- "analytics", "marketing", "billing"
    granted     BOOLEAN     NOT NULL DEFAULT false,
    granted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at  TIMESTAMPTZ NULL,
    PRIMARY KEY (tenant_id, purpose)
);
