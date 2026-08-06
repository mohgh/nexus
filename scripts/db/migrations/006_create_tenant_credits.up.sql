-- Chapter 08: Per-tenant prepaid credit balance.
--
-- The tenant_credits table is the canonical example for the chapter's
-- transaction work:
--
--   * RecordCharge in the billing workflow now both inserts a row into
--     billing_records AND deducts from tenant_credits, in a single
--     Postgres transaction. If either side fails, neither commits —
--     atomicity across two tables.
--
--   * The lost-update integration test deducts the same balance from
--     two concurrent transactions to demonstrate the anomaly under
--     READ COMMITTED, then re-runs with SELECT ... FOR UPDATE to show
--     the fix on a real domain object (rather than a scratch table).
--
-- balance_cents is stored as BIGINT for the same reason billing_records
-- uses cents: floats lose precision on summed money.

CREATE TABLE IF NOT EXISTS tenant_credits (
    tenant_id    UUID         PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    balance_cents BIGINT      NOT NULL DEFAULT 0 CHECK (balance_cents >= 0),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Auto-create a zero-balance row whenever a tenant is created so the
-- application can rely on the row existing (one less edge case in the
-- charge path). The trigger fires AFTER INSERT — the tenant row is
-- visible to the credits row's foreign key check by then.
CREATE OR REPLACE FUNCTION nexus_init_tenant_credits()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO tenant_credits (tenant_id, balance_cents)
    VALUES (NEW.id, 0)
    ON CONFLICT (tenant_id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tenants_init_credits ON tenants;
CREATE TRIGGER tenants_init_credits
    AFTER INSERT ON tenants
    FOR EACH ROW
    EXECUTE FUNCTION nexus_init_tenant_credits();

-- Backfill existing tenants — needed for any data that predates this
-- migration. ON CONFLICT keeps it idempotent on re-runs.
INSERT INTO tenant_credits (tenant_id, balance_cents)
SELECT id, 0 FROM tenants
ON CONFLICT (tenant_id) DO NOTHING;
