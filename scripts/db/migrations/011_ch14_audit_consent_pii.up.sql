-- Chapter 14 (audit follow-up): make several Ch14 claims true.
--
--  1. audit_log append-only enforcement via a trigger. Application
--     code already only does INSERTs, but a trigger that raises on
--     UPDATE/DELETE turns "by convention" into "by the database."
--
--  2. consent_records gains a `version` column. Consent under GDPR
--     is bound to a specific privacy policy version; revisions
--     re-prompt the user. Existing rows default to version 1.
--
--  3. events gains a `pii_erased` boolean. The Ch14 README claims an
--     anonymisation-in-place flow that keeps the row for audit
--     purposes but replaces PII with [REDACTED]. The flag marks rows
--     that have been anonymised so they can be excluded from
--     re-anonymisation passes.

-- ─── 1. Append-only audit_log ─────────────────────────────────────────────

CREATE OR REPLACE FUNCTION nexus_audit_log_append_only()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION
        'audit_log is append-only — % rejected on table %',
        TG_OP, TG_TABLE_NAME
        USING ERRCODE = 'check_violation';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS audit_log_no_update ON audit_log;
DROP TRIGGER IF EXISTS audit_log_no_delete ON audit_log;

CREATE TRIGGER audit_log_no_update
    BEFORE UPDATE ON audit_log
    FOR EACH ROW
    EXECUTE FUNCTION nexus_audit_log_append_only();

CREATE TRIGGER audit_log_no_delete
    BEFORE DELETE ON audit_log
    FOR EACH ROW
    EXECUTE FUNCTION nexus_audit_log_append_only();

-- ─── 2. consent_records.version ───────────────────────────────────────────

ALTER TABLE consent_records
    ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;

-- ─── 3. events.pii_erased ─────────────────────────────────────────────────

ALTER TABLE events
    ADD COLUMN IF NOT EXISTS pii_erased BOOLEAN NOT NULL DEFAULT FALSE;

-- Partial index keeps the anonymisation-pass query (find rows still
-- carrying PII) cheap even when the table grows.
CREATE INDEX IF NOT EXISTS events_pending_anonymisation
    ON events (tenant_id, occurred_at)
    WHERE pii_erased = FALSE;
