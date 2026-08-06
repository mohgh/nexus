DROP INDEX IF EXISTS events_pending_anonymisation;
ALTER TABLE events DROP COLUMN IF EXISTS pii_erased;

ALTER TABLE consent_records DROP COLUMN IF EXISTS version;

DROP TRIGGER IF EXISTS audit_log_no_update ON audit_log;
DROP TRIGGER IF EXISTS audit_log_no_delete ON audit_log;
DROP FUNCTION IF EXISTS nexus_audit_log_append_only();
