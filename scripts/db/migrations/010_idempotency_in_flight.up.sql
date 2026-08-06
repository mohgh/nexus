-- Chapter 13 (audit follow-up): add an in_flight state to the
-- idempotency table so concurrent same-key retries truly dedup.
--
-- The previous middleware shape was lookup-then-execute: both
-- concurrent retries miss the cache, both run the handler, and
-- only race at the final INSERT ON CONFLICT DO NOTHING. For
-- side-effecting handlers like POST /api/v1/events, both writes
-- happen — defeating idempotency exactly when the client retried
-- because they thought the first attempt had failed.
--
-- The fix is reserve-then-execute. The middleware INSERTs a row
-- with state='in_flight' BEFORE running the handler. A concurrent
-- retry's INSERT conflicts; the retry SELECTs the existing row,
-- sees state='in_flight', and returns 409 IDEMPOTENCY_REQUEST_IN_PROGRESS
-- with a Retry-After header. The original then UPDATEs the row to
-- state='completed' on 2xx, or DELETEs it on non-2xx / panic so
-- retries can proceed cleanly.
--
-- Schema changes:
--
--   * state is the new lifecycle column (in_flight | completed).
--   * response_status, response_body, response_content_type all
--     become NULLable: an in_flight row hasn't seen its response
--     yet. The middleware's reads check state first and only
--     dereference the response columns when state='completed'.
--   * partial index on (created_at) WHERE state='in_flight' so the
--     cleanup goroutine can sweep stale reservations efficiently.

ALTER TABLE processed_idempotency_keys
    ALTER COLUMN response_status DROP NOT NULL,
    ALTER COLUMN response_body DROP NOT NULL,
    ALTER COLUMN response_content_type DROP NOT NULL,
    ADD COLUMN state TEXT NOT NULL DEFAULT 'completed'
        CHECK (state IN ('in_flight', 'completed'));

-- Existing rows are 'completed' by default. The new in_flight rows
-- write to a separate index for fast stale-cleanup.
CREATE INDEX IF NOT EXISTS processed_idempotency_keys_in_flight_age
    ON processed_idempotency_keys (created_at)
    WHERE state = 'in_flight';
