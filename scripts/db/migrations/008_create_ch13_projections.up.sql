-- Chapter 13: a second projection, durable projection positions, and
-- the idempotency-key dedup table.
--
-- The chapter teaches that:
--   1. Multiple projections coexist from the same event log.
--   2. Each projection persists its position so a restart resumes
--      from where it left off and a replay (DELETE + reset) is a
--      first-class operation.
--   3. Mutating API endpoints carry an Idempotency-Key the server
--      uses to deduplicate within a 24h window.
--
-- This migration adds the storage for all three.

-- ─── 1. Second projection: per-tenant per-day event counts ────────────────
-- This is the time-bucketed companion to tenant_event_counts (created in
-- migration 004). The composite PK lets ON CONFLICT DO UPDATE keep the
-- projection idempotent under replay.

CREATE TABLE IF NOT EXISTS daily_event_counts (
    tenant_id    UUID    NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_date   DATE    NOT NULL,
    event_type   TEXT    NOT NULL,
    event_count  BIGINT  NOT NULL DEFAULT 0,
    total_value  FLOAT8  NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, event_date, event_type)
);

-- Query: "show me the last 30 days of activity for a tenant."
CREATE INDEX IF NOT EXISTS daily_event_counts_recent
    ON daily_event_counts (tenant_id, event_date DESC);

-- ─── 2. Durable projection positions ──────────────────────────────────────
-- Each projection writes its last processed stream_position here after a
-- CatchUp sweep. On restart the runner loads the value back — without
-- this, every restart would replay every event from position 0.

CREATE TABLE IF NOT EXISTS projection_positions (
    projection_name  TEXT         PRIMARY KEY,
    last_position    BIGINT       NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- ─── 3. Idempotency-key dedup table ────────────────────────────────────────
-- Mutating endpoints look up Idempotency-Key here; on hit they return the
-- cached response without re-executing the handler. Storing the full
-- response (status + headers + body) means a retry sees the same answer
-- the original request saw, including any side-effect details.
--
-- The TTL is enforced by the cleanup query in the middleware (24h window)
-- rather than by Postgres alone — PG has no TTL — so we keep created_at
-- and a btree index to drive the periodic DELETE.

CREATE TABLE IF NOT EXISTS processed_idempotency_keys (
    idempotency_key   TEXT         PRIMARY KEY,
    response_status   INT          NOT NULL,
    response_body     BYTEA        NOT NULL,
    response_content_type TEXT     NOT NULL DEFAULT 'application/json',
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS processed_idempotency_keys_created
    ON processed_idempotency_keys (created_at);
