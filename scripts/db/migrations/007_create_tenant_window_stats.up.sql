-- Chapter 12: per-tenant windowed aggregates flushed from Redis.
--
-- The stream processor maintains counts and sums in Redis hashes
-- bucketed by event_time. Once a window has been closed (the
-- aggregator's high watermark passes window_end + allowed_lateness),
-- the flusher copies the values into this table and deletes the
-- Redis hash.
--
-- Storing tenant_id and event_type as TEXT — not FK to tenants — so
-- the flusher can land aggregates without taking a lock on the
-- tenant row and so an erased tenant's analytics history is preserved
-- separately from their live data. tenant deletion is handled by
-- the GDPR path in Ch14.
--
-- window_duration is a short label ("1m", "1h") rather than an
-- interval so it's easy to query and easy to extend without
-- migration churn if new window widths are added later.
--
-- is_late distinguishes the on-time aggregate from the side-output
-- "late" bucket so analytics queries can ignore stragglers cleanly.
-- It's part of the primary key so on-time and late aggregates for
-- the same window can coexist as separate rows.

CREATE TABLE IF NOT EXISTS tenant_window_stats (
    tenant_id        TEXT             NOT NULL,
    event_type       TEXT             NOT NULL,
    window_duration  TEXT             NOT NULL,
    window_start     TIMESTAMPTZ      NOT NULL,
    event_count      BIGINT           NOT NULL DEFAULT 0,
    sum_value        DOUBLE PRECISION NOT NULL DEFAULT 0,
    is_late          BOOLEAN          NOT NULL DEFAULT FALSE,
    flushed_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, event_type, window_duration, window_start, is_late)
);

-- Most queries are "give me the recent windows for this tenant" —
-- index supports that without scanning the whole table.
CREATE INDEX IF NOT EXISTS tenant_window_stats_recent
    ON tenant_window_stats (tenant_id, window_duration, window_start DESC);
