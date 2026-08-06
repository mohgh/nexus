-- Chapter 13: Append-only event store for CQRS / event sourcing.
--
-- This table is the write-side (command model). It is append-only:
-- no UPDATE or DELETE should ever touch this table.
-- stream_position is a BIGSERIAL — globally ordered, gap-free.

CREATE TABLE IF NOT EXISTS events_store (
    stream_position  BIGSERIAL   PRIMARY KEY,
    stream_name      TEXT        NOT NULL,   -- e.g. "tenant-{id}", "billing-{id}"
    event_type       TEXT        NOT NULL,   -- e.g. "TenantCreated", "EventIngested"
    data             JSONB       NOT NULL DEFAULT '{}',
    metadata         JSONB       NOT NULL DEFAULT '{}',
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Replay a single stream (aggregate reconstruction).
CREATE INDEX IF NOT EXISTS events_store_stream
    ON events_store (stream_name, stream_position);

-- Catch-up subscription: "give me everything after position X."
-- The primary key already covers this, but naming makes intent clear.

-- ─── Read-side projection table ───────────────────────────────────────────
-- This is the query model — denormalized for fast reads.
-- It is disposable: DELETE FROM tenant_event_counts and re-run the
-- projection from position 0 to rebuild it.

CREATE TABLE IF NOT EXISTS tenant_event_counts (
    tenant_id    UUID    NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type   TEXT    NOT NULL,
    event_count  BIGINT  NOT NULL DEFAULT 0,
    total_value  FLOAT8  NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, event_type)
);
