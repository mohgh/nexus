-- Chapter 03: Events table with JSONB payload.
--
-- Design: the envelope columns (tenant_id, event_type, value, occurred_at)
-- are relational and carry indexes. The payload is JSONB — schema-on-read.
-- This is the hybrid relational/document model discussed in DDIA Ch03.

CREATE TABLE IF NOT EXISTS events (
    id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id   UUID        NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    event_type  TEXT        NOT NULL,
    payload     JSONB       NOT NULL DEFAULT '{}',
    value       FLOAT8      NOT NULL DEFAULT 0,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Time-series access pattern: list events for a tenant ordered by time.
CREATE INDEX IF NOT EXISTS events_tenant_time
    ON events (tenant_id, occurred_at DESC);

-- JSONB containment queries: WHERE payload @> '{"user_id": "x"}'
CREATE INDEX IF NOT EXISTS events_payload_gin
    ON events USING GIN (payload);

-- Ch03: trigram full-text search over payload text.
-- ILIKE '%query%' uses this. Demonstrates PostgreSQL's search capability
-- before we move to Elasticsearch in Ch04.
CREATE INDEX IF NOT EXISTS events_payload_trgm
    ON events USING GIN ((payload::text) gin_trgm_ops);
