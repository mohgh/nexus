-- Chapter 08: Billing records with idempotency key and transactional outbox.
--
-- Design decisions:
--   idempotency_key: unique per tenant — prevents double-charging on retries.
--   outbox_sent_at:  NULL = not yet published to Kafka. The outbox worker
--                    selects WHERE outbox_sent_at IS NULL, publishes, then
--                    UPDATE sets the timestamp. All in the same Postgres
--                    transaction — no 2PC needed.

CREATE TABLE IF NOT EXISTS billing_records (
    id               UUID          PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id        UUID          NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    idempotency_key  TEXT          NOT NULL,
    amount_cents     BIGINT        NOT NULL CHECK (amount_cents > 0),
    currency         TEXT          NOT NULL DEFAULT 'USD',
    status           TEXT          NOT NULL DEFAULT 'pending'
                                   CHECK (status IN ('pending', 'completed', 'failed')),
    description      TEXT          NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    outbox_sent_at   TIMESTAMPTZ   NULL      -- NULL = pending outbox delivery
);

-- Idempotency: one charge per tenant per key.
CREATE UNIQUE INDEX IF NOT EXISTS billing_idempotency
    ON billing_records (tenant_id, idempotency_key);

-- Outbox worker query: find unsent records efficiently.
CREATE INDEX IF NOT EXISTS billing_outbox_pending
    ON billing_records (created_at)
    WHERE outbox_sent_at IS NULL;
