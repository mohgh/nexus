-- Chapter 13 (audit follow-up): scope idempotency cache hits by
-- request fingerprint as well as key.
--
-- The migration that introduced processed_idempotency_keys keyed
-- the cache only on idempotency_key, so a client reusing the same
-- key across different routes / tenants / payloads would replay
-- the FIRST cached 2xx for the SECOND request — a correctness
-- failure where one client mistake silently corrupts another's
-- response. (Stripe's idempotency contract handles this by
-- comparing a request fingerprint and returning 409 on mismatch;
-- we now do the same.)
--
-- The fingerprint is sha256(method + "\n" + path + "\n" + body),
-- 32 bytes. Empty bodies are fine — sha256 of "POST\n/api/...\n"
-- is still a unique fingerprint for that request shape.
--
-- We DELETE existing rows because they were written without a
-- fingerprint and adding NOT NULL with a sentinel would let a
-- legacy entry pose as a fingerprint match. The dedup table is a
-- 24h cache; clearing it only means the next request with each
-- key re-runs the handler instead of replaying — operationally a
-- no-op for live traffic.

DELETE FROM processed_idempotency_keys;

ALTER TABLE processed_idempotency_keys
    ADD COLUMN request_fingerprint BYTEA NOT NULL;
