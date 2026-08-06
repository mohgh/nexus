-- ClickHouse schema for Nexus OLAP layer
-- Populated via Kafka + Debezium CDC pipeline (Chapter 12)
-- Or directly via batch ETL for Chapter 04 demos

-- Nexus app user. Matches the postgres convention (user "nexus",
-- password "nexus_secret") so a single env var pattern works for
-- both: postgres://nexus:nexus_secret@... and
-- clickhouse://nexus:nexus_secret@...
--
-- Without this block a fresh ClickHouse container only has the
-- "default" user with no password, and every connection from the
-- aggregator/server using the documented DSN fails with
-- AUTHENTICATION_FAILED. Found by actually running ch11-verify
-- against a clean stack — the auth assumption was never tested
-- end-to-end until then.
--
-- plaintext_password is fine for the educational deployment; real
-- production would use sha256_password or LDAP.
CREATE USER IF NOT EXISTS nexus IDENTIFIED WITH plaintext_password BY 'nexus_secret';

CREATE DATABASE IF NOT EXISTS nexus;

GRANT ALL ON nexus.* TO nexus;
GRANT SELECT ON system.* TO nexus;

-- Events table: ReplacingMergeTree so Kafka redeliveries from the
-- stream processor don't overcount on the OLAP side.
--
-- The (tenant_id, occurred_at, event_id) ORDER BY is also the
-- deduplication key for ReplacingMergeTree: two rows that agree on
-- all three columns are considered the same event and collapse to
-- one during background compaction. event_id has to be IN the
-- ORDER BY for this to work — without it, two distinct events that
-- happen to share (tenant, time) would collapse into one (silent
-- data loss).
--
-- The version column is occurred_at: when duplicates exist before
-- compaction, the row with the highest occurred_at wins. For
-- Kafka redeliveries the duplicate is byte-identical so the choice
-- is moot; for any future case where the producer republishes
-- with an updated timestamp the "later writer wins" semantic is
-- the intuitive one.
--
-- Important read-side consequence: between insert and compaction
-- duplicates ARE visible. Queries that need exactly-once analytics
-- must use SELECT ... FROM nexus.events FINAL — see the
-- DailyStats query in internal/storage/clickhouse/event_repo.go.
-- FINAL is more expensive than a plain SELECT, which is the
-- chapter's headline exactly-once-vs-throughput trade-off in this
-- engine.
CREATE TABLE IF NOT EXISTS nexus.events (
    tenant_id    String,
    event_id     String,
    event_type   LowCardinality(String),   -- dictionary-encoded: 'page_view', 'click', etc.
    value        Float64,
    payload      String,                   -- JSON blob (schema-on-read)
    occurred_at  DateTime
) ENGINE = ReplacingMergeTree(occurred_at)
ORDER BY (tenant_id, occurred_at, event_id)
PARTITION BY toYYYYMM(occurred_at);       -- one partition per month, easy TTL

-- Tenant daily stats: pre-aggregated rollup refreshed by the Ch11
-- batch job. ReplacingMergeTree (NOT SummingMergeTree) because the
-- aggregator emits one canonical row per (tenant, event_type, day)
-- representing the full day's totals — re-running the job for the
-- same day must REPLACE that row, not ADD to it.
--
-- An earlier version of this table used SummingMergeTree, which silently
-- doubled counts when the batch was re-run (e.g. for a backfill or after
-- a transient failure). SummingMergeTree is the right engine for
-- *incremental* upserts ("here's another +5 events for this key"), not
-- for *full-replacement* upserts ("here's the new total for this key").
--
-- Version column is inserted_at: when duplicates exist before
-- compaction, the row with the highest inserted_at wins. Queries that
-- need exactly-once semantics must use SELECT ... FROM
-- nexus.tenant_daily_stats FINAL — same FINAL pattern as nexus.events.
CREATE TABLE IF NOT EXISTS nexus.tenant_daily_stats (
    tenant_id    String,
    event_type   LowCardinality(String),
    date         Date,
    event_count  UInt64,
    total_value  Float64,
    inserted_at  DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (tenant_id, event_type, date);
