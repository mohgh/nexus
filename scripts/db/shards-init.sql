-- Ch07: create per-shard databases.
--
-- Run once against the main Postgres instance before `migrate up`
-- can be applied per-shard:
--
--   psql "postgres://nexus:nexus_secret@localhost:5432/postgres" \
--        -f scripts/db/shards-init.sql
--
-- The shard count here (4) must match SHARD_COUNT in the
-- application config — Router.ShardIndex returns values in
-- [0, SHARD_COUNT), and each must map to an existing database.
--
-- In production each shard would live on a separate Postgres
-- host (or cluster); the application's only requirement is that
-- Router.ShardDSN can connect to each. Here all four shards
-- share one host.

CREATE DATABASE nexus_shard0;
CREATE DATABASE nexus_shard1;
CREATE DATABASE nexus_shard2;
CREATE DATABASE nexus_shard3;
