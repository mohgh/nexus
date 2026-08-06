-- Nexus — PostgreSQL initialisation
-- Runs once when the Docker container is first created.
-- Schema is managed by numbered migrations in scripts/db/migrations/.
-- Run: make migrate-up

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm"; -- Chapter 04: trigram full-text search
