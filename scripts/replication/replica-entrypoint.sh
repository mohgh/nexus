#!/bin/bash
# Ch06: Bootstrap the PostgreSQL replica via pg_basebackup.
#
# This script:
# 1. Waits for the primary to be ready
# 2. If PGDATA is empty, runs pg_basebackup to clone the primary
# 3. Configures the replica to follow the primary via streaming replication
# 4. Starts PostgreSQL in standby mode
#
# On subsequent starts (PGDATA already populated), it just starts as a standby.

set -e

PGDATA="/var/lib/postgresql/data"
PRIMARY_HOST="${PRIMARY_HOST:-postgres}"
PRIMARY_PORT="${PRIMARY_PORT:-5432}"
REPLICATION_USER="${REPLICATION_USER:-replicator}"
REPLICATION_PASSWORD="${REPLICATION_PASSWORD:-replicator_secret}"

echo "Replica: waiting for primary at ${PRIMARY_HOST}:${PRIMARY_PORT}..."
until PGPASSWORD="$REPLICATION_PASSWORD" pg_isready -h "$PRIMARY_HOST" -p "$PRIMARY_PORT" -U "$REPLICATION_USER" 2>/dev/null; do
    sleep 1
done
echo "Replica: primary is ready."

# If PGDATA is empty, do a base backup from the primary. Run pg_basebackup
# as the postgres user so the resulting files have the right ownership —
# the postgres server later refuses to start on root-owned data files.
if [ ! -s "$PGDATA/PG_VERSION" ]; then
    echo "Replica: PGDATA is empty — running pg_basebackup..."

    rm -rf "$PGDATA"/*
    chown postgres:postgres "$PGDATA"
    PGPASSWORD="$REPLICATION_PASSWORD" gosu postgres pg_basebackup \
        -h "$PRIMARY_HOST" \
        -p "$PRIMARY_PORT" \
        -U "$REPLICATION_USER" \
        -D "$PGDATA" \
        -Fp -Xs -P -R

    echo "Replica: base backup complete."
else
    echo "Replica: PGDATA exists — starting as standby."
    # Defensive chown in case PGDATA was left in an inconsistent
    # state by a previous failed run (e.g. earlier root-owned files
    # from the bug this script-version fixes).
    chown -R postgres:postgres "$PGDATA"
fi

# Postgres requires PGDATA to be u=rwx (0700) or u=rwx,g=rx (0750).
# Docker volume mounts default to 0755, which Postgres rejects.
chmod 0700 "$PGDATA"

# Ensure standby signal file exists (PostgreSQL 12+ uses standby.signal).
touch "$PGDATA/standby.signal"
chown postgres:postgres "$PGDATA/standby.signal"

# Write the primary connection info for streaming replication.
cat > "$PGDATA/postgresql.auto.conf" <<EOF
primary_conninfo = 'host=${PRIMARY_HOST} port=${PRIMARY_PORT} user=${REPLICATION_USER} password=${REPLICATION_PASSWORD}'
hot_standby = on
EOF
chown postgres:postgres "$PGDATA/postgresql.auto.conf"

# Start PostgreSQL in the foreground as the postgres user. Without
# gosu (or su-exec), Postgres refuses to run because the server
# must be started under an unprivileged user ID.
exec gosu postgres postgres -D "$PGDATA"
