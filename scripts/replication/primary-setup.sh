#!/bin/bash
# Ch06: Configure PostgreSQL primary for streaming replication.
# This script runs as an init script on the primary container.

set -e

# Create a replication user (if it doesn't already exist).
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
            CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD 'replicator_secret';
        END IF;
    END
    \$\$;
EOSQL

# Allow replication connections from the replica container.
# The Docker network assigns IPs in the 172.x.x.x range.
echo "host replication replicator all md5" >> "$PGDATA/pg_hba.conf"

# Reload config to apply pg_hba changes.
pg_ctl reload -D "$PGDATA"

echo "Primary configured for streaming replication."
