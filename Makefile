.PHONY: run build test lint docker-up docker-down migrate-up migrate-down seed

# ─── Development ──────────────────────────────────────────────────────────────
run:
	go run ./cmd/server

build:
	go build -o bin/nexus ./cmd/server
	go build -o bin/batch-aggregator ./cmd/batch-aggregator
	go build -o bin/stream-processor ./cmd/stream-processor
	go build -o bin/projection-rebuild ./cmd/projection-rebuild
	go build -o bin/pii-scanner ./cmd/pii-scanner
	go build -o bin/storage-benchmark ./cmd/storage-benchmark

test:
	go test ./... -v -race

lint:
	golangci-lint run ./...

# ─── Docker ───────────────────────────────────────────────────────────────────
docker-up:
	docker compose up -d postgres redis

docker-up-all:
	docker compose --profile streaming --profile observability --profile olap --profile workflows --profile replication up -d

docker-down:
	docker compose down

docker-down-volumes:
	docker compose down -v

# ─── Database ─────────────────────────────────────────────────────────────────
migrate-up:
	migrate -path ./scripts/db/migrations -database "postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable" up

migrate-down:
	migrate -path ./scripts/db/migrations -database "postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable" down

seed:
	@echo "Seeding 10,000 events across tenants..."
	go run ./scripts/seed/main.go

# ─── Load testing ─────────────────────────────────────────────────────────────
# Baseline: 20 VUs / 3 min — k6 exits non-zero if any SLO threshold (see
# docs/SLOs.md) breaches. Use in CI / pre-merge.
# Stress: 50 VUs / 6 min, soft thresholds — for finding the capacity ceiling.
load-baseline:
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed — see scripts/load/README.md"; exit 1; }
	k6 run scripts/load/baseline.js

load-stress:
	@command -v k6 >/dev/null 2>&1 || { echo "k6 not installed — see scripts/load/README.md"; exit 1; }
	NEXUS_LOAD=stress k6 run scripts/load/baseline.js

# ─── Demo targets, by problem ─────────────────────────────────────────────────
# Each target starts exactly the Docker services that problem needs, then runs
# the right binary. Services a demo doesn't need are simply not started.
#   run-*    boot the infra and a binary
#   test-*   integration tests against live infra
#   bench-*  benchmarks

.PHONY: run-base run-observability run-datamodel run-olap bench-storage \
        run-encoding run-replication run-partitioning init-shards \
        run-transactions test-anomalies run-chaos run-consensus test-fencing \
        run-batch test-batch run-stream run-cqrs run-privacy

run-base:
	@echo "Trade-offs — base server (postgres + redis)"
	docker compose up -d postgres redis
	go run ./cmd/server

run-observability:
	@echo "Non-functional requirements — with Prometheus + Grafana"
	docker compose up -d postgres redis
	docker compose --profile observability up -d
	go run ./cmd/server

run-datamodel:
	@echo "Data models — PostgreSQL events API"
	docker compose up -d postgres redis
	go run ./cmd/server

run-olap:
	@echo "Storage & retrieval — with ClickHouse"
	docker compose up -d postgres redis
	docker compose --profile olap up -d
	go run ./cmd/server

bench-storage:
	@echo "OLTP vs OLAP — same aggregation in Postgres vs ClickHouse"
	@echo "Requires the olap profile and seeded events. Find a tenant:"
	@echo "  docker exec nexus-postgres psql -U nexus -c 'SELECT id FROM tenants LIMIT 1;'"
	go run ./cmd/storage-benchmark -tenant $${TENANT:?set TENANT=<id> or use the SELECT above}

run-encoding:
	@echo "Schema evolution — with Kafka + Schema Registry"
	docker compose up -d postgres redis
	docker compose --profile streaming up -d
	go run ./cmd/server

run-replication:
	@echo "Replication — with read replica"
	docker compose up -d postgres redis
	docker compose --profile replication up -d
	go run ./cmd/server

run-partitioning:
	@echo "Partitioning — shard router"
	docker compose up -d postgres redis
	go run ./cmd/server

init-shards:
	@echo "Partitioning: create per-shard databases (idempotent — safe to re-run)"
	docker compose up -d postgres
	@for n in 0 1 2 3; do \
		docker exec nexus-postgres psql -U nexus -d postgres \
		  -c "CREATE DATABASE nexus_shard$$n;" 2>/dev/null || true; \
		docker exec nexus-postgres psql -U nexus -d nexus_shard$$n \
		  -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"; CREATE EXTENSION IF NOT EXISTS pg_trgm;" >/dev/null; \
	done
	@echo "Apply migrations to each shard:"
	@for n in 0 1 2 3; do \
		echo "  -> nexus_shard$$n"; \
		migrate -path ./scripts/db/migrations \
		  -database "postgres://nexus:nexus_secret@localhost:5432/nexus_shard$$n?sslmode=disable" up; \
	done
	@echo
	@echo "Run the server in sharded mode:"
	@echo "  SHARD_DSN_TEMPLATE=postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable \\"
	@echo "      go run ./cmd/server"

run-transactions:
	@echo "Transactions — Temporal workflow + transactional outbox"
	@echo "Streaming profile is required: the outbox worker publishes billing_records"
	@echo "to Kafka. Without it the outbox loop logs publish errors forever."
	docker compose up -d postgres redis
	docker compose --profile workflows up -d
	docker compose --profile streaming up -d
	go run ./cmd/server

test-anomalies:
	@echo "Transaction anomaly demos + outbox integration"
	@echo "Requires postgres running. Reproduces lost-update and write-skew"
	@echo "anomalies, then asserts the outbox pattern: workflow leaves rows"
	@echo "outbox_sent_at=NULL, only the worker marks them sent (post-publish)."
	docker compose up -d postgres
	POSTGRES_DSN=postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable \
	    go test -tags=integration -v -count=1 \
	        ./internal/transactions/anomalies/... \
	        ./internal/billing/outbox/...

run-chaos:
	@echo "Failure in distributed systems — circuit breakers + chaos toggles"
	@echo "Streaming profile lets the chaos drop_publish toggle demonstrate"
	@echo "asymmetric failure (DB commits, Kafka consumer sees nothing)."
	docker compose up -d postgres redis
	docker compose --profile streaming up -d
	go run ./cmd/server

run-consensus:
	@echo "Consistency & consensus — leader election + fencing tokens"
	docker compose up -d postgres redis
	go run ./cmd/server

test-fencing:
	@echo "Live-Redis integration tests for fencing tokens"
	@echo "Proves: token strictly increases across acquisitions, losing"
	@echo "acquisitions don't consume tokens, stale leader writes are fenced off,"
	@echo "renew does not advance the token."
	docker compose up -d redis
	REDIS_DSN=redis://localhost:6379/0 \
	    go test -tags=integration -v -count=1 ./internal/election/...

run-batch:
	@echo "Batch processing — aggregate events to ClickHouse"
	docker compose up -d postgres redis
	docker compose --profile olap up -d
	go run ./cmd/batch-aggregator

test-batch:
	@echo "Integration tests covering idempotent re-run + Validate + drift detection"
	@echo "Requires postgres + clickhouse running (make run-batch brings them up)."
	POSTGRES_DSN=postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable \
	CLICKHOUSE_DSN=clickhouse://nexus:nexus_secret@localhost:9000/nexus \
	    go test -tags=integration -v -count=1 ./internal/batch/...

run-stream:
	@echo "Stream processing — Kafka consumer + tumbling windows"
	@echo "This target runs the stream-processor binary. To see the SSE"
	@echo "live-stats endpoint, also run 'make run' in another terminal —"
	@echo "the processor populates Redis windows, the server streams them."
	docker compose up -d postgres redis
	docker compose --profile streaming up -d
	go run ./cmd/stream-processor

run-cqrs:
	@echo "Event sourcing — CQRS event store"
	docker compose up -d postgres redis
	go run ./cmd/server

run-privacy:
	@echo "Privacy, erasure & audit — GDPR endpoints + audit trail + PII masking"
	@echo "Observability profile lets you verify the Prometheus-label fix:"
	@echo "request labels use chi route patterns, never raw paths (no PII)."
	docker compose up -d postgres redis
	docker compose --profile observability up -d
	go run ./cmd/server
