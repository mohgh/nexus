//go:build integration

// Live Postgres + ClickHouse integration tests for the Ch11 batch
// aggregator.
//
// What they pin:
//   1. RunDay twice on the same date does NOT double counts. This is
//      the headline regression test for the SummingMergeTree →
//      ReplacingMergeTree fix. The earlier engine choice silently
//      summed re-runs together; this test fails loudly if anything
//      reverts it.
//   2. Validate() catches divergence between Postgres source and
//      ClickHouse sink. We provoke divergence by deleting a source
//      row after the run and assert Validate returns an error.
//   3. Checksum() is deterministic across re-aggregations of the same
//      source data — necessary for a backfill-replay workflow.
//
// Run via:
//   POSTGRES_DSN=postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable \
//   CLICKHOUSE_DSN=clickhouse://nexus:nexus_secret@localhost:9000/nexus \
//   go test -tags=integration ./internal/batch/...

package batch_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/batch"
	"go.uber.org/zap"
)

func dsnsOrSkip(t *testing.T) (string, string) {
	t.Helper()
	pg := os.Getenv("POSTGRES_DSN")
	ch := os.Getenv("CLICKHOUSE_DSN")
	if pg == "" || ch == "" {
		t.Skip("POSTGRES_DSN and CLICKHOUSE_DSN must be set; skipping integration test")
	}
	return pg, ch
}

func setup(t *testing.T) (*pgxpool.Pool, driver.Conn, *batch.Aggregator) {
	t.Helper()
	pgDSN, chDSN := dsnsOrSkip(t)
	ctx := context.Background()

	pgPool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		t.Fatalf("postgres connect: %v", err)
	}
	t.Cleanup(pgPool.Close)

	opts, err := chdriver.ParseDSN(chDSN)
	if err != nil {
		t.Fatalf("clickhouse DSN: %v", err)
	}
	chConn, err := chdriver.Open(opts)
	if err != nil {
		t.Fatalf("clickhouse open: %v", err)
	}
	t.Cleanup(func() { _ = chConn.Close() })

	if err := chConn.Ping(ctx); err != nil {
		t.Fatalf("clickhouse ping: %v", err)
	}

	logger := zap.NewNop()
	return pgPool, chConn, batch.NewAggregator(pgPool, chConn, logger)
}

// seedEvents inserts a known set of events for a given day and
// returns the tenant ID and date used. Cleans up after the test
// regardless of pass/fail so subsequent runs aren't poisoned.
func seedEvents(t *testing.T, pg *pgxpool.Pool, ch driver.Conn) (string, time.Time) {
	t.Helper()
	ctx := context.Background()

	tenantID := uuid.New().String()
	// Test isolation comes from the per-tenant UUID and the
	// cleanup function below, NOT from the date choice. A fixed
	// historical date is convenient (it won't collide with
	// "yesterday" runs from cmd/batch-aggregator) but other tests
	// could legitimately use the same date — aggregation is
	// multi-tenant, and Validate works against the full per-day
	// rollup, not a single tenant's slice.
	day := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)

	if _, err := pg.Exec(ctx,
		`INSERT INTO tenants (id, name, plan, created_at, updated_at)
		 VALUES ($1, $2, 'free', NOW(), NOW())`,
		tenantID, "batch-int-"+tenantID[:8],
	); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}

	// Three events at 10:00, 11:00, 12:00 — same event_type so
	// they aggregate to one stat row (count=3, sum=1.0+2.0+3.0=6.0).
	for i, hour := range []int{10, 11, 12} {
		_, err := pg.Exec(ctx,
			`INSERT INTO events (id, tenant_id, event_type, payload, value, occurred_at)
			 VALUES ($1, $2, 'page_view', '{}', $3, $4)`,
			uuid.New().String(),
			tenantID,
			float64(i+1),
			day.Add(time.Duration(hour)*time.Hour),
		)
		if err != nil {
			t.Fatalf("insert event: %v", err)
		}
	}

	t.Cleanup(func() {
		// Order matters: events first (FK -> tenants), then tenant,
		// then any ClickHouse rows we wrote. ClickHouse delete is
		// a mutation; fire-and-forget cleanup is fine since the
		// test selects rows by tenant and the next run picks a
		// fresh UUID anyway.
		_, _ = pg.Exec(context.Background(), `DELETE FROM events WHERE tenant_id = $1`, tenantID)
		_, _ = pg.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, tenantID)
		_ = ch.Exec(context.Background(),
			`ALTER TABLE nexus.tenant_daily_stats DELETE WHERE tenant_id = ?`,
			tenantID,
		)
	})

	return tenantID, day
}

// TestRunDay_IdempotentUnderRerun is the regression guard for the
// SummingMergeTree → ReplacingMergeTree fix. Running the aggregator
// twice on the same date must yield the SAME event_count, not 2x.
// SummingMergeTree would have produced count=6 / sum=12 here; we
// assert count=3 / sum=6.
func TestRunDay_IdempotentUnderRerun(t *testing.T) {
	pg, ch, agg := setup(t)
	tenantID, day := seedEvents(t, pg, ch)
	ctx := context.Background()

	if err := agg.RunDay(ctx, day); err != nil {
		t.Fatalf("first RunDay: %v", err)
	}
	if err := agg.RunDay(ctx, day); err != nil {
		t.Fatalf("second RunDay: %v", err)
	}

	// FINAL collapses duplicates introduced by the second insert.
	// Without ReplacingMergeTree, no amount of FINAL would fix the
	// sum semantic of SummingMergeTree — this query would return
	// the doubled counts.
	row := ch.QueryRow(ctx,
		`SELECT event_count, total_value
		 FROM nexus.tenant_daily_stats FINAL
		 WHERE tenant_id = ? AND date = ?`,
		tenantID, day,
	)
	var count uint64
	var total float64
	if err := row.Scan(&count, &total); err != nil {
		t.Fatalf("scan rollup: %v", err)
	}
	if count != 3 {
		t.Fatalf("idempotency broken: event_count = %d, want 3 (second run doubled the count)", count)
	}
	if total != 6.0 {
		t.Fatalf("idempotency broken: total_value = %f, want 6.0", total)
	}
}

// TestValidate_PassesAfterRun pins the happy path: a fresh
// aggregator run followed by Validate() returns no error.
func TestValidate_PassesAfterRun(t *testing.T) {
	pg, ch, agg := setup(t)
	_, day := seedEvents(t, pg, ch)
	ctx := context.Background()

	if err := agg.RunDay(ctx, day); err != nil {
		t.Fatalf("RunDay: %v", err)
	}
	if err := agg.Validate(ctx, day); err != nil {
		t.Fatalf("Validate should pass on freshly-run day, got: %v", err)
	}
}

// TestValidate_DetectsDrift provokes divergence between the
// Postgres source and the ClickHouse sink by deleting a source
// event AFTER the run. The aggregator's stored result no longer
// matches what a fresh source-of-truth query would produce, and
// Validate must surface this.
//
// This is the DDIA Ch10 invariant: derived data is always
// re-derivable from source, and any disagreement is a bug to
// investigate, not a fact of life.
func TestValidate_DetectsDrift(t *testing.T) {
	pg, ch, agg := setup(t)
	tenantID, day := seedEvents(t, pg, ch)
	ctx := context.Background()

	if err := agg.RunDay(ctx, day); err != nil {
		t.Fatalf("RunDay: %v", err)
	}

	// Delete one source event — the rollup is now stale by exactly
	// one event for this tenant on this day.
	if _, err := pg.Exec(ctx,
		`DELETE FROM events
		 WHERE tenant_id = $1 AND occurred_at = $2`,
		tenantID, day.Add(10*time.Hour),
	); err != nil {
		t.Fatalf("delete source event: %v", err)
	}

	err := agg.Validate(ctx, day)
	if err == nil {
		t.Fatalf("Validate should detect source/sink divergence, got nil")
	}
	if !strings.Contains(err.Error(), "source/sink mismatch") {
		t.Fatalf("Validate error should name the mismatch, got: %v", err)
	}
}

// TestChecksum_DeterministicAcrossReaggregations: running the
// source query twice on identical data produces the same checksum.
// This is the precondition for a backfill-replay workflow — without
// it, "checksum the replayed run and compare to the original" has
// no signal.
func TestChecksum_DeterministicAcrossReaggregations(t *testing.T) {
	pg, ch, agg := setup(t)
	_, day := seedEvents(t, pg, ch)
	ctx := context.Background()

	a, err := agg.AggregateFromSource(ctx, day)
	if err != nil {
		t.Fatalf("first aggregate: %v", err)
	}
	b, err := agg.AggregateFromSource(ctx, day)
	if err != nil {
		t.Fatalf("second aggregate: %v", err)
	}
	if got, want := batch.Checksum(a), batch.Checksum(b); got != want {
		t.Fatalf("Checksum non-deterministic: %s vs %s", got, want)
	}
}
