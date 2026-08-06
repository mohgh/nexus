// Package batch implements the nightly batch aggregation job for Nexus.
//
// Ch11 teaching points:
//  1. Batch processing is fundamentally different from request/response —
//     it trades latency for throughput. One run processes all data at once.
//  2. The aggregator reads raw events from PostgreSQL, computes daily stats
//     (count + sum per tenant/event_type/day), and writes them to ClickHouse.
//  3. Idempotency: tenant_daily_stats is a ReplacingMergeTree, so writing
//     the same (tenant_id, event_type, date) twice collapses to a single
//     row at compaction. Queries that need exactly-once semantics use
//     FROM nexus.tenant_daily_stats FINAL. Re-running the same day is
//     therefore safe — counts don't double.
//  4. Verifiable runs: Checksum() produces a deterministic hash of the
//     aggregator's output and Validate() compares that hash against a
//     fresh Postgres aggregation. If the two ever disagree, a derived
//     copy has drifted from the source of truth.
//  5. Atomic per-day output: the ClickHouse batch insert is all-or-nothing
//     per day. A partial write doesn't corrupt the row set; either every
//     row for the day lands or none does.
//
// DDIA reference: Chapter 10 — "Batch Processing" — Unix pipelines,
// MapReduce, and the importance of deterministic, retry-safe jobs.
package batch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// DailyStat is one row of the aggregated output.
type DailyStat struct {
	TenantID   string
	EventType  string
	Date       time.Time
	EventCount uint64
	TotalValue float64
}

// Aggregator reads events from PostgreSQL and writes daily rollups to ClickHouse.
type Aggregator struct {
	pg     *pgxpool.Pool
	ch     driver.Conn
	logger *zap.Logger
}

// NewAggregator creates a batch aggregator.
func NewAggregator(pg *pgxpool.Pool, ch driver.Conn, logger *zap.Logger) *Aggregator {
	return &Aggregator{pg: pg, ch: ch, logger: logger}
}

// RunDay aggregates all events for the given date (UTC midnight-to-midnight)
// and writes the result to ClickHouse's tenant_daily_stats table.
//
// Idempotent by virtue of the ReplacingMergeTree(inserted_at) engine:
// every row this function inserts carries an inserted_at = now(), and
// duplicates with the same (tenant_id, event_type, date) key collapse
// to the most recent insert during background compaction. Readers that
// need exactly-once semantics use SELECT ... FINAL.
//
// Additive-only: RunDay does NOT delete sink rows for a day that no
// longer has any source events. If a day previously had events that
// were subsequently erased (e.g. by Ch14 GDPR erasure), an existing
// rollup row remains in tenant_daily_stats and Validate() will flag
// the drift. Repair is operator-driven (manual ALTER TABLE DELETE or
// a dedicated re-derivation job) rather than implicit in the
// aggregator, because the "source has no events for this day" state
// can come from either real emptiness or a misconfigured query
// window, and silently nuking sink rows is the wrong default.
func (a *Aggregator) RunDay(ctx context.Context, date time.Time) error {
	stats, err := a.AggregateFromSource(ctx, date)
	if err != nil {
		return err
	}
	if len(stats) == 0 {
		a.logger.Info("batch: no events for date", zap.Time("date", date.Truncate(24*time.Hour)))
		return nil
	}
	return a.WriteSink(ctx, stats)
}

// AggregateFromSource runs the GROUP BY on the Postgres source of
// truth and returns the result in memory. Extracted from RunDay so
// the same query can be re-issued during Validate() against the same
// source data the run was supposed to mirror — that's what makes the
// validation a meaningful check rather than a tautology.
func (a *Aggregator) AggregateFromSource(ctx context.Context, date time.Time) ([]DailyStat, error) {
	day := date.Truncate(24 * time.Hour)
	nextDay := day.Add(24 * time.Hour)

	a.logger.Info("batch: starting aggregation",
		zap.Time("date", day),
	)

	// Full table scan filtered by date — acceptable for nightly batch.
	// In production, you'd partition the events table by month (Ch07).
	rows, err := a.pg.Query(ctx,
		`SELECT tenant_id::text, event_type, COUNT(*), COALESCE(SUM(value), 0)
		 FROM events
		 WHERE occurred_at >= $1 AND occurred_at < $2
		 GROUP BY tenant_id, event_type`,
		day, nextDay,
	)
	if err != nil {
		return nil, fmt.Errorf("batch: query events: %w", err)
	}
	defer rows.Close()

	var stats []DailyStat
	for rows.Next() {
		var s DailyStat
		if err := rows.Scan(&s.TenantID, &s.EventType, &s.EventCount, &s.TotalValue); err != nil {
			return nil, fmt.Errorf("batch: scan: %w", err)
		}
		s.Date = day
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("batch: rows: %w", err)
	}
	return stats, nil
}

// WriteSink performs the all-or-nothing batch insert into ClickHouse.
// Each row gets inserted_at = now() so the ReplacingMergeTree picks
// this run's row over any previous run's row at compaction time.
func (a *Aggregator) WriteSink(ctx context.Context, stats []DailyStat) error {
	batch, err := a.ch.PrepareBatch(ctx,
		`INSERT INTO nexus.tenant_daily_stats
		 (tenant_id, event_type, date, event_count, total_value, inserted_at)`,
	)
	if err != nil {
		return fmt.Errorf("batch: prepare ClickHouse batch: %w", err)
	}

	now := time.Now().UTC()
	for _, s := range stats {
		if err := batch.Append(s.TenantID, s.EventType, s.Date, s.EventCount, s.TotalValue, now); err != nil {
			return fmt.Errorf("batch: append row: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("batch: send to ClickHouse: %w", err)
	}

	a.logger.Info("batch: aggregation complete",
		zap.Int("stat_rows", len(stats)),
	)
	return nil
}

// Checksum computes a deterministic SHA-256 hash of the aggregator's
// output. Used for replay verification: re-running the same day must
// produce the same checksum. Sort order is fixed (tenant, event_type,
// date) so the hash is stable regardless of how the source query
// happened to order rows.
//
// The hash deliberately excludes inserted_at — that field is
// run-specific and would make every checksum differ across runs even
// when the aggregation is otherwise identical.
func Checksum(stats []DailyStat) string {
	sorted := make([]DailyStat, len(stats))
	copy(sorted, stats)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].TenantID != sorted[j].TenantID {
			return sorted[i].TenantID < sorted[j].TenantID
		}
		if sorted[i].EventType != sorted[j].EventType {
			return sorted[i].EventType < sorted[j].EventType
		}
		return sorted[i].Date.Before(sorted[j].Date)
	})

	h := sha256.New()
	for _, s := range sorted {
		// Pipe-delimited canonical form. The exact format doesn't
		// matter as long as it's deterministic — two stats with
		// the same fields must always produce the same hash input.
		fmt.Fprintf(h, "%s|%s|%s|%d|%s\n",
			s.TenantID,
			s.EventType,
			s.Date.Format("2006-01-02"),
			s.EventCount,
			strconv.FormatFloat(s.TotalValue, 'f', -1, 64),
		)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Validate compares the aggregator's stored output for `date` against
// a fresh source-of-truth aggregation. If the two checksums differ,
// the derived copy in ClickHouse has drifted from the Postgres source
// (corruption, partial-run leftover, etc.) and the caller can
// decide whether to re-run RunDay or alert.
//
// This is the DDIA Ch10 "verifiable batch output" idea operationalised:
// derived data is always re-derivable from the source, and
// disagreement between them is a bug, not a fact of life.
func (a *Aggregator) Validate(ctx context.Context, date time.Time) error {
	day := date.Truncate(24 * time.Hour)

	sourceStats, err := a.AggregateFromSource(ctx, day)
	if err != nil {
		return fmt.Errorf("validate: re-aggregate source: %w", err)
	}

	// Read the stored aggregate FROM ... FINAL so duplicates from
	// partial runs are collapsed before we compare. Without FINAL
	// this comparison would flap until ClickHouse got around to
	// compacting.
	rows, err := a.ch.Query(ctx,
		`SELECT tenant_id, event_type, date, event_count, total_value
		 FROM nexus.tenant_daily_stats FINAL
		 WHERE date = ?`,
		day,
	)
	if err != nil {
		return fmt.Errorf("validate: query ClickHouse: %w", err)
	}
	defer rows.Close()

	var storedStats []DailyStat
	for rows.Next() {
		var s DailyStat
		if err := rows.Scan(&s.TenantID, &s.EventType, &s.Date, &s.EventCount, &s.TotalValue); err != nil {
			return fmt.Errorf("validate: scan ClickHouse: %w", err)
		}
		storedStats = append(storedStats, s)
	}

	sourceHash := Checksum(sourceStats)
	storedHash := Checksum(storedStats)
	if sourceHash != storedHash {
		return fmt.Errorf("validate: source/sink mismatch for %s: source=%s sink=%s (rows: source=%d sink=%d)",
			day.Format("2006-01-02"), sourceHash, storedHash, len(sourceStats), len(storedStats))
	}

	a.logger.Info("batch: validation passed",
		zap.Time("date", day),
		zap.String("checksum", sourceHash),
		zap.Int("rows", len(sourceStats)),
	)
	return nil
}

// RunRange aggregates each day in [from, to) sequentially.
// Used for backfilling historical data.
func (a *Aggregator) RunRange(ctx context.Context, from, to time.Time) error {
	from = from.Truncate(24 * time.Hour)
	to = to.Truncate(24 * time.Hour)

	for day := from; day.Before(to); day = day.Add(24 * time.Hour) {
		if err := a.RunDay(ctx, day); err != nil {
			return fmt.Errorf("batch: day %s: %w", day.Format("2006-01-02"), err)
		}
	}
	return nil
}
