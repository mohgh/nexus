// storage-benchmark — runs the same daily-stats aggregation against
// Postgres and ClickHouse and prints a side-by-side latency report.
//
// Usage:
//
//	go run ./cmd/storage-benchmark [-tenant <id>] [-iters 5] [-days 30]
//
// The chapter's central OLTP-vs-OLAP demo lives here. The query is
// the same logical aggregation in both engines:
//
//   SELECT DATE(occurred_at), event_type, count(*), sum(value)
//   FROM events
//   WHERE tenant_id = $1 AND occurred_at BETWEEN ? AND ?
//   GROUP BY 1, 2 ORDER BY 1 DESC, 3 DESC
//
// Postgres scans rows from a B-tree-indexed table. ClickHouse
// scans the columnar MergeTree. The ratio Nexus typically sees
// with ~1M rows is 10–100× in ClickHouse's favour for this shape
// of query.
//
// The tool reports min/median/max for each engine across -iters
// runs to filter out cold-cache noise. Run it twice: once to warm
// both engines, then again for the comparison.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/config"
	chstore "github.com/mohgh/nexus/internal/storage/clickhouse"
)

func main() {
	var (
		tenantID string
		iters    int
		days     int
	)
	flag.StringVar(&tenantID, "tenant", "", "tenant to query (required)")
	flag.IntVar(&iters, "iters", 5, "number of times to run each query")
	flag.IntVar(&days, "days", 30, "lookback window in days")
	flag.Parse()

	if tenantID == "" {
		fmt.Fprintln(os.Stderr, "usage: -tenant <id> is required (try: SELECT id FROM tenants LIMIT 1)")
		os.Exit(2)
	}

	cfg := config.Load()
	ctx := context.Background()

	// Postgres connection. The query reads directly from the
	// `events` table — the same table the API server's
	// EventRepository writes to.
	pgPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		fail("postgres connect: %v", err)
	}
	defer pgPool.Close()

	// ClickHouse connection. The query reads from nexus.events,
	// which the stream-processor populates from the same Kafka
	// stream the API server publishes to.
	if cfg.ClickHouseDSN == "" {
		fail("CLICKHOUSE_DSN is empty — start the olap profile and re-run")
	}
	chClient, err := chstore.NewClient(ctx, cfg.ClickHouseDSN)
	if err != nil {
		fail("clickhouse connect: %v", err)
	}
	defer chClient.Close()
	chRepo := chstore.NewEventRepository(chClient)

	to := time.Now().UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)
	from := to.Add(-time.Duration(days) * 24 * time.Hour)

	pgDurations, pgRows := runPostgres(ctx, pgPool, tenantID, from, to, iters)
	chDurations, chRows := runClickHouse(ctx, chRepo, tenantID, from, to, iters)

	fmt.Printf("daily-stats benchmark — tenant=%s window=%d days iters=%d\n\n",
		tenantID, days, iters)
	fmt.Println("engine       min ms    median ms    max ms    rows")
	fmt.Println("-----------  --------  -----------  --------  ----")
	report("postgres", pgDurations, pgRows)
	report("clickhouse", chDurations, chRows)

	// Compute the headline ratio on medians. Skip if either engine
	// returned no data — the ratio would be meaningless.
	if len(pgDurations) > 0 && len(chDurations) > 0 {
		pgMed := median(pgDurations)
		chMed := median(chDurations)
		if chMed > 0 {
			fmt.Printf("\nclickhouse is %.1fx %s than postgres on the median run\n",
				ratio(pgMed, chMed), faster(pgMed, chMed))
		}
	}
}

func runPostgres(ctx context.Context, pool *pgxpool.Pool, tenantID string, from, to time.Time, iters int) ([]time.Duration, int) {
	q := `SELECT DATE(occurred_at) AS date, event_type, count(*) AS event_count, sum(value) AS total_value
	      FROM events
	      WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3
	      GROUP BY DATE(occurred_at), event_type
	      ORDER BY date DESC, event_count DESC`

	var ds []time.Duration
	var rows int
	for i := 0; i < iters; i++ {
		start := time.Now()
		r, err := pool.Query(ctx, q, tenantID, from, to)
		if err != nil {
			fmt.Fprintf(os.Stderr, "postgres iter %d: %v\n", i, err)
			continue
		}
		n := 0
		for r.Next() {
			n++
		}
		r.Close()
		ds = append(ds, time.Since(start))
		rows = n
	}
	return ds, rows
}

func runClickHouse(ctx context.Context, repo *chstore.EventRepository, tenantID string, from, to time.Time, iters int) ([]time.Duration, int) {
	var ds []time.Duration
	var rows int
	for i := 0; i < iters; i++ {
		start := time.Now()
		stats, err := repo.DailyStats(ctx, tenantID, from, to)
		if err != nil {
			fmt.Fprintf(os.Stderr, "clickhouse iter %d: %v\n", i, err)
			continue
		}
		ds = append(ds, time.Since(start))
		rows = len(stats)
	}
	return ds, rows
}

func report(label string, ds []time.Duration, rows int) {
	if len(ds) == 0 {
		fmt.Printf("%-11s  (no successful runs)\n", label)
		return
	}
	fmt.Printf("%-11s  %7.1f  %10.1f  %7.1f  %4d\n",
		label,
		ms(min(ds)),
		ms(median(ds)),
		ms(max(ds)),
		rows,
	)
}

func min(ds []time.Duration) time.Duration {
	m := ds[0]
	for _, d := range ds[1:] {
		if d < m {
			m = d
		}
	}
	return m
}

func max(ds []time.Duration) time.Duration {
	m := ds[0]
	for _, d := range ds[1:] {
		if d > m {
			m = d
		}
	}
	return m
}

func median(ds []time.Duration) time.Duration {
	cp := append([]time.Duration(nil), ds...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return cp[len(cp)/2]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func ratio(a, b time.Duration) float64 {
	if a > b {
		return float64(a) / float64(b)
	}
	return float64(b) / float64(a)
}

func faster(a, b time.Duration) string {
	if a > b {
		return "faster"
	}
	return "SLOWER (unexpected — check data layout)"
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "storage-benchmark: "+format+"\n", args...)
	os.Exit(1)
}
