package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/mohgh/nexus/internal/domain"
	"github.com/mohgh/nexus/internal/metrics"
)

// EventRepository writes and queries events in ClickHouse.
//
// Ch04 teaching point: compare this query against the PostgreSQL version —
// ClickHouse scans columnar data and returns aggregates 10–100× faster for
// analytics workloads, while PostgreSQL is faster for single-row lookups.
//
// metrics is optional. When wired, DailyStats observes
// OLAPQueryDuration{engine="clickhouse",query="daily_stats"} so the
// chapter's Postgres-vs-ClickHouse latency claim is queryable from
// the dashboards, not just from the one-shot cmd/storage-benchmark.
type EventRepository struct {
	client  *Client
	metrics *metrics.Registry
}

func NewEventRepository(client *Client) *EventRepository {
	return &EventRepository{client: client}
}

// WithMetrics returns a copy of the repository that observes
// OLAPQueryDuration on each query. Use the builder rather than
// adding to NewEventRepository's signature so existing callers
// (and tests) don't have to pass nil.
func (r *EventRepository) WithMetrics(m *metrics.Registry) *EventRepository {
	out := *r
	out.metrics = m
	return &out
}

// Insert writes a single event to ClickHouse.
// In production, batch inserts (ch12) are far more efficient.
// ClickHouse's MergeTree engine merges small inserts asynchronously —
// querying immediately after an insert may not see the row.
func (r *EventRepository) Insert(ctx context.Context, e *domain.Event) error {
	batch, err := r.client.conn.PrepareBatch(ctx,
		`INSERT INTO nexus.events
		 (tenant_id, event_id, event_type, value, payload, occurred_at)`,
	)
	if err != nil {
		return fmt.Errorf("clickhouse: prepare batch: %w", err)
	}

	if err := batch.Append(
		e.TenantID,
		e.ID,
		e.EventType,
		e.Value,
		string(e.Payload),
		e.OccurredAt,
	); err != nil {
		return fmt.Errorf("clickhouse: append: %w", err)
	}

	return batch.Send()
}

// DailyStats returns event counts and total values per event_type for a tenant
// within a date range.
//
// Ch04: run this query against both PostgreSQL and ClickHouse with 1M+ rows
// and observe the latency difference. This is the core OLAP vs OLTP demo.
type DailyStat struct {
	Date       time.Time `json:"date"`
	EventType  string    `json:"event_type"`
	EventCount uint64    `json:"event_count"`
	TotalValue float64   `json:"total_value"`
}

func (r *EventRepository) DailyStats(ctx context.Context, tenantID string, from, to time.Time) ([]DailyStat, error) {
	start := time.Now()
	defer r.observe("daily_stats", start)

	// SELECT … FROM nexus.events FINAL forces the ReplacingMergeTree
	// to deduplicate at query time. Without FINAL, rows that have
	// been redelivered by Kafka but haven't yet been merged would
	// be counted multiple times — defeating the chapter's claim
	// that the engines see the same data. FINAL is more expensive
	// than a plain SELECT; the exactly-once-analytics trade-off
	// is one of the chapter's teaching points.
	rows, err := r.client.conn.Query(ctx,
		`SELECT
		    toDate(occurred_at)  AS date,
		    event_type,
		    count()              AS event_count,
		    sum(value)           AS total_value
		 FROM nexus.events FINAL
		 WHERE tenant_id   = ?
		   AND occurred_at >= ?
		   AND occurred_at <  ?
		 GROUP BY date, event_type
		 ORDER BY date DESC, event_count DESC`,
		tenantID, from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("clickhouse: daily stats query: %w", err)
	}
	defer rows.Close()

	var out []DailyStat
	for rows.Next() {
		var s DailyStat
		if err := rows.Scan(&s.Date, &s.EventType, &s.EventCount, &s.TotalValue); err != nil {
			return nil, fmt.Errorf("clickhouse: scan: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (r *EventRepository) observe(query string, start time.Time) {
	if r.metrics != nil {
		r.metrics.OLAPQueryDuration.
			WithLabelValues("clickhouse", query).
			Observe(time.Since(start).Seconds())
	}
}
