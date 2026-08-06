package handlers

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	chstore "github.com/mohgh/nexus/internal/storage/clickhouse"
)

// DailyStatsQuerier returns per-(day, event_type) aggregates from
// the columnar OLAP store. Implemented by
// *clickhouse.EventRepository.
type DailyStatsQuerier interface {
	DailyStats(ctx context.Context, tenantID string, from, to time.Time) ([]chstore.DailyStat, error)
}

// DailyStats serves the Ch04 OLAP read path. The columnar store
// (ClickHouse) handles a sum + count + group-by over millions of
// rows in tens of milliseconds; the same query against the row
// store (Postgres) is order(s) of magnitude slower for that
// workload. The pair is the chapter's central OLTP-vs-OLAP demo.
//
//	GET /api/v1/tenants/{tenantID}/daily-stats?from=2026-04-01&to=2026-05-01
//
// `from` / `to` are inclusive-start / exclusive-end ISO dates.
// Defaults: from = now-30d, to = now+1d. Range is capped at 366d
// to keep a misclick from hammering ClickHouse.
func DailyStats(q DailyStatsQuerier) http.HandlerFunc {
	const (
		defaultRange = 30 * 24 * time.Hour
		maxRange     = 366 * 24 * time.Hour
	)
	return func(w http.ResponseWriter, r *http.Request) {
		tenantID := chi.URLParam(r, "tenantID")
		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "tenantID is required")
			return
		}
		now := time.Now().UTC()
		to := now.Add(24 * time.Hour).Truncate(24 * time.Hour)
		from := to.Add(-defaultRange)

		if v := r.URL.Query().Get("from"); v != "" {
			t, err := parseISODate(v)
			if err != nil {
				writeError(w, http.StatusBadRequest, "from must be ISO date (YYYY-MM-DD)")
				return
			}
			from = t
		}
		if v := r.URL.Query().Get("to"); v != "" {
			t, err := parseISODate(v)
			if err != nil {
				writeError(w, http.StatusBadRequest, "to must be ISO date (YYYY-MM-DD)")
				return
			}
			to = t
		}
		if !from.Before(to) {
			writeError(w, http.StatusBadRequest, "from must be strictly before to")
			return
		}
		if to.Sub(from) > maxRange {
			writeError(w, http.StatusBadRequest, "range exceeds 366 days; narrow the window")
			return
		}

		stats, err := q.DailyStats(r.Context(), tenantID, from, to)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "daily stats query failed")
			return
		}
		// limit response size on the off chance a tenant has
		// thousands of event_types — surface limit explicitly so a
		// caller knows to paginate by date.
		limit := 5000
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 10000 {
				limit = n
			}
		}
		if len(stats) > limit {
			stats = stats[:limit]
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"tenant_id": tenantID,
			"from":      from.Format("2006-01-02"),
			"to":        to.Format("2006-01-02"),
			"days":      stats,
		})
	}
}

func parseISODate(s string) (time.Time, error) {
	return time.ParseInLocation("2006-01-02", s, time.UTC)
}
