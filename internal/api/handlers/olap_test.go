package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/mohgh/nexus/internal/api/handlers"
	chstore "github.com/mohgh/nexus/internal/storage/clickhouse"
)

type fakeDailyStats struct {
	got struct {
		tenantID string
		from, to time.Time
	}
	rows []chstore.DailyStat
	err  error
}

func (f *fakeDailyStats) DailyStats(_ context.Context, tenantID string, from, to time.Time) ([]chstore.DailyStat, error) {
	f.got.tenantID = tenantID
	f.got.from = from
	f.got.to = to
	return f.rows, f.err
}

func mount(h http.HandlerFunc) http.Handler {
	r := chi.NewRouter()
	r.Get("/api/v1/tenants/{tenantID}/daily-stats", h)
	return r
}

// TestDailyStats_Defaults: no from/to query params -> defaults to
// "last 30 days ending tomorrow." Locks down the default window so
// a caller hitting the endpoint without args gets a sensible answer.
func TestDailyStats_Defaults(t *testing.T) {
	t.Parallel()
	fake := &fakeDailyStats{}
	rr := httptest.NewRecorder()
	mount(handlers.DailyStats(fake)).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet, "/api/v1/tenants/abc/daily-stats", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rr.Code)
	}
	if fake.got.tenantID != "abc" {
		t.Fatalf("tenantID: got %q, want %q", fake.got.tenantID, "abc")
	}
	if span := fake.got.to.Sub(fake.got.from); span != 30*24*time.Hour {
		t.Fatalf("default window: got %v, want 30d", span)
	}
}

// TestDailyStats_RangeCap rejects windows beyond 366 days so a
// misclick can't trigger a hot-table scan in ClickHouse.
func TestDailyStats_RangeCap(t *testing.T) {
	t.Parallel()
	fake := &fakeDailyStats{}
	rr := httptest.NewRecorder()
	mount(handlers.DailyStats(fake)).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet,
			"/api/v1/tenants/abc/daily-stats?from=2020-01-01&to=2026-01-01", nil))

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400 (range cap)", rr.Code)
	}
}

// TestDailyStats_InvalidDate returns 400 with a clear hint about
// the expected ISO date format.
func TestDailyStats_InvalidDate(t *testing.T) {
	t.Parallel()
	fake := &fakeDailyStats{}
	rr := httptest.NewRecorder()
	mount(handlers.DailyStats(fake)).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet,
			"/api/v1/tenants/abc/daily-stats?from=notadate", nil))

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "ISO date") {
		t.Fatalf("error message should hint at ISO date format, got %s", rr.Body.String())
	}
}

// TestDailyStats_SerializesRows: the JSON shape includes the
// tenant_id, from, to, and days array. A breaking change to the
// shape would surface as a chapter-doc drift, so pin it here.
func TestDailyStats_SerializesRows(t *testing.T) {
	t.Parallel()
	day := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	fake := &fakeDailyStats{
		rows: []chstore.DailyStat{
			{Date: day, EventType: "page_view", EventCount: 42, TotalValue: 100.5},
			{Date: day, EventType: "click", EventCount: 17, TotalValue: 0.0},
		},
	}
	rr := httptest.NewRecorder()
	mount(handlers.DailyStats(fake)).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet,
			"/api/v1/tenants/abc/daily-stats?from=2026-04-01&to=2026-04-30", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d", rr.Code)
	}
	var resp struct {
		TenantID string             `json:"tenant_id"`
		From     string             `json:"from"`
		To       string             `json:"to"`
		Days     []chstore.DailyStat `json:"days"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TenantID != "abc" || resp.From != "2026-04-01" || resp.To != "2026-04-30" {
		t.Fatalf("envelope mismatch: %+v", resp)
	}
	if len(resp.Days) != 2 || resp.Days[0].EventCount != 42 {
		t.Fatalf("rows mismatch: %+v", resp.Days)
	}
}
