package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/mohgh/nexus/internal/api/handlers"
)

// TestLiveStats_RequiresQueryParam: without ?event_type, the handler
// returns 400 immediately. SSE clients should never see an open
// stream when their request is malformed.
//
// We don't unit-test the steady-state SSE emission here: doing so
// would require wrapping redis.Client behind an interface just for
// the test, which the handler doesn't otherwise need. The format
// (text/event-stream, `data: {json}\n\n` frames, JSON shape) is
// covered by manual smoke via `make ch12` + `curl -N`.
func TestLiveStats_RequiresQueryParam(t *testing.T) {
	t.Parallel()

	r := chi.NewRouter()
	// nil redis client is fine — the validation runs before any Redis call.
	r.Get("/api/v1/tenants/{tenantID}/live-stats", handlers.LiveStats(nil, handlers.LiveStatsConfig{}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/abc/live-stats", nil)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400 (missing event_type)", rr.Code)
	}
}
