package handlers_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mohgh/nexus/internal/api/handlers"
	"github.com/mohgh/nexus/internal/chaos"
)

// TestChaosState_AndSet drives the GET → POST → GET round trip
// students will use to confirm a toggle change took effect. Uses
// a real *chaos.Profile because the type is small and easier to
// instantiate than a fake.
func TestChaosState_AndSet(t *testing.T) {
	t.Parallel()
	p := chaos.New()

	// GET returns the zero profile.
	rr := httptest.NewRecorder()
	handlers.ChaosState(p).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet, "/api/v1/chaos", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status: got %d", rr.Code)
	}
	var snap chaos.Snapshot
	_ = json.Unmarshal(rr.Body.Bytes(), &snap)
	if snap.DBDelayMS != 0 || snap.ErrorRate != 0 || snap.DropPublish {
		t.Fatalf("initial snapshot should be all zero: %+v", snap)
	}

	// POST sets two of three toggles; the third stays at zero.
	rr = httptest.NewRecorder()
	handlers.ChaosSet(p).ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/api/v1/chaos",
		bytes.NewReader([]byte(`{"db_delay_ms":4000,"error_rate":30}`)),
	))
	if rr.Code != http.StatusOK {
		t.Fatalf("POST status: got %d", rr.Code)
	}
	_ = json.Unmarshal(rr.Body.Bytes(), &snap)
	if snap.DBDelayMS != 4000 || snap.ErrorRate != 30 || snap.DropPublish {
		t.Fatalf("partial set should preserve omitted field: %+v", snap)
	}

	// Reset wipes everything.
	rr = httptest.NewRecorder()
	handlers.ChaosReset(p).ServeHTTP(rr,
		httptest.NewRequest(http.MethodPost, "/api/v1/chaos/reset", nil))
	_ = json.Unmarshal(rr.Body.Bytes(), &snap)
	if snap.DBDelayMS != 0 || snap.ErrorRate != 0 {
		t.Fatalf("reset should clear everything: %+v", snap)
	}
}

// TestChaosSet_ClampsOutOfRange: db_delay_ms > 60s and error_rate
// out of [0,100] are clamped, not rejected. A misclick shouldn't
// 400 — chaos demos run interactively.
func TestChaosSet_ClampsOutOfRange(t *testing.T) {
	t.Parallel()
	p := chaos.New()

	rr := httptest.NewRecorder()
	handlers.ChaosSet(p).ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/api/v1/chaos",
		bytes.NewReader([]byte(`{"db_delay_ms":9999999,"error_rate":500}`)),
	))
	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200 (clamping should not 400)", rr.Code)
	}

	var snap chaos.Snapshot
	_ = json.Unmarshal(rr.Body.Bytes(), &snap)
	if snap.DBDelayMS != 60_000 {
		t.Fatalf("db_delay_ms should be clamped to 60_000, got %d", snap.DBDelayMS)
	}
	if snap.ErrorRate != 100 {
		t.Fatalf("error_rate should be clamped to 100, got %d", snap.ErrorRate)
	}
}
