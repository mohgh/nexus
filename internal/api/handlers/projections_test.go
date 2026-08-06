package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mohgh/nexus/internal/api/handlers"
	"github.com/mohgh/nexus/internal/projections"
)

type fakeRunner struct {
	lags []projections.Lag
}

func (f *fakeRunner) LagFor(context.Context) ([]projections.Lag, error) {
	return f.lags, nil
}

// TestProjectionLag_ReportsRunnerState pins down the happy-path
// shape so a future change to the JSON envelope is a deliberate
// breakage.
func TestProjectionLag_ReportsRunnerState(t *testing.T) {
	t.Parallel()
	r := &fakeRunner{lags: []projections.Lag{
		{ProjectionName: "tenant_event_counts", LastPosition: 100, HeadPosition: 100, Lag: 0},
	}}
	rr := httptest.NewRecorder()
	handlers.ProjectionLag(r).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet, "/api/v1/projections", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rr.Code)
	}
	var resp struct {
		HeadPosition int64             `json:"head_position"`
		Projections  []projections.Lag `json:"projections"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.HeadPosition != 100 || len(resp.Projections) != 1 {
		t.Fatalf("envelope mismatch: %+v", resp)
	}
}

// TestProjectionsUnavailable_503 is the regression test for the
// audit's "sharded mode silently breaks the live projection path"
// finding. In sharded mode the central runner reads an empty
// events_store and would report "lag=0", misleading the operator.
// The sentinel handler returns 503 with the reason so a curl +
// jq tells the operator what's actually happening.
func TestProjectionsUnavailable_503(t *testing.T) {
	t.Parallel()
	const reason = "projection runner is not shard-aware (Ch07 v2 follow-up)"
	rr := httptest.NewRecorder()
	handlers.ProjectionsUnavailable(reason).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet, "/api/v1/projections", nil))

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503.\n"+
			"This is the audit's regression case — sharded mode must NOT serve "+
			"the central runner's misleading lag=0 response.",
			rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "PROJECTIONS_UNAVAILABLE") {
		t.Fatalf("response should carry the machine-readable code, got %s", body)
	}
	if !strings.Contains(body, reason) {
		t.Fatalf("response should echo the human-readable reason, got %s", body)
	}
}
