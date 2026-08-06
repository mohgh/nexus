package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/mohgh/nexus/internal/chaos"
)

// BreakerInspector can report all circuit breaker states.
// Returns map[string]any so handlers don't import the resilience package.
type BreakerInspector interface {
	States() map[string]any
}

// CircuitBreakerStatus reports the state of all circuit breakers.
// GET /api/v1/circuit-breakers
//
// Ch09: use this endpoint while load testing to watch circuit breakers
// transition: closed → open → half-open → closed.
// Also accessible via the Grafana dashboard when Prometheus is running.
func CircuitBreakerStatus(inspector BreakerInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"circuit_breakers": inspector.States(),
		})
	}
}

// ChaosProfile is the mutable knob the GET/POST endpoints below
// expose. Defined here (rather than reaching directly into
// *chaos.Profile) so a test can supply a fake.
type ChaosProfile interface {
	Snapshot() chaos.Snapshot
	SetDBDelay(ms int64)
	SetErrorRate(pct int64)
	SetDropPublish(b bool)
	Reset()
}

// ChaosState reports the current fault-injection profile.
// GET /api/v1/chaos
//
// Ch09: hit this before drawing conclusions from a load test — it's
// the only way to confirm the toggles you set via POST are actually
// in effect.
func ChaosState(p ChaosProfile) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, p.Snapshot())
	}
}

// ChaosSet updates one or more toggles. POST /api/v1/chaos
//
// Body shape (every field optional):
//
//	{"db_delay_ms": 4000, "error_rate": 30, "drop_publish": false}
//
// Omitted fields keep their current value. Reset every toggle at
// once via POST /api/v1/chaos/reset.
//
// Mounted under the public /api/v1 surface rather than an
// /internal/ prefix because the course has no internal network
// boundary. In production this knob belongs behind an admin auth
// check — the chapter discusses but does not enforce that.
func ChaosSet(p ChaosProfile) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			DBDelayMS   *int64 `json:"db_delay_ms,omitempty"`
			ErrorRate   *int64 `json:"error_rate,omitempty"`
			DropPublish *bool  `json:"drop_publish,omitempty"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		if req.DBDelayMS != nil {
			p.SetDBDelay(*req.DBDelayMS)
		}
		if req.ErrorRate != nil {
			p.SetErrorRate(*req.ErrorRate)
		}
		if req.DropPublish != nil {
			p.SetDropPublish(*req.DropPublish)
		}
		writeJSON(w, http.StatusOK, p.Snapshot())
	}
}

// ChaosReset clears every toggle. POST /api/v1/chaos/reset
func ChaosReset(p ChaosProfile) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p.Reset()
		writeJSON(w, http.StatusOK, p.Snapshot())
	}
}
