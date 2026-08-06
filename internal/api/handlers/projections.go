package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/mohgh/nexus/internal/projections"
)

// ProjectionLagInspector reports the lag of every projection
// against the event store head. Implemented by *projections.Runner.
type ProjectionLagInspector interface {
	LagFor(ctx context.Context) ([]projections.Lag, error)
}

// ProjectionsUnavailable returns a 503 with an explanatory body.
// The Ch07 sharded mode uses this to be honest about the fact
// that the central projection runner is not shard-aware — without
// it, /api/v1/projections would return a misleading "everything's
// fine, lag is 0" response in sharded mode because the central
// events_store is empty.
//
// The handler is wired by the Server when ShardDSNTemplate is set
// (see WithProjectionsUnavailable). The body includes the operator
// hint so a curl + jq tells the operator what's going on without
// needing to grep through the chapter README.
func ProjectionsUnavailable(reason string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		body := fmt.Sprintf(
			`{"error":"projection runner is not available in this mode","code":"PROJECTIONS_UNAVAILABLE","reason":%q}`,
			reason,
		)
		_, _ = w.Write([]byte(body))
	}
}

// ProjectionLag exposes the catch-up state of every projection so
// an operator can answer "are my read models keeping up?"
//
//	GET /api/v1/projections
//
// Response shape:
//
//	{"head_position": 1234,
//	 "projections": [
//	    {"projection":"tenant_event_counts","last_position":1234,"head_position":1234,"lag":0},
//	    {"projection":"daily_event_counts","last_position":1230,"head_position":1234,"lag":4}
//	 ]}
//
// The head is reported once at the response level for convenience and
// also on each row (so a client that filters can still see the head
// without a separate field).
func ProjectionLag(insp ProjectionLagInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lags, err := insp.LagFor(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to read projection lag")
			return
		}
		var head int64
		if len(lags) > 0 {
			head = lags[0].HeadPosition
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"head_position": head,
			"projections":   lags,
		})
	}
}
