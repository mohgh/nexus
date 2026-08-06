package handlers

import (
	"context"
	"net/http"
	"time"
)

type healthResponse struct {
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	Version   string    `json:"version"`
	Chapter   string    `json:"chapter"`
}

// Health returns basic liveness information.
// A failing health check means the process itself is broken.
// Kubernetes restarts the pod on liveness failure.
func Health() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, healthResponse{
			Status:    "ok",
			Timestamp: time.Now().UTC(),
			Version:   "0.4.0",
			Chapter:   "04 — Storage and Retrieval",
		})
	}
}

// Pinger is implemented by any dependency that can be health-checked.
// Ch04: postgres pool, redis cache, and clickhouse client all implement this.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Ready is the Kubernetes readiness probe.
// Returns 200 only when all registered dependencies are reachable.
// Returns 503 with a JSON body listing which checks failed.
//
// Ch04: postgres and redis are checked. ClickHouse is optional — the server
// continues serving OLTP traffic if the OLAP layer is down.
func Ready(deps map[string]Pinger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		failed := map[string]string{}
		for name, dep := range deps {
			if err := dep.Ping(ctx); err != nil {
				failed[name] = err.Error()
			}
		}

		if len(failed) > 0 {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"status": "degraded",
				"failed": failed,
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	}
}
