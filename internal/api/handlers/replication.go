package handlers

import (
	"context"
	"net/http"
	"strconv"

	pgstore "github.com/mohgh/nexus/internal/storage/postgres"
)

// LagReader can report replication lag in bytes.
type LagReader interface {
	ReplicationLag(ctx context.Context) (uint64, error)
}

// RoutingInspector exposes the inputs to the replica routing decision.
// Implemented by *postgres.ReplicaPool.
type RoutingInspector interface {
	RoutingStatus(ctx context.Context) (pgstore.RoutingStatus, error)
}

// ReplicationLag reports how far the replica is behind the primary.
// GET /api/v1/replication-lag
//
// Ch06: use this endpoint while running load tests to observe how lag grows
// under write pressure and recovers when writes slow down.
// A lag of 0 means the replica is fully caught up (or no replica is configured).
func ReplicationLag(reader LagReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lag, err := reader.ReplicationLag(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to measure replication lag")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"lag_bytes": lag,
			"status":    lagStatus(lag),
		})
	}
}

// ReplicationStatus exposes the read-routing decision for the current
// state of the pool. Hit this between writes and reads to see exactly
// which side a read would land on and why.
// GET /api/v1/replication-status
//
// Ch06: this is what makes the RYOW machinery inspectable. The fields
// returned are the same values the pool uses internally to decide
// between primary and replica.
func ReplicationStatus(insp RoutingInspector) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st, err := insp.RoutingStatus(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to read routing status")
			return
		}
		writeJSON(w, http.StatusOK, st)
	}
}

func lagStatus(lagBytes uint64) string {
	switch {
	case lagBytes == 0:
		return "caught_up"
	case lagBytes < 1<<20: // < 1 MiB
		return "healthy"
	case lagBytes < 10<<20: // < 10 MiB
		return "lagging"
	default:
		return "critical"
	}
}

// RYOWMiddleware threads read-your-own-writes guarantees through HTTP:
//
//   - Inbound: if the request carries X-Nexus-Min-LSN, attach it to the
//     context so subsequent reads route to the replica only if it has
//     replayed at least that far.
//
//   - Per-request write tracker: a fresh WriteLSNRecorder is attached to
//     the context. ReplicaPool.Exec publishes the LSN of every successful
//     write to it, so the recorder reflects writes performed BY THIS
//     REQUEST — not unrelated concurrent writes that bumped the pool's
//     global watermark. This is the fix for the obvious "stamp any
//     successful POST with whatever LSN happens to be lying around"
//     bug where async/no-write handlers (e.g. Temporal workflow start)
//     would falsely advertise durability through some other request's LSN.
//
//   - Outbound: for successful mutating requests, stamp X-Nexus-Write-LSN
//     with the recorder's value if and only if the recorder advanced
//     during the request. If the handler didn't write through
//     ReplicaPool.Exec, we don't stamp.
//
// This is the application-level RYOW protocol: write -> capture LSN ->
// echo on response -> client sends on next read -> server gates routing.
// Same shape used by CockroachDB's follower-read tokens and PlanetScale's
// read-after-write tokens.
func RYOWMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			if h := r.Header.Get("X-Nexus-Min-LSN"); h != "" {
				if lsn, err := strconv.ParseUint(h, 10, 64); err == nil && lsn > 0 {
					ctx = pgstore.WithMinLSN(ctx, lsn)
				}
			}
			ctx, writeRec := pgstore.WithWriteLSNRecorder(ctx)

			ww := &lsnResponseWriter{ResponseWriter: w, rec: writeRec, method: r.Method}
			next.ServeHTTP(ww, r.WithContext(ctx))
		})
	}
}

// lsnResponseWriter stamps X-Nexus-Write-LSN on the response right before
// the first WriteHeader call, so the header is set whether the handler
// writes a body, calls WriteHeader explicitly, or returns 200 implicitly.
type lsnResponseWriter struct {
	http.ResponseWriter
	rec     *pgstore.WriteLSNRecorder
	method  string
	stamped bool
}

func (w *lsnResponseWriter) WriteHeader(status int) {
	w.stamp(status)
	w.ResponseWriter.WriteHeader(status)
}

func (w *lsnResponseWriter) Write(b []byte) (int, error) {
	w.stamp(http.StatusOK)
	return w.ResponseWriter.Write(b)
}

// Flush delegates to the underlying writer so the Ch12 SSE handler
// can do live streaming. Without this delegation, the http.Flusher
// type assertion in the SSE handler fails because Go interface
// satisfaction is by concrete type — embedding ResponseWriter is
// not enough to surface Flush() on this wrapper. RYOWMiddleware
// runs on EVERY route (it's mounted on the root chi router), so a
// missing Flush() here breaks every streaming endpoint, not just
// the one we noticed.
func (w *lsnResponseWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

func (w *lsnResponseWriter) stamp(status int) {
	if w.stamped {
		return
	}
	w.stamped = true
	if !isMutating(w.method) || status >= 300 {
		return
	}
	if lsn := w.rec.Load(); lsn > 0 {
		w.Header().Set("X-Nexus-Write-LSN", strconv.FormatUint(lsn, 10))
	}
}

func isMutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}
