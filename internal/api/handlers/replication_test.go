package handlers_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/mohgh/nexus/internal/api/handlers"
	pgstore "github.com/mohgh/nexus/internal/storage/postgres"
)

// TestRYOWMiddleware_StampsWhenThisRequestAdvancedLSN simulates a handler
// that wrote through ReplicaPool.Exec (which would have called
// rec.Record on the per-request recorder). The middleware should stamp
// the response with that exact LSN.
func TestRYOWMiddleware_StampsWhenThisRequestAdvancedLSN(t *testing.T) {
	t.Parallel()

	mw := handlers.RYOWMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := pgstore.WriteLSNRecorderFromContext(r.Context())
		if rec == nil {
			t.Fatal("expected per-request recorder in context")
		}
		rec.Record(42)
		w.WriteHeader(http.StatusCreated)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get("X-Nexus-Write-LSN"); got != "42" {
		t.Fatalf("X-Nexus-Write-LSN: got %q, want %q", got, "42")
	}
}

// TestRYOWMiddleware_DoesNotStampWhenRequestDidNotAdvanceLSN is the
// regression test for the original review finding: a successful POST
// whose handler does NOT write through ReplicaPool.Exec must NOT be
// stamped, even if some other concurrent request advanced the pool's
// global watermark. The per-request recorder is the source of truth.
func TestRYOWMiddleware_DoesNotStampWhenRequestDidNotAdvanceLSN(t *testing.T) {
	t.Parallel()

	mw := handlers.RYOWMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handler is a "kicks off async work" type endpoint: returns
		// 202 successfully but never writes through ReplicaPool.Exec.
		// The recorder stays at 0.
		_ = pgstore.WriteLSNRecorderFromContext(r.Context())
		w.WriteHeader(http.StatusAccepted)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/billing/charge", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get("X-Nexus-Write-LSN"); got != "" {
		t.Fatalf("non-writing handler should not be stamped, got %q", got)
	}
}

func TestRYOWMiddleware_NoHeaderOnGET(t *testing.T) {
	t.Parallel()

	mw := handlers.RYOWMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Even if a GET handler somehow recorded an LSN, GET responses
		// don't get stamped — the protocol echoes only on writes.
		if rec := pgstore.WriteLSNRecorderFromContext(r.Context()); rec != nil {
			rec.Record(99)
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get("X-Nexus-Write-LSN"); got != "" {
		t.Fatalf("GET should not be stamped, got %q", got)
	}
}

func TestRYOWMiddleware_NoHeaderOnFailure(t *testing.T) {
	t.Parallel()

	mw := handlers.RYOWMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Simulate: the handler successfully wrote, but later failed and
		// returned 500. We don't stamp on non-2xx because the client
		// shouldn't treat this as a successful state advance.
		if rec := pgstore.WriteLSNRecorderFromContext(r.Context()); rec != nil {
			rec.Record(42)
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if got := rr.Header().Get("X-Nexus-Write-LSN"); got != "" {
		t.Fatalf("failed POST should not be stamped, got %q", got)
	}
}

func TestRYOWMiddleware_InjectsMinLSNFromHeader(t *testing.T) {
	t.Parallel()

	var seen uint64
	mw := handlers.RYOWMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = pgstore.MinLSNFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	req.Header.Set("X-Nexus-Min-LSN", strconv.FormatUint(99887766, 10))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if seen != 99887766 {
		t.Fatalf("min LSN injected: got %d, want 99887766", seen)
	}
}

func TestRYOWMiddleware_IgnoresGarbageMinLSN(t *testing.T) {
	t.Parallel()

	var seen uint64
	mw := handlers.RYOWMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = pgstore.MinLSNFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
	req.Header.Set("X-Nexus-Min-LSN", "not-a-number")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if seen != 0 {
		t.Fatalf("garbage min LSN should be ignored, got %d", seen)
	}
}

// TestRYOWMiddleware_AttachesFreshRecorderPerRequest verifies that the
// recorder is per-request (no leakage between concurrent calls).
func TestRYOWMiddleware_AttachesFreshRecorderPerRequest(t *testing.T) {
	t.Parallel()

	mw := handlers.RYOWMiddleware()
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := pgstore.WriteLSNRecorderFromContext(r.Context())
		if rec == nil || rec.Load() != 0 {
			t.Fatalf("each request must get a fresh recorder, got %v", rec)
		}
		rec.Record(123)
		w.WriteHeader(http.StatusCreated)
	}))

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if got := rr.Header().Get("X-Nexus-Write-LSN"); got != "123" {
			t.Fatalf("iteration %d: got %q, want 123", i, got)
		}
	}
}
