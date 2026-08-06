//go:build integration

// Live-Postgres integration tests for the idempotency middleware.
// The unit tests in idempotency_test.go cover the pure fingerprint
// helper; these cover the middleware end-to-end including the
// reviewer's finding that key reuse across different requests
// silently replays the wrong cached response.
//
// Run via:
//
//	POSTGRES_DSN=postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable \
//	    go test -tags=integration -v ./internal/api/middleware/...

package middleware_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/api/middleware"
	"github.com/mohgh/nexus/internal/auth"
)

func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// freshKey returns a key that is unique across test runs so we
// don't collide with rows that the cleanup goroutine hasn't yet
// removed.
func freshKey(t *testing.T) string {
	t.Helper()
	return "test-" + t.Name() + "-" + time.Now().Format("150405.000000000")
}

// echoHandler responds with status from a header (or 200) and
// echoes the request body. Useful for asserting cache contents.
func echoHandler(callCount *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount.Add(1)
		body, _ := io.ReadAll(r.Body)
		status := http.StatusCreated
		if v := r.Header.Get("X-Test-Status"); v == "500" {
			status = http.StatusInternalServerError
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	})
}

// TestIdempotency_CachesIdenticalRequest is the happy path: same
// key + same body + same path = cached replay on second call.
func TestIdempotency_CachesIdenticalRequest(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(echoHandler(calls))

	key := freshKey(t)
	body := []byte(`{"tenant_id":"t1","value":42}`)

	// First call: handler runs, response cached.
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req1.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rr1, req1)

	if rr1.Code != http.StatusCreated {
		t.Fatalf("first call: status %d, want 201", rr1.Code)
	}
	if rr1.Header().Get("X-Idempotent-Replay") != "" {
		t.Fatalf("first call must not be a replay")
	}

	// Second call: same key + same body = replay.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req2.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusCreated {
		t.Fatalf("second call: status %d, want 201 (replayed)", rr2.Code)
	}
	if rr2.Header().Get("X-Idempotent-Replay") != "true" {
		t.Fatalf("second call must carry X-Idempotent-Replay: true")
	}
	if !bytes.Equal(rr1.Body.Bytes(), rr2.Body.Bytes()) {
		t.Fatalf("replayed body differs:\n  first=%s\n  second=%s",
			rr1.Body.String(), rr2.Body.String())
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler call count: got %d, want 1 (second call must not run handler)", got)
	}
}

// TestIdempotency_RejectsKeyReuseWithDifferentBody is the
// regression test for the audit's high-severity finding. Reusing
// the same key with a DIFFERENT request body must NOT replay the
// first response — it must return 409.
func TestIdempotency_RejectsKeyReuseWithDifferentBody(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(echoHandler(calls))

	key := freshKey(t)

	// First request — cache the 201.
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events",
		bytes.NewReader([]byte(`{"tenant_id":"t1","value":1}`)))
	req1.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusCreated {
		t.Fatalf("setup: first call status %d", rr1.Code)
	}

	// Second request — SAME KEY, DIFFERENT BODY.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events",
		bytes.NewReader([]byte(`{"tenant_id":"t2","value":999}`)))
	req2.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusConflict {
		t.Fatalf("second call (different body) must be 409, got %d.\n"+
			"This is the audit's high-severity regression case — without "+
			"fingerprint scoping, the second request would replay the FIRST "+
			"request's body verbatim.",
			rr2.Code)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("handler should NOT run for the conflicting retry, got %d calls", got)
	}
}

// TestIdempotency_RejectsKeyReuseAcrossPaths verifies that the
// fingerprint includes the URL path, so reusing a key across
// endpoints (e.g. POST /api/v1/events vs POST /api/v1/billing/charge)
// produces a 409, not a cross-route replay.
func TestIdempotency_RejectsKeyReuseAcrossPaths(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(echoHandler(calls))

	key := freshKey(t)
	body := []byte(`{"x":1}`)

	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req1.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rr1, req1)

	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/billing/charge", bytes.NewReader(body))
	req2.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rr2, req2)

	if rr2.Code != http.StatusConflict {
		t.Fatalf("cross-path reuse must be 409, got %d", rr2.Code)
	}
}

// TestIdempotency_DoesNotCacheNon2xx verifies that a 5xx response
// is not cached — a transient server error should leave the
// caller free to retry with the same key.
func TestIdempotency_DoesNotCacheNon2xx(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(echoHandler(calls))

	key := freshKey(t)
	body := []byte(`{"x":1}`)

	// First call returns 500 — not cached.
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req1.Header.Set("Idempotency-Key", key)
	req1.Header.Set("X-Test-Status", "500")
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusInternalServerError {
		t.Fatalf("setup: status %d, want 500", rr1.Code)
	}

	// Second call with same key — handler should run again
	// because the 500 was not cached. (Same body so no 409.)
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req2.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rr2, req2)

	if got := calls.Load(); got != 2 {
		t.Fatalf("handler call count: got %d, want 2 (5xx must not cache)", got)
	}
}

// TestIdempotency_NoKeyMeansNoCaching keeps the opt-in contract:
// requests without the header skip the middleware entirely.
func TestIdempotency_NoKeyMeansNoCaching(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(echoHandler(calls))

	body := []byte(`{"x":1}`)
	for i := 0; i < 3; i++ {
		rr := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
		// No Idempotency-Key header.
		handler.ServeHTTP(rr, req)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("handler call count: got %d, want 3 (no key => no caching)", got)
	}
}

// TestIdempotency_IsolatesTenants is the regression test for the
// cross-tenant collision introduced when events stopped carrying tenant_id
// in the body. Two tenants reuse the SAME Idempotency-Key with the SAME
// body; because the middleware namespaces the stored key by the
// authenticated principal, the second tenant's request must run the handler
// (not replay the first tenant's cached response), and a same-tenant retry
// must still replay.
func TestIdempotency_IsolatesTenants(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(echoHandler(calls))

	key := freshKey(t)
	body := []byte(`{"events":[{"event_type":"x"}]}`)

	withTenant := func(id string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", bytes.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		ctx := auth.ContextWithPrincipal(req.Context(),
			auth.Principal{TenantID: id, Scope: auth.ScopeTenant})
		return req.WithContext(ctx)
	}

	// Tenant 1 — handler runs, response cached under t:tenant-1.
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, withTenant("tenant-1"))
	if rr1.Code != http.StatusCreated || rr1.Header().Get("X-Idempotent-Replay") != "" {
		t.Fatalf("tenant-1 first call: code=%d replay=%q", rr1.Code, rr1.Header().Get("X-Idempotent-Replay"))
	}

	// Tenant 2 — SAME key + body, different tenant. Must NOT replay; the
	// handler must run for tenant-2's own ingest.
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, withTenant("tenant-2"))
	if rr2.Code != http.StatusCreated {
		t.Fatalf("tenant-2 call: code=%d, want 201", rr2.Code)
	}
	if rr2.Header().Get("X-Idempotent-Replay") == "true" {
		t.Fatal("tenant-2 must NOT replay tenant-1's cached response — cross-tenant leak")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("handler call count: got %d, want 2 (both tenants must run)", got)
	}

	// Tenant 1 again — now it replays its own cached response.
	rr3 := httptest.NewRecorder()
	handler.ServeHTTP(rr3, withTenant("tenant-1"))
	if rr3.Header().Get("X-Idempotent-Replay") != "true" {
		t.Fatalf("same-tenant retry must replay; replay=%q", rr3.Header().Get("X-Idempotent-Replay"))
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("same-tenant retry must not run handler; calls=%d, want 2", got)
	}
}

// slowHandler waits on a release channel before responding, so a
// test can observe the in_flight state from a concurrent request.
func slowHandler(release <-chan struct{}, calls *atomic.Int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body)
	})
}

// TestIdempotency_ConcurrentRetryReturns409InProgress is the
// regression test for the audit's finding. The earlier middleware
// allowed two concurrent same-key retries to BOTH miss the cache,
// BOTH run the handler, and only collide at the final INSERT —
// meaning side-effecting handlers wrote twice. The reserve-then-
// execute fix means the second retry sees an in_flight reservation
// and gets 409 IDEMPOTENCY_REQUEST_IN_PROGRESS.
//
// We slow the handler with a release channel so we can deterministically
// observe the in_flight state. Both requests carry the same key and
// body; we assert exactly one handler invocation, exactly one 201,
// and exactly one 409.
func TestIdempotency_ConcurrentRetryReturns409InProgress(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	release := make(chan struct{})
	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(slowHandler(release, calls))

	key := freshKey(t)
	body := []byte(`{"x":1}`)

	// Goroutine A: starts first, holds the in_flight reservation
	// while waiting on `release`.
	rrA := httptest.NewRecorder()
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		handler.ServeHTTP(rrA, req)
	}()

	// Wait until A has actually reached the handler — i.e. the
	// reservation is committed and the in_flight row is visible.
	deadline := time.After(2 * time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatalf("goroutine A never entered the handler (calls=%d)", calls.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Goroutine B: same key, same body, fired while A is in flight.
	rrB := httptest.NewRecorder()
	reqB := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	reqB.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rrB, reqB)

	if rrB.Code != http.StatusConflict {
		t.Fatalf("B status: got %d, want 409 (in-flight reservation)", rrB.Code)
	}
	if got := rrB.Header().Get("Retry-After"); got == "" {
		t.Fatalf("B should carry Retry-After header on in-flight conflict")
	}
	if calls.Load() != 1 {
		t.Fatalf("handler should have run exactly once across A+B; got %d.\n"+
			"This is the audit's regression case: without reserve-then-execute, "+
			"both retries would run the handler concurrently.",
			calls.Load())
	}

	// Let A finish, confirm it succeeds with 201.
	close(release)
	select {
	case <-doneA:
	case <-time.After(2 * time.Second):
		t.Fatalf("goroutine A did not finish")
	}
	if rrA.Code != http.StatusCreated {
		t.Fatalf("A status: got %d, want 201", rrA.Code)
	}

	// And: a NEW retry (after A completed) should now get the cached
	// replay, not 409 — the row is no longer in_flight.
	rrC := httptest.NewRecorder()
	reqC := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	reqC.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rrC, reqC)
	if rrC.Code != http.StatusCreated {
		t.Fatalf("post-completion retry status: got %d, want 201 (replay)", rrC.Code)
	}
	if rrC.Header().Get("X-Idempotent-Replay") != "true" {
		t.Fatalf("post-completion retry must be marked as replay")
	}
	if calls.Load() != 1 {
		t.Fatalf("post-completion retry must NOT run the handler again; got %d total calls", calls.Load())
	}
}

// TestIdempotency_NonSuccessDeletesReservation verifies that a 5xx
// response drops the in_flight row, so a retry isn't 409'd by a
// dead reservation.
func TestIdempotency_NonSuccessDeletesReservation(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(echoHandler(calls))

	key := freshKey(t)
	body := []byte(`{"x":1}`)

	// First call returns 500.
	rr1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req1.Header.Set("Idempotency-Key", key)
	req1.Header.Set("X-Test-Status", "500")
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusInternalServerError {
		t.Fatalf("setup: status %d, want 500", rr1.Code)
	}

	// Immediate retry with the same key — must NOT see an in_flight
	// reservation; the prior 500 should have deleted it.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	req2.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rr2, req2)

	if rr2.Code == http.StatusConflict {
		t.Fatalf("retry after 5xx must not 409: a failed request must release its reservation. got %d", rr2.Code)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler call count: got %d, want 2", calls.Load())
	}
}
