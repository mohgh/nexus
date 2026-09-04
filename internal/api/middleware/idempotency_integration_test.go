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
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
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

// reservationState reads the stored row for a raw (un-namespaced)
// idempotency key and returns its state. The middleware prefixes the
// stored key with a per-principal namespace, so we match on
// containment rather than equality — freshKey() values are unique per
// run, so a substring match can only hit our own row.
//
// The read deliberately uses context.Background(): a test asserting
// that cleanup survives a dead request context must not itself be
// hostage to one.
func reservationState(t *testing.T, pool *pgxpool.Pool, key string) (string, bool) {
	t.Helper()
	var state string
	err := pool.QueryRow(context.Background(),
		`SELECT state FROM processed_idempotency_keys
		 WHERE strpos(idempotency_key, $1) > 0`,
		key,
	).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatalf("reservationState(%q): %v", key, err)
	}
	return state, true
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

// TestIdempotency_ReleasesReservationWhenRequestContextDies is the
// regression test for the reservation leak.
//
// The middleware is reserve-then-execute: it INSERTs an in_flight row
// before running the handler, and that row IS the mutual-exclusion
// primitive. Cleanup afterwards used to run on r.Context() — the same
// context that chi's middleware.Timeout(10s) cancels on a slow request
// and that net/http cancels when the client hangs up. So the cleanup
// DB call failed instantly in exactly the two situations that make a
// client retry, the row stayed in_flight, and every retry got
// 409 IDEMPOTENCY_REQUEST_IN_PROGRESS until the 5-minute sweeper ran.
//
// Both exit paths are covered, because both were broken:
//
//   - 2xx after the deadline: the handler's side effects committed, so
//     the response must still be CACHED (a retry replays it rather
//     than re-running the handler and duplicating the write).
//   - non-2xx after the deadline: the reservation must be RELEASED, so
//     a retry can genuinely re-run.
//
// In either case the one outcome that must never happen is a surviving
// in_flight row, and the client-visible assertion is the same: the
// retry is not 409.
//
// Against the pre-fix middleware both subtests fail on the in_flight
// assertion.
func TestIdempotency_ReleasesReservationWhenRequestContextDies(t *testing.T) {
	pool := openPool(t)

	cases := []struct {
		name             string
		status           int
		wantState        string // "" means the row must be gone entirely
		wantRetryReplays bool
	}{
		{
			name:             "2xx written after the request deadline is still cached",
			status:           http.StatusCreated,
			wantState:        "completed",
			wantRetryReplays: true,
		},
		{
			name:             "non-2xx after the request deadline releases the reservation",
			status:           http.StatusInternalServerError,
			wantState:        "",
			wantRetryReplays: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := &atomic.Int32{}
			mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
			handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.ReadAll(r.Body)
				// Outlive the request context, but only when the
				// caller actually set a deadline — the retry below
				// runs with a plain context and must not block.
				if _, ok := r.Context().Deadline(); ok {
					<-r.Context().Done()
					// Let the cancellation settle so the middleware's
					// cleanup definitely runs against a dead context.
					time.Sleep(50 * time.Millisecond)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))

			key := freshKey(t)
			body := []byte(`{"x":1}`)

			// A request whose context dies while the handler is still
			// running — the chi Timeout / client-disconnect case.
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			rr1 := httptest.NewRecorder()
			req1 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body)).WithContext(ctx)
			req1.Header.Set("Idempotency-Key", key)
			handler.ServeHTTP(rr1, req1)

			if got := calls.Load(); got != 1 {
				t.Fatalf("setup: handler ran %d times, want 1", got)
			}

			state, found := reservationState(t, pool, key)
			if found && state == "in_flight" {
				t.Fatalf("reservation leaked: row is still in_flight after the request ended.\n" +
					"Cleanup ran on the request context, which was already cancelled, so the " +
					"DB call failed instantly. Every retry now 409s until the sweeper runs.")
			}
			if tc.wantState == "" && found {
				t.Fatalf("reservation state = %q, want the row to be deleted", state)
			}
			if tc.wantState != "" && (!found || state != tc.wantState) {
				t.Fatalf("reservation state = %q (found=%v), want %q", state, found, tc.wantState)
			}

			// The client-visible half: the retry must not be refused.
			rr2 := httptest.NewRecorder()
			req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
			req2.Header.Set("Idempotency-Key", key)
			handler.ServeHTTP(rr2, req2)

			if rr2.Code == http.StatusConflict {
				t.Fatalf("retry got 409 %s — a request that timed out must not "+
					"poison its own idempotency key", rr2.Body.String())
			}

			replayed := rr2.Header().Get("X-Idempotent-Replay") == "true"
			if replayed != tc.wantRetryReplays {
				t.Fatalf("retry replayed=%v, want %v (status %d)", replayed, tc.wantRetryReplays, rr2.Code)
			}
			wantCalls := int32(2)
			if tc.wantRetryReplays {
				// Cached 2xx: the handler's side effects already
				// happened, so the retry must NOT run it again.
				wantCalls = 1
			}
			if got := calls.Load(); got != wantCalls {
				t.Fatalf("handler call count after retry: got %d, want %d", got, wantCalls)
			}
		})
	}
}

// TestIdempotency_RetryAfterIsAchievable pins the second half of the
// fix: the Retry-After advertised on a 409 in-progress has to be a
// number a client can actually obey. It used to say 5 seconds while a
// stuck reservation was only cleared by a 5-minute sweeper on a
// 1-minute tick — an obedient client burned ~60 pointless retries.
//
// The value must be at least the request timeout (a genuinely
// in-flight original cannot resolve sooner) and must not be so large
// that a normal concurrent retry is punished.
func TestIdempotency_RetryAfterIsAchievable(t *testing.T) {
	pool := openPool(t)
	calls := &atomic.Int32{}
	release := make(chan struct{})
	defer close(release)

	mw := middleware.Idempotency(middleware.IdempotencyConfig{Pool: pool})
	handler := mw(slowHandler(release, calls))

	key := freshKey(t)
	body := []byte(`{"x":1}`)

	rrA := httptest.NewRecorder()
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		handler.ServeHTTP(rrA, req)
	}()
	deadline := time.After(2 * time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatalf("goroutine A never entered the handler")
		case <-time.After(5 * time.Millisecond):
		}
	}

	rrB := httptest.NewRecorder()
	reqB := httptest.NewRequest(http.MethodPost, "/api/v1/events", bytes.NewReader(body))
	reqB.Header.Set("Idempotency-Key", key)
	handler.ServeHTTP(rrB, reqB)

	if rrB.Code != http.StatusConflict {
		t.Fatalf("B status: got %d, want 409", rrB.Code)
	}
	raw := rrB.Header().Get("Retry-After")
	secs, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("Retry-After = %q, want an integer number of seconds", raw)
	}
	// The original holds its reservation until the router's 10s
	// request timeout fires, so anything below that guarantees a
	// wasted retry.
	if secs < 10 {
		t.Fatalf("Retry-After = %ds, but an in-flight original can hold the "+
			"reservation for the full 10s request timeout — the client is being "+
			"told to retry before the answer can possibly exist", secs)
	}
	// And it must not have swung the other way into the sweeper's
	// worst case, which would stall every legitimate concurrent retry.
	if secs > 60 {
		t.Fatalf("Retry-After = %ds is punitively long for the common case "+
			"(original still running, resolves within the 10s request timeout)", secs)
	}
}
