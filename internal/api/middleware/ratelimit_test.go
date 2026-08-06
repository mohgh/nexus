package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/auth"
	"github.com/mohgh/nexus/internal/ratelimit"
)

// fakeLimiter returns a fixed result and records whether it was consulted
// and with which key/limit.
type fakeLimiter struct {
	res      ratelimit.Result
	called   bool
	gotKey   string
	gotLimit ratelimit.Limit
}

func (f *fakeLimiter) Allow(key string, lim ratelimit.Limit) ratelimit.Result {
	f.called = true
	f.gotKey = key
	f.gotLimit = lim
	return f.res
}

func withPrincipal(req *http.Request, p auth.Principal) *http.Request {
	return req.WithContext(auth.ContextWithPrincipal(req.Context(), p))
}

func TestRateLimit_AllowedSetsHeaders(t *testing.T) {
	t.Parallel()
	fl := &fakeLimiter{res: ratelimit.Result{Allowed: true, Remaining: 7}}
	h := RateLimit(fl)(okHandler())

	req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/events", nil),
		auth.Principal{TenantID: "tenant-1", Scope: auth.ScopeTenant, Plan: "pro"})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("allowed request: code = %d, want 200", rr.Code)
	}
	if got := rr.Header().Get("X-RateLimit-Remaining"); got != "7" {
		t.Fatalf("X-RateLimit-Remaining = %q, want 7", got)
	}
	// pro plan => burst 200 exposed as the limit.
	if got := rr.Header().Get("X-RateLimit-Limit"); got != "200" {
		t.Fatalf("X-RateLimit-Limit = %q, want 200 (pro burst)", got)
	}
	if fl.gotKey != "t:tenant-1" {
		t.Fatalf("limiter key = %q, want t:tenant-1", fl.gotKey)
	}
}

func TestRateLimit_DeniedReturns429(t *testing.T) {
	t.Parallel()
	fl := &fakeLimiter{res: ratelimit.Result{Allowed: false, RetryAfter: 1500 * time.Millisecond, Remaining: 0}}
	h := RateLimit(fl)(okHandler())

	req := withPrincipal(httptest.NewRequest(http.MethodPost, "/api/v1/events", nil),
		auth.Principal{TenantID: "tenant-1", Scope: auth.ScopeTenant, Plan: "free"})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("denied request: code = %d, want 429", rr.Code)
	}
	// 1.5s rounds up to a 2s Retry-After.
	if got := rr.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("Retry-After = %q, want 2", got)
	}
}

func TestRateLimit_AdminKeyUsesAdminLimit(t *testing.T) {
	t.Parallel()
	fl := &fakeLimiter{res: ratelimit.Result{Allowed: true, Remaining: 1}}
	h := RateLimit(fl)(okHandler())

	req := withPrincipal(httptest.NewRequest(http.MethodGet, "/api/v1/leader", nil),
		auth.Principal{KeyID: "admin-1", Scope: auth.ScopeAdmin})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if fl.gotKey != "k:admin-1" {
		t.Fatalf("admin key bucket = %q, want k:admin-1", fl.gotKey)
	}
	if fl.gotLimit != ratelimit.AdminLimit() {
		t.Fatalf("admin should use AdminLimit, got %+v", fl.gotLimit)
	}
}

func TestRateLimit_NoPrincipalPassesThrough(t *testing.T) {
	t.Parallel()
	fl := &fakeLimiter{res: ratelimit.Result{Allowed: false}} // would deny if consulted
	h := RateLimit(fl)(okHandler())

	req := httptest.NewRequest(http.MethodGet, "/health", nil) // no principal in context
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("unauthenticated request must pass through, got %d", rr.Code)
	}
	if fl.called {
		t.Fatal("limiter must not be consulted without a principal")
	}
}
