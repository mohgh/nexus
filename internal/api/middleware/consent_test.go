package middleware_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/mohgh/nexus/internal/api/middleware"
)

// fakeChecker implements ConsentChecker with canned answers per
// (tenantID, purpose).
type fakeChecker struct {
	state map[string]int
	err   error
}

func (f *fakeChecker) ConsentState(_ context.Context, tenantID, purpose string) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.state[tenantID+":"+purpose], nil
}

func mountGate(checker middleware.ConsentChecker) http.Handler {
	r := chi.NewRouter()
	gated := middleware.ConsentGate(checker, "analytics", nil)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`ok`))
	}))
	r.Method(http.MethodGet, "/api/v1/tenants/{tenantID}/x", gated)
	return r
}

func TestConsentGate_AllowsOnNoRecord(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	mountGate(&fakeChecker{state: map[string]int{}}).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet, "/api/v1/tenants/abc/x", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("no-record state should pass through (lenient default), got %d", rr.Code)
	}
}

func TestConsentGate_AllowsOnGranted(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	mountGate(&fakeChecker{state: map[string]int{
		"abc:analytics": middleware.ConsentStateGranted,
	}}).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/tenants/abc/x", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("granted state should pass through, got %d", rr.Code)
	}
}

// TestConsentGate_DeniesOnExplicitRevoke is the load-bearing test:
// withdrawing consent must take effect at the next request.
func TestConsentGate_DeniesOnExplicitRevoke(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	mountGate(&fakeChecker{state: map[string]int{
		"abc:analytics": middleware.ConsentStateRevoked,
	}}).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/tenants/abc/x", nil))

	if rr.Code != http.StatusForbidden {
		t.Fatalf("explicit revoke must 403, got %d", rr.Code)
	}
	if got := rr.Header().Get("X-Consent-Required"); got != "analytics" {
		t.Fatalf("X-Consent-Required: got %q, want %q", got, "analytics")
	}
}

// TestConsentGate_DeniesOnlyForRequestedPurpose verifies the gate
// is purpose-specific: a tenant who's revoked marketing but granted
// analytics still sees an analytics-gated route succeed.
func TestConsentGate_DeniesOnlyForRequestedPurpose(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	mountGate(&fakeChecker{state: map[string]int{
		"abc:analytics": middleware.ConsentStateGranted,
		"abc:marketing": middleware.ConsentStateRevoked,
	}}).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/tenants/abc/x", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("revoking marketing should not affect analytics gate; got %d", rr.Code)
	}
}

// TestConsentGate_AllowsOnCheckerError pins down the soft-fail
// policy: a transient consent-store outage must NOT block requests.
// The chapter README documents this trade-off explicitly.
func TestConsentGate_AllowsOnCheckerError(t *testing.T) {
	t.Parallel()
	rr := httptest.NewRecorder()
	mountGate(&fakeChecker{err: errors.New("db down")}).ServeHTTP(rr,
		httptest.NewRequest(http.MethodGet, "/api/v1/tenants/abc/x", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("checker error should pass through; got %d", rr.Code)
	}
}
