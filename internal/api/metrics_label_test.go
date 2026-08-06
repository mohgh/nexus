package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mohgh/nexus/internal/api"
	"github.com/mohgh/nexus/internal/config"
	"github.com/mohgh/nexus/internal/domain"
	"go.uber.org/zap"
)

// fakeTenants satisfies domain.TenantRepository with empty
// implementations. The metrics-label test never invokes these —
// it just needs NewServer to accept something that satisfies the
// interface.
type fakeTenants struct{}

func (fakeTenants) List(context.Context) ([]*domain.Tenant, error)        { return nil, nil }
func (fakeTenants) Get(context.Context, string) (*domain.Tenant, error)   { return nil, nil }
func (fakeTenants) Create(context.Context, *domain.Tenant) error          { return nil }

// TestMetricsLabel_UsesRoutePatternNotURLPath is the regression
// test for the audit's "PII leaks into Prometheus labels" finding.
// The original metrics middleware used r.URL.Path as the label
// value, which means a URL containing email/IP/phone would leave a
// Prometheus time series carrying the PII for the metric retention
// window. The fix is to label by chi's matched route pattern,
// which only contains what the developer wrote.
//
// As a bonus we also avoid the cardinality explosion that comes
// from per-tenant URL paths (one time series per tenant ID).
//
// We drive a request through a matched route with PII in the query
// string, then scrape /api/v1/metrics and assert the metric body
// does NOT contain the email.
func TestMetricsLabel_UsesRoutePatternNotURLPath(t *testing.T) {
	t.Parallel()

	srv := api.NewServer(&config.Config{Env: "test", Addr: ":0"}, zap.NewNop(), fakeTenants{})
	router := srv.Router()

	// Drive a request whose URL carries an email. /api/v1/tenants
	// is mounted unconditionally (Ch01) so chi's route context
	// will report a matched pattern.
	pii := `/api/v1/tenants?email=alice@example.com`
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, pii, nil))

	// Scrape /api/v1/metrics — the registry has been updated by
	// the request above.
	scrape := httptest.NewRecorder()
	router.ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil))

	body := scrape.Body.String()
	if strings.Contains(body, "alice@example.com") {
		t.Fatalf("metrics body must not contain the email from the request URL.\n"+
			"This is the audit's regression case — Prometheus labels were unmasked URLs.\n"+
			"Body excerpt:\n%s", excerpt(body, "alice"))
	}
}

// excerpt returns ~200 chars around the first match of needle in s.
func excerpt(s, needle string) string {
	i := strings.Index(s, needle)
	if i < 0 {
		if len(s) > 400 {
			return s[:400] + "..."
		}
		return s
	}
	start := i - 100
	if start < 0 {
		start = 0
	}
	end := i + 200
	if end > len(s) {
		end = len(s)
	}
	return s[start:end]
}
