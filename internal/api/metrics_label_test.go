package api_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
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

// TestMetricsLabel_ClampsHTTPMethodToKnownSet is the regression test
// for the unbounded-`method`-label finding.
//
// The `path` label was already hardened to the chi route pattern, but
// `r.Method` was passed through verbatim. An HTTP method is an
// RFC-7230 *token*, not an enum: Go's server hands the handler
// whatever the client sent, so `WithLabelValues(r.Method, ...)` mints
// a brand-new time series per invented method. /api/v1/metrics is
// mounted outside every auth group and the rate limiter lives inside
// them, so an unauthenticated, unthrottled caller could both inflate
// the registry and scrape /metrics to watch it grow.
//
// The fix clamps the label to the closed set of RFC methods with a
// single "other" bucket. This test drives 60 invented methods and
// asserts (a) none of them reaches a label value and (b) the set of
// distinct `method` label values stays inside the allow-list.
func TestMetricsLabel_ClampsHTTPMethodToKnownSet(t *testing.T) {
	t.Parallel()

	srv := api.NewServer(&config.Config{Env: "test", Addr: ":0"}, zap.NewNop(), fakeTenants{})
	router := srv.Router()

	// A real request first, so a legitimate method label is present
	// and we can prove the clamp doesn't flatten everything to "other".
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/tenants", nil))

	// Now the attack: 60 distinct, syntactically valid RFC-7230 method
	// tokens that no route serves.
	const attackMethods = 60
	invented := make([]string, 0, attackMethods)
	for i := 0; i < attackMethods; i++ {
		m := fmt.Sprintf("NEXUSPWN%d", i)
		invented = append(invented, m)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(m, "/api/v1/tenants", nil))
	}

	scrape := httptest.NewRecorder()
	router.ServeHTTP(scrape, httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil))
	body := scrape.Body.String()

	for _, m := range invented {
		if strings.Contains(body, m) {
			t.Fatalf("attacker-controlled method %q reached a Prometheus label — "+
				"the method label is unbounded.\nBody excerpt:\n%s", m, excerpt(body, m))
		}
	}

	// Whatever survived must be inside the allow-list.
	allowed := map[string]bool{
		http.MethodGet: true, http.MethodHead: true, http.MethodPost: true,
		http.MethodPut: true, http.MethodPatch: true, http.MethodDelete: true,
		http.MethodConnect: true, http.MethodOptions: true, http.MethodTrace: true,
		"other": true,
	}
	got := methodLabelValues(body, "nexus_http_requests_total")
	if len(got) == 0 {
		t.Fatalf("no nexus_http_requests_total series were recorded at all — "+
			"the fix must clamp the label, not drop the metric.\nBody excerpt:\n%s",
			excerpt(body, "nexus_http_requests_total"))
	}
	for m := range got {
		if !allowed[m] {
			t.Fatalf("method label %q is outside the allow-list %v (observed: %v)",
				m, sortedKeys(allowed), sortedKeys(got))
		}
	}
	if !got["other"] {
		t.Fatalf("the 60 unknown methods must be counted in the \"other\" bucket, "+
			"not silently dropped (observed method labels: %v)", sortedKeys(got))
	}
	if !got[http.MethodGet] {
		t.Fatalf("the legitimate GET must keep its own label, not collapse into "+
			"\"other\" (observed method labels: %v)", sortedKeys(got))
	}
}

// methodLabelValues extracts the distinct `method="…"` label values
// carried by the named metric family in a Prometheus text exposition.
func methodLabelValues(body, metricName string) map[string]bool {
	out := map[string]bool{}
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, metricName+"{") {
			continue
		}
		labels := line[len(metricName)+1:]
		i := strings.Index(labels, `method="`)
		if i < 0 {
			continue
		}
		rest := labels[i+len(`method="`):]
		j := strings.Index(rest, `"`)
		if j < 0 {
			continue
		}
		out[rest[:j]] = true
	}
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
