package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mohgh/nexus/internal/api/middleware"
	"github.com/mohgh/nexus/internal/pii"
)

// TestPIIDetect_StampsMaskedPathOnPIIInQuery verifies that the
// middleware scans path+query, finds PII, and stamps a masked
// version into the context for downstream loggers.
func TestPIIDetect_StampsMaskedPathOnPIIInQuery(t *testing.T) {
	t.Parallel()

	masker := pii.NewMasker()
	var observedMasked string
	mw := middleware.PIIDetect(masker, nil)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedMasked = middleware.MaskedPathFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/search?email=alice@example.com", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status: got %d, want 200", rr.Code)
	}
	if observedMasked == "" {
		t.Fatalf("expected masked path in context when PII was detected")
	}
	if strings.Contains(observedMasked, "alice@example.com") {
		t.Fatalf("masked path still contains the email: %q", observedMasked)
	}
	if !strings.Contains(observedMasked, "[REDACTED]") {
		t.Fatalf("masked path should carry the [REDACTED] placeholder, got %q", observedMasked)
	}
}

// TestPIIDetect_NoStampWhenClean: if the URL has no PII patterns,
// the context value stays empty and the access logger uses
// r.URL.Path verbatim. This avoids paying mask overhead for the
// 99% case.
func TestPIIDetect_NoStampWhenClean(t *testing.T) {
	t.Parallel()

	masker := pii.NewMasker()
	var observedMasked string
	mw := middleware.PIIDetect(masker, nil)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedMasked = middleware.MaskedPathFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/tenants/abc-123/events", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if observedMasked != "" {
		t.Fatalf("clean URL should not stamp a masked path, got %q", observedMasked)
	}
}

// TestPIIDetect_NilMaskerIsNoOp: PIIDetect(nil, _) returns the
// next handler unchanged. Lets main.go optionally wire the masker
// without conditionals at the call site.
func TestPIIDetect_NilMaskerIsNoOp(t *testing.T) {
	t.Parallel()

	mw := middleware.PIIDetect(nil, nil)
	called := false
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/x?email=a@b.com", nil)
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if !called {
		t.Fatalf("nil-masker path should still run the handler")
	}
}
