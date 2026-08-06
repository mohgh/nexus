package middleware

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

// Consent state constants. The interface returns plain int so a
// store package can satisfy it without importing middleware (which
// would be circular). The values must match consent.State* in
// internal/consent/store.go — the test there pins them down.
const (
	ConsentStateNoRecord = 0
	ConsentStateGranted  = 1
	ConsentStateRevoked  = 2
)

// ConsentChecker reports the current consent state for a (tenant,
// purpose) pair. Returns 0 (NoRecord), 1 (Granted), or 2 (Revoked).
// Implemented by *consent.Store.
//
// Defined here (rather than imported from consent) so the middleware
// can be tested without a real Postgres pool.
type ConsentChecker interface {
	ConsentState(ctx context.Context, tenantID, purpose string) (int, error)
}

// ConsentGate returns a middleware that 403's tenant-scoped requests
// when consent for `purpose` has been explicitly revoked.
//
// The tenant is extracted from chi URL param `tenantID`. Policy:
//
//   * NoRecord -> pass through (lenient default; see Ch14 README).
//   * Granted  -> pass through.
//   * Revoked  -> 403 with X-Consent-Required: <purpose>.
//
// On checker errors the gate logs and passes through. Failing
// requests outright on a transient consent-store outage would be
// less safe than running the request — the GDPR mandate is on
// withdrawal, not on uptime.
func ConsentGate(checker ConsentChecker, purpose string, logger *zap.Logger) func(http.Handler) http.Handler {
	if checker == nil {
		panic("middleware.ConsentGate: checker is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenantID := chi.URLParam(r, "tenantID")
			if tenantID == "" {
				next.ServeHTTP(w, r)
				return
			}

			state, err := checker.ConsentState(r.Context(), tenantID, purpose)
			if err != nil {
				logger.Warn("consent gate: lookup failed (allowing through)",
					zap.String("tenant_id", tenantID),
					zap.String("purpose", purpose),
					zap.Error(err),
				)
				next.ServeHTTP(w, r)
				return
			}

			if state == ConsentStateRevoked {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Consent-Required", purpose)
				w.WriteHeader(http.StatusForbidden)
				_, _ = fmt.Fprintf(w,
					`{"error":"consent for %q is required","code":"CONSENT_REQUIRED","purpose":%q}`,
					purpose, purpose,
				)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
