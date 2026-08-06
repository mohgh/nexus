package middleware

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/mohgh/nexus/internal/pii"
	"go.uber.org/zap"
)

// piiPathKey is the context key under which PIIDetect stores the
// masked URL.Path for downstream loggers.
type piiPathKey struct{}

// MaskedPathFromContext returns the masked URL path stamped by
// PIIDetect, or "" if no masker ran. The metrics/access logger
// prefers this over r.URL.Path so PII never lands in the log file.
func MaskedPathFromContext(ctx context.Context) string {
	v, _ := ctx.Value(piiPathKey{}).(string)
	return v
}

// WithMaskedPath attaches the masked path to ctx. Exported so other
// middlewares (e.g., a future access logger) can produce their own
// masked variants without re-running the masker.
func WithMaskedPath(ctx context.Context, masked string) context.Context {
	return context.WithValue(ctx, piiPathKey{}, masked)
}

// PIIDetect scans the URL path and query string for PII patterns
// (emails, IPs, phone numbers via the regexes in internal/pii). When
// any are found, a masked version of `path?query` is stamped into
// the request context for downstream loggers, AND a single warn-line
// is emitted noting which categories appeared.
//
// The middleware deliberately does NOT log the matched values — the
// whole point is to keep PII out of structured logs. The detection
// log line names categories only (e.g. "email,ip"), not the strings.
//
// What this is NOT: a replacement for masking inside the application
// layer. The pii.Masker is also called by the GDPR anonymisation
// path (see Service.AnonymiseTenantEvents) and by cmd/pii-scanner
// over the events table. The middleware catches the URL/query
// surface specifically — the place where developer mistakes most
// often leak PII into application logs.
func PIIDetect(masker *pii.Masker, logger *zap.Logger) func(http.Handler) http.Handler {
	if masker == nil {
		// No-op if no masker — keeps the wiring optional in main.go.
		return func(next http.Handler) http.Handler { return next }
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// json.RawMessage is just []byte; the regexes operate on
			// raw bytes either way. Saves a method on the masker.
			full := r.URL.Path
			if r.URL.RawQuery != "" {
				full += "?" + r.URL.RawQuery
			}
			categories := masker.Detect(json.RawMessage(full))
			if len(categories) > 0 {
				masked := masker.MaskString(full)
				logger.Warn("pii detected in request URL",
					zap.String("masked_path", masked),
					zap.Any("categories", categories),
				)
				ctx := WithMaskedPath(r.Context(), masked)
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
