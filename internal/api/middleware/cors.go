package middleware

import (
	"net/http"
	"strings"
)

// CORSConfig configures the CORS middleware.
//
// AllowedOrigins is matched verbatim against the request's Origin header.
// A single "*" entry permits any origin (echoed back rather than literal
// "*" so that requests with credentials remain valid). An empty slice
// disables CORS entirely — the middleware becomes a no-op.
type CORSConfig struct {
	AllowedOrigins []string
}

// CORS returns a middleware that handles browser preflight (OPTIONS)
// requests and stamps Access-Control-* headers on every response.
//
// Without this, a browser SDK calling the API from a different origin
// (the SDK demo page, any tenant's own site) is blocked by the same-origin
// policy before the request ever reaches a handler.
func CORS(cfg CORSConfig) func(http.Handler) http.Handler {
	if len(cfg.AllowedOrigins) == 0 {
		return func(next http.Handler) http.Handler { return next }
	}

	wildcard := false
	allowed := make(map[string]struct{}, len(cfg.AllowedOrigins))
	for _, o := range cfg.AllowedOrigins {
		if o == "*" {
			wildcard = true
			continue
		}
		allowed[o] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			originOK := origin != "" && (wildcard || originAllowed(allowed, origin))
			if originOK {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Vary", "Origin")
				w.Header().Set("Access-Control-Allow-Credentials", "true")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key, X-Nexus-Min-LSN, Authorization, X-API-Key")
				w.Header().Set("Access-Control-Expose-Headers", "X-Request-Id, X-Nexus-Write-LSN, X-Idempotent-Replay, Retry-After, X-RateLimit-Limit, X-RateLimit-Remaining")
				w.Header().Set("Access-Control-Max-Age", "600")
			}

			// Only short-circuit preflights we actually answered.
			// OPTIONS from a disallowed cross-origin caller should
			// still fall through — the browser will reject the
			// response for lack of ACAO, but a same-origin OPTIONS
			// (no Origin) or a non-browser OPTIONS reaches the
			// handler tree (where chi may legitimately 405 or
			// answer it). Without this guard the middleware
			// silently 204s OPTIONS for any future route.
			if r.Method == http.MethodOptions && originOK {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func originAllowed(set map[string]struct{}, origin string) bool {
	if _, ok := set[origin]; ok {
		return true
	}
	// Tolerate trailing slashes that some hosts send.
	if _, ok := set[strings.TrimRight(origin, "/")]; ok {
		return true
	}
	return false
}
