package middleware

import (
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/mohgh/nexus/internal/auth"
	"github.com/mohgh/nexus/internal/ratelimit"
)

// RateLimit throttles requests per authenticated principal. It MUST run
// after Authenticate so the principal — and its plan — is already in
// context. Requests without a principal (public routes, or handler-level
// tests that don't wire auth) pass through unthrottled.
//
// The bucket key is the tenant ("t:<id>") for tenant keys or the key id
// ("k:<id>") for admin keys, so tenants are isolated from each other and
// each admin key has its own ceiling. Limits come from the tenant's plan.
//
// On rejection it returns 429 with Retry-After; every response carries
// X-RateLimit-Limit / X-RateLimit-Remaining so clients can self-pace.
func RateLimit(limiter ratelimit.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			p, ok := auth.PrincipalFromContext(r.Context())
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			key, lim := rateLimitKeyAndLimit(p)
			res := limiter.Allow(key, lim)

			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(lim.Burst))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(res.Remaining))

			if !res.Allowed {
				retry := max(int(math.Ceil(res.RetryAfter.Seconds())), 1)
				w.Header().Set("Retry-After", strconv.Itoa(retry))
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = fmt.Fprintf(w,
					`{"error":"rate limit exceeded — retry in %ds","code":"RATE_LIMITED"}`, retry)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// rateLimitKeyAndLimit derives the bucket key and per-plan limit for a
// principal. Admin keys get their own ceiling; tenant keys are limited by
// the tenant's plan.
func rateLimitKeyAndLimit(p auth.Principal) (string, ratelimit.Limit) {
	if p.IsAdmin() {
		return "k:" + p.KeyID, ratelimit.AdminLimit()
	}
	return "t:" + p.TenantID, ratelimit.LimitForPlan(p.Plan)
}
