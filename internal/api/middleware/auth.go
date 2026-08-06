// Package middleware provides HTTP middleware for Nexus.
//
// Authentication: every request to a tenant- or admin-scoped route must
// carry a valid API key (X-API-Key, or Authorization: Bearer <key>). The
// Authenticator validates the key against a DB-backed store, resolves it to
// a Principal, and injects that principal into the request context. From
// there:
//
//   - Handlers read the tenant via auth.TenantFromContext — never from the
//     request body or URL. This is the "override" isolation model: a caller
//     physically cannot act on a tenant other than the one its key names.
//   - RequireAdmin gates operational / cross-tenant routes to admin keys.
//   - EnforceTenantParam guards {tenantID}-addressed routes, rejecting any
//     request whose path tenant differs from the key's tenant.
//
// Public routes (health, ready, metrics) are mounted outside these
// middlewares.
package middleware

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/mohgh/nexus/internal/auth"
)

// Authenticator validates API-key credentials against a Repository and
// injects the resolved Principal into the request context.
type Authenticator struct {
	repo auth.Repository
}

// NewAuthenticator builds an Authenticator over the given key store.
func NewAuthenticator(repo auth.Repository) *Authenticator {
	return &Authenticator{repo: repo}
}

// Authenticate rejects any request without a valid, active API key (401)
// and otherwise attaches the principal to the context.
func (a *Authenticator) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := apiKeyFromRequest(r)
		if raw == "" || !auth.HasValidFormat(raw) {
			unauthorized(w, "missing or malformed API key — send X-API-Key or Authorization: Bearer <key>")
			return
		}
		p, err := a.repo.LookupByHash(r.Context(), auth.HashKey(raw))
		if err != nil {
			// Identical response for unknown and revoked keys: never
			// reveal which keys exist.
			unauthorized(w, "invalid or revoked API key")
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.ContextWithPrincipal(r.Context(), p)))
	})
}

// RequireAdmin gates a route group to admin-scoped principals. It must be
// mounted after Authenticate.
func (a *Authenticator) RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := auth.PrincipalFromContext(r.Context())
		if !ok || !p.IsAdmin() {
			forbidden(w, "admin scope required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// EnforceTenantParam guards routes carrying a {tenantID} URL parameter. It
// requires a tenant-scoped principal whose tenant matches the path — the
// override model's defense for path-addressed resources. Admin keys (which
// carry no tenant) are rejected here; admin operates via the admin routes.
func (a *Authenticator) EnforceTenantParam(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := auth.PrincipalFromContext(r.Context())
		if !ok || p.TenantID == "" {
			forbidden(w, "tenant-scoped API key required")
			return
		}
		if pathID := chi.URLParam(r, "tenantID"); pathID != "" && pathID != p.TenantID {
			forbidden(w, "tenant mismatch — this key is not authorized for the requested tenant")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// apiKeyFromRequest extracts the raw key, preferring the X-API-Key header,
// then Authorization: Bearer, then a ?api_key= query parameter.
//
// The query-param path exists only for navigator.sendBeacon, which cannot
// set custom headers — the JS SDK uses it on page-unload flushes (the same
// reason it falls back to ?idempotency_key=). Keys in query strings can be
// captured by proxy/access logs, so headers are strongly preferred; Nexus's
// own access log records the masked path, not the query, so it won't leak it.
func apiKeyFromRequest(r *http.Request) string {
	if k := r.Header.Get("X-API-Key"); k != "" {
		return k
	}
	const bearer = "Bearer "
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, bearer) {
		return strings.TrimSpace(strings.TrimPrefix(h, bearer))
	}
	if k := r.URL.Query().Get("api_key"); k != "" {
		return k
	}
	return ""
}

func unauthorized(w http.ResponseWriter, msg string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="nexus"`)
	writeAuthError(w, http.StatusUnauthorized, "UNAUTHORIZED", msg)
}

func forbidden(w http.ResponseWriter, msg string) {
	writeAuthError(w, http.StatusForbidden, "FORBIDDEN", msg)
}

func writeAuthError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":%q,"code":%q}`, msg, code)
}
