package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/mohgh/nexus/internal/auth"
)

// fakeRepo is an in-memory auth.Repository keyed by the hash of each raw
// key, so the middleware's HashKey-then-lookup path is exercised end to end.
type fakeRepo struct {
	byHash map[string]auth.Principal
}

func newFakeRepo() *fakeRepo { return &fakeRepo{byHash: map[string]auth.Principal{}} }

func (f *fakeRepo) add(raw string, p auth.Principal) {
	f.byHash[string(auth.HashKey(raw))] = p
}

func (f *fakeRepo) LookupByHash(_ context.Context, hash []byte) (auth.Principal, error) {
	if p, ok := f.byHash[string(hash)]; ok {
		return p, nil
	}
	return auth.Principal{}, auth.ErrKeyNotFound
}

func (f *fakeRepo) Create(context.Context, *auth.APIKey) error { return nil }

// okHandler records that it ran and echoes the resolved tenant.
func okHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(auth.TenantFromContext(r.Context())))
	}
}

func TestAuthenticate(t *testing.T) {
	t.Parallel()
	repo := newFakeRepo()
	repo.add("nxs_live_goodkey", auth.Principal{KeyID: "k1", TenantID: "tenant-1", Scope: auth.ScopeTenant})
	a := NewAuthenticator(repo)
	h := a.Authenticate(okHandler())

	cases := []struct {
		name     string
		header   string
		value    string
		wantCode int
		wantBody string
	}{
		{"no key", "", "", http.StatusUnauthorized, ""},
		{"malformed key", "X-API-Key", "not-a-nexus-key", http.StatusUnauthorized, ""},
		{"unknown key", "X-API-Key", "nxs_live_nope", http.StatusUnauthorized, ""},
		{"valid X-API-Key", "X-API-Key", "nxs_live_goodkey", http.StatusOK, "tenant-1"},
		{"valid Bearer", "Authorization", "Bearer nxs_live_goodkey", http.StatusOK, "tenant-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/events", nil)
			if tc.header != "" {
				req.Header.Set(tc.header, tc.value)
			}
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			if rr.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body %q)", rr.Code, tc.wantCode, rr.Body.String())
			}
			if tc.wantBody != "" && rr.Body.String() != tc.wantBody {
				t.Fatalf("body = %q, want %q", rr.Body.String(), tc.wantBody)
			}
			if tc.wantCode == http.StatusUnauthorized {
				if rr.Header().Get("WWW-Authenticate") == "" {
					t.Error("401 responses must carry a WWW-Authenticate header")
				}
			}
		})
	}
}

// TestAuthenticate_APIKeyQueryParam covers the sendBeacon path: browsers
// can't set headers on a beacon, so the key arrives as ?api_key=.
func TestAuthenticate_APIKeyQueryParam(t *testing.T) {
	t.Parallel()
	repo := newFakeRepo()
	repo.add("nxs_live_beacon", auth.Principal{KeyID: "k1", TenantID: "tenant-1", Scope: auth.ScopeTenant})
	a := NewAuthenticator(repo)
	h := a.Authenticate(okHandler())

	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/events/batch?idempotency_key=abc&api_key=nxs_live_beacon", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("query-param key: code = %d, want 200", rr.Code)
	}
	if rr.Body.String() != "tenant-1" {
		t.Fatalf("resolved tenant = %q, want tenant-1", rr.Body.String())
	}

	// Header still wins over the query param when both are present.
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/events?api_key=nxs_live_beacon", nil)
	req2.Header.Set("X-API-Key", "nxs_live_unknown")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusUnauthorized {
		t.Fatalf("header must take precedence over query param: code = %d, want 401", rr2.Code)
	}
}

func TestRequireAdmin(t *testing.T) {
	t.Parallel()
	repo := newFakeRepo()
	repo.add("nxs_admin_root", auth.Principal{KeyID: "a1", Scope: auth.ScopeAdmin})
	repo.add("nxs_live_tenant", auth.Principal{KeyID: "k1", TenantID: "tenant-1", Scope: auth.ScopeTenant})
	a := NewAuthenticator(repo)
	h := a.Authenticate(a.RequireAdmin(okHandler()))

	tests := []struct {
		key      string
		wantCode int
	}{
		{"nxs_admin_root", http.StatusOK},
		{"nxs_live_tenant", http.StatusForbidden}, // tenant key cannot reach admin routes
		{"nxs_live_unknown", http.StatusUnauthorized},
	}
	for _, tc := range tests {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/leader", nil)
		req.Header.Set("X-API-Key", tc.key)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != tc.wantCode {
			t.Errorf("key %q: code = %d, want %d", tc.key, rr.Code, tc.wantCode)
		}
	}
}

func TestEnforceTenantParam(t *testing.T) {
	t.Parallel()
	repo := newFakeRepo()
	repo.add("nxs_live_t1", auth.Principal{KeyID: "k1", TenantID: "tenant-1", Scope: auth.ScopeTenant})
	repo.add("nxs_admin_root", auth.Principal{KeyID: "a1", Scope: auth.ScopeAdmin})
	a := NewAuthenticator(repo)

	// Drive through a chi router so the {tenantID} URL param resolves.
	newRouter := func() http.Handler {
		r := chi.NewRouter()
		r.With(a.Authenticate, a.EnforceTenantParam).
			Get("/api/v1/tenants/{tenantID}/daily-stats", okHandler())
		return r
	}

	tests := []struct {
		name     string
		key      string
		path     string
		wantCode int
	}{
		{"own tenant", "nxs_live_t1", "/api/v1/tenants/tenant-1/daily-stats", http.StatusOK},
		{"other tenant", "nxs_live_t1", "/api/v1/tenants/tenant-2/daily-stats", http.StatusForbidden},
		{"admin key rejected on tenant route", "nxs_admin_root", "/api/v1/tenants/tenant-1/daily-stats", http.StatusForbidden},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.Header.Set("X-API-Key", tc.key)
			rr := httptest.NewRecorder()
			newRouter().ServeHTTP(rr, req)
			if rr.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d", rr.Code, tc.wantCode)
			}
		})
	}
}
