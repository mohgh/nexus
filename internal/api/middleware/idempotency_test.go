package middleware

import (
	"bytes"
	"context"
	"testing"

	"github.com/mohgh/nexus/internal/auth"
)

// TestIdempotencyNamespace_SeparatesPrincipals pins the cross-tenant
// isolation fix: each tenant (and each admin key) gets a distinct key
// namespace, so reusing the same Idempotency-Key across tenants can never
// collide on the shared dedup row. An unauthenticated context maps to the
// empty (legacy) namespace.
func TestIdempotencyNamespace_SeparatesPrincipals(t *testing.T) {
	t.Parallel()

	t1 := auth.ContextWithPrincipal(context.Background(),
		auth.Principal{TenantID: "tenant-1", Scope: auth.ScopeTenant})
	t2 := auth.ContextWithPrincipal(context.Background(),
		auth.Principal{TenantID: "tenant-2", Scope: auth.ScopeTenant})
	admin := auth.ContextWithPrincipal(context.Background(),
		auth.Principal{KeyID: "admin-key", Scope: auth.ScopeAdmin})

	ns1 := idempotencyNamespace(t1)
	ns2 := idempotencyNamespace(t2)
	nsAdmin := idempotencyNamespace(admin)
	nsNone := idempotencyNamespace(context.Background())

	if ns1 == ns2 {
		t.Fatalf("two tenants must get distinct namespaces, both = %q", ns1)
	}
	if ns1 == nsAdmin || ns2 == nsAdmin {
		t.Fatal("admin namespace must differ from tenant namespaces")
	}
	if nsNone != "" {
		t.Fatalf("unauthenticated namespace = %q, want empty", nsNone)
	}
}

// TestRequestFingerprint_DeterministicAcrossCalls pins down that the
// same (method, path, body) always yields the same 32-byte digest.
// This is the basis of cache hit determination — a non-deterministic
// fingerprint would turn every cache lookup into an unconditional
// miss and silently disable the protection.
func TestRequestFingerprint_DeterministicAcrossCalls(t *testing.T) {
	t.Parallel()

	body := []byte(`{"tenant_id":"t1","amount":100}`)

	a := requestFingerprint("POST", "/api/v1/billing/charge", body)
	b := requestFingerprint("POST", "/api/v1/billing/charge", body)

	if !bytes.Equal(a, b) {
		t.Fatalf("same inputs must produce same digest:\n  a=%x\n  b=%x", a, b)
	}
	if len(a) != 32 {
		t.Fatalf("sha256 digest length: got %d, want 32", len(a))
	}
}

// TestRequestFingerprint_DiffersOnEachInput is the load-bearing
// property: any change to method, path, OR body yields a different
// digest. If any of these failed, the audit's "wrong cached
// response on key reuse" failure mode would still be present.
func TestRequestFingerprint_DiffersOnEachInput(t *testing.T) {
	t.Parallel()

	base := requestFingerprint("POST", "/api/v1/events", []byte(`{"x":1}`))

	cases := []struct {
		name        string
		method      string
		path        string
		body        []byte
	}{
		{"different method", "PUT", "/api/v1/events", []byte(`{"x":1}`)},
		{"different path", "POST", "/api/v1/billing/charge", []byte(`{"x":1}`)},
		{"different body", "POST", "/api/v1/events", []byte(`{"x":2}`)},
		{"empty body vs non-empty", "POST", "/api/v1/events", nil},
		{"different tenant in path", "POST", "/api/v1/tenants/abc/credits", []byte(`{}`)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := requestFingerprint(c.method, c.path, c.body)
			if bytes.Equal(base, got) {
				t.Fatalf("fingerprint must differ when %s changes:\n  base=%x\n  got =%x",
					c.name, base, got)
			}
		})
	}
}

// TestRequestFingerprint_FieldDelimiterPreventsAmbiguity guards
// against a sneaky bug class: concatenating method + path + body
// without a delimiter lets a clever caller construct two requests
// where the join boundaries differ but the concatenation is equal.
// E.g. method="POSTab", path="/x" vs. method="POST", path="ab/x" —
// without a delimiter they'd hash the same. The "\n" between
// fields prevents this.
func TestRequestFingerprint_FieldDelimiterPreventsAmbiguity(t *testing.T) {
	t.Parallel()

	a := requestFingerprint("POSTab", "/x", []byte("body"))
	b := requestFingerprint("POST", "ab/x", []byte("body"))

	if bytes.Equal(a, b) {
		t.Fatalf("method/path boundary ambiguity:\n  a=%x\n  b=%x", a, b)
	}
}
