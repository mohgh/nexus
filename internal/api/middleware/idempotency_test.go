package middleware

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

// TestIdempotencyTimings_AdvertisedRetryAfterIsAchievable pins the
// invariants that make the 409 IDEMPOTENCY_REQUEST_IN_PROGRESS
// response honest. These are pure constant relationships, so they
// belong in the unit tier — the integration test only observes the
// resulting header.
//
// The old numbers violated every one of these: Retry-After said 5
// seconds while an in-flight original could legitimately hold the
// reservation for the full 10s request timeout, and a genuinely
// abandoned row was cleared only by a 5-minute sweep on a 1-minute
// tick — a 6-minute worst case behind a 5-second promise.
func TestIdempotencyTimings_AdvertisedRetryAfterIsAchievable(t *testing.T) {
	t.Parallel()

	// A client told to retry sooner than the request timeout is being
	// told to retry before the answer can possibly exist.
	if retryAfterInFlight < maxHandlerRuntime {
		t.Fatalf("retryAfterInFlight (%v) < maxHandlerRuntime (%v): every "+
			"obedient retry is a guaranteed 409", retryAfterInFlight, maxHandlerRuntime)
	}

	// Retry-After is serialised as whole seconds, so a sub-second
	// value would truncate to "0" and turn the backoff into a spin.
	if int(retryAfterInFlight.Seconds()) < 1 {
		t.Fatalf("retryAfterInFlight (%v) truncates to 0 seconds on the wire",
			retryAfterInFlight)
	}

	// The sweeper must never reclaim a reservation that a live
	// request could still be holding — that would re-admit the
	// double-execution the reservation exists to prevent.
	if inFlightStaleTTL <= maxHandlerRuntime {
		t.Fatalf("inFlightStaleTTL (%v) <= maxHandlerRuntime (%v): the sweeper "+
			"can delete a reservation that is still live, letting a concurrent "+
			"retry run the handler a second time", inFlightStaleTTL, maxHandlerRuntime)
	}

	// And the backstop must stay within an order of magnitude of what
	// we advertise, or the header is fiction again for the crashed-
	// process case.
	worstCase := inFlightStaleTTL + inFlightSweepInterval
	if worstCase > 10*retryAfterInFlight {
		t.Fatalf("worst-case stale-reservation clearance (%v) is more than 10x "+
			"the advertised Retry-After (%v); a client obeying the header burns "+
			"%d pointless retries", worstCase, retryAfterInFlight,
			int(worstCase/retryAfterInFlight))
	}
}

// TestCleanupContext_SurvivesRequestCancellation is the unit-tier
// guard for the reservation leak: the context used to release a
// reservation must NOT die with the request, because the request dies
// exactly when the client is about to retry. It must still carry the
// request's values so the failure logs stay attributable.
func TestCleanupContext_SurvivesRequestCancellation(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}

	reqCtx, cancel := context.WithCancel(
		context.WithValue(context.Background(), ctxKey{}, "request-id-42"))
	r := httptest.NewRequest(http.MethodPost, "/api/v1/events", nil).WithContext(reqCtx)

	cleanupCtx, cleanupCancel := cleanupContext(r)
	defer cleanupCancel()

	// The request dies — timeout or client disconnect.
	cancel()

	if err := cleanupCtx.Err(); err != nil {
		t.Fatalf("cleanup context died with the request: %v.\n"+
			"A mutual-exclusion primitive must not share a lifetime with the "+
			"thing it protects.", err)
	}
	if got := cleanupCtx.Value(ctxKey{}); got != "request-id-42" {
		t.Fatalf("cleanup context lost the request's values: got %v.\n"+
			"WithoutCancel is chosen over Background precisely to keep them.", got)
	}
	deadline, ok := cleanupCtx.Deadline()
	if !ok {
		t.Fatal("cleanup context must be bounded; an unbounded one can pin a " +
			"goroutine on a wedged database forever")
	}
	if until := time.Until(deadline); until > cleanupTimeout+time.Second {
		t.Fatalf("cleanup deadline %v out is longer than cleanupTimeout (%v)",
			until, cleanupTimeout)
	}
}
