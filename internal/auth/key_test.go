package auth

import (
	"bytes"
	"context"
	"testing"
)

func TestHashKey_DeterministicAnd32Bytes(t *testing.T) {
	t.Parallel()
	h1 := HashKey("nxs_live_abc")
	h2 := HashKey("nxs_live_abc")
	if !bytes.Equal(h1, h2) {
		t.Fatal("HashKey must be deterministic for the same input")
	}
	if len(h1) != 32 {
		t.Fatalf("expected 32-byte SHA-256 digest, got %d", len(h1))
	}
	if bytes.Equal(h1, HashKey("nxs_live_xyz")) {
		t.Fatal("different inputs must hash differently")
	}
}

func TestNewKey_Tenant(t *testing.T) {
	t.Parallel()
	raw, key, err := NewKey(ScopeTenant, "tenant-1", "my key")
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}
	if !HasValidFormat(raw) {
		t.Fatalf("raw key %q must start with %q", raw, KeyPrefix)
	}
	if key.Scope != ScopeTenant || key.TenantID != "tenant-1" {
		t.Fatalf("unexpected scope/tenant: %+v", key)
	}
	if !bytes.Equal(key.Hash, HashKey(raw)) {
		t.Fatal("stored hash must equal HashKey(raw) so lookups match")
	}
	if key.Prefix != PrefixOf(raw) {
		t.Fatalf("prefix %q must be the leading slice of raw", key.Prefix)
	}
}

func TestNewKey_Admin(t *testing.T) {
	t.Parallel()
	raw, key, err := NewKey(ScopeAdmin, "", "ops")
	if err != nil {
		t.Fatalf("NewKey admin: %v", err)
	}
	if key.Scope != ScopeAdmin || key.TenantID != "" {
		t.Fatalf("admin key must carry no tenant: %+v", key)
	}
	if !bytes.Equal(key.Hash, HashKey(raw)) {
		t.Fatal("hash mismatch")
	}
}

func TestNewKey_RejectsInvalidScopeTenantCombos(t *testing.T) {
	t.Parallel()
	if _, _, err := NewKey(ScopeTenant, "", "x"); err == nil {
		t.Fatal("tenant key without tenant_id must error")
	}
	if _, _, err := NewKey(ScopeAdmin, "tenant-1", "x"); err == nil {
		t.Fatal("admin key with a tenant_id must error")
	}
	if _, _, err := NewKey(Scope("bogus"), "t", "x"); err == nil {
		t.Fatal("unknown scope must error")
	}
}

func TestNewKey_UniquePerCall(t *testing.T) {
	t.Parallel()
	raw1, _, _ := NewKey(ScopeTenant, "t", "")
	raw2, _, _ := NewKey(ScopeTenant, "t", "")
	if raw1 == raw2 {
		t.Fatal("each minted key must be unique (256 bits of entropy)")
	}
}

func TestPrincipalContextRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := ContextWithPrincipal(context.Background(),
		Principal{KeyID: "k1", TenantID: "t1", Scope: ScopeTenant})

	if got := TenantFromContext(ctx); got != "t1" {
		t.Fatalf("TenantFromContext = %q, want t1", got)
	}
	p, ok := PrincipalFromContext(ctx)
	if !ok || p.KeyID != "k1" {
		t.Fatalf("PrincipalFromContext = %+v, ok=%v", p, ok)
	}
	if p.IsAdmin() {
		t.Fatal("tenant principal must not report IsAdmin")
	}

	// Empty context yields no tenant and no principal.
	if TenantFromContext(context.Background()) != "" {
		t.Fatal("bare context must have empty tenant")
	}
	if _, ok := PrincipalFromContext(context.Background()); ok {
		t.Fatal("bare context must have no principal")
	}
}
