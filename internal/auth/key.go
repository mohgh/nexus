// Package auth provides API-key authentication primitives for Nexus:
// secret generation, hashing, the principal/identity model, and the
// context plumbing that the HTTP middleware, handlers, and storage layer
// all share.
//
// Threat model: API keys are high-entropy random secrets (256 bits), so
// unlike user passwords they are not brute-forceable. That lets us store a
// fast SHA-256 hash rather than a slow password hash (bcrypt/argon2): the
// database holds only hashes, the raw secret is shown to the operator
// exactly once at creation, and a database leak yields no usable keys.
//
// This package imports nothing from the rest of Nexus, so middleware,
// handlers, and the Postgres repository can all depend on it without
// risking an import cycle.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Scope distinguishes a tenant-scoped credential from an operational
// (admin) one. Tenant keys can only ever act on their own tenant; admin
// keys reach the cross-tenant / operational routes and carry no tenant.
type Scope string

const (
	ScopeTenant Scope = "tenant"
	ScopeAdmin  Scope = "admin"
)

// KeyPrefix is the human-visible namespace on every Nexus key, e.g.
// "nxs_live_…". It lets operators grep logs and secret stores, and lets
// us reject obviously-malformed keys before touching the database.
const KeyPrefix = "nxs_"

// ErrKeyNotFound is returned by Repository.LookupByHash when no active
// key matches the presented secret.
var ErrKeyNotFound = errors.New("auth: api key not found")

// Principal is the authenticated identity attached to a request.
type Principal struct {
	KeyID    string
	TenantID string // empty for admin keys
	Scope    Scope
	// Plan is the tenant's plan ("free"/"pro"/"enterprise"), resolved at
	// lookup time. Empty for admin keys. Drives per-tenant rate limits.
	Plan string
}

// IsAdmin reports whether the principal holds an admin-scoped key.
func (p Principal) IsAdmin() bool { return p.Scope == ScopeAdmin }

// APIKey is a stored credential. The raw secret is never held here — only
// its hash. Hash is the SHA-256 of the raw key bytes.
type APIKey struct {
	ID         string
	TenantID   string
	Hash       []byte
	Prefix     string
	Scope      Scope
	Name       string
	CreatedAt  time.Time
	LastUsedAt *time.Time
	RevokedAt  *time.Time
}

// Repository persists and looks up API keys. Implemented by
// *postgres.APIKeyRepository; an in-memory fake backs the middleware tests.
//
// Lookups are by hash, never by raw secret: the raw key never crosses into
// the storage layer, so it can never be logged or persisted by accident.
type Repository interface {
	LookupByHash(ctx context.Context, hash []byte) (Principal, error)
	Create(ctx context.Context, k *APIKey) error
}

// HashKey returns the SHA-256 digest of a raw key. The same function is
// used at creation (to derive what we store) and at every request (to look
// the presented key up), so the two can never drift.
func HashKey(raw string) []byte {
	sum := sha256.Sum256([]byte(raw))
	return sum[:]
}

// PrefixOf returns the leading, non-secret portion of a raw key for display
// and audit ("nxs_live_AbCd…"). Enough to identify a key in a list without
// revealing anything usable.
func PrefixOf(raw string) string {
	const n = 16
	if len(raw) < n {
		return raw
	}
	return raw[:n]
}

// HasValidFormat reports whether raw looks like a Nexus key. Used to reject
// junk cheaply before a database round-trip.
func HasValidFormat(raw string) bool {
	return strings.HasPrefix(raw, KeyPrefix)
}

// NewKey mints a fresh credential for the given scope. It returns the raw
// secret (show once, never stored) and the APIKey record to persist
// (carrying only the hash). For an admin key pass tenantID "".
func NewKey(scope Scope, tenantID, name string) (raw string, key *APIKey, err error) {
	switch scope {
	case ScopeTenant:
		if tenantID == "" {
			return "", nil, errors.New("auth: tenant key requires a tenant id")
		}
	case ScopeAdmin:
		if tenantID != "" {
			return "", nil, errors.New("auth: admin key must not carry a tenant id")
		}
	default:
		return "", nil, fmt.Errorf("auth: unknown scope %q", scope)
	}

	secret := make([]byte, 32) // 256 bits of entropy
	if _, err := rand.Read(secret); err != nil {
		return "", nil, fmt.Errorf("auth: read entropy: %w", err)
	}
	// base64url without padding keeps the key copy-paste safe and header/URL
	// safe (no '=', '+', '/').
	body := base64.RawURLEncoding.EncodeToString(secret)
	raw = KeyPrefix + scopeTag(scope) + "_" + body

	key = &APIKey{
		TenantID: tenantID,
		Hash:     HashKey(raw),
		Prefix:   PrefixOf(raw),
		Scope:    scope,
		Name:     name,
	}
	return raw, key, nil
}

// scopeTag is the cosmetic segment that makes a key's scope visible at a
// glance: "nxs_live_…" for tenant keys, "nxs_admin_…" for admin keys.
func scopeTag(s Scope) string {
	if s == ScopeAdmin {
		return "admin"
	}
	return "live"
}

// ── Context plumbing ────────────────────────────────────────────────────
//
// The principal lives in the request context under an unexported key type,
// so only this package can set or read it. Middleware sets it after a
// successful lookup; handlers read the tenant from it via TenantFromContext.

type ctxKey struct{}

// ContextWithPrincipal returns a copy of ctx carrying the principal.
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// PrincipalFromContext returns the authenticated principal, or ok=false on
// an unauthenticated request (e.g. a public route).
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// TenantFromContext returns the authenticated tenant ID, or "" when the
// request is unauthenticated or carries an admin (tenant-less) key.
//
// Handlers MUST derive the tenant from this — never from the request body
// or URL — so a caller can only ever act on its own tenant.
func TenantFromContext(ctx context.Context) string {
	p, _ := PrincipalFromContext(ctx)
	return p.TenantID
}
