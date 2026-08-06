package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/auth"
)

// APIKeyRepository implements auth.Repository against PostgreSQL.
//
// It uses the PRIMARY pool directly (not the Ch06 replica pool) on purpose:
// authentication and revocation must be read-your-writes consistent. A key
// revoked a moment ago must not keep authenticating from a lagging replica,
// and a key minted a moment ago must work on its first request.
type APIKeyRepository struct {
	pool *pgxpool.Pool
}

func NewAPIKeyRepository(pool *pgxpool.Pool) *APIKeyRepository {
	return &APIKeyRepository{pool: pool}
}

// Compile-time assertion: the repository satisfies auth.Repository.
var _ auth.Repository = (*APIKeyRepository)(nil)

// LookupByHash resolves an active key's hash to its principal, returning
// auth.ErrKeyNotFound when the key is unknown OR revoked. The two are
// deliberately indistinguishable to the caller so a probing client cannot
// learn which keys exist.
func (r *APIKeyRepository) LookupByHash(ctx context.Context, hash []byte) (auth.Principal, error) {
	var (
		id       string
		tenantID *string
		scope    string
		plan     *string
	)
	// LEFT JOIN so admin keys (NULL tenant_id) still resolve; the tenant's
	// plan rides along in the same round-trip to drive rate limiting without
	// a second query on the hot path.
	err := r.pool.QueryRow(ctx,
		`SELECT k.id, k.tenant_id, k.scope, t.plan
		   FROM api_keys k
		   LEFT JOIN tenants t ON t.id = k.tenant_id
		  WHERE k.key_hash = $1 AND k.revoked_at IS NULL`,
		hash,
	).Scan(&id, &tenantID, &scope, &plan)
	if errors.Is(err, pgx.ErrNoRows) {
		return auth.Principal{}, auth.ErrKeyNotFound
	}
	if err != nil {
		return auth.Principal{}, fmt.Errorf("lookup api key: %w", err)
	}

	p := auth.Principal{KeyID: id, Scope: auth.Scope(scope)}
	if tenantID != nil {
		p.TenantID = *tenantID
	}
	if plan != nil {
		p.Plan = *plan
	}

	r.touchLastUsed(id)
	return p, nil
}

// touchLastUsed stamps last_used_at asynchronously so the auth hot path
// stays a single synchronous round-trip. The update is throttled to once
// per 5 minutes per key to avoid write amplification on busy keys, and its
// failure never affects the request.
func (r *APIKeyRepository) touchLastUsed(id string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, _ = r.pool.Exec(ctx,
			`UPDATE api_keys
			    SET last_used_at = NOW()
			  WHERE id = $1
			    AND (last_used_at IS NULL OR last_used_at < NOW() - INTERVAL '5 minutes')`,
			id)
	}()
}

// Create persists a new key. The caller supplies the already-hashed secret
// via auth.NewKey; the raw secret never reaches the repository. On success
// the key's ID and CreatedAt are populated from the row.
func (r *APIKeyRepository) Create(ctx context.Context, k *auth.APIKey) error {
	var tenantID *string
	if k.TenantID != "" {
		tenantID = &k.TenantID
	}
	var name *string
	if k.Name != "" {
		name = &k.Name
	}
	err := r.pool.QueryRow(ctx,
		`INSERT INTO api_keys (tenant_id, key_hash, prefix, scope, name)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING id, created_at`,
		tenantID, k.Hash, k.Prefix, string(k.Scope), name,
	).Scan(&k.ID, &k.CreatedAt)
	if err != nil {
		return fmt.Errorf("create api key: %w", err)
	}
	return nil
}

// Revoke soft-deletes a key by ID, returning domain.ErrNotFound if no
// active key with that ID exists. The row is retained (revoked_at set) so
// the audit trail of who-held-access survives.
func (r *APIKeyRepository) Revoke(ctx context.Context, id string) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE api_keys SET revoked_at = NOW()
		  WHERE id = $1 AND revoked_at IS NULL`,
		id)
	if err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return auth.ErrKeyNotFound
	}
	return nil
}

// EnsureAdminKey upserts an admin key by hash — used to bootstrap the first
// operator credential from NEXUS_BOOTSTRAP_ADMIN_KEY. Idempotent: re-running
// with the same secret is a no-op, so restarts don't accumulate rows.
func (r *APIKeyRepository) EnsureAdminKey(ctx context.Context, hash []byte, prefix string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO api_keys (key_hash, prefix, scope, name)
		 VALUES ($1, $2, 'admin', 'bootstrap')
		 ON CONFLICT (key_hash) DO NOTHING`,
		hash, prefix)
	if err != nil {
		return fmt.Errorf("ensure admin key: %w", err)
	}
	return nil
}
