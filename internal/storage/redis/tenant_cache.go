package redis

import (
	"context"
	"errors"
	"time"

	"github.com/mohgh/nexus/internal/domain"
	"github.com/mohgh/nexus/internal/metrics"
)

// CachedTenantRepository is the Ch04 read-through cache demo. It
// wraps any domain.TenantRepository with a Redis lookup that
// short-circuits the underlying Get. The cache name is "tenants"
// so the Prometheus counters (nexus_cache_hits_total{cache="tenants"}
// etc.) bucket per-cache without a new counter per wrapper.
//
// Reads:
//   * cache hit  → return, record CacheHits.
//   * cache miss → call inner.Get, store on success with TTL, return.
//   * cache lookup error → log via CacheErrors and fall back to
//     inner.Get; a transient Redis outage should not 5xx the API.
//
// Writes:
//   * List bypasses the cache (collection invalidation is messy;
//     the cost/benefit doesn't warrant it for the chapter).
//   * Create bypasses the cache (new tenants have no cached row).
//
// The wrapper preserves errors verbatim — including domain.ErrNotFound
// — because handlers check via errors.Is.
type CachedTenantRepository struct {
	inner   domain.TenantRepository
	cache   *Cache
	metrics *metrics.Registry
	ttl     time.Duration
}

// NewCachedTenantRepository wraps inner with a Redis cache. The
// metrics registry is optional — passing nil disables instrumentation.
// ttl <= 0 defaults to 30s, matching the chapter README claim.
func NewCachedTenantRepository(inner domain.TenantRepository, cache *Cache, m *metrics.Registry, ttl time.Duration) *CachedTenantRepository {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &CachedTenantRepository{inner: inner, cache: cache, metrics: m, ttl: ttl}
}

const cacheName = "tenants"

var _ domain.TenantRepository = (*CachedTenantRepository)(nil)

// Ping delegates to whichever side responds — preferring the inner
// so /ready continues to reflect the upstream's health. (Redis is
// covered by the bare Cache's own Ping.)
func (r *CachedTenantRepository) Ping(ctx context.Context) error {
	type pinger interface {
		Ping(context.Context) error
	}
	if p, ok := r.inner.(pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

func (r *CachedTenantRepository) Get(ctx context.Context, id string) (*domain.Tenant, error) {
	key := "tenant:" + id

	cached, err := Get[*domain.Tenant](ctx, r.cache, key)
	switch {
	case err == nil:
		r.observeHit()
		return cached, nil
	case errors.Is(err, ErrCacheMiss):
		r.observeMiss()
		// Fall through to inner.
	default:
		r.observeError("get")
		// Soft-fail: continue to the inner. The underlying Postgres
		// query is the authoritative source; missing the cache is a
		// performance hit, not a correctness one.
	}

	t, ierr := r.inner.Get(ctx, id)
	if ierr != nil {
		// Do NOT cache negative results — a freshly-created tenant
		// would otherwise return ErrNotFound for up to TTL seconds.
		return nil, ierr
	}

	if setErr := Set(ctx, r.cache, key, t, r.ttl); setErr != nil {
		r.observeError("set")
		// non-fatal
	}
	return t, nil
}

// List passes through; the chapter's lesson is about per-key
// caching, not collection caching (the latter would need explicit
// invalidation logic on Create).
func (r *CachedTenantRepository) List(ctx context.Context) ([]*domain.Tenant, error) {
	return r.inner.List(ctx)
}

// Create passes through. We could defensively delete any cached
// row for this ID, but a new tenant has no cached row by
// definition.
func (r *CachedTenantRepository) Create(ctx context.Context, t *domain.Tenant) error {
	return r.inner.Create(ctx, t)
}

// Invalidate drops a specific tenant from the cache. Wired here
// even though no caller invokes it today — having the operation
// available means a future endpoint that mutates a tenant doesn't
// have to learn the key format.
func (r *CachedTenantRepository) Invalidate(ctx context.Context, id string) error {
	if err := r.cache.Delete(ctx, "tenant:"+id); err != nil {
		r.observeError("delete")
		return err
	}
	return nil
}

func (r *CachedTenantRepository) observeHit() {
	if r.metrics != nil {
		r.metrics.CacheHits.WithLabelValues(cacheName).Inc()
	}
}

func (r *CachedTenantRepository) observeMiss() {
	if r.metrics != nil {
		r.metrics.CacheMisses.WithLabelValues(cacheName).Inc()
	}
}

func (r *CachedTenantRepository) observeError(op string) {
	if r.metrics != nil {
		r.metrics.CacheErrors.WithLabelValues(cacheName, op).Inc()
	}
}
