package shard

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ShardedPool owns one connection pool per shard and routes lookups
// by tenant_id through the Router. This is what makes sharding
// *actually sharding* in Nexus — without it the Router is just
// "routing math" against a single Postgres, which was the audit's
// specific complaint.
//
// One pool per shard means each shard's connection budget is
// independent: a hot shard's pool can be exhausted without
// affecting traffic to other shards. The trade-off is that the
// process holds N×poolSize idle connections; we use a small per-pool
// max (10) to keep the total bounded.
//
// Each shard is a separate Postgres database (nexus_shard0,
// nexus_shard1, ...). For the course they all live on one Postgres
// host; in production each shard would point at its own primary
// (and optionally a replica per Ch06).
type ShardedPool struct {
	router *Router
	pools  []*pgxpool.Pool
}

// NewShardedPool opens one connection pool per shard. The router's
// ShardCount determines how many pools are opened; dsnTemplate
// must be a valid Postgres DSN whose database name will get
// `_shard{N}` appended (the same scheme the Router.ShardDSN uses).
//
// Fail-fast: if any shard's pool cannot be opened, ALL pools that
// were opened so far are closed and the function returns the
// error. Operating with a missing shard would silently lose any
// tenant who hashed to it.
func NewShardedPool(ctx context.Context, router *Router, dsnTemplate string) (*ShardedPool, error) {
	if router == nil {
		return nil, fmt.Errorf("sharded pool: router is required")
	}
	pools := make([]*pgxpool.Pool, router.ShardCount())
	for i := 0; i < router.ShardCount(); i++ {
		dsn := router.ShardDSN(dsnTemplate, i)
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			closeAll(pools[:i])
			return nil, fmt.Errorf("sharded pool: parse shard %d DSN: %w", i, err)
		}
		// Per-shard pool size is small; total across N shards is the
		// process-level budget.
		cfg.MaxConns = 10
		cfg.MinConns = 1
		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			closeAll(pools[:i])
			return nil, fmt.Errorf("sharded pool: open shard %d: %w", i, err)
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			closeAll(pools[:i])
			return nil, fmt.Errorf("sharded pool: ping shard %d: %w", i, err)
		}
		pools[i] = pool
	}
	return &ShardedPool{router: router, pools: pools}, nil
}

func closeAll(pools []*pgxpool.Pool) {
	for _, p := range pools {
		if p != nil {
			p.Close()
		}
	}
}

// Close shuts down every shard's pool.
func (s *ShardedPool) Close() {
	closeAll(s.pools)
}

// ShardCount returns the number of shards.
func (s *ShardedPool) ShardCount() int { return s.router.ShardCount() }

// Router exposes the router so callers can compute shard indices
// without re-hashing.
func (s *ShardedPool) Router() *Router { return s.router }

// For returns the pool that owns tenant_id.
func (s *ShardedPool) For(tenantID string) *pgxpool.Pool {
	return s.pools[s.router.ShardIndex(tenantID)]
}

// ForIndex returns the pool at a specific shard index. Out-of-range
// indices return nil — callers that fan out across all shards
// should iterate via Pools() instead.
func (s *ShardedPool) ForIndex(i int) *pgxpool.Pool {
	if i < 0 || i >= len(s.pools) {
		return nil
	}
	return s.pools[i]
}

// Pools returns the pool slice indexed by shard number. The caller
// must not mutate it. Used by the scatter-gather helpers.
func (s *ShardedPool) Pools() []*pgxpool.Pool {
	out := make([]*pgxpool.Pool, len(s.pools))
	copy(out, s.pools)
	return out
}

// Ping checks every shard. The first failing shard's error wins,
// but every shard is pinged — a missing shard is the most important
// thing the /ready probe can surface.
func (s *ShardedPool) Ping(ctx context.Context) error {
	for i, p := range s.pools {
		if err := p.Ping(ctx); err != nil {
			return fmt.Errorf("sharded pool: ping shard %d: %w", i, err)
		}
	}
	return nil
}
