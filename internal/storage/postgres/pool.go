// Package postgres provides pgx-backed repository implementations.
//
// Ch03: tenant + event repositories.
// Ch06: read-replica pool with LSN-aware routing.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool wraps pgxpool.Pool. Embedding gives callers access to the full pgx
// API (Exec, Query, QueryRow, BeginTx, …) while letting us attach helpers.
type Pool struct {
	*pgxpool.Pool
}

// NewPool opens a pgxpool connection to the given DSN and pings it.
// The caller should defer pool.Close().
//
// Connection limits are tuned for a small single-node deployment;
// Ch06 revisits these when we add a read replica.
func NewPool(ctx context.Context, dsn string) (*Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}

	// Ch03: conservative pool settings — enough for local dev + load tests.
	// Ch08 revisits these when transaction contention becomes measurable.
	cfg.MaxConns = 25
	cfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	return &Pool{pool}, nil
}
