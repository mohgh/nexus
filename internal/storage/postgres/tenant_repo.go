package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/mohgh/nexus/internal/domain"
)

// TenantRepository is the Ch03 replacement for domain.InMemoryTenantRepository.
// It implements domain.TenantRepository against PostgreSQL.
// The interface is identical — the switch in main.go is a one-liner.
//
// Ch06: backed by ReplicaPool so writes go to the primary (recording the
// resulting WAL LSN) and reads route to the replica when it has caught up.
type TenantRepository struct {
	pool *ReplicaPool
}

func NewTenantRepository(pool *ReplicaPool) *TenantRepository {
	return &TenantRepository{pool: pool}
}

// Compile-time assertion: TenantRepository satisfies domain.TenantRepository.
var _ domain.TenantRepository = (*TenantRepository)(nil)

// Ping delegates to the underlying pool. Used by the /ready health probe.
func (r *TenantRepository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

func (r *TenantRepository) List(ctx context.Context) ([]*domain.Tenant, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, name, plan, created_at, updated_at
		 FROM tenants
		 ORDER BY created_at DESC`,
	)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	var out []*domain.Tenant
	for rows.Next() {
		t := &domain.Tenant{}
		if err := rows.Scan(&t.ID, &t.Name, &t.Plan, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r *TenantRepository) Get(ctx context.Context, id string) (*domain.Tenant, error) {
	t := &domain.Tenant{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, name, plan, created_at, updated_at
		 FROM tenants WHERE id = $1`,
		id,
	).Scan(&t.ID, &t.Name, &t.Plan, &t.CreatedAt, &t.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("tenant %q: %w", id, domain.ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("get tenant: %w", err)
	}
	return t, nil
}

func (r *TenantRepository) Create(ctx context.Context, t *domain.Tenant) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO tenants (id, name, plan, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		t.ID, t.Name, t.Plan, t.CreatedAt, t.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create tenant: %w", err)
	}
	return nil
}
