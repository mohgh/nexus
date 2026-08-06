package domain_test

import (
	"context"
	"testing"

	"github.com/mohgh/nexus/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ch01: InMemoryTenantRepository satisfies TenantRepository.
func TestInMemoryTenantRepository(t *testing.T) {
	ctx := context.Background()

	t.Run("empty list", func(t *testing.T) {
		repo := domain.NewInMemoryTenantRepository()
		tenants, err := repo.List(ctx)
		require.NoError(t, err)
		assert.Empty(t, tenants)
	})

	t.Run("create and get", func(t *testing.T) {
		repo := domain.NewInMemoryTenantRepository()
		want := &domain.Tenant{ID: "t1", Name: "Acme", Plan: "pro"}
		require.NoError(t, repo.Create(ctx, want))

		got, err := repo.Get(ctx, "t1")
		require.NoError(t, err)
		assert.Equal(t, want, got)
	})

	t.Run("get missing returns ErrNotFound", func(t *testing.T) {
		repo := domain.NewInMemoryTenantRepository()
		_, err := repo.Get(ctx, "missing")
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("list returns all created tenants", func(t *testing.T) {
		repo := domain.NewInMemoryTenantRepository()
		_ = repo.Create(ctx, &domain.Tenant{ID: "a", Name: "A", Plan: "free"})
		_ = repo.Create(ctx, &domain.Tenant{ID: "b", Name: "B", Plan: "pro"})

		tenants, err := repo.List(ctx)
		require.NoError(t, err)
		assert.Len(t, tenants, 2)
	})

	t.Run("interface compliance", func(t *testing.T) {
		// Compile-time assertion: InMemoryTenantRepository implements TenantRepository.
		var _ domain.TenantRepository = domain.NewInMemoryTenantRepository()
	})
}
