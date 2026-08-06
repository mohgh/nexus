package shard_test

import (
	"fmt"
	"testing"

	"github.com/mohgh/nexus/internal/storage/shard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_Deterministic(t *testing.T) {
	r, err := shard.NewRouter(4)
	require.NoError(t, err)

	tenantID := "550e8400-e29b-41d4-a716-446655440000"
	idx1 := r.ShardIndex(tenantID)
	idx2 := r.ShardIndex(tenantID)

	assert.Equal(t, idx1, idx2, "same tenant must always map to the same shard")
	assert.GreaterOrEqual(t, idx1, 0)
	assert.Less(t, idx1, 4)
}

func TestRouter_Distribution(t *testing.T) {
	// With 1000 tenants and 4 shards, each shard should hold ~25% ± 5%.
	// This verifies SHA-256 gives a uniform distribution.
	r, err := shard.NewRouter(4)
	require.NoError(t, err)

	tenants := make([]string, 1000)
	for i := range tenants {
		tenants[i] = generateTenantID(i)
	}

	dist := r.Distribution(tenants)
	assert.Len(t, dist, 4, "all 4 shards should receive tenants")

	for idx, ids := range dist {
		pct := float64(len(ids)) / float64(len(tenants)) * 100
		assert.InDelta(t, 25.0, pct, 5.0,
			"shard %d holds %.1f%% of tenants (expected ~25%%)", idx, pct)
	}
}

func TestRouter_InvalidShardCount(t *testing.T) {
	_, err := shard.NewRouter(0)
	assert.Error(t, err)
}

// TestRouter_ShardDSN_PathMutation is the regression test for the
// earlier ShardDSN bug. The original implementation appended
// "_shardN" to the raw DSN string, producing
// "postgres://...?sslmode=disable_shard2" — broken because the
// suffix landed in the query string, not the database name. The
// fix parses the URL, appends to the path segment, and re-renders.
func TestRouter_ShardDSN_PathMutation(t *testing.T) {
	r, err := shard.NewRouter(4)
	require.NoError(t, err)

	cases := []struct {
		name     string
		template string
		index    int
		want     string
	}{
		{
			name:     "standard postgres DSN with sslmode",
			template: "postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable",
			index:    2,
			want:     "postgres://nexus:nexus_secret@localhost:5432/nexus_shard2?sslmode=disable",
		},
		{
			name:     "no query string",
			template: "postgres://nexus:nexus_secret@localhost:5432/nexus",
			index:    0,
			want:     "postgres://nexus:nexus_secret@localhost:5432/nexus_shard0",
		},
		{
			name:     "missing database name defaults to nexus",
			template: "postgres://nexus:nexus_secret@localhost:5432/?sslmode=disable",
			index:    3,
			want:     "postgres://nexus:nexus_secret@localhost:5432/nexus_shard3?sslmode=disable",
		},
		{
			name:     "different host port preserved",
			template: "postgres://user:pw@db.internal:6543/nexus?application_name=nexus&sslmode=require",
			index:    1,
			want:     "postgres://user:pw@db.internal:6543/nexus_shard1?application_name=nexus&sslmode=require",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := r.ShardDSN(c.template, c.index)
			assert.Equal(t, c.want, got)
		})
	}
}

// generateTenantID produces a reproducible tenant ID string for testing.
func generateTenantID(n int) string {
	return fmt.Sprintf("tenant-%06d", n)
}
