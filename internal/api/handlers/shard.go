package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	shardstore "github.com/mohgh/nexus/internal/storage/shard"
)

// ShardMapper routes a tenant ID to a shard index.
// Implemented by shard.Router.
type ShardMapper interface {
	ShardIndex(tenantID string) int
	ShardCount() int
}

// ShardAdminer is the cross-shard admin surface — the things the
// scatter-gather endpoints need. Implemented by *shard.EventRepository.
type ShardAdminer interface {
	Distribution(ctx context.Context) ([]shardstore.ShardStats, error)
	TopTenantsByLoad(ctx context.Context, k int) ([]shardstore.TenantLoad, error)
}

// ShardInfo returns the shard index for a given tenant.
// GET /api/v1/shard?tenant_id=…
//
// Ch07: use this to visualise how your tenants distribute across shards.
// Try different tenant IDs to see the hash-based routing in action.
func ShardInfo(r ShardMapper) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		tenantID := req.URL.Query().Get("tenant_id")
		if tenantID == "" {
			writeError(w, http.StatusBadRequest, "tenant_id is required")
			return
		}
		idx := r.ShardIndex(tenantID)
		writeJSON(w, http.StatusOK, map[string]any{
			"tenant_id":   tenantID,
			"shard_index": idx,
			"shard_count": r.ShardCount(),
			"shard_name":  fmt.Sprintf("nexus_shard%d", idx),
		})
	}
}

// ShardDistribution fans out a COUNT query to every shard and
// returns per-shard tenant + event totals. Lets an operator
// answer "is one shard disproportionately loaded?" in one call.
//
// GET /api/v1/shard/distribution
func ShardDistribution(a ShardAdminer) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		stats, err := a.Distribution(req.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "shard distribution failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"shards": stats,
		})
	}
}

// ShardLoad returns the top-K tenants by event volume across all
// shards. The "celebrity tenant" diagnostic — when one tenant
// dominates a shard's traffic, this surfaces it without needing
// a query plan.
//
// GET /api/v1/shard/load?k=20
func ShardLoad(a ShardAdminer) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		k := 20
		if v := req.URL.Query().Get("k"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
				k = n
			}
		}
		loads, err := a.TopTenantsByLoad(req.Context(), k)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "shard load failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"top_tenants": loads,
			"k":           k,
		})
	}
}
