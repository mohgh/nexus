// Package shard implements tenant-based horizontal sharding for Nexus.
//
// Ch07 teaching point: sharding moves data onto multiple nodes by splitting
// the keyspace. Nexus uses tenant_id as the shard key — all data for a
// tenant lives on one shard. This gives us:
//   - Simple cross-table JOINs within a tenant (no cross-shard joins needed)
//   - Natural isolation boundary for multi-tenancy
//   - Predictable, stable routing (no resharding unless tenant count explodes)
//
// The downside: tenants cannot be moved between shards without a migration,
// and a large tenant ("whale") can create a hot shard.
package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
)

// Router maps a tenant ID to a shard index deterministically.
// The same tenant always lands on the same shard — no external state needed.
type Router struct {
	shardCount int
}

// NewRouter creates a shard router with the given number of shards.
// Changing shardCount requires a full data migration — choose it conservatively.
func NewRouter(shardCount int) (*Router, error) {
	if shardCount < 1 {
		return nil, fmt.Errorf("shard: count must be >= 1, got %d", shardCount)
	}
	return &Router{shardCount: shardCount}, nil
}

// ShardIndex returns the shard (0-indexed) that owns the given tenant.
//
// Algorithm: SHA-256(tenantID) → take first 8 bytes as uint64 → mod shardCount.
// SHA-256 gives uniform distribution; mod gives the index.
//
// Why not MD5 or CRC32? SHA-256 has better avalanche properties and is
// constant-time (no timing side-channels on tenant IDs). The cost is
// negligible — this runs on every request, but SHA-256 on a UUID takes ~80ns.
func (r *Router) ShardIndex(tenantID string) int {
	h := sha256.Sum256([]byte(tenantID))
	n := binary.BigEndian.Uint64(h[:8])
	return int(n % uint64(r.shardCount))
}

// ShardDSN builds a DSN for the given shard index by mutating the
// database-name path segment of the template DSN.
//
// Example:
//
//	template: "postgres://nexus:secret@localhost:5432/nexus?sslmode=disable"
//	shard 2:  "postgres://nexus:secret@localhost:5432/nexus_shard2?sslmode=disable"
//
// (An earlier version simply concatenated the suffix onto the raw
// DSN string, which produced "...?sslmode=disable_shard2" — broken.
// The URL-aware parse + mutate + format chain handles trailing
// query strings, multiple path segments, missing dbnames, etc.)
//
// In production each shard would point to a different host. For
// the course, all shards run on the same Postgres instance with
// different databases.
func (r *Router) ShardDSN(template string, index int) string {
	u, err := url.Parse(template)
	if err != nil {
		// Fall back to the naive suffix path. This shouldn't happen
		// for valid DSNs but keeps the function total.
		return fmt.Sprintf("%s_shard%d", template, index)
	}
	// u.Path is "/dbname" for a standard postgres URL; "" if the
	// DSN omitted the database. Append the suffix to the dbname
	// portion regardless.
	dbname := strings.TrimPrefix(u.Path, "/")
	if dbname == "" {
		dbname = "nexus" // sensible default — matches the project's standard name
	}
	u.Path = "/" + dbname + fmt.Sprintf("_shard%d", index)
	return u.String()
}

// ShardCount returns the total number of shards.
func (r *Router) ShardCount() int {
	return r.shardCount
}

// Distribution returns a map of shardIndex → list of tenantIDs for the given
// set of tenant IDs. Useful for visualising how tenants spread across shards.
func (r *Router) Distribution(tenantIDs []string) map[int][]string {
	dist := make(map[int][]string, r.shardCount)
	for _, id := range tenantIDs {
		idx := r.ShardIndex(id)
		dist[idx] = append(dist[idx], id)
	}
	return dist
}
