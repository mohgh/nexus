package shard

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mohgh/nexus/internal/domain"
	"golang.org/x/sync/errgroup"
)

// EventRepository is the shard-aware events store. It implements
// domain.EventRepository by routing each call to the pool that
// owns the tenant. Reads (ListByTenant, Search) hit exactly one
// shard; writes (Create) hit exactly one shard; cross-tenant
// aggregations go through ScatterGather (admin path).
//
// This is what makes Nexus *actually* sharded rather than just
// "the router computes an index nobody uses." The audit's Ch07
// findings called out the difference; this type is the answer.
type EventRepository struct {
	pool *ShardedPool
}

// NewEventRepository wraps a ShardedPool. The repository satisfies
// the same domain.EventRepository interface as the single-Postgres
// repo, so handlers don't need to know they're talking to a
// sharded backend.
func NewEventRepository(pool *ShardedPool) *EventRepository {
	return &EventRepository{pool: pool}
}

var _ domain.EventRepository = (*EventRepository)(nil)

// Create writes to the shard owning the event's tenant. Both
// events_store (the canonical immutable log) AND events (the
// synchronous primary projection) are written in one shard-local
// transaction — matching the single-Postgres EventRepository.Create
// contract from Ch13.
//
// Without the events_store write the canonical log goes dark in
// sharded mode: projection runs would derive nothing, projection
// rebuilds would produce empty read models, audit-style queries
// against the log would miss every sharded write. The audit flagged
// this as a "drop-in replacement that drops a contract."
//
// One shard's tx covers both writes; cross-shard atomicity isn't
// needed because a single tenant's events all live on one shard.
//
// Caveat documented in the chapter README: the Ch13 projection
// runner and idempotency middleware are wired against the CENTRAL
// Postgres pool, not per shard. In sharded mode those layers
// operate on an empty events_store; making them shard-aware is
// follow-up work for Ch07 v2 / Ch13 v2. The canonical-log
// completeness side that the audit cared about IS preserved here —
// each shard's events_store carries the full history of its
// tenants' events.
func (r *EventRepository) Create(ctx context.Context, e *domain.Event) error {
	if e.TenantID == "" {
		return fmt.Errorf("shard events: Create: tenant_id is required")
	}
	shardIdx := r.pool.Router().ShardIndex(e.TenantID)
	pool := r.pool.For(e.TenantID)

	storePayload, err := json.Marshal(map[string]any{
		"tenant_id":  e.TenantID,
		"event_type": e.EventType,
		"payload":    e.Payload,
		"value":      e.Value,
		"id":         e.ID,
	})
	if err != nil {
		return fmt.Errorf("shard events: marshal store payload: %w", err)
	}
	streamName := "tenant-" + e.TenantID

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("shard events: begin tx on shard %d: %w", shardIdx, err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Lazy tenant materialisation. The events table's tenant_id is a
	// FK on tenants(id) so the shard must have a matching row before
	// any event for that tenant can land. The canonical tenants
	// table lives only on the central nexus DB (the tenant repo
	// isn't shard-aware), so without this UPSERT every first-time
	// event for a tenant on a shard fails with FK violation.
	//
	// Production approaches: replicate tenants via CDC (Ch12), or
	// drop the FK and enforce in app code. For the educational
	// system we accept a stub row — name/plan are placeholders that
	// the API never reads back (reads go through the central tenant
	// repo). The audit's "shipped Ch07 fails first ingest after a
	// fresh setup" gap.
	if _, err := tx.Exec(ctx,
		`INSERT INTO tenants (id, name, plan, created_at, updated_at)
		 VALUES ($1, 'sharded-stub', 'free', NOW(), NOW())
		 ON CONFLICT (id) DO NOTHING`,
		e.TenantID,
	); err != nil {
		return fmt.Errorf("shard events: tenant upsert on shard %d: %w", shardIdx, err)
	}

	// events_store FIRST — same ordering as the single-Postgres
	// repository. If the projection write below fails, we want both
	// rolled back together.
	if _, err := tx.Exec(ctx,
		`INSERT INTO events_store (stream_name, event_type, data, metadata, occurred_at)
		 VALUES ($1, 'EventIngested', $2, '{}'::jsonb, $3)`,
		streamName, storePayload, e.OccurredAt,
	); err != nil {
		return fmt.Errorf("shard events: events_store insert on shard %d: %w", shardIdx, err)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO events (id, tenant_id, event_type, payload, value, occurred_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		e.ID, e.TenantID, e.EventType, []byte(e.Payload), e.Value, e.OccurredAt,
	); err != nil {
		return fmt.Errorf("shard events: events insert on shard %d: %w", shardIdx, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("shard events: commit on shard %d: %w", shardIdx, err)
	}
	return nil
}

// ListByTenant routes by tenant_id; the query touches exactly one
// shard. The Ch07 lesson on "tenant-scoped queries don't need
// fan-out" lives here.
func (r *EventRepository) ListByTenant(ctx context.Context, tenantID string, limit int) ([]*domain.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	pool := r.pool.For(tenantID)
	rows, err := pool.Query(ctx,
		`SELECT id, tenant_id, event_type, payload, value, occurred_at
		 FROM events
		 WHERE tenant_id = $1
		 ORDER BY occurred_at DESC
		 LIMIT $2`,
		tenantID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("shard events: list: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// Search runs the same JSONB-substring query the Ch03 repo does,
// scoped to one shard via the tenant_id parameter.
func (r *EventRepository) Search(ctx context.Context, tenantID, query string, limit int) ([]*domain.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	pool := r.pool.For(tenantID)
	rows, err := pool.Query(ctx,
		`SELECT id, tenant_id, event_type, payload, value, occurred_at
		 FROM events
		 WHERE tenant_id = $1
		   AND payload::text ILIKE '%' || $2 || '%'
		 ORDER BY occurred_at DESC
		 LIMIT $3`,
		tenantID, query, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("shard events: search: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

func scanEvents(rows pgx.Rows) ([]*domain.Event, error) {
	var out []*domain.Event
	for rows.Next() {
		e := &domain.Event{}
		var payload []byte
		if err := rows.Scan(&e.ID, &e.TenantID, &e.EventType, &payload, &e.Value, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("shard events: scan: %w", err)
		}
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	return out, rows.Err()
}

// ─── Admin / cross-shard helpers ─────────────────────────────────────────

// ShardStats summarises one shard's contents for the admin
// distribution endpoint. Returned by Distribution.
type ShardStats struct {
	Shard      int   `json:"shard"`
	Tenants    int64 `json:"tenants"`
	Events     int64 `json:"events"`
	LastEvent  *time.Time `json:"last_event,omitempty"`
}

// Distribution fans out a COUNT query to every shard in parallel
// and returns per-shard tenant + event counts. This is the
// scatter-gather pattern the audit asked for: tenant-scoped
// queries hit one shard, but cross-tenant admin queries need to
// touch them all.
//
// errgroup limits errors to the first one and cancels in-flight
// queries. We could return partial results on failure instead,
// but for an admin view "show me every shard or fail" is the
// right contract — partial counts are usually worse than no
// answer (operator might miss that a shard was down).
func (r *EventRepository) Distribution(ctx context.Context) ([]ShardStats, error) {
	g, gctx := errgroup.WithContext(ctx)
	results := make([]ShardStats, r.pool.ShardCount())
	for i, pool := range r.pool.Pools() {
		i, pool := i, pool
		g.Go(func() error {
			row := pool.QueryRow(gctx,
				`SELECT
				   (SELECT COUNT(DISTINCT tenant_id) FROM events),
				   (SELECT COUNT(*) FROM events),
				   (SELECT MAX(occurred_at) FROM events)`,
			)
			var tenants, events int64
			var last *time.Time
			if err := row.Scan(&tenants, &events, &last); err != nil {
				return fmt.Errorf("shard %d: %w", i, err)
			}
			results[i] = ShardStats{
				Shard:     i,
				Tenants:   tenants,
				Events:    events,
				LastEvent: last,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// TenantLoad pairs a tenant with its event count for the load
// endpoint.
type TenantLoad struct {
	TenantID string `json:"tenant_id"`
	Shard    int    `json:"shard"`
	Events   int64  `json:"events"`
}

// TopTenantsByLoad fans out to every shard, collects the top-K
// tenants per shard by event count, merges the results, and
// returns the global top-K.
//
// The merge is naive: take K from each of N shards (NK candidates)
// then pick the top K. For NK small this is fine; for very large
// shard counts you'd want a streaming heap merge.
//
// The shard column lets an operator answer "is one shard
// disproportionately loaded?" without a second query — the
// "celebrity tenant" diagnostic the chapter discusses.
func (r *EventRepository) TopTenantsByLoad(ctx context.Context, k int) ([]TenantLoad, error) {
	if k <= 0 || k > 1000 {
		k = 50
	}

	g, gctx := errgroup.WithContext(ctx)
	type shardLoad struct {
		shard int
		loads []TenantLoad
	}
	resultsCh := make(chan shardLoad, r.pool.ShardCount())
	for i, pool := range r.pool.Pools() {
		i, pool := i, pool
		g.Go(func() error {
			rows, err := pool.Query(gctx,
				`SELECT tenant_id, COUNT(*) AS n
				 FROM events
				 GROUP BY tenant_id
				 ORDER BY n DESC
				 LIMIT $1`,
				k,
			)
			if err != nil {
				return fmt.Errorf("shard %d: %w", i, err)
			}
			defer rows.Close()

			var local []TenantLoad
			for rows.Next() {
				var tl TenantLoad
				if err := rows.Scan(&tl.TenantID, &tl.Events); err != nil {
					return fmt.Errorf("shard %d scan: %w", i, err)
				}
				tl.Shard = i
				local = append(local, tl)
			}
			if err := rows.Err(); err != nil {
				return fmt.Errorf("shard %d rows: %w", i, err)
			}
			resultsCh <- shardLoad{shard: i, loads: local}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	close(resultsCh)

	var merged []TenantLoad
	for sl := range resultsCh {
		merged = append(merged, sl.loads...)
	}
	sort.Slice(merged, func(i, j int) bool { return merged[i].Events > merged[j].Events })
	if len(merged) > k {
		merged = merged[:k]
	}
	return merged, nil
}

// concurrencyHelper is unused but kept as a comment anchor for
// future enhancements: a wider scatter-gather framework would
// live here.
var _ = sync.WaitGroup{}
