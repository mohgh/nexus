package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/mohgh/nexus/internal/domain"
)

// EventRepository stores and retrieves events from PostgreSQL.
//
// Ch03 introduced this on top of a single `events` table. Ch06 made
// reads replica-aware. Ch13 promotes the append-only `events_store`
// table to source-of-truth status: every Create now writes BOTH
// rows in a single transaction. `events_store` carries the canonical
// immutable record; `events` is treated as the synchronous primary
// projection used by the fast read paths (List, Search). Other
// projections — tenant_event_counts, daily_event_counts — catch up
// asynchronously off `events_store`.
//
// The dual-write is in one transaction by design: it would be the
// classic "dual write" antipattern only if the two destinations
// were different storage systems with no joint commit. Both
// destinations here are the same Postgres, so the local transaction
// gives us atomic dual-write for free.
type EventRepository struct {
	pool *ReplicaPool
}

func NewEventRepository(pool *ReplicaPool) *EventRepository {
	return &EventRepository{pool: pool}
}

// Compile-time assertion.
var _ domain.EventRepository = (*EventRepository)(nil)

// Create writes the event to both the canonical events_store and the
// fast-read events projection in a single Postgres transaction.
// After commit, the post-write WAL LSN is recorded so the Ch06 RYOW
// header threading still works.
func (r *EventRepository) Create(ctx context.Context, e *domain.Event) error {
	primary := r.pool.Primary()

	// Encode the event-sourced payload up front so the transaction
	// is held only for the SQL round-trips.
	storePayload, err := json.Marshal(map[string]any{
		"tenant_id":  e.TenantID,
		"event_type": e.EventType,
		"payload":    e.Payload,
		"value":      e.Value,
		"id":         e.ID,
	})
	if err != nil {
		return fmt.Errorf("create event: marshal store payload: %w", err)
	}
	streamName := "tenant-" + e.TenantID

	tx, err := primary.Begin(ctx)
	if err != nil {
		return fmt.Errorf("create event: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// events_store FIRST — it's the canonical record. If the
	// projection write below were to fail, we'd want to surface the
	// error and rollback both; that's what the tx is for.
	if _, err := tx.Exec(ctx,
		`INSERT INTO events_store (stream_name, event_type, data, metadata, occurred_at)
		 VALUES ($1, 'EventIngested', $2, '{}'::jsonb, $3)`,
		streamName, storePayload, e.OccurredAt,
	); err != nil {
		return fmt.Errorf("create event: events_store insert: %w", err)
	}

	// events: the synchronous primary projection — populated in the
	// same tx so the existing fast read paths see the row as soon as
	// IngestEvent returns 201.
	if _, err := tx.Exec(ctx,
		`INSERT INTO events (id, tenant_id, event_type, payload, value, occurred_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		e.ID, e.TenantID, e.EventType, []byte(e.Payload), e.Value, e.OccurredAt,
	); err != nil {
		return fmt.Errorf("create event: events insert: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("create event: commit: %w", err)
	}

	// Ch06: capture post-commit LSN so RYOW header threading still
	// works for this code path. The dedicated method on ReplicaPool
	// keeps the LSN-recording logic in one place.
	r.pool.RecordPostWriteLSN(ctx)
	return nil
}

func (r *EventRepository) ListByTenant(ctx context.Context, tenantID string, limit int) ([]*domain.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, event_type, payload, value, occurred_at
		 FROM events
		 WHERE tenant_id = $1
		 ORDER BY occurred_at DESC
		 LIMIT $2`,
		tenantID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()
	return scanEvents(rows)
}

// Search does a case-insensitive substring match against the JSONB payload
// text representation. The events_payload_trgm index (GIN + pg_trgm) makes
// this fast for short queries, but it degrades for high-cardinality payloads.
//
// Ch03 teaching point: this works, but it's a hack. Elasticsearch (Ch04)
// is the right tool for full-text search.
func (r *EventRepository) Search(ctx context.Context, tenantID, query string, limit int) ([]*domain.Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, event_type, payload, value, occurred_at
		 FROM events
		 WHERE tenant_id = $1
		   AND payload::text ILIKE '%' || $2 || '%'
		 ORDER BY occurred_at DESC
		 LIMIT $3`,
		tenantID, query, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("search events: %w", err)
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
			return nil, fmt.Errorf("scan event: %w", err)
		}
		e.Payload = json.RawMessage(payload)
		out = append(out, e)
	}
	return out, rows.Err()
}
