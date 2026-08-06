package projections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/eventstore"
	"go.uber.org/zap"
)

const TenantStatsName = "tenant_event_counts"

// TenantStatsProjection counts events per (tenant, event_type). It's
// the "give me running totals" view — small, fast to query,
// suitable for billing / dashboard.
//
// lastPosition is atomic because the Runner advances it from a
// background goroutine while the /api/v1/projections lag handler
// reads it from the request goroutine. A plain int64 here is racy
// under Go's memory model, even though the value itself is just
// being copied — the race detector will flag it and the read can
// (in principle) see a torn value.
type TenantStatsProjection struct {
	pool         *pgxpool.Pool
	logger       *zap.Logger
	lastPosition atomic.Int64
}

func NewTenantStatsProjection(pool *pgxpool.Pool, logger *zap.Logger) *TenantStatsProjection {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &TenantStatsProjection{pool: pool, logger: logger}
}

var _ Projection = (*TenantStatsProjection)(nil)

func (p *TenantStatsProjection) Name() string        { return TenantStatsName }
func (p *TenantStatsProjection) LastPosition() int64 { return p.lastPosition.Load() }

func (p *TenantStatsProjection) LoadPosition(ctx context.Context) error {
	pos, err := loadPosition(ctx, p.pool, p.Name())
	if err != nil {
		return err
	}
	p.lastPosition.Store(pos)
	return nil
}

// Apply handles one event. EventIngested is the only event type
// this projection cares about; everything else is silently skipped
// so adding new event types in future chapters doesn't break the
// projection.
func (p *TenantStatsProjection) Apply(ctx context.Context, e eventstore.StoredEvent) error {
	if e.EventType != "EventIngested" {
		// Advance the position even on no-op events so a long tail
		// of skipped events doesn't repeatedly re-fetch them.
		p.lastPosition.Store(e.StreamPosition)
		return persistPosition(ctx, p.pool, p.Name(), e.StreamPosition)
	}

	var data struct {
		TenantID  string  `json:"tenant_id"`
		EventType string  `json:"event_type"`
		Value     float64 `json:"value"`
	}
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return fmt.Errorf("tenant_stats: unmarshal: %w", err)
	}

	// Apply + position update in one tx so a crash after the apply
	// but before the position advance doesn't double-count on
	// restart.
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tenant_stats: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx,
		`INSERT INTO tenant_event_counts (tenant_id, event_type, event_count, total_value)
		 VALUES ($1, $2, 1, $3)
		 ON CONFLICT (tenant_id, event_type)
		 DO UPDATE SET
		     event_count = tenant_event_counts.event_count + 1,
		     total_value = tenant_event_counts.total_value + $3`,
		data.TenantID, data.EventType, data.Value,
	); err != nil {
		return fmt.Errorf("tenant_stats: upsert: %w", err)
	}

	if err := upsertPositionInTx(ctx, tx, p.Name(), e.StreamPosition); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tenant_stats: commit: %w", err)
	}
	p.lastPosition.Store(e.StreamPosition)
	return nil
}

func (p *TenantStatsProjection) Reset(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tenant_stats: reset begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `TRUNCATE tenant_event_counts`); err != nil {
		return fmt.Errorf("tenant_stats: truncate: %w", err)
	}
	if err := upsertPositionInTx(ctx, tx, p.Name(), 0); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("tenant_stats: reset commit: %w", err)
	}
	p.lastPosition.Store(0)
	return nil
}

// ─── projection_positions helpers ─────────────────────────────────────────

func loadPosition(ctx context.Context, pool *pgxpool.Pool, name string) (int64, error) {
	var pos int64
	err := pool.QueryRow(ctx,
		`SELECT last_position FROM projection_positions WHERE projection_name = $1`,
		name,
	).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("load position %q: %w", name, err)
	}
	return pos, nil
}

func persistPosition(ctx context.Context, pool *pgxpool.Pool, name string, pos int64) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO projection_positions (projection_name, last_position, updated_at)
		 VALUES ($1, $2, NOW())
		 ON CONFLICT (projection_name)
		 DO UPDATE SET last_position = EXCLUDED.last_position, updated_at = NOW()`,
		name, pos,
	)
	if err != nil {
		return fmt.Errorf("persist position %q: %w", name, err)
	}
	return nil
}

func upsertPositionInTx(ctx context.Context, tx pgx.Tx, name string, pos int64) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO projection_positions (projection_name, last_position, updated_at)
		 VALUES ($1, $2, NOW())
		 ON CONFLICT (projection_name)
		 DO UPDATE SET last_position = EXCLUDED.last_position, updated_at = NOW()`,
		name, pos,
	)
	if err != nil {
		return fmt.Errorf("upsert position %q (tx): %w", name, err)
	}
	return nil
}
