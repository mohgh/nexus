package projections

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/eventstore"
	"go.uber.org/zap"
)

const DailyStatsName = "daily_event_counts"

// DailyEventCountsProjection is the time-bucketed companion to
// TenantStatsProjection. Same input event (EventIngested), different
// shape: rolled up to (tenant, date, event_type). The chapter uses
// the pair to demonstrate that multiple projections coexist from
// the same event log, each optimised for a different query.
//
// Query examples:
//
//	-- 30-day trend for one tenant:
//	SELECT event_date, event_type, event_count
//	FROM daily_event_counts
//	WHERE tenant_id = $1 AND event_date >= CURRENT_DATE - 30
//	ORDER BY event_date DESC;
//
// lastPosition is atomic — see TenantStatsProjection for the same
// rationale (Runner writes from a goroutine, lag handler reads).
type DailyEventCountsProjection struct {
	pool         *pgxpool.Pool
	logger       *zap.Logger
	lastPosition atomic.Int64
}

func NewDailyEventCountsProjection(pool *pgxpool.Pool, logger *zap.Logger) *DailyEventCountsProjection {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DailyEventCountsProjection{pool: pool, logger: logger}
}

var _ Projection = (*DailyEventCountsProjection)(nil)

func (p *DailyEventCountsProjection) Name() string        { return DailyStatsName }
func (p *DailyEventCountsProjection) LastPosition() int64 { return p.lastPosition.Load() }

func (p *DailyEventCountsProjection) LoadPosition(ctx context.Context) error {
	pos, err := loadPosition(ctx, p.pool, p.Name())
	if err != nil {
		return err
	}
	p.lastPosition.Store(pos)
	return nil
}

func (p *DailyEventCountsProjection) Apply(ctx context.Context, e eventstore.StoredEvent) error {
	if e.EventType != "EventIngested" {
		p.lastPosition.Store(e.StreamPosition)
		return persistPosition(ctx, p.pool, p.Name(), e.StreamPosition)
	}

	var data struct {
		TenantID  string  `json:"tenant_id"`
		EventType string  `json:"event_type"`
		Value     float64 `json:"value"`
	}
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return fmt.Errorf("daily_stats: unmarshal: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("daily_stats: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Bucket by the event-store row's occurred_at (the same time the
	// event was recorded with). DATE() truncates to the calendar day
	// in UTC, matching the event_store column.
	if _, err := tx.Exec(ctx,
		`INSERT INTO daily_event_counts (tenant_id, event_date, event_type, event_count, total_value)
		 VALUES ($1, $2::date, $3, 1, $4)
		 ON CONFLICT (tenant_id, event_date, event_type)
		 DO UPDATE SET
		     event_count = daily_event_counts.event_count + 1,
		     total_value = daily_event_counts.total_value + $4`,
		data.TenantID, e.OccurredAt, data.EventType, data.Value,
	); err != nil {
		return fmt.Errorf("daily_stats: upsert: %w", err)
	}

	if err := upsertPositionInTx(ctx, tx, p.Name(), e.StreamPosition); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("daily_stats: commit: %w", err)
	}
	p.lastPosition.Store(e.StreamPosition)
	return nil
}

func (p *DailyEventCountsProjection) Reset(ctx context.Context) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("daily_stats: reset begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if _, err := tx.Exec(ctx, `TRUNCATE daily_event_counts`); err != nil {
		return fmt.Errorf("daily_stats: truncate: %w", err)
	}
	if err := upsertPositionInTx(ctx, tx, p.Name(), 0); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("daily_stats: reset commit: %w", err)
	}
	p.lastPosition.Store(0)
	return nil
}
