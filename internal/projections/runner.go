package projections

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/mohgh/nexus/internal/eventstore"
	"go.uber.org/zap"
)

// Runner drives a set of projections forward against an event store.
// One Runner per process. On each tick, it asks each projection for
// its last position and feeds it any new events from the store; the
// projection applies them and persists its new position. Projections
// catch up independently — a slow one doesn't hold back the others.
//
// In a multi-instance deploy a single Runner per role (leader-elected
// via Ch10) is the typical setup; without leader election multiple
// runners would safely double-apply via the upsert path but would
// waste work. We don't gate on leader election here — the chapter's
// lesson lives in the catch-up loop, not the deployment topology.
// EventReader is the slice of *eventstore.Store the Runner uses.
// Extracting it as an interface lets tests inject a fake store
// without standing up Postgres. Production wires the concrete
// *eventstore.Store; nothing in the Runner's hot path touches
// anything outside this interface.
type EventReader interface {
	ReadAllFrom(ctx context.Context, after int64, limit int) ([]eventstore.StoredEvent, error)
	HeadPosition(ctx context.Context) (int64, error)
}

type Runner struct {
	store        EventReader
	projections  []Projection
	pollInterval time.Duration
	batchSize    int
	logger       *zap.Logger

	eventsApplied uint64
}

// Config bundles the optional knobs.
type Config struct {
	PollInterval time.Duration // default 1s
	BatchSize    int           // default 500
}

func NewRunner(store EventReader, ps []Projection, logger *zap.Logger, cfg Config) *Runner {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 500
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Runner{
		store:        store,
		projections:  ps,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
		logger:       logger,
	}
}

// EventsApplied returns a process-lifetime counter of (projection,
// event) pairs that have been applied. Two projections seeing the
// same event count as two here.
func (r *Runner) EventsApplied() uint64 {
	return atomic.LoadUint64(&r.eventsApplied)
}

// Run loops until ctx is cancelled. On startup, every projection's
// position is loaded from projection_positions so a restart resumes
// where the previous process left off.
func (r *Runner) Run(ctx context.Context) error {
	r.logger.Info("projection runner: starting",
		zap.Int("projections", len(r.projections)),
		zap.Duration("poll_interval", r.pollInterval),
	)
	defer r.logger.Info("projection runner: stopped",
		zap.Uint64("events_applied_total", r.EventsApplied()),
	)

	for _, p := range r.projections {
		if err := p.LoadPosition(ctx); err != nil {
			r.logger.Warn("projection runner: load position failed (will start from 0)",
				zap.String("projection", p.Name()),
				zap.Error(err),
			)
		} else {
			r.logger.Info("projection runner: position loaded",
				zap.String("projection", p.Name()),
				zap.Int64("position", p.LastPosition()),
			)
		}
	}

	// Sweep once immediately so a backlog from a previous process
	// gets caught up without waiting for the first tick.
	r.sweep(ctx)

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			r.sweep(ctx)
		}
	}
}

func (r *Runner) sweep(ctx context.Context) {
	for _, p := range r.projections {
		if ctx.Err() != nil {
			return
		}
		if err := r.catchUp(ctx, p); err != nil {
			r.logger.Warn("projection runner: catch up failed",
				zap.String("projection", p.Name()),
				zap.Int64("position", p.LastPosition()),
				zap.Error(err),
			)
		}
	}
}

// catchUp drains as many events as available, batchSize at a time,
// from the projection's current position. Stops when the store
// returns fewer events than the batch size — that means we've
// drained.
func (r *Runner) catchUp(ctx context.Context, p Projection) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		events, err := r.store.ReadAllFrom(ctx, p.LastPosition(), r.batchSize)
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		if len(events) == 0 {
			return nil
		}
		for _, e := range events {
			if err := p.Apply(ctx, e); err != nil {
				// Stop this projection's catch-up for this sweep —
				// don't advance past an event we failed to apply.
				return fmt.Errorf("apply event %d: %w", e.StreamPosition, err)
			}
			atomic.AddUint64(&r.eventsApplied, 1)
		}
		if len(events) < r.batchSize {
			return nil
		}
	}
}

// Lag returns the difference between the event store's current
// highest position and each projection's position. Used by the
// admin endpoint to surface "how far behind is each read model?"
type Lag struct {
	ProjectionName string `json:"projection"`
	LastPosition   int64  `json:"last_position"`
	HeadPosition   int64  `json:"head_position"`
	Lag            int64  `json:"lag"`
}

// LagFor reports the lag for each projection at this moment. The
// HeadPosition is read once and reused so the numbers are
// internally consistent (otherwise a projection that just advanced
// while another was being read could show a higher position than
// the head we read earlier — confusing in a single snapshot).
func (r *Runner) LagFor(ctx context.Context) ([]Lag, error) {
	head, err := r.store.HeadPosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("head position: %w", err)
	}
	out := make([]Lag, 0, len(r.projections))
	for _, p := range r.projections {
		last := p.LastPosition()
		lag := head - last
		if lag < 0 {
			lag = 0
		}
		out = append(out, Lag{
			ProjectionName: p.Name(),
			LastPosition:   last,
			HeadPosition:   head,
			Lag:            lag,
		})
	}
	return out, nil
}
