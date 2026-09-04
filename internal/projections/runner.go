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
//
// Contract, and it is the load-bearing part of this package: a
// stream_position is assigned at INSERT time but becomes visible at
// COMMIT time, so "the highest position I can see" is not the same
// as "the highest position that exists below which nothing more can
// appear". An implementation of ReadAllFrom MUST NOT return an event
// whose position sits above a gap that a still-running transaction
// could yet fill — the Runner advances its bookmark to the last event
// it applied, so anything handed over a gap is skipped permanently.
// Returning a short batch (or none) is always allowed and simply
// means "nothing more is safe yet"; the Runner comes back next tick.
type EventReader interface {
	ReadAllFrom(ctx context.Context, after int64, limit int) ([]eventstore.StoredEvent, error)

	// HeadPosition is the raw MAX(stream_position): everything written,
	// including events that are not yet safe to consume.
	HeadPosition(ctx context.Context) (int64, error)

	// SafeHeadPosition is the highest position a consumer sitting at
	// `after` may advance to right now. It is relative to `after`
	// because safety is a property of the run of positions between the
	// consumer and the head, not of the head alone.
	SafeHeadPosition(ctx context.Context, after int64) (int64, error)
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

// catchUp drains as many events as the store is willing to hand
// over, batchSize at a time, from the projection's current position.
//
// Stops when the store returns fewer events than the batch size.
// Note what that does and does not mean: it means "nothing more is
// available to me right now", not "the log is drained". The store
// withholds events that sit above a position a still-uncommitted
// transaction may yet fill (see EventReader), so a sweep can end
// early with visible events left in the table. That is the point —
// the alternative is advancing the bookmark past them forever. The
// gap resolves on a later tick, usually the very next one.
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

// Lag describes how far behind the event store one projection is.
// Used by the admin endpoint to surface "how far behind is each read
// model?"
//
// Two heads, deliberately. HeadPosition is everything written;
// SafeHeadPosition is how far this projection is currently allowed to
// advance. Splitting Lag the same way is what makes a stall
// diagnosable: Lag > 0 with ReadyLag == 0 means the runner is not
// behind at all, it is blocked behind an uncommitted writer holding a
// position — nothing to page anyone about unless it persists. Lag > 0
// with ReadyLag > 0 means the runner genuinely has work outstanding.
//
// Before the visibility fix this endpoint could not say either, because
// the runner would step over the uncommitted writer's position and then
// report a comfortable lag of 0 while the event was lost for good.
type Lag struct {
	ProjectionName string `json:"projection"`
	LastPosition   int64  `json:"last_position"`
	HeadPosition   int64  `json:"head_position"`
	// SafeHeadPosition is the highest position this projection may
	// currently advance to. It trails HeadPosition while a writer holds
	// an uncommitted position below the head.
	SafeHeadPosition int64 `json:"safe_head_position"`
	Lag              int64 `json:"lag"`
	// ReadyLag is the part of Lag the runner can act on right now.
	ReadyLag int64 `json:"ready_lag"`
}

// LagFor reports the lag for each projection at this moment. The
// HeadPosition is read once and reused so the numbers are
// internally consistent (otherwise a projection that just advanced
// while another was being read could show a higher position than
// the head we read earlier — confusing in a single snapshot). The
// safe head, by contrast, is per projection: it depends on where that
// projection is sitting, so it cannot be hoisted out of the loop.
func (r *Runner) LagFor(ctx context.Context) ([]Lag, error) {
	head, err := r.store.HeadPosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("head position: %w", err)
	}
	out := make([]Lag, 0, len(r.projections))
	for _, p := range r.projections {
		last := p.LastPosition()

		safeHead, err := r.store.SafeHeadPosition(ctx, last)
		if err != nil {
			return nil, fmt.Errorf("safe head position: %w", err)
		}
		if safeHead > head {
			// Only reachable if a write landed between the two reads.
			safeHead = head
		}

		out = append(out, Lag{
			ProjectionName:   p.Name(),
			LastPosition:     last,
			HeadPosition:     head,
			SafeHeadPosition: safeHead,
			Lag:              nonNegative(head - last),
			ReadyLag:         nonNegative(safeHead - last),
		})
	}
	return out, nil
}

func nonNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
