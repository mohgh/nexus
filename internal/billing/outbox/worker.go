// Package outbox implements the transactional outbox pattern for billing events.
//
// Ch08 teaching point: 2PC between Postgres and Kafka is expensive and
// fragile (the prepared-transactions coordinator is a single point of
// failure). The outbox pattern sidesteps the problem entirely:
//
//  1. The application writes to billing_records and effectively to the
//     "outbox" (the same row, with outbox_sent_at = NULL) in a single
//     local Postgres transaction. Atomic by construction.
//
//  2. A separate worker — this package — polls billing_records WHERE
//     outbox_sent_at IS NULL, publishes each to Kafka, and marks the
//     row sent.
//
//  3. The worker provides at-least-once delivery: a crash between
//     publish and mark replays the publish next sweep. Consumers must
//     therefore be idempotent (Ch12 covers this).
//
// Compared to dual-writes (write to Postgres, then write to Kafka in
// the handler) this guarantees no event is ever lost — there is no
// window where Postgres has the record but Kafka does not.
package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/mohgh/nexus/internal/domain"
	"go.uber.org/zap"
)

// Publisher delivers a single billing record to its downstream
// destination (typically a Kafka topic). Returning a non-nil error
// causes the worker to leave the row unmarked so the next sweep
// retries it. Implementations must be safe to call concurrently and
// must never partially deliver — either the record is durably accepted
// downstream (return nil) or it is not (return error).
type Publisher interface {
	Publish(ctx context.Context, rec *domain.BillingRecord) error
}

// Config controls worker timing. Zero values resolve to sensible
// defaults — the worker is meant to be drop-in.
type Config struct {
	// PollInterval is how often the worker scans for unsent records.
	// Default 2s. The trade-off is publish latency vs. database load.
	PollInterval time.Duration

	// BatchSize caps how many records are loaded per sweep. Default 100.
	BatchSize int

	// PublishTimeout bounds each individual publish call so a stuck
	// broker can't pin the worker forever. Default 10s.
	PublishTimeout time.Duration
}

func (c *Config) defaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 2 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
}

// Worker polls the outbox table and publishes unsent records.
//
// Concurrency model: a single Worker.Run goroutine per process. In a
// multi-instance deployment, only one instance should run the worker
// at a time — Ch10 wraps it in leader election to enforce that. With
// the worker leaderless, two instances racing on the same row both
// call Publish (at-least-once is preserved) and one of the MarkOutboxSent
// calls is a redundant SET. Acceptable but wasteful.
type Worker struct {
	repo      domain.BillingRepository
	publisher Publisher
	cfg       Config
	logger    *zap.Logger

	// Stats — read via Stats(); intended for tests and observability.
	published uint64
	failed    uint64
}

// New constructs a Worker. Either repo or publisher being nil is a
// programming error and panics fast — the worker is never useful
// without both.
func New(repo domain.BillingRepository, publisher Publisher, logger *zap.Logger, cfg Config) *Worker {
	if repo == nil || publisher == nil {
		panic("outbox.New: repo and publisher are required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg.defaults()
	return &Worker{repo: repo, publisher: publisher, cfg: cfg, logger: logger}
}

// Stats holds counters useful for tests and logs.
type Stats struct {
	Published uint64
	Failed    uint64
}

// Stats returns the current counters. Not safe for concurrent updates
// (it's a read of two non-atomic uint64s) but fine for end-of-test
// checks where the worker has stopped.
func (w *Worker) Stats() Stats {
	return Stats{Published: w.published, Failed: w.failed}
}

// Run loops until ctx is cancelled, sweeping the outbox each tick.
//
// Returns nil on clean shutdown (ctx cancelled). A return value other
// than nil indicates the worker hit a non-recoverable error — currently
// not produced by this loop, but reserved for future fatal conditions
// (e.g. publisher reports a permanent config error).
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("outbox worker: starting",
		zap.Duration("poll_interval", w.cfg.PollInterval),
		zap.Int("batch_size", w.cfg.BatchSize),
	)
	defer w.logger.Info("outbox worker: stopped",
		zap.Uint64("published_total", w.published),
		zap.Uint64("failed_total", w.failed),
	)

	// Run a sweep immediately on startup so a backlog left over from
	// a previous process gets drained without waiting for the first tick.
	w.sweep(ctx)

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

// sweep drains one batch of pending records.
//
// On a publish error we log and move on — the row stays unmarked
// (outbox_sent_at IS NULL) and gets retried on the next sweep. We do
// NOT mark it sent on failure: that would lose the event. We also do
// NOT abort the whole batch on a single error: one bad record should
// not block delivery for healthy ones.
func (w *Worker) sweep(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	pending, err := w.repo.PendingOutbox(ctx, w.cfg.BatchSize)
	if err != nil {
		w.logger.Warn("outbox worker: load pending failed",
			zap.Error(err),
		)
		return
	}
	if len(pending) == 0 {
		return
	}

	for _, rec := range pending {
		if ctx.Err() != nil {
			return
		}
		w.publishOne(ctx, rec)
	}
}

func (w *Worker) publishOne(ctx context.Context, rec *domain.BillingRecord) {
	pubCtx, cancel := context.WithTimeout(ctx, w.cfg.PublishTimeout)
	defer cancel()

	if err := w.publisher.Publish(pubCtx, rec); err != nil {
		// Don't mark sent on failure — leave it for retry on next sweep.
		// At-least-once delivery means downstream consumers must dedupe.
		w.failed++
		w.logger.Warn("outbox worker: publish failed",
			zap.String("record_id", rec.ID),
			zap.String("tenant_id", rec.TenantID),
			zap.Error(err),
		)
		return
	}

	if err := w.repo.MarkOutboxSent(ctx, rec.ID); err != nil {
		// Publish succeeded but we couldn't mark the row sent. Next
		// sweep will republish — that's the at-least-once contract.
		// Surfacing this to the consumer is expected and correct.
		w.failed++
		w.logger.Warn("outbox worker: mark sent failed (record will be republished)",
			zap.String("record_id", rec.ID),
			zap.Error(err),
		)
		return
	}

	w.published++
	w.logger.Debug("outbox worker: published",
		zap.String("record_id", rec.ID),
		zap.String("tenant_id", rec.TenantID),
	)
}

// ErrPublisherDisabled is returned by NoopPublisher.Publish.
//
// NoopPublisher is occasionally useful in tests where you want to
// observe the worker's polling behavior without delivering anything.
var ErrPublisherDisabled = errors.New("outbox: publisher disabled")

// NoopPublisher returns ErrPublisherDisabled on every call. Records
// are left unmarked, which means the worker keeps trying on every
// sweep — useful for verifying retry behavior under failure.
type NoopPublisher struct{}

func (NoopPublisher) Publish(context.Context, *domain.BillingRecord) error {
	return ErrPublisherDisabled
}
