package stream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Flusher copies closed window aggregates from Redis to PostgreSQL
// and deletes the Redis hash once the row is durable.
//
// Why this exists: Redis is the right place to hold *live* window
// state — it's cheap to increment a hash field per event — but the
// wrong place to hold *historical* analytics. Hashes don't survive a
// Redis flush, and SCAN is the wrong query for "show me last week's
// hourly counts." The flusher gives us the read/write split: hot
// path writes to Redis at sub-millisecond cost, cold path queries
// Postgres with proper indexing.
//
// Concurrency model: a single Flusher.Run goroutine per process. In
// a multi-instance deploy this would want leader-elected ownership
// (Ch10 fencing tokens would fit here cleanly) — for now the
// idempotent INSERT ... ON CONFLICT keeps the at-least-once
// double-flush safe.
type Flusher struct {
	redis        *redis.Client
	pg           *pgxpool.Pool
	aggregators  []*WindowAggregator
	pollInterval time.Duration
	scanBatch    int64
	logger       *zap.Logger

	flushed uint64
	errors  uint64
}

// FlusherConfig bundles the optional knobs. Zero values resolve to
// sensible defaults.
type FlusherConfig struct {
	PollInterval time.Duration // default: 30s
	ScanBatch    int64         // default: 100
}

// NewFlusher wires the flusher. aggregators are the windows whose
// keyspaces the flusher will sweep — typically one each for 1m and
// 1h, both backed by the same Redis client.
func NewFlusher(redisClient *redis.Client, pg *pgxpool.Pool, aggregators []*WindowAggregator, logger *zap.Logger, cfg FlusherConfig) *Flusher {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if cfg.ScanBatch <= 0 {
		cfg.ScanBatch = 100
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Flusher{
		redis:        redisClient,
		pg:           pg,
		aggregators:  aggregators,
		pollInterval: cfg.PollInterval,
		scanBatch:    cfg.ScanBatch,
		logger:       logger,
	}
}

// FlushedCount returns how many rows have been written to Postgres
// since startup. Useful for tests and metrics.
func (f *Flusher) FlushedCount() uint64 { return f.flushed }

// Run loops until ctx is cancelled, sweeping every aggregator's
// keyspace on each tick.
func (f *Flusher) Run(ctx context.Context) error {
	f.logger.Info("flusher: starting",
		zap.Duration("poll_interval", f.pollInterval),
		zap.Int("aggregators", len(f.aggregators)),
	)
	defer f.logger.Info("flusher: stopped",
		zap.Uint64("flushed_total", f.flushed),
		zap.Uint64("errors_total", f.errors),
	)

	// Bootstrap each aggregator's watermark from Redis BEFORE the
	// first sweep. Without this, IsClosed would return false for
	// every existing on-time key (in-memory watermark = 0) and
	// stranded buckets from the previous process would sit unflushed
	// until their TTL expired. LoadWatermark errors are warnings,
	// not fatal — operating without bootstrap is degraded (stranded
	// buckets won't flush until new traffic advances the watermark)
	// but still correct for live traffic.
	for _, agg := range f.aggregators {
		if err := agg.LoadWatermark(ctx); err != nil {
			f.logger.Warn("flusher: bootstrap watermark failed",
				zap.String("duration", durationLabel(agg.Duration())),
				zap.Error(err),
			)
		}
	}

	ticker := time.NewTicker(f.pollInterval)
	defer ticker.Stop()

	// Sweep immediately on startup so a backlog left from a previous
	// process gets drained without waiting for the first tick.
	f.sweep(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			f.sweep(ctx)
		}
	}
}

func (f *Flusher) sweep(ctx context.Context) {
	for _, agg := range f.aggregators {
		if ctx.Err() != nil {
			return
		}
		pattern := bucketScanPattern(agg.Duration())
		if err := f.sweepPattern(ctx, agg, pattern); err != nil {
			f.errors++
			f.logger.Warn("flusher: sweep failed",
				zap.String("pattern", pattern),
				zap.Error(err),
			)
		}
	}
}

// bucketScanPattern returns the SCAN glob the flusher uses for an
// aggregator. The trailing ":*:*" excludes the watermark key
// (window:{dur}:watermark, only one segment after the duration);
// bucket keys always have at least the tenant + event_type + bucket
// suffix, so they need two more colons. Without this exclusion the
// flusher would feed the watermark key into parseWindowKey on every
// sweep and log a "malformed window key" warning forever.
func bucketScanPattern(duration time.Duration) string {
	return fmt.Sprintf("window:%s:*:*", durationLabel(duration))
}

func (f *Flusher) sweepPattern(ctx context.Context, agg *WindowAggregator, pattern string) error {
	var cursor uint64
	for {
		if ctx.Err() != nil {
			return nil
		}
		keys, next, err := f.redis.Scan(ctx, cursor, pattern, f.scanBatch).Result()
		if err != nil {
			return fmt.Errorf("scan %q: %w", pattern, err)
		}
		for _, key := range keys {
			if err := f.tryFlushKey(ctx, agg, key); err != nil {
				f.errors++
				f.logger.Warn("flusher: flush key failed",
					zap.String("key", key),
					zap.Error(err),
				)
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return nil
}

// keyInfo describes a parsed window key. The "late" sentinel comes
// in as IsLate=true with WindowStart zeroed; the flusher uses the
// current time as window_start for late aggregates so they can be
// stored as rows alongside on-time ones without colliding.
type keyInfo struct {
	TenantID    string
	EventType   string
	IsLate      bool
	WindowStart time.Time
}

// parseWindowKey parses a key of the form
// "window:{dur}:{tenant}:{eventType}:{bucketUnix|late}". Returns an
// error if the shape doesn't match.
func parseWindowKey(key string) (keyInfo, error) {
	parts := strings.Split(key, ":")
	if len(parts) != 5 || parts[0] != "window" {
		return keyInfo{}, fmt.Errorf("malformed window key %q", key)
	}
	out := keyInfo{
		TenantID:  parts[2],
		EventType: parts[3],
	}
	if parts[4] == "late" {
		out.IsLate = true
		return out, nil
	}
	unix, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return keyInfo{}, fmt.Errorf("malformed bucket suffix %q: %w", parts[4], err)
	}
	out.WindowStart = time.Unix(unix, 0).UTC()
	return out, nil
}

// tryFlushKey reads a single window key and, if the window is
// considered closed, writes it to Postgres and deletes from Redis.
//
// Crucially, the read-write-delete is not atomic: between HGETALL
// and DEL, a late event could race in and increment the bucket. We
// accept that — the late event would land in a re-incremented Redis
// hash that the *next* sweep picks up, and the ON CONFLICT clause
// makes the second write idempotent (it overwrites). The window has
// already been declared closed by the watermark, so events arriving
// in this race window are themselves late and should land in the
// late bucket — there's an unavoidable corner here that the
// allowed_lateness setting is meant to minimise.
func (f *Flusher) tryFlushKey(ctx context.Context, agg *WindowAggregator, key string) error {
	info, err := parseWindowKey(key)
	if err != nil {
		// Garbage key in the namespace — skip and don't delete.
		return err
	}

	if !info.IsLate && !agg.IsClosed(info.WindowStart) {
		// Not yet ready to flush. Keep it in Redis.
		return nil
	}

	fields, err := f.redis.HMGet(ctx, key, "count", "sum").Result()
	if err != nil {
		return fmt.Errorf("hmget: %w", err)
	}
	count, sum := parseCountSum(fields)
	if count == 0 && sum == 0 {
		// Empty key — probably evicted by TTL between SCAN and
		// HMGET. Delete and move on.
		_ = f.redis.Del(ctx, key).Err()
		return nil
	}

	windowStart := info.WindowStart
	if info.IsLate {
		// The late bucket is a single key per (tenant, event_type)
		// — there is no natural window_start. Use the flush time so
		// each flush of the late bucket lands in its own row.
		windowStart = time.Now().UTC().Truncate(time.Second)
	}

	if err := f.upsert(ctx, info.TenantID, info.EventType, durationLabel(agg.Duration()),
		windowStart, count, sum, info.IsLate); err != nil {
		return fmt.Errorf("upsert: %w", err)
	}

	// Delete the bucket hash now that its Postgres row is durable.
	//
	// We DO NOT delete the companion :applied set here — that's the
	// fix for the "redelivery after flush" failure mode. Kafka's
	// retention is much longer than a single bucket's lifetime, so
	// a redelivery that arrives after the bucket was flushed would
	// otherwise create a brand-new bucket hash from a stale event,
	// and the next sweep would UPSERT that count over the correct
	// one in tenant_window_stats. Keeping the :applied set alive
	// until its natural TTL (4*duration + 2*lateness) means the
	// re-Add finds the event_id, returns 0, and the bucket is never
	// recreated.
	//
	// Memory cost: each :applied set carries the event_ids of its
	// bucket for the TTL window. For a 1m bucket at N events/min,
	// the set holds ~N entries and self-deletes ~5 minutes after
	// flush. For 1h buckets the window is ~4.5h.
	//
	// Residual exposure: redeliveries arriving AFTER the :applied
	// set's TTL has expired are still possible to overcount.
	// Closing this window completely would require a persistent
	// event_id store outside Redis (Postgres or a dedicated
	// dedup service) — a CQRS pattern that crosses the storage
	// boundary on the hot path. Documented in the Ch12 README as
	// the bounded-vs-unbounded dedup trade-off.
	if err := f.redis.Del(ctx, key).Err(); err != nil {
		// We've already written the row — a re-flush on the next
		// sweep will land idempotently via ON CONFLICT, so this is
		// not fatal. Surface as a warning at the caller.
		return fmt.Errorf("del after upsert: %w", err)
	}

	f.flushed++
	return nil
}

func parseCountSum(fields []any) (int64, float64) {
	var count int64
	var sum float64
	if v, ok := fields[0].(string); ok {
		_, _ = fmt.Sscanf(v, "%d", &count)
	}
	if v, ok := fields[1].(string); ok {
		_, _ = fmt.Sscanf(v, "%f", &sum)
	}
	return count, sum
}

// upsert writes one aggregate row, idempotent under at-least-once
// re-flushing.
func (f *Flusher) upsert(ctx context.Context, tenantID, eventType, durLabel string, windowStart time.Time, count int64, sum float64, isLate bool) error {
	_, err := f.pg.Exec(ctx,
		`INSERT INTO tenant_window_stats
		 (tenant_id, event_type, window_duration, window_start, event_count, sum_value, is_late, flushed_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
		 ON CONFLICT (tenant_id, event_type, window_duration, window_start, is_late)
		 DO UPDATE SET event_count = EXCLUDED.event_count,
		               sum_value = EXCLUDED.sum_value,
		               flushed_at = NOW()`,
		tenantID, eventType, durLabel, windowStart, count, sum, isLate,
	)
	if err != nil {
		return err
	}
	return nil
}

// ErrFlusherShuttingDown is returned from Run if a future fatal
// condition is added. Kept as a sentinel so callers don't have to
// special-case error messages.
var ErrFlusherShuttingDown = errors.New("flusher: shutting down")
