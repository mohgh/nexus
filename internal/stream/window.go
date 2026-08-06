// Ch12: tumbling window aggregation for real-time event counting.
//
// Two ideas that the audit found missing in the prior version and
// this file now implements:
//
//   1. Event time, not processing time. The prior version bucketed by
//      time.Now(), which means a late-arriving event landed in the
//      window where it was processed, not the window when it happened.
//      That makes the analytics wrong: a page view at 13:59:59 that
//      arrives at 14:00:01 should count in the 13:59 minute, not 14:00.
//      We now bucket by event.OccurredAt.
//
//   2. Watermark + allowed lateness for stragglers. The aggregator
//      tracks the highest event_time it has ever seen (the "high
//      watermark"). An event whose event_time is older than
//      watermark - allowedLateness is treated as "late" and counted
//      in a separate bucket — the on-time aggregate is not corrupted
//      by stragglers arriving after the window has been considered
//      closed. The DDIA Ch12 watermark discussion is the source.
//
// The aggregator is duration-parametric: the stream processor
// instantiates one per window width (1m and 1h are the chapter's
// claims; one is no harder than two).
package stream

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// WindowDuration is kept as the 1-minute default for callers that
// don't pass a duration explicitly. New code should use NewWindow
// with an explicit duration instead.
const WindowDuration = 1 * time.Minute

// DefaultAllowedLateness is how far behind the high watermark an
// event can arrive and still be counted in its true window. Beyond
// this it goes to the late bucket.
const DefaultAllowedLateness = 30 * time.Second

// WindowAggregator maintains per-tenant, per-event-type counts in
// Redis, bucketed by event-time. The same type implements both the
// 1-minute and 1-hour windows — instantiate one of each.
//
// The watermark is per-aggregator (one watermark for the 1m
// aggregator, one for the 1h aggregator) and global across tenants.
// Holding it per-tenant would be more precise (a quiet tenant
// wouldn't drag everyone's watermark forward) but is overkill for
// the chapter and complicates the late-event story.
//
// The watermark is held in process memory (atomic.Int64) AND
// persisted to a Redis key (`window:{duration}:watermark`) so a
// restart picks up where the previous process left off. Without
// persistence, a restart would reset the watermark to zero and
// stranded Redis buckets from the previous process would never be
// flushed on a quiet stream — they'd just TTL out and the data
// would be lost. The Redis write is one round-trip per Add, batched
// into the same pipeline as the bucket increment.
type WindowAggregator struct {
	client          *redis.Client
	duration        time.Duration
	allowedLateness time.Duration
	logger          *zap.Logger

	highWatermark atomic.Int64 // event_time of latest event seen, as unix nanos
}

// NewWindow constructs an aggregator with the given window width.
// Pass DefaultAllowedLateness for the lateness arg unless you have
// a reason to differ.
func NewWindow(client *redis.Client, duration, allowedLateness time.Duration, logger *zap.Logger) *WindowAggregator {
	if logger == nil {
		logger = zap.NewNop()
	}
	if allowedLateness <= 0 {
		allowedLateness = DefaultAllowedLateness
	}
	return &WindowAggregator{
		client:          client,
		duration:        duration,
		allowedLateness: allowedLateness,
		logger:          logger,
	}
}

// NewTumblingWindow keeps the prior call site working with the same
// semantics: a 1-minute window with the default allowed lateness.
// Prefer NewWindow in new code.
func NewTumblingWindow(client *redis.Client, logger *zap.Logger) *WindowAggregator {
	return NewWindow(client, WindowDuration, DefaultAllowedLateness, logger)
}

// Duration returns the window width — useful for the flusher loop
// which needs to know when a window is closed.
func (w *WindowAggregator) Duration() time.Duration { return w.duration }

// HighWatermark returns the latest event_time ever observed, in UTC.
// Returns the zero Time if no events have been Add'd yet.
func (w *WindowAggregator) HighWatermark() time.Time {
	n := w.highWatermark.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

// Add records one event in the appropriate window bucket. The bucket
// is determined by event_time, NOT processing time. Events older
// than (high_watermark - allowedLateness) go to a side "late" bucket
// so they don't quietly corrupt the on-time aggregate after the
// window has been considered closed.
//
// Key formats:
//   - On time: "window:{duration}:{tenantID}:{eventType}:{bucketUnix}"
//   - Late:    "window:{duration}:{tenantID}:{eventType}:late"
//
// **Idempotent on eventID.** The Kafka consumer is at-least-once;
// any retry (process crash between handler success and offset commit,
// or an error from a later sink that re-runs the handler) replays the
// same event. The increment is gated by a per-bucket "applied" SET —
// a second call with an event_id already in the set is a no-op.
// Without this, redeliveries overcounted the window aggregates: the
// Ch04 fail-closed ClickHouse fix made this previously-latent bug
// reach production behaviour.
//
// The watermark is advanced first (in memory and in Redis) so a
// concurrent reader of the persisted watermark sees the new max as
// soon as the event lands.
func (w *WindowAggregator) Add(ctx context.Context, tenantID, eventType, eventID string, value float64, eventTime time.Time) error {
	eventTime = eventTime.UTC()
	if eventTime.IsZero() {
		return fmt.Errorf("window: Add: eventTime is zero")
	}
	if eventID == "" {
		return fmt.Errorf("window: Add: eventID is empty (needed for idempotency)")
	}

	// Advance the high watermark monotonically. We do this BEFORE the
	// lateness check so the watermark reflects the latest arrival,
	// even when this event itself turns out to be on-time. A late
	// event does NOT advance the watermark — it can't, because the
	// watermark is the max event_time seen.
	w.advanceWatermark(eventTime)
	if err := w.persistWatermark(ctx, eventTime); err != nil {
		// Persistence failure should not block ingest — the in-memory
		// watermark is still correct for this process. Log and continue.
		w.logger.Warn("window: persist watermark failed",
			zap.Error(err),
		)
	}

	if w.isLate(eventTime) {
		key := lateKey(w.duration, tenantID, eventType)
		w.logger.Debug("window: late event routed to late bucket",
			zap.String("tenant_id", tenantID),
			zap.String("event_type", eventType),
			zap.Time("event_time", eventTime),
			zap.Time("high_watermark", w.HighWatermark()),
		)
		return w.incrementKey(ctx, key, eventID, value)
	}

	bucketStart := eventTime.Truncate(w.duration)
	key := bucketKey(w.duration, tenantID, eventType, bucketStart)
	return w.incrementKey(ctx, key, eventID, value)
}

// WindowSnapshot is what Snapshot returns: both the event count and
// the sum of event values for the bucket. Counts are useful for "how
// many page views?"; sums are useful for "what was the total
// purchase amount?". Storing both costs one extra hash field per
// bucket.
type WindowSnapshot struct {
	Count int64
	Sum   float64
}

// Snapshot returns the on-time aggregate for the window covering the
// given event-time on this tenant/event_type. Returns a zero
// snapshot if no events have landed (key absent).
func (w *WindowAggregator) Snapshot(ctx context.Context, tenantID, eventType string, atEventTime time.Time) (WindowSnapshot, error) {
	bucketStart := atEventTime.UTC().Truncate(w.duration)
	key := bucketKey(w.duration, tenantID, eventType, bucketStart)
	return w.readSnapshot(ctx, key)
}

// LateSnapshot returns the aggregate for events routed to the late
// bucket since startup. Zero snapshot if nothing has been late.
func (w *WindowAggregator) LateSnapshot(ctx context.Context, tenantID, eventType string) (WindowSnapshot, error) {
	key := lateKey(w.duration, tenantID, eventType)
	return w.readSnapshot(ctx, key)
}

func (w *WindowAggregator) readSnapshot(ctx context.Context, key string) (WindowSnapshot, error) {
	res, err := w.client.HMGet(ctx, key, "count", "sum").Result()
	if err != nil {
		return WindowSnapshot{}, err
	}
	var snap WindowSnapshot
	if v, ok := res[0].(string); ok {
		_, _ = fmt.Sscanf(v, "%d", &snap.Count)
	}
	if v, ok := res[1].(string); ok {
		_, _ = fmt.Sscanf(v, "%f", &snap.Sum)
	}
	return snap, nil
}

// IsClosed reports whether the bucket starting at bucketStart should
// be considered no longer accepting events. Used by the flusher to
// decide which buckets are safe to persist and delete.
//
// A bucket is closed when:
//
//	high_watermark >= bucket_start + duration + allowed_lateness
//
// i.e., enough event-time has passed that any straggler would now
// land in the late bucket, not this one.
func (w *WindowAggregator) IsClosed(bucketStart time.Time) bool {
	wm := w.HighWatermark()
	if wm.IsZero() {
		return false
	}
	return !wm.Before(bucketStart.Add(w.duration).Add(w.allowedLateness))
}

func (w *WindowAggregator) advanceWatermark(eventTime time.Time) {
	candidate := eventTime.UnixNano()
	for {
		current := w.highWatermark.Load()
		if candidate <= current {
			return
		}
		if w.highWatermark.CompareAndSwap(current, candidate) {
			return
		}
	}
}

// persistWatermarkScript atomically MAX-updates the Redis watermark
// key. Redis has no native MAX-on-string, so we do the compare-and-set
// in Lua to avoid a TOCTOU race between two stream-processor instances.
var persistWatermarkScript = redis.NewScript(`
local cur = tonumber(redis.call("GET", KEYS[1])) or 0
local cand = tonumber(ARGV[1])
if cand > cur then
    redis.call("SET", KEYS[1], ARGV[1])
end
return cand
`)

// persistWatermark MAX-updates the persisted watermark. The value is
// stored as event_time unix nanoseconds, the same encoding the
// in-memory atomic uses, so reads and writes round-trip exactly.
//
// We deliberately do NOT short-circuit when in-memory > eventTime —
// a different process could have advanced the persisted watermark
// past our in-memory view, and we still want to participate in
// updates from this Add. The Lua script's MAX makes both sides safe.
func (w *WindowAggregator) persistWatermark(ctx context.Context, eventTime time.Time) error {
	key := watermarkKey(w.duration)
	_, err := persistWatermarkScript.Run(ctx, w.client,
		[]string{key},
		eventTime.UnixNano(),
	).Result()
	if err != nil {
		return fmt.Errorf("persistWatermark: %w", err)
	}
	return nil
}

// LoadWatermark reads the persisted watermark from Redis and
// installs it as the in-memory watermark, taking max with whatever
// is already there. Call once at startup (the flusher constructor
// does this for each aggregator it owns) so a restart picks up where
// the previous process left off — without this, stranded buckets
// would sit in Redis until their TTL expired and the flusher would
// never see them on a quiet stream.
func (w *WindowAggregator) LoadWatermark(ctx context.Context) error {
	key := watermarkKey(w.duration)
	val, err := w.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		// No prior watermark — nothing to do.
		return nil
	}
	if err != nil {
		return fmt.Errorf("LoadWatermark: %w", err)
	}
	for {
		current := w.highWatermark.Load()
		if val <= current {
			return nil
		}
		if w.highWatermark.CompareAndSwap(current, val) {
			return nil
		}
	}
}

// PersistedWatermark returns the watermark as currently stored in
// Redis, or the zero Time if none. Used by the SSE handler so it
// can pick the bucket the stream processor is actually writing to
// rather than the wall-clock bucket (the two can disagree on
// boundaries and around late arrivals).
func (w *WindowAggregator) PersistedWatermark(ctx context.Context) (time.Time, error) {
	key := watermarkKey(w.duration)
	val, err := w.client.Get(ctx, key).Int64()
	if err == redis.Nil {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, val).UTC(), nil
}

// PersistedWatermarkForDuration is a thin helper for callers that
// only have a *redis.Client (the SSE handler) and don't want to
// construct a full aggregator just to read the watermark.
func PersistedWatermarkForDuration(ctx context.Context, client *redis.Client, duration time.Duration) (time.Time, error) {
	val, err := client.Get(ctx, watermarkKey(duration)).Int64()
	if err == redis.Nil {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, val).UTC(), nil
}

func watermarkKey(duration time.Duration) string {
	return fmt.Sprintf("window:%s:watermark", durationLabel(duration))
}

func (w *WindowAggregator) isLate(eventTime time.Time) bool {
	wm := w.HighWatermark()
	if wm.IsZero() {
		return false
	}
	// Use After/Before semantics for clarity over arithmetic on times.
	threshold := wm.Add(-w.allowedLateness)
	return eventTime.Before(threshold)
}

// incrementScript is the idempotent bucket-increment + applied-set
// update + TTL refresh, all atomic in one Lua call.
//
//   KEYS[1] = bucket hash       e.g. "window:1m:T:click:1234"
//   KEYS[2] = applied SET       e.g. "window:1m:T:click:1234:applied"
//   ARGV[1] = event_id
//   ARGV[2] = value (float string)
//   ARGV[3] = ttl seconds
//
// Returns 1 if the event was newly applied, 0 if it was a duplicate
// (already in the applied set). Callers can ignore the return value;
// it's primarily for tests and observability.
var incrementScript = redis.NewScript(`
if redis.call("SISMEMBER", KEYS[2], ARGV[1]) == 1 then
    return 0
end
redis.call("SADD", KEYS[2], ARGV[1])
redis.call("EXPIRE", KEYS[2], ARGV[3])
redis.call("HINCRBY", KEYS[1], "count", 1)
redis.call("HINCRBYFLOAT", KEYS[1], "sum", ARGV[2])
redis.call("EXPIRE", KEYS[1], ARGV[3])
return 1
`)

func (w *WindowAggregator) incrementKey(ctx context.Context, key, eventID string, value float64) error {
	ttl := int(((4*w.duration + 2*w.allowedLateness) / time.Second).Round(time.Second).Seconds())
	if ttl <= 0 {
		ttl = 60
	}
	_, err := incrementScript.Run(ctx, w.client,
		[]string{key, key + ":applied"},
		eventID, value, ttl,
	).Int()
	if err != nil {
		return fmt.Errorf("window: incr %s: %w", key, err)
	}
	return nil
}

// AppliedSetKey returns the Redis key used to track applied event IDs
// for the given bucket. Exported so the flusher can delete it
// alongside the bucket on flush — leaving it would slowly bloat
// Redis memory across a long-running deployment.
func AppliedSetKey(bucketKey string) string { return bucketKey + ":applied" }

func bucketKey(duration time.Duration, tenantID, eventType string, bucketStart time.Time) string {
	return fmt.Sprintf("window:%s:%s:%s:%d",
		durationLabel(duration), tenantID, eventType, bucketStart.Unix())
}

func lateKey(duration time.Duration, tenantID, eventType string) string {
	return fmt.Sprintf("window:%s:%s:%s:late",
		durationLabel(duration), tenantID, eventType)
}

// durationLabel returns a short human-readable tag for a window
// duration — used in Redis keys and the Postgres window_duration
// column so an operator can tell aggregators apart at a glance.
func durationLabel(d time.Duration) string {
	switch d {
	case time.Minute:
		return "1m"
	case time.Hour:
		return "1h"
	default:
		return d.String()
	}
}
