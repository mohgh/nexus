//go:build integration

// Live-Redis integration tests for the watermark persistence and the
// restart-flushability fix. The unit tests in window_test.go cover the
// pure in-memory logic; these cover the parts that talk to Redis.
//
// Run via:
//
//	REDIS_DSN=redis://localhost:6379/0 \
//	    go test -tags=integration -v ./internal/stream/...

package stream_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/stream"
	"github.com/redis/go-redis/v9"
)

// Existing Add calls in older Adds got promoted via the new
// event_id arg; we pre-declare a small helper so the rest of the
// tests can pass a stable empty-ish event_id. Real production
// callers always pass a real ID (UUIDs from domain.Event).
//
// Update for the integration tests: every Add now requires an
// event_id; we generate one per call via fmt.Sprintf below.

func openRedis(t *testing.T) *redis.Client {
	t.Helper()
	dsn := os.Getenv("REDIS_DSN")
	if dsn == "" {
		t.Skip("REDIS_DSN not set; skipping integration test")
	}
	opt, err := redis.ParseURL(dsn)
	if err != nil {
		t.Fatalf("parse REDIS_DSN: %v", err)
	}
	client := redis.NewClient(opt)
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// freshDuration returns a unique duration for the test, plus
// cleanup that wipes the matching Redis keys. We can't easily
// segregate test runs by duration label (1m/1h are hard-coded), so
// instead we use a unique duration that produces an unused label,
// and we clean up by SCAN+DEL on test completion.
func cleanupKeys(t *testing.T, client *redis.Client, durationLabel string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		pattern := fmt.Sprintf("window:%s:*", durationLabel)
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				return
			}
			if len(keys) > 0 {
				_ = client.Del(ctx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
	})
}

// TestWindow_WatermarkPersistsAcrossRestart is the regression test
// for the audit's finding: a fresh aggregator constructed after a
// "process restart" must be able to recover the previous process's
// watermark from Redis so it can flush stranded historical buckets.
func TestWindow_WatermarkPersistsAcrossRestart(t *testing.T) {
	client := openRedis(t)
	ctx := context.Background()

	// Use a non-standard duration so the test doesn't collide with a
	// real running stream-processor on the same Redis instance.
	const dur = 23 * time.Second
	durationLabelLocal := dur.String() // "23s" — matches durationLabel fallback
	cleanupKeys(t, client, durationLabelLocal)

	// First process: ingest a few events, all in the same bucket.
	procA := stream.NewWindow(client, dur, 5*time.Second, nil)
	bucketTime := time.Now().UTC().Truncate(dur)
	if err := procA.Add(ctx, "tenant-X", "page_view", "evt-restart-1", 1, bucketTime.Add(time.Second)); err != nil {
		t.Fatalf("procA Add 1: %v", err)
	}
	maxEventTime := bucketTime.Add(5 * time.Second)
	if err := procA.Add(ctx, "tenant-X", "page_view", "evt-restart-2", 1, maxEventTime); err != nil {
		t.Fatalf("procA Add 2: %v", err)
	}

	// Stand up a second aggregator instance — this is "process B,
	// just started, in-memory watermark = 0." If LoadWatermark
	// works, calling it should pull the persisted value into B's
	// in-memory atomic so IsClosed agrees with procA's worldview.
	procB := stream.NewWindow(client, dur, 5*time.Second, nil)
	if !procB.HighWatermark().IsZero() {
		t.Fatalf("fresh aggregator should have zero watermark, got %v", procB.HighWatermark())
	}

	if err := procB.LoadWatermark(ctx); err != nil {
		t.Fatalf("LoadWatermark: %v", err)
	}

	got := procB.HighWatermark()
	if !got.Equal(maxEventTime) {
		t.Fatalf("after LoadWatermark: got %v, want %v", got, maxEventTime)
	}

	// And critically: a bucket from procA's run that is now stale
	// (bucketStart + duration + lateness < watermark) must be
	// considered closed by procB, so the flusher will pick it up.
	if !procB.IsClosed(bucketTime) {
		t.Fatalf("after LoadWatermark, a historical bucket must be IsClosed (watermark=%v, bucket=%v, duration=%v)",
			procB.HighWatermark(), bucketTime, dur)
	}
}

// TestWindow_PersistedWatermarkUpdatedMonotonically — concurrent
// Add()s from "two processes" sharing the same Redis must end with
// the Redis watermark equal to the max event_time observed across
// both processes, never decreasing.
func TestWindow_PersistedWatermarkUpdatedMonotonically(t *testing.T) {
	client := openRedis(t)
	ctx := context.Background()

	const dur = 29 * time.Second
	cleanupKeys(t, client, dur.String())

	proc := stream.NewWindow(client, dur, 5*time.Second, nil)
	t0 := time.Now().UTC()

	// Burst of events, intentionally out of order.
	events := []time.Time{
		t0,
		t0.Add(5 * time.Second),
		t0.Add(2 * time.Second),  // older
		t0.Add(10 * time.Second), // new max
		t0.Add(7 * time.Second),  // older
		t0.Add(1 * time.Second),  // older
	}
	maxTime := t0.Add(10 * time.Second)

	for i, et := range events {
		eid := fmt.Sprintf("evt-mono-%d-%d", t0.UnixNano(), i)
		if err := proc.Add(ctx, "tenant-W", "click", eid, 1, et); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	gotPersisted, err := stream.PersistedWatermarkForDuration(ctx, client, dur)
	if err != nil {
		t.Fatalf("PersistedWatermarkForDuration: %v", err)
	}
	if !gotPersisted.Equal(maxTime) {
		t.Fatalf("persisted watermark: got %v, want %v (must be max across all writes)", gotPersisted, maxTime)
	}
}

// TestWindow_IdempotentOnEventID is the regression test for the
// audit's finding that the fail-closed ClickHouse sink turned a
// latent window non-idempotency into actual overcounting. The
// Kafka consumer is at-least-once; any retry (crash between
// handler success and offset commit, or any later sink that
// returns an error) replays the same event. The window's
// per-bucket applied SET makes the second Add a no-op rather
// than a second HINCRBY.
//
// Without this guarantee, a transient ClickHouse outage that
// causes 3 retries before success would increment the windows
// 3× while ClickHouse sees the event once — which is exactly
// the divergence the Ch04 work was supposed to prevent.
func TestWindow_IdempotentOnEventID(t *testing.T) {
	client := openRedis(t)
	ctx := context.Background()

	const dur = 37 * time.Second
	cleanupKeys(t, client, dur.String())

	proc := stream.NewWindow(client, dur, 5*time.Second, nil)
	now := time.Now().UTC()
	eventID := "evt-idempotent-" + fmt.Sprintf("%d", now.UnixNano())

	// Add the same event five times — as if Kafka redelivered it.
	for i := 0; i < 5; i++ {
		if err := proc.Add(ctx, "tenant-IDM", "purchase", eventID, 10.0, now); err != nil {
			t.Fatalf("Add iteration %d: %v", i, err)
		}
	}

	// Snapshot the bucket: count must be 1, sum must be 10.
	snap, err := proc.Snapshot(ctx, "tenant-IDM", "purchase", now)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Count != 1 {
		t.Fatalf("count: got %d, want 1 (idempotency must collapse 5 same-event Adds to 1).\n"+
			"This is the audit's regression case — without per-bucket dedup, a retry "+
			"after a ClickHouse outage would 2-3× the window aggregates.",
			snap.Count)
	}
	if snap.Sum != 10.0 {
		t.Fatalf("sum: got %v, want 10.0", snap.Sum)
	}
}

// TestWindow_DedupSurvivesBucketFlush is the regression test for
// the audit's "redelivery after flush" finding. Sequence:
//
//   1. Add an event → :applied set contains event_id, bucket hash
//      contains count=1.
//   2. Simulate the flusher: DEL the bucket hash (but NOT the
//      :applied set — the new flusher's behavior).
//   3. Re-Add the same event_id (as Kafka redelivering after flush).
//   4. Assert: the bucket hash is NOT recreated. If it were, the
//      flusher would UPSERT the post-flush count over the correct
//      one in tenant_window_stats.
//
// This is what the post-flush dedup window buys us — Kafka can
// redeliver up to (4*duration + 2*lateness) after the original
// Add and still hit the dedup. Past that window we accept some
// overcounting (documented in the Ch12 README).
func TestWindow_DedupSurvivesBucketFlush(t *testing.T) {
	client := openRedis(t)
	ctx := context.Background()

	const dur = 43 * time.Second
	cleanupKeys(t, client, dur.String())

	proc := stream.NewWindow(client, dur, 5*time.Second, nil)
	now := time.Now().UTC()
	eventID := fmt.Sprintf("evt-postflush-%d", now.UnixNano())

	if err := proc.Add(ctx, "tenant-PF", "purchase", eventID, 50.0, now); err != nil {
		t.Fatalf("initial Add: %v", err)
	}

	// Locate the bucket hash key directly so we can simulate the
	// flusher. The key format is window:<label>:<tenant>:<type>:<bucketUnix>.
	bucketStart := now.Truncate(dur)
	bucketKey := fmt.Sprintf("window:%s:tenant-PF:purchase:%d",
		dur.String(), bucketStart.Unix(),
	)
	appliedKey := stream.AppliedSetKey(bucketKey)

	// Sanity: the keys exist after the Add.
	if existed, _ := client.Exists(ctx, bucketKey).Result(); existed != 1 {
		t.Fatalf("setup: bucket key %s should exist", bucketKey)
	}
	if existed, _ := client.Exists(ctx, appliedKey).Result(); existed != 1 {
		t.Fatalf("setup: applied key %s should exist", appliedKey)
	}

	// Simulate the flush: delete the bucket hash but NOT the
	// applied set. This is the new flusher behavior we're testing
	// against.
	if err := client.Del(ctx, bucketKey).Err(); err != nil {
		t.Fatalf("simulate flush: %v", err)
	}

	// Redelivery: same event_id arrives again.
	if err := proc.Add(ctx, "tenant-PF", "purchase", eventID, 50.0, now); err != nil {
		t.Fatalf("redelivery Add: %v", err)
	}

	// The bucket hash must NOT have been recreated. If it had,
	// count would be 1 and the flusher's next sweep would UPSERT
	// that 1 over the original (correct) count in
	// tenant_window_stats.
	if existed, _ := client.Exists(ctx, bucketKey).Result(); existed != 0 {
		t.Fatalf("post-flush dedup failed: the bucket hash was recreated by a duplicate event.\n"+
			"This is the audit's regression case — Kafka redelivery after flush would "+
			"overcount aggregates if the :applied set didn't survive the flush.")
	}
}

// TestWindow_DistinctEventsStillAccumulate is the negative pair to
// the idempotency test: two events with DIFFERENT event_ids must
// both apply. Otherwise the dedup set would silently swallow real
// events that happened to share a bucket.
func TestWindow_DistinctEventsStillAccumulate(t *testing.T) {
	client := openRedis(t)
	ctx := context.Background()

	const dur = 41 * time.Second
	cleanupKeys(t, client, dur.String())

	proc := stream.NewWindow(client, dur, 5*time.Second, nil)
	now := time.Now().UTC()

	for i := 0; i < 5; i++ {
		eid := fmt.Sprintf("evt-distinct-%d-%d", now.UnixNano(), i)
		if err := proc.Add(ctx, "tenant-D", "click", eid, 1.0, now); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	snap, err := proc.Snapshot(ctx, "tenant-D", "click", now)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Count != 5 {
		t.Fatalf("count: got %d, want 5 (distinct event_ids must each apply)", snap.Count)
	}
}

// TestWindow_PersistedWatermarkSurvivesLowerLaterWrites locks down
// the Lua MAX semantics: a later Add with an OLDER event time must
// NOT regress the persisted watermark.
func TestWindow_PersistedWatermarkSurvivesLowerLaterWrites(t *testing.T) {
	client := openRedis(t)
	ctx := context.Background()

	const dur = 31 * time.Second
	cleanupKeys(t, client, dur.String())

	proc := stream.NewWindow(client, dur, 5*time.Second, nil)
	t0 := time.Now().UTC()

	if err := proc.Add(ctx, "tenant-Y", "purchase", "evt-low-write-1", 1, t0.Add(time.Minute)); err != nil {
		t.Fatalf("Add high: %v", err)
	}
	if err := proc.Add(ctx, "tenant-Y", "purchase", "evt-low-write-2", 1, t0); err != nil {
		t.Fatalf("Add low: %v", err)
	}

	got, err := stream.PersistedWatermarkForDuration(ctx, client, dur)
	if err != nil {
		t.Fatalf("PersistedWatermarkForDuration: %v", err)
	}
	want := t0.Add(time.Minute)
	if !got.Equal(want) {
		t.Fatalf("persisted watermark regressed: got %v, want %v", got, want)
	}
}
