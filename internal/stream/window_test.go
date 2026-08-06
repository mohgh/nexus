package stream

import (
	"path"
	"testing"
	"time"
)

// TestWatermark_AdvancesMonotonically ensures that a window
// aggregator's high watermark never moves backwards even when events
// arrive out of order. This is the core property the late-event
// routing relies on — if the watermark moved backwards, an event
// that's actually a straggler could end up moving the watermark
// before itself and not get routed late.
func TestWatermark_AdvancesMonotonically(t *testing.T) {
	t.Parallel()
	w := &WindowAggregator{duration: time.Minute, allowedLateness: 30 * time.Second}

	t0 := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	w.advanceWatermark(t0)
	w.advanceWatermark(t0.Add(-1 * time.Hour)) // far in the past: ignored
	w.advanceWatermark(t0.Add(-1 * time.Minute))
	w.advanceWatermark(t0.Add(5 * time.Minute)) // advances

	if got := w.HighWatermark(); !got.Equal(t0.Add(5 * time.Minute)) {
		t.Fatalf("HighWatermark: got %v, want %v", got, t0.Add(5*time.Minute))
	}
}

// TestIsLate_GovernsByWatermarkMinusAllowedLateness pins down the
// late-event predicate: an event is late iff its event_time is
// strictly before (watermark - allowed_lateness).
func TestIsLate_GovernsByWatermarkMinusAllowedLateness(t *testing.T) {
	t.Parallel()
	w := &WindowAggregator{
		duration:        time.Minute,
		allowedLateness: 30 * time.Second,
	}

	now := time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC)
	w.advanceWatermark(now)

	cases := []struct {
		name      string
		eventTime time.Time
		wantLate  bool
	}{
		{"on the watermark", now, false},
		{"5s before watermark (within lateness)", now.Add(-5 * time.Second), false},
		{"30s before watermark (exactly at boundary)", now.Add(-30 * time.Second), false},
		{"31s before watermark", now.Add(-31 * time.Second), true},
		{"5 minutes before watermark", now.Add(-5 * time.Minute), true},
		{"future event (after watermark)", now.Add(time.Minute), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := w.isLate(c.eventTime)
			if got != c.wantLate {
				t.Fatalf("isLate(%v) with watermark %v, lateness %v: got %v, want %v",
					c.eventTime, w.HighWatermark(), w.allowedLateness, got, c.wantLate)
			}
		})
	}
}

// TestIsLate_BeforeAnyEventIsNeverLate verifies that with no events
// observed yet, no event is considered late. Otherwise we'd risk
// routing the very first event to the late bucket, which would be
// nonsense.
func TestIsLate_BeforeAnyEventIsNeverLate(t *testing.T) {
	t.Parallel()
	w := &WindowAggregator{duration: time.Minute, allowedLateness: 30 * time.Second}

	if w.isLate(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("with no watermark yet, no event should be late")
	}
}

// TestIsClosed_OnlyWhenWatermarkPassesEndPlusLateness governs when
// the flusher considers a bucket safe to persist. Closing too early
// loses late events; closing too late leaves stale state in Redis.
func TestIsClosed_OnlyWhenWatermarkPassesEndPlusLateness(t *testing.T) {
	t.Parallel()
	w := &WindowAggregator{duration: time.Minute, allowedLateness: 30 * time.Second}

	bucketStart := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	// bucket end = 10:01:00; allowed lateness 30s; closes at 10:01:30

	// Watermark before bucket end + lateness: still open.
	w.advanceWatermark(bucketStart.Add(time.Minute + 29*time.Second))
	if w.IsClosed(bucketStart) {
		t.Fatalf("bucket should still be open with watermark <T_end + lateness")
	}

	// Watermark exactly at the threshold: closed.
	w.advanceWatermark(bucketStart.Add(time.Minute + 30*time.Second))
	if !w.IsClosed(bucketStart) {
		t.Fatalf("bucket should be closed when watermark = end + lateness")
	}

	// Watermark well past: closed.
	w.advanceWatermark(bucketStart.Add(time.Hour))
	if !w.IsClosed(bucketStart) {
		t.Fatalf("bucket should be closed when watermark >> end + lateness")
	}
}

// TestWatermarkKey pins down the key format so any rename here would
// also fail integration with the SSE handler (which reads the same key).
func TestWatermarkKey(t *testing.T) {
	t.Parallel()

	if got := watermarkKey(time.Minute); got != "window:1m:watermark" {
		t.Fatalf("watermarkKey(1m): got %q, want %q", got, "window:1m:watermark")
	}
	if got := watermarkKey(time.Hour); got != "window:1h:watermark" {
		t.Fatalf("watermarkKey(1h): got %q, want %q", got, "window:1h:watermark")
	}
}

// TestBucketScanPattern_ExcludesWatermarkKey is the regression test
// for the flusher logging a "malformed window key" warning every
// poll once a watermark exists. The SCAN pattern must NOT match the
// watermark key — that key has too few segments for parseWindowKey
// to accept, so feeding it in produces noisy false warnings forever.
//
// We use path.Match here because its semantics ('*' matches any
// run of non-/ chars) line up with Redis SCAN's glob for the
// '*' patterns we use. Don't use this matcher for production code
// — Redis's glob has '?' and '[...]' that path.Match doesn't.
func TestBucketScanPattern_ExcludesWatermarkKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		dur   time.Duration
		key   string
		match bool
	}{
		{"1m bucket key matches", time.Minute, "window:1m:tenant-A:click:1234567890", true},
		{"1m late key matches", time.Minute, "window:1m:tenant-A:click:late", true},
		{"1m watermark key does NOT match", time.Minute, "window:1m:watermark", false},
		{"1h bucket key matches", time.Hour, "window:1h:tenant-B:purchase:1234567890", true},
		{"1h watermark key does NOT match", time.Hour, "window:1h:watermark", false},
		{"different duration's bucket does NOT match", time.Minute, "window:1h:tenant-A:click:123", false},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pattern := bucketScanPattern(c.dur)
			got, err := path.Match(pattern, c.key)
			if err != nil {
				t.Fatalf("path.Match(%q, %q): %v", pattern, c.key, err)
			}
			if got != c.match {
				t.Fatalf("pattern %q vs key %q: got match=%v, want %v",
					pattern, c.key, got, c.match)
			}
		})
	}
}

func TestParseWindowKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		key       string
		wantErr   bool
		wantLate  bool
		wantBucketUnix int64
		wantTenant string
		wantType   string
	}{
		{
			name: "valid 1m on-time bucket",
			key:  "window:1m:tenant-A:page_view:1735718400",
			wantTenant: "tenant-A", wantType: "page_view",
			wantBucketUnix: 1735718400,
		},
		{
			name: "valid 1h on-time bucket",
			key:  "window:1h:tenant-B:click:1735718400",
			wantTenant: "tenant-B", wantType: "click",
			wantBucketUnix: 1735718400,
		},
		{
			name: "late bucket",
			key:  "window:1m:tenant-C:purchase:late",
			wantTenant: "tenant-C", wantType: "purchase",
			wantLate: true,
		},
		{
			name:    "missing parts",
			key:     "window:1m:tenant-A",
			wantErr: true,
		},
		{
			name:    "wrong prefix",
			key:     "queue:1m:tenant-A:page_view:1735718400",
			wantErr: true,
		},
		{
			name:    "non-numeric bucket suffix that isn't 'late'",
			key:     "window:1m:tenant-A:page_view:not-a-time",
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseWindowKey(c.key)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; parsed=%+v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.TenantID != c.wantTenant {
				t.Fatalf("TenantID: got %q, want %q", got.TenantID, c.wantTenant)
			}
			if got.EventType != c.wantType {
				t.Fatalf("EventType: got %q, want %q", got.EventType, c.wantType)
			}
			if got.IsLate != c.wantLate {
				t.Fatalf("IsLate: got %v, want %v", got.IsLate, c.wantLate)
			}
			if !c.wantLate && got.WindowStart.Unix() != c.wantBucketUnix {
				t.Fatalf("WindowStart unix: got %d, want %d", got.WindowStart.Unix(), c.wantBucketUnix)
			}
		})
	}
}
