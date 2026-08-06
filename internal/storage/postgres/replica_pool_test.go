package postgres

import (
	"context"
	"testing"
)

func TestMinLSNContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	if got := MinLSNFromContext(ctx); got != 0 {
		t.Fatalf("empty context should return 0, got %d", got)
	}

	ctx = WithMinLSN(ctx, 12345)
	if got := MinLSNFromContext(ctx); got != 12345 {
		t.Fatalf("WithMinLSN(12345) -> got %d", got)
	}

	// Later WithMinLSN overrides the earlier one (last value wins via
	// context.WithValue chaining).
	ctx = WithMinLSN(ctx, 67890)
	if got := MinLSNFromContext(ctx); got != 67890 {
		t.Fatalf("WithMinLSN(67890) -> got %d", got)
	}
}

func TestRecordLSNMonotonic(t *testing.T) {
	t.Parallel()

	rp := &ReplicaPool{}

	rp.recordLSN(100)
	if rp.LastWriteLSN() != 100 {
		t.Fatalf("after recordLSN(100): got %d", rp.LastWriteLSN())
	}

	// Lower LSN must not move the watermark backwards.
	rp.recordLSN(50)
	if rp.LastWriteLSN() != 100 {
		t.Fatalf("after recordLSN(50) on top of 100: got %d, want 100", rp.LastWriteLSN())
	}

	// Equal value is also a no-op.
	rp.recordLSN(100)
	if rp.LastWriteLSN() != 100 {
		t.Fatalf("after recordLSN(100) on top of 100: got %d", rp.LastWriteLSN())
	}

	// Higher value advances.
	rp.recordLSN(200)
	if rp.LastWriteLSN() != 200 {
		t.Fatalf("after recordLSN(200): got %d", rp.LastWriteLSN())
	}
}

func TestWriteLSNRecorderMonotonic(t *testing.T) {
	t.Parallel()

	r := &WriteLSNRecorder{}
	if r.Load() != 0 {
		t.Fatalf("zero value should Load 0, got %d", r.Load())
	}

	r.Record(100)
	r.Record(50) // backwards: ignored
	r.Record(100) // equal: ignored
	r.Record(200) // higher: advances

	if got := r.Load(); got != 200 {
		t.Fatalf("Load: got %d, want 200", got)
	}
}

func TestWriteLSNRecorderContextRoundTrip(t *testing.T) {
	t.Parallel()

	parent := context.Background()
	if got := WriteLSNRecorderFromContext(parent); got != nil {
		t.Fatalf("empty context: expected nil recorder, got %v", got)
	}

	ctx, rec := WithWriteLSNRecorder(parent)
	if rec == nil {
		t.Fatal("WithWriteLSNRecorder returned nil recorder")
	}

	got := WriteLSNRecorderFromContext(ctx)
	if got != rec {
		t.Fatalf("recorder identity: got %p, want %p", got, rec)
	}

	// Recording via the returned pointer is visible via the context.
	rec.Record(777)
	if got.Load() != 777 {
		t.Fatalf("after Record(777): Load = %d", got.Load())
	}
}

// TestRequiredLSNForRead encodes the precedence rule that issue #2 was
// about: per-request MinLSN wins over the pool watermark, and falls
// back to the watermark when no override is present.
func TestRequiredLSNForRead(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		ctxMinLSN uint64 // 0 means "no override in context"
		watermark uint64
		want      uint64
	}{
		{"no override, no watermark", 0, 0, 0},
		{"no override, watermark only", 0, 500, 500},
		{"override only, no watermark", 250, 0, 250},
		{"override beats watermark when higher", 600, 500, 600},
		{"override wins even when lower (per-request scoping)", 100, 500, 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.ctxMinLSN > 0 {
				ctx = WithMinLSN(ctx, tc.ctxMinLSN)
			}
			if got := requiredLSNForRead(ctx, tc.watermark); got != tc.want {
				t.Fatalf("requiredLSNForRead: got %d, want %d", got, tc.want)
			}
		})
	}
}
