package eventstore

import "testing"

// These are the two pieces of the visibility fix that can be tested
// without Postgres: the trim rule that decides how much of a batch is
// safe to hand over, and the horizon that decides when a gap is dead
// rather than merely uncommitted. The wiring around them (and the
// behaviour of the real sequence) is covered in store_integration_test.go.

func positions(events []StoredEvent) []int64 {
	out := make([]int64, 0, len(events))
	for _, e := range events {
		out = append(out, e.StreamPosition)
	}
	return out
}

func events(ps ...int64) []StoredEvent {
	out := make([]StoredEvent, 0, len(ps))
	for _, p := range ps {
		out = append(out, StoredEvent{StreamPosition: p})
	}
	return out
}

func TestTrimToVisibleRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      []int64
		after   int64
		settled int64
		want    []int64
	}{
		{
			name: "dense run is returned whole",
			in:   []int64{6, 7, 8}, after: 5, settled: 0,
			want: []int64{6, 7, 8},
		},
		{
			name: "a gap right after the bookmark withholds everything",
			in:   []int64{7, 8, 9}, after: 5, settled: 0,
			want: nil,
		},
		{
			name: "a gap mid-batch cuts the batch there",
			in:   []int64{6, 7, 9, 10}, after: 5, settled: 0,
			want: []int64{6, 7},
		},
		{
			name: "a gap proven dead is stepped over",
			in:   []int64{7, 8, 9}, after: 5, settled: 9,
			want: []int64{7, 8, 9},
		},
		{
			name: "the settled horizon only covers positions up to itself",
			in:   []int64{7, 8, 11}, after: 5, settled: 8,
			want: []int64{7, 8},
		},
		{
			name: "empty batch stays empty",
			in:   nil, after: 5, settled: 99,
			want: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := positions(trimToVisibleRun(events(tc.in...), tc.after, tc.settled))
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v, want %v", got, tc.want)
				}
			}
		})
	}
}

// TestVisibilityHorizon_IdleDatabaseSettlesImmediately: xmin == xmax
// means no write transaction is in flight anywhere, so nothing can be
// holding a sequence value back and everything visible is final.
func TestVisibilityHorizon_IdleDatabaseSettlesImmediately(t *testing.T) {
	t.Parallel()

	var h visibilityHorizon
	if got := h.observe(10, 500, 500); got != 10 {
		t.Fatalf("settled=%d, want 10", got)
	}
}

// TestVisibilityHorizon_WaitsForInFlightWriters: while a writer that
// was running when we sampled is still running, the horizon must not
// advance — that writer may hold a position below the max we saw.
func TestVisibilityHorizon_WaitsForInFlightWriters(t *testing.T) {
	t.Parallel()

	var h visibilityHorizon

	// A writer with xid 500 is in flight; xmax is one past it.
	if got := h.observe(10, 500, 501); got != 0 {
		t.Fatalf("settled=%d on first sample, want 0", got)
	}
	// Still running two samples later, even as the log grows.
	if got := h.observe(12, 500, 503); got != 0 {
		t.Fatalf("settled=%d while writer 500 still runs, want 0", got)
	}
	// It ends. Every transaction that was running when we took the
	// candidate (max 10, deadline xmax 501) is now finished.
	if got := h.observe(12, 501, 504); got != 10 {
		t.Fatalf("settled=%d after writer 500 ended, want 10", got)
	}
	// The next sample opens a fresh candidate at 12, which matures on
	// the sample after that.
	if got := h.observe(12, 504, 505); got != 12 {
		t.Fatalf("settled=%d, want 12", got)
	}
}

// TestVisibilityHorizon_SteadyWriteStreamStillAdvances is the reason
// only one candidate is in flight at a time. If every sample reset the
// deadline to the current xmax, a database that is never quiet would
// push the deadline out forever and the horizon would freeze — which
// would in turn strand any gap left by a rolled back insert.
func TestVisibilityHorizon_SteadyWriteStreamStillAdvances(t *testing.T) {
	t.Parallel()

	var h visibilityHorizon

	// Sample 1 fixes the candidate at position 100 with deadline 1001.
	if got := h.observe(100, 1000, 1001); got != 0 {
		t.Fatalf("settled=%d, want 0", got)
	}
	// Writes keep arriving and xids keep being handed out, but the
	// oldest running transaction has moved past the deadline.
	if got := h.observe(140, 1001, 1050); got != 100 {
		t.Fatalf("settled=%d under a steady write stream, want 100", got)
	}
	if got := h.observe(180, 1050, 1090); got != 140 {
		t.Fatalf("settled=%d, want 140", got)
	}
}

// TestVisibilityHorizon_NeverGoesBackwards: the horizon is a
// monotonic promise. A later sample taken while a long transaction is
// running must not retract what was already proven settled.
func TestVisibilityHorizon_NeverGoesBackwards(t *testing.T) {
	t.Parallel()

	var h visibilityHorizon
	if got := h.observe(50, 700, 700); got != 50 {
		t.Fatalf("settled=%d, want 50", got)
	}
	if got := h.observe(60, 701, 999); got != 50 {
		t.Fatalf("settled=%d with a long writer in flight, want 50", got)
	}
	if got := h.lastMax(); got != 60 {
		t.Fatalf("lastMax=%d, want 60", got)
	}
}
