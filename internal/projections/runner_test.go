package projections_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/eventstore"
	"github.com/mohgh/nexus/internal/projections"
)

// fakeStore serves a pre-loaded slice of events from ReadAllFrom and
// reports a fixed head position. The slice is treated as the truth:
// each ReadAllFrom returns the events strictly after the requested
// position, up to limit.
type fakeStore struct {
	events []eventstore.StoredEvent
	head   int64

	readCalls atomic.Int32
}

func (s *fakeStore) ReadAllFrom(_ context.Context, after int64, limit int) ([]eventstore.StoredEvent, error) {
	s.readCalls.Add(1)
	out := []eventstore.StoredEvent{}
	for _, e := range s.events {
		if e.StreamPosition > after {
			out = append(out, e)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *fakeStore) HeadPosition(_ context.Context) (int64, error) {
	return s.head, nil
}

// fakeProjection records every Apply for assertion. It supports
// optional position pre-loading and per-event apply errors.
type fakeProjection struct {
	name         string
	applied      []int64
	loadFromPos  int64
	applyErr     error
	resetCalled  atomic.Bool
	lastPosition int64
}

func (p *fakeProjection) Name() string        { return p.name }
func (p *fakeProjection) LastPosition() int64 { return p.lastPosition }

func (p *fakeProjection) LoadPosition(_ context.Context) error {
	p.lastPosition = p.loadFromPos
	return nil
}

func (p *fakeProjection) Apply(_ context.Context, e eventstore.StoredEvent) error {
	if p.applyErr != nil {
		return p.applyErr
	}
	p.applied = append(p.applied, e.StreamPosition)
	p.lastPosition = e.StreamPosition
	return nil
}

func (p *fakeProjection) Reset(_ context.Context) error {
	p.resetCalled.Store(true)
	p.applied = nil
	p.lastPosition = 0
	return nil
}

func mkEvent(pos int64) eventstore.StoredEvent {
	return eventstore.StoredEvent{
		StreamPosition: pos,
		StreamName:     "tenant-a",
		EventType:      "EventIngested",
		Data:           []byte(`{"tenant_id":"t","event_type":"x","value":1}`),
		Metadata:       []byte(`{}`),
		OccurredAt:     time.Now().UTC(),
	}
}

// TestRunner_AppliesAllPendingEventsToEachProjection: the happy path.
// Two projections, three events; both projections see all three.
func TestRunner_AppliesAllPendingEventsToEachProjection(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		events: []eventstore.StoredEvent{mkEvent(1), mkEvent(2), mkEvent(3)},
		head:   3,
	}
	pa := &fakeProjection{name: "a"}
	pb := &fakeProjection{name: "b"}

	r := projections.NewRunner(store, []projections.Projection{pa, pb}, nil,
		projections.Config{PollInterval: time.Hour}) // disable ticker, only the immediate startup sweep runs

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	deadline := time.After(500 * time.Millisecond)
	for {
		if len(pa.applied) == 3 && len(pb.applied) == 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("not caught up. a=%v b=%v", pa.applied, pb.applied)
		case <-time.After(5 * time.Millisecond):
		}
	}

	if want := []int64{1, 2, 3}; !equal(pa.applied, want) || !equal(pb.applied, want) {
		t.Fatalf("applied: a=%v, b=%v, want both [1 2 3]", pa.applied, pb.applied)
	}
}

// TestRunner_ResumesFromLoadedPosition: a projection that
// LoadPosition()s to N should only see events with position > N.
// This is the regression test for the persistence guarantee — a
// restart that reset positions to 0 would replay everything.
func TestRunner_ResumesFromLoadedPosition(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		events: []eventstore.StoredEvent{mkEvent(1), mkEvent(2), mkEvent(3), mkEvent(4)},
		head:   4,
	}
	p := &fakeProjection{name: "lagging", loadFromPos: 2}

	r := projections.NewRunner(store, []projections.Projection{p}, nil,
		projections.Config{PollInterval: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	deadline := time.After(500 * time.Millisecond)
	for {
		if len(p.applied) == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected only events 3 and 4 applied; got %v", p.applied)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if want := []int64{3, 4}; !equal(p.applied, want) {
		t.Fatalf("applied: %v, want %v (LoadPosition resumed from 2)", p.applied, want)
	}
}

// TestRunner_StopsCatchUpForOneProjectionOnApplyError verifies that
// an Apply error halts that projection's catch-up but doesn't break
// the overall runner — other projections in the same sweep still
// make progress.
func TestRunner_StopsCatchUpForOneProjectionOnApplyError(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		events: []eventstore.StoredEvent{mkEvent(1), mkEvent(2), mkEvent(3)},
		head:   3,
	}
	broken := &fakeProjection{name: "broken", applyErr: errors.New("simulated")}
	healthy := &fakeProjection{name: "healthy"}

	r := projections.NewRunner(store, []projections.Projection{broken, healthy}, nil,
		projections.Config{PollInterval: time.Hour})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	deadline := time.After(500 * time.Millisecond)
	for {
		if len(healthy.applied) == 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("healthy projection did not catch up; broken=%v healthy=%v",
				broken.applied, healthy.applied)
		case <-time.After(5 * time.Millisecond):
		}
	}
	if len(broken.applied) != 0 {
		t.Fatalf("broken projection should record nothing on apply error, got %v", broken.applied)
	}
}

// TestRunner_LagFor reports head minus each projection's last
// position. If LagFor were to read head AFTER a projection
// advanced, the numbers would be inconsistent (a projection could
// appear ahead of head); this test pins the snapshot semantics.
func TestRunner_LagFor(t *testing.T) {
	t.Parallel()

	store := &fakeStore{events: nil, head: 100}
	pa := &fakeProjection{name: "a", lastPosition: 90}
	pb := &fakeProjection{name: "b", lastPosition: 50}

	r := projections.NewRunner(store, []projections.Projection{pa, pb}, nil,
		projections.Config{PollInterval: time.Hour})

	lags, err := r.LagFor(context.Background())
	if err != nil {
		t.Fatalf("LagFor: %v", err)
	}
	if len(lags) != 2 {
		t.Fatalf("len(lags)=%d, want 2", len(lags))
	}
	want := map[string]int64{"a": 10, "b": 50}
	for _, l := range lags {
		if l.HeadPosition != 100 {
			t.Fatalf("head: got %d, want 100", l.HeadPosition)
		}
		if l.Lag != want[l.ProjectionName] {
			t.Fatalf("lag for %s: got %d, want %d", l.ProjectionName, l.Lag, want[l.ProjectionName])
		}
	}
}

// TestRunner_BatchedCatchUp: with batchSize=2 and 5 events, the
// runner should issue 3 ReadAllFrom calls (2+2+1) before draining.
func TestRunner_BatchedCatchUp(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		events: []eventstore.StoredEvent{mkEvent(1), mkEvent(2), mkEvent(3), mkEvent(4), mkEvent(5)},
		head:   5,
	}
	p := &fakeProjection{name: "p"}

	r := projections.NewRunner(store, []projections.Projection{p}, nil,
		projections.Config{PollInterval: time.Hour, BatchSize: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	deadline := time.After(500 * time.Millisecond)
	for {
		if len(p.applied) == 5 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected 5 applies, got %v", p.applied)
		case <-time.After(5 * time.Millisecond):
		}
	}
	// 5 events / batchSize=2 = ceil(5/2) = 3 reads minimum from
	// the startup sweep alone. The runner stops batching when a
	// returned batch is short (final batch had 1 < 2).
	if got := store.readCalls.Load(); got < 3 {
		t.Fatalf("read calls: got %d, want >= 3 (batched)", got)
	}
}

func equal(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
