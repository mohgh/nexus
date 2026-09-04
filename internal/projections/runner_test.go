package projections_test

import (
	"context"
	"errors"
	"sync"
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
//
// It also models the thing a naive in-memory double cannot: positions
// that have been handed out by the sequence but are not visible yet
// because the transaction that took them has not committed. `pending`
// holds those. A real event store must not serve an event that sits
// above a pending position — doing so lets the consumer's bookmark
// step over it permanently — and this fake enforces the same rule, so
// the unit tier can express the visibility gap instead of quietly
// pretending every write is instantly visible.
type fakeStore struct {
	mu      sync.Mutex // guards events/pending; tests commit from another goroutine
	events  []eventstore.StoredEvent
	head    int64
	pending map[int64]bool

	readCalls atomic.Int32
}

// commit makes a pending position visible, as COMMIT does.
func (s *fakeStore) commit(e eventstore.StoredEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.pending, e.StreamPosition)
	at := len(s.events)
	for i, ex := range s.events {
		if ex.StreamPosition > e.StreamPosition {
			at = i
			break
		}
	}
	s.events = append(s.events, eventstore.StoredEvent{})
	copy(s.events[at+1:], s.events[at:])
	s.events[at] = e
	if e.StreamPosition > s.head {
		s.head = e.StreamPosition
	}
}

// firstPendingAfter is the lowest uncommitted position above `after`,
// or 0 if there is none. Nothing at or above it may be served.
// Callers must hold s.mu.
func (s *fakeStore) firstPendingAfter(after int64) int64 {
	var first int64
	for pos := range s.pending {
		if pos > after && (first == 0 || pos < first) {
			first = pos
		}
	}
	return first
}

func (s *fakeStore) ReadAllFrom(_ context.Context, after int64, limit int) ([]eventstore.StoredEvent, error) {
	s.readCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	barrier := s.firstPendingAfter(after)
	out := []eventstore.StoredEvent{}
	for _, e := range s.events {
		if e.StreamPosition <= after {
			continue
		}
		if barrier != 0 && e.StreamPosition > barrier {
			break
		}
		out = append(out, e)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (s *fakeStore) HeadPosition(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head, nil
}

func (s *fakeStore) SafeHeadPosition(_ context.Context, after int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if barrier := s.firstPendingAfter(after); barrier != 0 {
		return barrier - 1, nil
	}
	return s.head, nil
}

// fakeProjection records every Apply for assertion. It supports
// optional position pre-loading and per-event apply errors.
//
// The Runner applies from its own goroutine while the test polls, so
// the recorded slice is behind a mutex and read through appliedSoFar.
type fakeProjection struct {
	mu           sync.Mutex
	name         string
	applied      []int64
	loadFromPos  int64
	applyErr     error
	resetCalled  atomic.Bool
	lastPosition int64
}

func (p *fakeProjection) Name() string { return p.name }

func (p *fakeProjection) LastPosition() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastPosition
}

// appliedSoFar is a snapshot of the positions applied so far.
func (p *fakeProjection) appliedSoFar() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int64(nil), p.applied...)
}

func (p *fakeProjection) LoadPosition(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastPosition = p.loadFromPos
	return nil
}

func (p *fakeProjection) Apply(_ context.Context, e eventstore.StoredEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.applyErr != nil {
		return p.applyErr
	}
	p.applied = append(p.applied, e.StreamPosition)
	p.lastPosition = e.StreamPosition
	return nil
}

func (p *fakeProjection) Reset(_ context.Context) error {
	p.resetCalled.Store(true)
	p.mu.Lock()
	defer p.mu.Unlock()
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
		if len(pa.appliedSoFar()) == 3 && len(pb.appliedSoFar()) == 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("not caught up. a=%v b=%v", pa.appliedSoFar(), pb.appliedSoFar())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if want := []int64{1, 2, 3}; !equal(pa.appliedSoFar(), want) || !equal(pb.appliedSoFar(), want) {
		t.Fatalf("applied: a=%v, b=%v, want both [1 2 3]", pa.appliedSoFar(), pb.appliedSoFar())
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
		if len(p.appliedSoFar()) == 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected only events 3 and 4 applied; got %v", p.appliedSoFar())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if want := []int64{3, 4}; !equal(p.appliedSoFar(), want) {
		t.Fatalf("applied: %v, want %v (LoadPosition resumed from 2)", p.appliedSoFar(), want)
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
		if len(healthy.appliedSoFar()) == 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("healthy projection did not catch up; broken=%v healthy=%v",
				broken.appliedSoFar(), healthy.appliedSoFar())
		case <-time.After(5 * time.Millisecond):
		}
	}
	if len(broken.appliedSoFar()) != 0 {
		t.Fatalf("broken projection should record nothing on apply error, got %v", broken.appliedSoFar())
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
		if len(p.appliedSoFar()) == 5 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected 5 applies, got %v", p.appliedSoFar())
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

// TestRunner_DoesNotStepOverAnUncommittedPosition is the unit-tier
// version of the bug this package was built around.
//
// Positions 1, 2 and 4 are visible; position 3 was taken by a
// transaction that has not committed yet. The old contract ("hand me
// everything I can see after N") made the runner apply 4, move its
// bookmark to 4, and never look at 3 again once it committed — a
// permanently lost event, reported as lag 0. The runner must instead
// stall at 2 and pick up 3 and 4, in order, once 3 lands.
func TestRunner_DoesNotStepOverAnUncommittedPosition(t *testing.T) {
	t.Parallel()

	inFlight := mkEvent(3)
	store := &fakeStore{
		events:  []eventstore.StoredEvent{mkEvent(1), mkEvent(2), mkEvent(4)},
		head:    4,
		pending: map[int64]bool{3: true},
	}
	p := &fakeProjection{name: "p"}

	r := projections.NewRunner(store, []projections.Projection{p}, nil,
		projections.Config{PollInterval: 5 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	waitFor(t, func() bool { return len(p.appliedSoFar()) == 2 },
		"projection never applied the events below the uncommitted position")

	// Give the runner several more sweeps to misbehave.
	time.Sleep(60 * time.Millisecond)
	if got := p.appliedSoFar(); !equal(got, []int64{1, 2}) {
		t.Fatalf("runner stepped over the uncommitted position 3: applied %v, want [1 2]", got)
	}

	// The writer commits. Both events must now be applied, in order.
	store.commit(inFlight)

	waitFor(t, func() bool { return len(p.appliedSoFar()) == 4 },
		"position 3 was never applied after its transaction committed")
	if got := p.appliedSoFar(); !equal(got, []int64{1, 2, 3, 4}) {
		t.Fatalf("applied %v, want [1 2 3 4]", got)
	}
}

// TestRunner_LagForSeparatesBlockedFromBehind pins the reporting half
// of the fix. A projection parked behind an uncommitted position is
// behind the head (lag > 0) but has nothing it is allowed to do about
// it (ready_lag == 0). Reporting only "lag" would have said 0 here
// under the old code, because the bookmark had already jumped the gap.
func TestRunner_LagForSeparatesBlockedFromBehind(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		events:  []eventstore.StoredEvent{mkEvent(1), mkEvent(2), mkEvent(4)},
		head:    4,
		pending: map[int64]bool{3: true},
	}
	p := &fakeProjection{name: "p", lastPosition: 2}

	r := projections.NewRunner(store, []projections.Projection{p}, nil,
		projections.Config{PollInterval: time.Hour})

	lags, err := r.LagFor(context.Background())
	if err != nil {
		t.Fatalf("LagFor: %v", err)
	}
	if len(lags) != 1 {
		t.Fatalf("len(lags)=%d, want 1", len(lags))
	}
	l := lags[0]
	if l.HeadPosition != 4 || l.SafeHeadPosition != 2 {
		t.Fatalf("head=%d safe_head=%d, want 4 and 2", l.HeadPosition, l.SafeHeadPosition)
	}
	if l.Lag != 2 {
		t.Fatalf("lag=%d, want 2 (position 4 exists but is not consumable)", l.Lag)
	}
	if l.ReadyLag != 0 {
		t.Fatalf("ready_lag=%d, want 0 (nothing is safe to consume yet)", l.ReadyLag)
	}
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}
