package outbox_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/billing/outbox"
	"github.com/mohgh/nexus/internal/domain"
)

// fakeRepo implements just enough of domain.BillingRepository for the
// outbox worker — only PendingOutbox and MarkOutboxSent are exercised.
// Behaviour:
//   - PendingOutbox returns rows whose IDs are not in `sent`.
//   - MarkOutboxSent moves an ID from `pending` to `sent`.
type fakeRepo struct {
	mu      sync.Mutex
	pending []*domain.BillingRecord
	sent    map[string]bool

	// Optional error injection.
	pendingErr error
	markErr    error
	markCalls  int
}

func newFakeRepo(records ...*domain.BillingRecord) *fakeRepo {
	return &fakeRepo{
		pending: records,
		sent:    map[string]bool{},
	}
}

func (r *fakeRepo) Create(context.Context, *domain.BillingRecord) error {
	return errors.New("not used in worker test")
}

func (r *fakeRepo) GetByIdempotencyKey(context.Context, string, string) (*domain.BillingRecord, error) {
	return nil, errors.New("not used in worker test")
}

func (r *fakeRepo) PendingOutbox(_ context.Context, limit int) ([]*domain.BillingRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pendingErr != nil {
		return nil, r.pendingErr
	}
	var out []*domain.BillingRecord
	for _, rec := range r.pending {
		if r.sent[rec.ID] {
			continue
		}
		out = append(out, rec)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r *fakeRepo) MarkOutboxSent(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markCalls++
	if r.markErr != nil {
		return r.markErr
	}
	r.sent[id] = true
	return nil
}

func (r *fakeRepo) sentIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.sent))
	for id := range r.sent {
		out = append(out, id)
	}
	return out
}

// recordingPublisher captures publish calls and optionally fails them.
type recordingPublisher struct {
	mu       sync.Mutex
	calls    []string
	failOnce map[string]error // ID -> error to return once, then nil
	failAll  error
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{failOnce: map[string]error{}}
}

func (p *recordingPublisher) Publish(_ context.Context, rec *domain.BillingRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, rec.ID)
	if p.failAll != nil {
		return p.failAll
	}
	if err, ok := p.failOnce[rec.ID]; ok {
		delete(p.failOnce, rec.ID)
		return err
	}
	return nil
}

func (p *recordingPublisher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// runUntil starts w.Run in a goroutine and stops it when cond returns true
// or after timeout. Returns the worker (already stopped) so tests can
// assert on Stats().
func runUntil(t *testing.T, w *outbox.Worker, timeout time.Duration, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()

	deadline := time.After(timeout)
	for {
		if cond() {
			cancel()
			break
		}
		select {
		case <-deadline:
			cancel()
			t.Fatalf("condition not met within %v", timeout)
		case <-time.After(2 * time.Millisecond):
		}
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("worker did not stop after cancel")
	}
}

func mkRecord(id string) *domain.BillingRecord {
	return &domain.BillingRecord{
		ID:             id,
		TenantID:       "tenant-" + id,
		IdempotencyKey: "key-" + id,
		AmountCents:    1000,
		Currency:       "USD",
		Status:         domain.BillingStatusPending,
	}
}

// TestWorker_PublishesAllPendingRecords is the happy path: pending rows
// are published and marked.
func TestWorker_PublishesAllPendingRecords(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo(mkRecord("a"), mkRecord("b"), mkRecord("c"))
	pub := newRecordingPublisher()

	w := outbox.New(repo, pub, nil, outbox.Config{
		PollInterval: 5 * time.Millisecond,
	})

	runUntil(t, w, time.Second, func() bool {
		return w.Stats().Published == 3
	})

	if got := w.Stats().Published; got != 3 {
		t.Fatalf("published: got %d, want 3", got)
	}
	if got := pub.callCount(); got != 3 {
		t.Fatalf("publish calls: got %d, want 3", got)
	}
	if got := len(repo.sentIDs()); got != 3 {
		t.Fatalf("marked sent: got %d, want 3", got)
	}
}

// TestWorker_LeavesRowsUnmarkedOnPublishFailure is the central
// at-least-once invariant: a publish failure must NOT mark the row sent.
func TestWorker_LeavesRowsUnmarkedOnPublishFailure(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo(mkRecord("flaky"))
	pub := newRecordingPublisher()
	pub.failAll = errors.New("kafka unreachable")

	w := outbox.New(repo, pub, nil, outbox.Config{
		PollInterval: 5 * time.Millisecond,
	})

	runUntil(t, w, time.Second, func() bool {
		return w.Stats().Failed >= 2 // multiple sweeps tried & failed
	})

	if w.Stats().Published != 0 {
		t.Fatalf("nothing should be marked published, got %d", w.Stats().Published)
	}
	if len(repo.sentIDs()) != 0 {
		t.Fatalf("row must remain unmarked, got %v", repo.sentIDs())
	}
}

// TestWorker_RetryAfterTransientFailure verifies that a record which
// failed once is republished on the next sweep (and eventually marked
// sent when the publisher recovers).
func TestWorker_RetryAfterTransientFailure(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo(mkRecord("eventually-ok"))
	pub := newRecordingPublisher()
	pub.failOnce["eventually-ok"] = errors.New("transient")

	w := outbox.New(repo, pub, nil, outbox.Config{
		PollInterval: 5 * time.Millisecond,
	})

	runUntil(t, w, time.Second, func() bool {
		return w.Stats().Published == 1
	})

	if got := pub.callCount(); got < 2 {
		t.Fatalf("expected at least 2 publish attempts, got %d", got)
	}
	if got := len(repo.sentIDs()); got != 1 {
		t.Fatalf("expected 1 sent, got %d", got)
	}
}

// TestWorker_RepublishesIfMarkSentFails covers the awkward case where
// the record IS published but the database can't be updated. The
// at-least-once contract requires the worker to retry — the consumer
// will see the same record again and must dedupe.
func TestWorker_RepublishesIfMarkSentFails(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo(mkRecord("mark-failure"))
	repo.markErr = errors.New("db down")
	pub := newRecordingPublisher()

	w := outbox.New(repo, pub, nil, outbox.Config{
		PollInterval: 5 * time.Millisecond,
	})

	// Wait for at least 2 publish attempts — proves the worker retries
	// even though the publish itself succeeded (mark sent failed each
	// time, leaving the row pending).
	runUntil(t, w, time.Second, func() bool {
		return pub.callCount() >= 2
	})

	if w.Stats().Published != 0 {
		t.Fatalf("published count should not advance when MarkOutboxSent fails, got %d",
			w.Stats().Published)
	}
}

// TestWorker_DrainsBacklogOnStartup verifies that pre-existing pending
// rows get processed without waiting for the first poll tick — useful
// when the previous process crashed mid-sweep.
func TestWorker_DrainsBacklogOnStartup(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo(mkRecord("backlog-1"), mkRecord("backlog-2"))
	pub := newRecordingPublisher()

	// Long poll interval — if the worker waited for the ticker we'd
	// time out. The immediate-startup-sweep is the contract this test
	// pins down.
	w := outbox.New(repo, pub, nil, outbox.Config{
		PollInterval: 5 * time.Second,
	})

	runUntil(t, w, time.Second, func() bool {
		return w.Stats().Published == 2
	})
}

// TestWorker_StopsOnContextCancel ensures the worker exits promptly
// when its parent context is cancelled.
func TestWorker_StopsOnContextCancel(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo()
	pub := newRecordingPublisher()
	w := outbox.New(repo, pub, nil, outbox.Config{
		PollInterval: time.Hour, // sleep "forever" between sweeps
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()

	// Let it run briefly, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("worker did not stop within 1s of cancel")
	}
}

// TestNew_PanicsOnNilDeps locks down the fail-fast contract — passing
// a nil repo or publisher is a programming error and should not be
// silently tolerated.
func TestNew_PanicsOnNilDeps(t *testing.T) {
	t.Parallel()

	mustPanic := func(name string, fn func()) {
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("%s: expected panic, got none", name)
			}
		}()
		fn()
	}

	mustPanic("nil repo", func() {
		_ = outbox.New(nil, &recordingPublisher{}, nil, outbox.Config{})
	})
	mustPanic("nil publisher", func() {
		_ = outbox.New(&fakeRepo{}, nil, nil, outbox.Config{})
	})
}

// silence unused atomic import if any future test removes it
var _ = atomic.LoadUint64
