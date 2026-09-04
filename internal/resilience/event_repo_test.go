package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/domain"
	"go.uber.org/zap"
)

// These tests run the production code paths at 1/10 of production
// timescales: DefaultCallTimeout 5s → 500ms, the pre-fix
// DefaultSettings.Interval 10s → 1s, and so on. Scaling *every*
// duration by the same factor preserves the arithmetic relationship
// between the per-call timeout, the trip threshold and the
// counter-reset Interval — which is exactly what
// TestBreakerOpensAfterConsecutiveSlowFailures is about. The factor is
// deliberately modest (not 1/1000) so timer jitter stays far smaller
// than the margins the assertions depend on. Nothing here touches
// Postgres.
const testScale = 10

// testCallTimeout is the scaled equivalent of DefaultCallTimeout.
var testCallTimeout = DefaultCallTimeout / testScale

// scaledDefaults returns DefaultSettings with every duration divided
// by testScale, so a settings bug in the defaults reproduces here.
func scaledDefaults() Settings {
	s := DefaultSettings
	s.Interval /= testScale
	s.Timeout /= testScale
	return s
}

var errInner = errors.New("inner repo exploded")

// fakeEventRepo stands in for pgstore.EventRepository.
//
// It mimics a real database driver in the one way that matters for
// these tests: it reports a dead context as an error instead of
// happily returning success. pgx does the same, which is why an
// expired per-call deadline surfaces to the breaker as a failure.
type fakeEventRepo struct {
	err   error         // returned on every call when non-nil
	delay time.Duration // simulated latency; aborts on ctx cancellation
	calls int
}

func (f *fakeEventRepo) op(ctx context.Context) error {
	f.calls++
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.delay):
		}
	}
	return f.err
}

func (f *fakeEventRepo) Create(ctx context.Context, _ *domain.Event) error {
	return f.op(ctx)
}

func (f *fakeEventRepo) ListByTenant(ctx context.Context, _ string, _ int) ([]*domain.Event, error) {
	if err := f.op(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

func (f *fakeEventRepo) Search(ctx context.Context, _, _ string, _ int) ([]*domain.Event, error) {
	if err := f.op(ctx); err != nil {
		return nil, err
	}
	return nil, nil
}

var _ domain.EventRepository = (*fakeEventRepo)(nil)

// newTestRepo wraps inner in a ResilientEventRepository using the
// scaled defaults.
func newTestRepo(inner domain.EventRepository) *ResilientEventRepository {
	return NewResilientEventRepositoryWith(
		inner, NewRegistry(), zap.NewNop(), scaledDefaults(), testCallTimeout,
	)
}

func testEvent() *domain.Event {
	return &domain.Event{TenantID: "t1", EventType: "page_view"}
}

// TestDefaultSettingsIntervalIsCoherentWithTimeout pins the arithmetic
// documented on DefaultSettings: Interval is a periodic counter WIPE in
// the closed state, so it must either be 0 (never wipe) or comfortably
// exceed the wall-clock time needed to accumulate ConsecutiveFailures
// slow failures. Otherwise the counter is cleared before the threshold
// is ever reached and the breaker cannot trip on a hanging dependency.
func TestDefaultSettingsIntervalIsCoherentWithTimeout(t *testing.T) {
	worstCaseTripTime := time.Duration(DefaultSettings.ConsecutiveFailures) * DefaultCallTimeout
	if DefaultSettings.Interval == 0 {
		return // never reset while closed — always able to trip
	}
	if DefaultSettings.Interval <= 2*worstCaseTripTime {
		t.Fatalf("DefaultSettings.Interval=%v wipes the failure counter too often: "+
			"%d consecutive failures at a %v per-call timeout take up to %v to accumulate",
			DefaultSettings.Interval, DefaultSettings.ConsecutiveFailures,
			DefaultCallTimeout, worstCaseTripTime)
	}
}

// TestBreakerOpensAfterConsecutiveFastFailures is the case that always
// worked: an inner repo that fails immediately reaches the threshold
// long before any counter wipe.
func TestBreakerOpensAfterConsecutiveFastFailures(t *testing.T) {
	inner := &fakeEventRepo{err: errInner}
	repo := newTestRepo(inner)
	ctx := context.Background()

	threshold := int(DefaultSettings.ConsecutiveFailures)
	for i := 0; i < threshold; i++ {
		if err := repo.Create(ctx, testEvent()); !errors.Is(err, errInner) {
			t.Fatalf("call %d: got %v, want the inner error", i+1, err)
		}
	}

	if got := repo.create.State(); got != "open" {
		t.Fatalf("after %d fast failures: breaker state = %q, want open (counts: %+v)",
			threshold, got, repo.create.Counts())
	}

	// Once open the breaker fails fast without touching the inner repo.
	before := inner.calls
	if err := repo.Create(ctx, testEvent()); err == nil {
		t.Fatal("call after trip: got nil error, want ErrOpenState")
	}
	if inner.calls != before {
		t.Fatalf("open breaker still called the inner repo (%d → %d)", before, inner.calls)
	}
}

// TestBreakerOpensAfterConsecutiveSlowFailures is the regression test
// for the "structurally blind to slow dependencies" bug. Each failure
// costs a full per-call timeout, so with the old Interval (10s, i.e.
// only 2× the 5s timeout) the counter was wiped every two failures and
// the consecutive count oscillated 1,0,1,0 — twelve straight timeouts
// left the breaker closed.
func TestBreakerOpensAfterConsecutiveSlowFailures(t *testing.T) {
	// Hangs far longer than the per-call timeout, like a wedged DB.
	inner := &fakeEventRepo{delay: 3 * testCallTimeout}
	repo := newTestRepo(inner)
	ctx := context.Background()

	const attempts = 12
	tripped := 0
	for i := 1; i <= attempts; i++ {
		err := repo.Create(ctx, testEvent())
		if err == nil {
			t.Fatalf("call %d: got nil error, want a timeout", i)
		}
		if repo.create.State() == "open" {
			tripped = i
			break
		}
	}

	if tripped == 0 {
		t.Fatalf("breaker still closed after %d consecutive timeouts (counts: %+v); "+
			"Interval=%v wipes the counter before %d consecutive failures accumulate",
			attempts, repo.create.Counts(), scaledDefaults().Interval,
			DefaultSettings.ConsecutiveFailures)
	}
	if want := int(DefaultSettings.ConsecutiveFailures); tripped != want {
		t.Errorf("breaker tripped on slow failure %d, want %d", tripped, want)
	}
}

// TestBreakerTimeoutIsAppliedPerCall documents that the per-call
// deadline — not the caller's context — is what bounds a slow call.
func TestBreakerTimeoutIsAppliedPerCall(t *testing.T) {
	inner := &fakeEventRepo{delay: 3 * testCallTimeout}
	repo := newTestRepo(inner)

	start := time.Now()
	err := repo.Create(context.Background(), testEvent())
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want context.DeadlineExceeded", err)
	}
	if elapsed > 2*testCallTimeout {
		t.Fatalf("call took %v, want it bounded by the %v per-call timeout", elapsed, testCallTimeout)
	}
}
