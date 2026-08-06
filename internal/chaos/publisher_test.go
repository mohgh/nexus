package chaos_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/mohgh/nexus/internal/chaos"
	"github.com/mohgh/nexus/internal/domain"
)

type countingPublisher struct {
	calls atomic.Int32
	err   error
}

func (p *countingPublisher) Publish(_ context.Context, _ *domain.Event) error {
	p.calls.Add(1)
	return p.err
}

// TestPublisher_DropsWhenToggleSet is the regression test for the
// audit's "drop_publish has no caller" finding. When the toggle is
// set, the chaos wrapper returns nil WITHOUT invoking the inner —
// so the DB write commits and the API returns success while Kafka
// sees nothing. The asymmetric-partial-failure demo.
func TestPublisher_DropsWhenToggleSet(t *testing.T) {
	t.Parallel()
	profile := chaos.New()
	inner := &countingPublisher{}
	p := chaos.NewPublisher(profile, inner)

	profile.SetDropPublish(true)

	for i := 0; i < 3; i++ {
		if err := p.Publish(context.Background(), &domain.Event{ID: "x"}); err != nil {
			t.Fatalf("publish should return nil when dropped, got %v", err)
		}
	}
	if inner.calls.Load() != 0 {
		t.Fatalf("inner publisher must NOT be called while drop_publish is on, got %d calls", inner.calls.Load())
	}
}

// TestPublisher_PassesThroughWhenToggleOff: with drop_publish off
// (the default), the wrapper transparently delegates — including
// surfacing inner errors. This is the path 99% of production
// requests take; we don't want the wrapper to silently swallow
// real failures.
func TestPublisher_PassesThroughWhenToggleOff(t *testing.T) {
	t.Parallel()
	profile := chaos.New()
	innerErr := errors.New("kafka down")
	inner := &countingPublisher{err: innerErr}
	p := chaos.NewPublisher(profile, inner)

	// drop_publish defaults to false; explicit re-set is just
	// belt-and-suspenders against a future default change.
	profile.SetDropPublish(false)

	err := p.Publish(context.Background(), &domain.Event{ID: "y"})
	if !errors.Is(err, innerErr) {
		t.Fatalf("inner error should surface verbatim, got %v", err)
	}
	if inner.calls.Load() != 1 {
		t.Fatalf("inner should have been called once, got %d", inner.calls.Load())
	}
}

// TestPublisher_NilProfileIsNoOp keeps the constructor unconditional
// in main.go: a build where the chaos package isn't wired
// shouldn't crash on Publish. Practically, nil-profile means "no
// drop_publish toggle exists," so pass through.
func TestPublisher_NilProfileIsNoOp(t *testing.T) {
	t.Parallel()
	inner := &countingPublisher{}
	p := chaos.NewPublisher(nil, inner)
	if err := p.Publish(context.Background(), &domain.Event{ID: "z"}); err != nil {
		t.Fatalf("nil profile path should pass through cleanly, got %v", err)
	}
	if inner.calls.Load() != 1 {
		t.Fatalf("inner should have been called once with nil profile, got %d", inner.calls.Load())
	}
}
