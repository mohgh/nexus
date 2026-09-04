package resilience

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/chaos"
	"github.com/mohgh/nexus/internal/domain"
	"go.uber.org/zap"
)

// The chaos wrappers are decorators, and a decorator can only inject
// faults into the layers BELOW it. These tests pin the wiring in
// cmd/server/main.go: resilience → chaos → storage. Wired the other way
// round the breaker observes nothing, which is asserted at the bottom
// as the regression case.

// TestChaosInsideResilience_InjectedErrorIsCountedByBreaker covers
// error_rate: the injected failure must reach the breaker's counters
// and eventually trip it.
func TestChaosInsideResilience_InjectedErrorIsCountedByBreaker(t *testing.T) {
	profile := chaos.New()
	profile.SetErrorRate(100)

	inner := &fakeEventRepo{}
	repo := newTestRepo(chaos.NewEventRepository(profile, inner))
	ctx := context.Background()

	threshold := int(DefaultSettings.ConsecutiveFailures)
	for i := 0; i < threshold; i++ {
		err := repo.Create(ctx, testEvent())
		if !errors.Is(err, chaos.ErrInjected) {
			t.Fatalf("call %d: got %v, want chaos.ErrInjected through the breaker", i+1, err)
		}
		// Check the counters BEFORE the trip: gobreaker starts a new
		// generation on every state change, which zeroes Counts.
		if i < threshold-1 {
			if got := repo.create.Counts().TotalFailures; got != uint32(i+1) {
				t.Fatalf("after %d injected errors the breaker recorded %d failures, want %d",
					i+1, got, i+1)
			}
		}
	}

	if got := repo.create.State(); got != "open" {
		t.Fatalf("breaker state = %q after %d injected errors, want open (counts: %+v)",
			got, threshold, repo.create.Counts())
	}
	if inner.calls != 0 {
		t.Errorf("chaos let %d calls through to the inner repo, want 0 at error_rate=100", inner.calls)
	}
}

// TestChaosInsideResilience_SlowVsDead is the Ch09 headline demo at
// 1/10 scale, with the delays expressed as the same 4/5 and 15/5
// ratios of the per-call timeout that db_delay_ms=4000 and
// db_delay_ms=15000 have against the production 5s timeout:
// the short delay finishes inside the timeout and leaves the breaker
// healthy, the long one blows it and is counted as a failure. The
// asymmetry only exists because chaos sits inside the timeout's scope.
func TestChaosInsideResilience_SlowVsDead(t *testing.T) {
	cases := []struct {
		name        string
		delay       time.Duration // scaled equivalent of db_delay_ms
		wantErr     bool
		wantFailure uint32
	}{
		{"4s delay, under the 5s timeout", testCallTimeout * 4 / 5, false, 0},
		{"15s delay, over the 5s timeout", testCallTimeout * 3, true, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			profile := chaos.New()
			profile.SetDBDelay(tc.delay.Milliseconds())

			repo := newTestRepo(chaos.NewEventRepository(profile, &fakeEventRepo{}))

			err := repo.Create(context.Background(), testEvent())
			if tc.wantErr && err == nil {
				t.Fatalf("delay %v with a %v timeout: got nil error, want a timeout", tc.delay, testCallTimeout)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("delay %v with a %v timeout: got %v, want success", tc.delay, testCallTimeout, err)
			}
			if got := repo.create.Counts().TotalFailures; got != tc.wantFailure {
				t.Errorf("breaker failures = %d, want %d", got, tc.wantFailure)
			}
		})
	}
}

// TestChaosOutsideResilience_BreakerSeesNothing is the bug this wiring
// fix exists for, kept as an executable explanation: with chaos as the
// OUTERMOST layer, MaybeError returns before the breaker is ever
// entered, so twenty failing requests leave it at requests:0
// failures:0 state:closed. Do not wire it this way in main.go.
func TestChaosOutsideResilience_BreakerSeesNothing(t *testing.T) {
	profile := chaos.New()
	profile.SetErrorRate(100)

	resilient := newTestRepo(&fakeEventRepo{})
	var wrongOrder domain.EventRepository = chaos.NewEventRepository(profile, resilient)

	for i := 0; i < 20; i++ {
		if err := wrongOrder.Create(context.Background(), testEvent()); err == nil {
			t.Fatalf("call %d: got nil error at error_rate=100", i+1)
		}
	}

	counts := resilient.create.Counts()
	if counts.Requests != 0 || counts.TotalFailures != 0 || resilient.create.State() != "closed" {
		t.Fatalf("expected the outer-chaos wiring to hide every fault from the breaker, "+
			"got state=%q counts=%+v — if this now fails, chaos is no longer outermost here",
			resilient.create.State(), counts)
	}
}

// TestRegistryStatesReportsBreakerCounters checks the shape consumed by
// /api/v1/circuit-breakers, since that endpoint is how the demo is
// observed.
func TestRegistryStatesReportsBreakerCounters(t *testing.T) {
	reg := NewRegistry()
	repo := NewResilientEventRepositoryWith(
		&fakeEventRepo{err: errInner}, reg, zap.NewNop(), scaledDefaults(), testCallTimeout,
	)

	_ = repo.Create(context.Background(), testEvent())

	states := reg.States()
	got, ok := states["event.create"].(map[string]any)
	if !ok {
		t.Fatalf("registry states missing event.create: %#v", states)
	}
	if got["state"] != "closed" {
		t.Errorf("state = %v, want closed after a single failure", got["state"])
	}
	if got["failures"] != uint32(1) {
		t.Errorf("failures = %v, want 1", got["failures"])
	}
}
