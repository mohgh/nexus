// Package chaos provides a small fault-injection surface for Ch09's
// "what happens when the world misbehaves?" demos.
//
// The point isn't to be a full chaos-engineering tool — it's to
// turn the chapter's three classes of trouble into knobs a student
// can flip and observe:
//
//   1. Slow vs. dead. A handler that takes 4 seconds (db_delay_ms=4000)
//      still succeeds; a handler that takes 15 seconds (db_delay_ms=15000)
//      times out via the per-call deadline and trips the breaker.
//      That asymmetry is the chapter's headline lesson on timeouts.
//
//   2. Transient errors. error_rate=30 returns errors on ~30% of calls;
//      students watch the circuit breaker count failures and trip at
//      the configured threshold.
//
//   3. Asymmetric partial failure. drop_publish drops only Kafka
//      publishes while DB writes still succeed; the consumer side
//      sees nothing despite the API server reporting success.
//
// All toggles are atomic so the chaos endpoint can twiddle them
// from the request goroutine while the storage goroutine reads
// them — no mutex, no per-event allocation.
package chaos

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"time"
)

// ErrInjected is the error returned when the configured error rate
// fires. Callers can errors.Is against this sentinel to detect a
// chaos-induced failure versus a real one (useful for the breaker
// observability tests).
var ErrInjected = errors.New("chaos: injected error")

// ErrPublishDropped is returned when drop_publish is set and the
// chaos wrapper short-circuits an outgoing message. Distinct from
// ErrInjected because the semantics differ — one is "the call
// failed," the other is "the call succeeded from the API server's
// perspective but downstream never saw it."
var ErrPublishDropped = errors.New("chaos: publish dropped")

// Profile holds the live tunables. Construct once, read by storage
// code on every operation, mutated by the /chaos endpoint.
type Profile struct {
	dbDelayMS   atomic.Int64
	errorRate   atomic.Int64 // 0-100, percent
	dropPublish atomic.Bool

	// rng is used to roll error_rate. atomic.Pointer keeps the
	// source swappable from tests if anyone needs determinism;
	// production uses time-seeded math/rand which is fine for
	// fault-injection demos.
	rng atomic.Pointer[rand.Rand]
}

// New constructs a Profile with all toggles at safe defaults
// (zero injection). The chaos endpoint flips them at runtime.
func New() *Profile {
	p := &Profile{}
	p.rng.Store(rand.New(rand.NewSource(time.Now().UnixNano())))
	return p
}

// Snapshot is the read-only view returned by the chaos endpoint.
type Snapshot struct {
	DBDelayMS   int64 `json:"db_delay_ms"`
	ErrorRate   int64 `json:"error_rate"`
	DropPublish bool  `json:"drop_publish"`
}

// Snapshot returns the current toggle values. Cheap — just three
// atomic loads.
func (p *Profile) Snapshot() Snapshot {
	return Snapshot{
		DBDelayMS:   p.dbDelayMS.Load(),
		ErrorRate:   p.errorRate.Load(),
		DropPublish: p.dropPublish.Load(),
	}
}

// SetDBDelay clamps to [0, 60000] — anything longer than a minute
// is overwhelmingly likely to be a typo.
func (p *Profile) SetDBDelay(ms int64) {
	if ms < 0 {
		ms = 0
	} else if ms > 60_000 {
		ms = 60_000
	}
	p.dbDelayMS.Store(ms)
}

// SetErrorRate clamps to [0, 100].
func (p *Profile) SetErrorRate(pct int64) {
	if pct < 0 {
		pct = 0
	} else if pct > 100 {
		pct = 100
	}
	p.errorRate.Store(pct)
}

// SetDropPublish toggles the publish-drop fault.
func (p *Profile) SetDropPublish(b bool) {
	p.dropPublish.Store(b)
}

// Reset returns every toggle to zero.
func (p *Profile) Reset() {
	p.dbDelayMS.Store(0)
	p.errorRate.Store(0)
	p.dropPublish.Store(false)
}

// MaybeDelayDB sleeps for the configured db_delay_ms, respecting
// ctx cancellation so a 15-second injected delay still aborts
// cleanly when a 10-second request timeout fires (which is exactly
// the "slow vs dead" lesson — the timeout machinery is the only
// thing that can rescue the caller).
func (p *Profile) MaybeDelayDB(ctx context.Context) {
	ms := p.dbDelayMS.Load()
	if ms <= 0 {
		return
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Duration(ms) * time.Millisecond):
	}
}

// MaybeError returns ErrInjected with probability error_rate/100.
// Called by the chaos-aware wrappers BEFORE the real operation,
// so an injected error doesn't waste a DB round-trip.
func (p *Profile) MaybeError() error {
	rate := p.errorRate.Load()
	if rate <= 0 {
		return nil
	}
	r := p.rng.Load()
	if r == nil {
		return nil
	}
	if r.Int63n(100) < rate {
		return ErrInjected
	}
	return nil
}

// ShouldDropPublish returns the current drop_publish flag. Named
// as a question so call sites read naturally:
//
//	if chaos.ShouldDropPublish() { return nil }  // pretend success
func (p *Profile) ShouldDropPublish() bool {
	return p.dropPublish.Load()
}
