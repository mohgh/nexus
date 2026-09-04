package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLimitForPlan(t *testing.T) {
	t.Parallel()
	tests := []struct {
		plan     string
		wantRPS  float64
		wantBurst int
	}{
		{"free", 10, 20},
		{"pro", 100, 200},
		{"enterprise", 1000, 2000},
		{"", 10, 20},        // empty -> free
		{"unknown", 10, 20}, // unknown -> free
	}
	for _, tc := range tests {
		got := LimitForPlan(tc.plan)
		if got.RPS != tc.wantRPS || got.Burst != tc.wantBurst {
			t.Errorf("LimitForPlan(%q) = %+v, want {%v %v}", tc.plan, got, tc.wantRPS, tc.wantBurst)
		}
	}
}

// TestMemoryLimiter_BurstThenDeny drives a small bucket to exhaustion and
// asserts the burst is served, the next request is denied with a positive
// Retry-After, and a different key is unaffected.
func TestMemoryLimiter_BurstThenDeny(t *testing.T) {
	t.Parallel()
	m := NewMemoryLimiter()
	defer m.Close()

	lim := Limit{RPS: 1, Burst: 3}

	// First 3 (the burst) succeed.
	for i := 0; i < 3; i++ {
		if res := m.Allow("t:acme", lim); !res.Allowed {
			t.Fatalf("request %d within burst must be allowed", i+1)
		}
	}
	// 4th is denied — bucket empty, refill is 1/s.
	res := m.Allow("t:acme", lim)
	if res.Allowed {
		t.Fatal("request past burst must be denied")
	}
	if res.RetryAfter <= 0 {
		t.Fatalf("denied result must carry a positive Retry-After, got %v", res.RetryAfter)
	}

	// A different key has its own independent bucket.
	if res := m.Allow("t:globex", lim); !res.Allowed {
		t.Fatal("a different key must not be throttled by acme's usage")
	}
}

// TestMemoryLimiter_RetunesWithoutDiscardingTokens verifies that changing
// a key's plan retunes the bucket (rate + burst cap) in place without
// resetting the accumulated token count. x/time/rate raises the ceiling but
// does not instantly mint tokens up to the new burst — they refill at the
// new rate — so the immediate post-upgrade count reflects what was left.
func TestMemoryLimiter_RetunesWithoutDiscardingTokens(t *testing.T) {
	t.Parallel()
	m := NewMemoryLimiter()
	defer m.Close()

	// Start on free (burst 20), consume 5 -> ~15 tokens remain.
	free := LimitForPlan("free")
	for i := 0; i < 5; i++ {
		m.Allow("t:x", free)
	}
	// Upgrade to enterprise: this call is allowed and consumes one more,
	// leaving ~14 — tokens preserved, neither reset to 0 nor jumped to 2000.
	res := m.Allow("t:x", LimitForPlan("enterprise"))
	if !res.Allowed {
		t.Fatal("after upgrade the request must be allowed")
	}
	if res.Remaining < 10 || res.Remaining > 19 {
		t.Fatalf("post-upgrade remaining should preserve accumulated tokens (~14), got %d", res.Remaining)
	}
}

// TestMemoryLimiter_ConcurrentDenialsDoNotLeakTokens is the regression test
// for the Reserve/Cancel token leak. Reservation.CancelAt refuses to refund a
// reservation if any other reservation was taken in between, so under
// concurrency a reject-by-cancel implementation burns a token on nearly every
// *rejected* request and drives the bucket arbitrarily negative — after which
// it never refills back to positive and no request past the initial burst is
// ever served again.
//
// The assertions are deliberately banded, not exact: over a window of
// `window` at RPS/Burst the served count should land near burst+rps*elapsed,
// and the bucket must never end up in debt.
func TestMemoryLimiter_ConcurrentDenialsDoNotLeakTokens(t *testing.T) {
	t.Parallel()

	const (
		goroutines = 64
		window     = time.Second
	)
	lim := Limit{RPS: 10, Burst: 10}
	const key = "t:hammer"

	m := NewMemoryLimiter()
	defer m.Close()

	var (
		allowed   atomic.Int64
		denied    atomic.Int64
		badRetry  atomic.Int64
		negRemain atomic.Int64
		wg        sync.WaitGroup
	)

	deadline := time.Now().Add(window)
	start := time.Now()
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				res := m.Allow(key, lim)
				if res.Allowed {
					allowed.Add(1)
				} else {
					denied.Add(1)
					if res.RetryAfter <= 0 {
						badRetry.Add(1)
					}
				}
				if res.Remaining < 0 {
					negRemain.Add(1)
				}
				// Keep the loop hot but not a pure spin — plenty of calls
				// per refilled token without pinning every core.
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	got := allowed.Load()
	if denied.Load() == 0 {
		t.Fatalf("test is not exercising the reject path: %d allowed, 0 denied", got)
	}
	if badRetry.Load() != 0 {
		t.Errorf("%d denied results carried a non-positive Retry-After", badRetry.Load())
	}
	if negRemain.Load() != 0 {
		t.Errorf("%d results reported a negative Remaining", negRemain.Load())
	}

	// Expected served ≈ burst + rps*elapsed. Band generously: the lower
	// bound only needs to be above `burst`, which is all a leaking bucket
	// ever manages, and the upper bound guards against over-serving.
	expect := float64(lim.Burst) + lim.RPS*elapsed.Seconds()
	lo := int64(float64(lim.Burst) + 0.5*lim.RPS*elapsed.Seconds())
	hi := int64(expect*1.5) + 5
	if got < lo || got > hi {
		t.Errorf("allowed=%d over %v, want within [%d,%d] (≈%.1f = burst+rps*elapsed); "+
			"a count stuck at the burst (%d) means rejected requests are burning tokens",
			got, elapsed, lo, hi, expect, lim.Burst)
	}

	// The bucket itself must not be in debt: every rejection must have left
	// the token count untouched.
	if tk := internalTokens(m, key); tk < -0.001 {
		t.Errorf("bucket went negative after the run: tokens=%.3f (rejections are consuming tokens)", tk)
	}
}

// internalTokens reads the raw (unclamped) token count of key's bucket.
// Result.Remaining clamps negatives to 0, which would hide a deficit.
func internalTokens(m *MemoryLimiter, key string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return 0
	}
	return e.lim.Tokens()
}
