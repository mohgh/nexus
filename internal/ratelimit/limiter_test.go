package ratelimit

import (
	"testing"
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
