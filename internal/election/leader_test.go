//go:build integration

// Live-Redis regression tests for the RunAsLeader loop.
//
// Run via:
//
//	REDIS_DSN=redis://localhost:6379/0 \
//	    go test -tags=integration -run RunAsLeader -v ./internal/election/...
//
// These are the "zombie leader" tests: they assert that the loop keeps
// the *work* running, not just the lease. A node that holds the lease
// and runs nothing is worse than a node that never became leader at
// all, because it also blocks every other node from taking over.
package election_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/election"
)

// RunAsLeader ticks every 5s and the lease TTL is 15s, so these tests
// are inherently slow. Budgets are generous multiples of those.
const (
	renewEveryForTest = 5 * time.Second // mirrors the package's defaultRenewEvery
	leaderPollStep    = 250 * time.Millisecond
	becomeLeaderIn    = 20 * time.Second
	recoverIn         = 45 * time.Second
)

// waitFor polls cond until it holds or the budget runs out.
func waitFor(t *testing.T, budget time.Duration, what string, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(leaderPollStep)
	}
	t.Logf("timed out after %s waiting for %s", budget, what)
	return false
}

// TestRunAsLeader_RecoversAfterTransientRenewError is the regression
// test for the zombie leader.
//
// A single transient Redis error during Renew used to leave isLeader
// set to true while the work goroutine had already been cancelled.
// Because the loop only ever starts fn on the TryAcquire branch, and
// IsLeader() stayed true forever, fn was never started again: the node
// reported is_leader:true, refreshed the lease indefinitely, did no
// work, and starved every other node.
//
// The transient error is induced the way it happens in production
// (a Redis-side failure of the renew round-trip, not a client bug):
// the lease STRING key is swapped for a LIST, so the Lua GET inside
// renewScript raises WRONGTYPE. Redis then "heals": the original lease
// value is restored, still owned by this node, with a TTL in the same
// ballpark as the one it had. From that moment on, the buggy loop
// renews happily forever and never does any work again.
func TestRunAsLeader_RecoversAfterTransientRenewError(t *testing.T) {
	t.Parallel()

	client := openRedis(t)
	role := freshRole(t, client, "renew-zombie")
	leaseKey := "nexus:leader:" + role

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		workTicks atomic.Int64 // increments while fn is running
		starts    atomic.Int64 // how many times fn has been entered
	)
	e := election.NewElector(client, role, "node-zombie")
	go e.RunAsLeader(ctx, func(leaderCtx context.Context) {
		starts.Add(1)
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-leaderCtx.Done():
				return
			case <-tick.C:
				workTicks.Add(1)
			}
		}
	})

	// 1. Become leader and confirm work is actually running.
	if !waitFor(t, becomeLeaderIn, "initial leadership + work", func() bool {
		return e.IsLeader() && workTicks.Load() > 0
	}) {
		t.Fatalf("never became a working leader: isLeader=%v starts=%d ticks=%d",
			e.IsLeader(), starts.Load(), workTicks.Load())
	}
	held, err := client.Get(ctx, leaseKey).Result()
	if err != nil {
		t.Fatalf("read lease value: %v", err)
	}
	t.Logf("leader up: lease=%q token=%d ticks=%d", held, e.FencingToken(), workTicks.Load())

	// 2. Break renew: replace the lease STRING with a LIST so the Lua
	//    GET raises WRONGTYPE on the next renew round-trip.
	if err := client.Del(ctx, leaseKey).Err(); err != nil {
		t.Fatalf("del lease: %v", err)
	}
	if err := client.LPush(ctx, leaseKey, "wrongtype").Err(); err != nil {
		t.Fatalf("lpush lease: %v", err)
	}

	// Give the loop at least one full renew tick against the broken key.
	time.Sleep(renewEveryForTest + 2*time.Second)

	// 3. Redis heals: the lease is exactly as this node left it, and
	//    will expire normally. The TTL is comfortably longer than one
	//    renew tick, so a loop that still believes it is the leader
	//    will catch it and PEXPIRE it back up to a full TTL — which is
	//    precisely the zombie behaviour being tested for.
	if err := client.Del(ctx, leaseKey).Err(); err != nil {
		t.Fatalf("del broken lease: %v", err)
	}
	if err := client.Set(ctx, leaseKey, held, 8*time.Second).Err(); err != nil {
		t.Fatalf("restore lease: %v", err)
	}
	time.Sleep(time.Second)
	baseTicks := workTicks.Load()
	baseStarts := starts.Load()
	t.Logf("redis healed: ticks=%d starts=%d isLeader=%v", baseTicks, baseStarts, e.IsLeader())

	// 4. Work must resume. The node re-acquires only after the
	//    restored lease lapses on its TTL (the acquire script gates on
	//    EXISTS, so it cannot re-take a key it still holds), and it
	//    comes back with a strictly higher fencing token.
	if !waitFor(t, recoverIn, "work to resume after Redis healed", func() bool {
		return starts.Load() > baseStarts && workTicks.Load() > baseTicks+3
	}) {
		ttl, _ := client.PTTL(ctx, leaseKey).Result()
		val, _ := client.Get(ctx, leaseKey).Result()
		t.Fatalf("zombie leader: no work resumed after Redis recovered. "+
			"isLeader=%v starts=%d (was %d) ticks=%d (was %d) lease=%q pttl=%s",
			e.IsLeader(), starts.Load(), baseStarts,
			workTicks.Load(), baseTicks, val, ttl)
	}
	t.Logf("recovered: starts=%d ticks=%d token=%d",
		starts.Load(), workTicks.Load(), e.FencingToken())
}

// TestRunAsLeader_RestartsWorkFunctionThatReturns covers the related
// hole: `go fn(leaderCtx)` was fire-and-forget, so a work function
// that returned on its own — cmd/server/main.go runs outboxWorker.Run,
// which returns as soon as it errors — was never restarted. The lease
// stayed held and the loop stayed on the renew branch forever.
func TestRunAsLeader_RestartsWorkFunctionThatReturns(t *testing.T) {
	t.Parallel()

	client := openRedis(t)
	role := freshRole(t, client, "fn-exits")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var starts atomic.Int64
	e := election.NewElector(client, role, "node-exits")
	go e.RunAsLeader(ctx, func(leaderCtx context.Context) {
		// Mimics outboxWorker.Run returning with an error immediately.
		starts.Add(1)
	})

	if !waitFor(t, becomeLeaderIn, "initial leadership", func() bool {
		return starts.Load() >= 1
	}) {
		t.Fatalf("fn never ran: isLeader=%v", e.IsLeader())
	}

	// The loop should notice the work goroutine is gone and start a
	// new one, paced by the renew ticker.
	if !waitFor(t, recoverIn, "work function to be restarted", func() bool {
		return starts.Load() >= 3
	}) {
		t.Fatalf("work function was never restarted after it returned: "+
			"starts=%d isLeader=%v", starts.Load(), e.IsLeader())
	}

	// And it should be paced, not spun: at one restart per 5s renew
	// tick, the elapsed time here cannot have produced dozens.
	if n := starts.Load(); n > 40 {
		t.Fatalf("work function restarted %d times — the loop is busy-spinning, "+
			"not pacing restarts on the renew ticker", n)
	}
	t.Logf("restarts observed: %d", starts.Load())
}
