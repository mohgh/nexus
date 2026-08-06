//go:build integration

// Live-Redis integration tests for the leader elector.
//
// Run via:
//
//	make ch10-fencing
//
// or directly:
//
//	REDIS_DSN=redis://localhost:6379/0 \
//	    go test -tags=integration -v ./internal/election/...

package election_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/election"
	"github.com/redis/go-redis/v9"
)

func openRedis(t *testing.T) *redis.Client {
	t.Helper()
	dsn := os.Getenv("REDIS_DSN")
	if dsn == "" {
		t.Skip("REDIS_DSN not set; skipping integration test")
	}
	opt, err := redis.ParseURL(dsn)
	if err != nil {
		t.Fatalf("parse REDIS_DSN: %v", err)
	}
	client := redis.NewClient(opt)
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// freshRole returns a unique role name and a cleanup that wipes the
// related Redis keys. Avoids cross-test contamination — important
// because the token counter is monotonic across the lifetime of the
// key and tests would otherwise observe each other's increments.
func freshRole(t *testing.T, client *redis.Client, label string) string {
	t.Helper()
	role := fmt.Sprintf("test-%s-%d-%d", label, time.Now().UnixNano(), os.Getpid())
	t.Cleanup(func() {
		ctx := context.Background()
		_ = client.Del(ctx,
			"nexus:leader:"+role,
			"nexus:leader:"+role+":token",
		).Err()
	})
	return role
}

// TestElector_FencingTokenStrictlyIncreasesAcrossAcquisitions is the
// load-bearing property of fencing tokens: every time a node acquires
// the lease, the token it receives must be strictly higher than any
// token any node has ever received for the same role. Otherwise a
// stale leader could end up holding a token equal to a fresh one and
// the downstream couldn't tell them apart.
func TestElector_FencingTokenStrictlyIncreasesAcrossAcquisitions(t *testing.T) {
	client := openRedis(t)
	role := freshRole(t, client, "monotonic")
	ctx := context.Background()

	// Three sequential rounds of "acquire by node, release, acquire by
	// another node". Token sequence must be strictly increasing.
	tokens := make([]int64, 0, 3)

	for i, node := range []string{"node-A", "node-B", "node-C"} {
		e := election.NewElector(client, role, node)
		ok, err := e.TryAcquire(ctx)
		if err != nil {
			t.Fatalf("round %d (%s): acquire: %v", i, node, err)
		}
		if !ok {
			t.Fatalf("round %d (%s): expected to acquire (no other holder), got false", i, node)
		}
		tk := e.FencingToken()
		if tk == 0 {
			t.Fatalf("round %d (%s): fencing token should be non-zero after acquire", i, node)
		}
		tokens = append(tokens, tk)

		if err := e.Release(ctx); err != nil {
			t.Fatalf("round %d (%s): release: %v", i, node, err)
		}
	}

	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			t.Fatalf("token sequence not strictly increasing: %v", tokens)
		}
	}
	t.Logf("token sequence across 3 acquisitions: %v", tokens)
}

// TestElector_LosingAcquisitionDoesNotConsumeTokens guards against
// the obvious off-by-one bug where every TryAcquire call INCRs the
// counter, even the ones that lost the race. The acquireScript
// arranges for INCR only when SET succeeds, so a parade of losing
// nodes should leave the token counter untouched.
func TestElector_LosingAcquisitionDoesNotConsumeTokens(t *testing.T) {
	client := openRedis(t)
	role := freshRole(t, client, "loser-noinc")
	ctx := context.Background()

	winner := election.NewElector(client, role, "winner")
	ok, err := winner.TryAcquire(ctx)
	if err != nil || !ok {
		t.Fatalf("winner acquire: ok=%v err=%v", ok, err)
	}
	tokenAfterWinner, err := winner.CurrentFencingToken(ctx)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	if tokenAfterWinner == 0 {
		t.Fatalf("token after first acquire must be >0, got %d", tokenAfterWinner)
	}

	// Five would-be acquirers, each finds the lease held, none should
	// bump the counter.
	for i := 0; i < 5; i++ {
		loser := election.NewElector(client, role, fmt.Sprintf("loser-%d", i))
		ok, _ := loser.TryAcquire(ctx)
		if ok {
			t.Fatalf("loser-%d should not have acquired (winner holds)", i)
		}
	}

	tokenAfterLosers, err := winner.CurrentFencingToken(ctx)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}
	if tokenAfterLosers != tokenAfterWinner {
		t.Fatalf("losing acquisitions advanced the counter: before=%d, after=%d",
			tokenAfterWinner, tokenAfterLosers)
	}
}

// TestElector_FencedResourceRejectsStaleLeaderWrites is the canonical
// "old leader, GC pause, new leader took over" demo. We use the real
// elector to issue tokens to two simulated leaders sequentially, then
// have the old one try to write to a FencedResource after the new
// one has — the downstream must reject the stale write.
func TestElector_FencedResourceRejectsStaleLeaderWrites(t *testing.T) {
	client := openRedis(t)
	role := freshRole(t, client, "stale-write")
	ctx := context.Background()

	oldLeader := election.NewElector(client, role, "old")
	if ok, err := oldLeader.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("old acquire: ok=%v err=%v", ok, err)
	}
	oldToken := oldLeader.FencingToken()

	// Old leader's "GC pause": releases the lease (in production this
	// would be the lease silently expiring). The old elector's
	// fencing token is preserved — that's the whole problem: the old
	// process still thinks it's the leader.
	if err := oldLeader.Release(ctx); err != nil {
		t.Fatalf("old release: %v", err)
	}

	newLeader := election.NewElector(client, role, "new")
	if ok, err := newLeader.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("new acquire: ok=%v err=%v", ok, err)
	}
	newToken := newLeader.FencingToken()
	if newToken <= oldToken {
		t.Fatalf("new token must be > old token, got new=%d old=%d", newToken, oldToken)
	}

	// New leader writes first.
	resource := election.NewFencedResource()
	if err := resource.Apply(newToken, "new-leader-write"); err != nil {
		t.Fatalf("new leader Apply: %v", err)
	}

	// Old leader wakes up, still has its token, tries to write — the
	// downstream rejects.
	err := resource.Apply(oldToken, "ghost-leader-write")
	if !errors.Is(err, election.ErrFencedOff) {
		t.Fatalf("expected ErrFencedOff for stale token, got %v", err)
	}

	if resource.Highest() != newToken {
		t.Fatalf("highest token applied: got %d, want %d", resource.Highest(), newToken)
	}
	for _, w := range resource.Applied() {
		if strings.Contains(w.Value, "ghost") {
			t.Fatalf("stale write persisted: %+v", w)
		}
	}
}

// TestElector_RenewDoesNotAdvanceFencingToken pins down that holding
// a lease across renewals does not bump the token — the token is
// only meant to advance on a fresh acquisition (which represents a
// possible change of leader from the downstream's perspective).
func TestElector_RenewDoesNotAdvanceFencingToken(t *testing.T) {
	client := openRedis(t)
	role := freshRole(t, client, "renew-stable")
	ctx := context.Background()

	e := election.NewElector(client, role, "holder")
	if ok, err := e.TryAcquire(ctx); err != nil || !ok {
		t.Fatalf("acquire: ok=%v err=%v", ok, err)
	}
	before := e.FencingToken()

	for i := 0; i < 3; i++ {
		ok, err := e.Renew(ctx)
		if err != nil || !ok {
			t.Fatalf("renew %d: ok=%v err=%v", i, ok, err)
		}
	}

	after := e.FencingToken()
	if before != after {
		t.Fatalf("renew advanced the fencing token: before=%d, after=%d", before, after)
	}
}
