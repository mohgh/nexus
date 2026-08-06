package election_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/mohgh/nexus/internal/election"
)

func TestFencedResource_AcceptsMonotonicallyIncreasingTokens(t *testing.T) {
	t.Parallel()

	r := election.NewFencedResource()

	for _, token := range []int64{1, 2, 3, 100, 101} {
		if err := r.Apply(token, "ok"); err != nil {
			t.Fatalf("Apply(%d): unexpected error %v", token, err)
		}
	}
	if r.Highest() != 101 {
		t.Fatalf("Highest: got %d, want 101", r.Highest())
	}
	if got := len(r.Applied()); got != 5 {
		t.Fatalf("Applied: got %d entries, want 5", got)
	}
}

// TestFencedResource_RejectsStaleToken is the canonical fencing demo:
// the highest token applied is 5; an old leader (still believing it
// holds the lease) tries to write with token 3; the downstream
// rejects it via ErrFencedOff and the state is unchanged.
func TestFencedResource_RejectsStaleToken(t *testing.T) {
	t.Parallel()

	r := election.NewFencedResource()
	_ = r.Apply(5, "new-leader-write")

	err := r.Apply(3, "ghost-leader-write")
	if !errors.Is(err, election.ErrFencedOff) {
		t.Fatalf("expected ErrFencedOff for stale token, got %v", err)
	}

	// Confirm the stale write didn't slip in.
	if r.Highest() != 5 {
		t.Fatalf("stale write moved Highest: got %d, want 5", r.Highest())
	}
	if got := len(r.Applied()); got != 1 {
		t.Fatalf("stale write appended to Applied: got %d entries, want 1", got)
	}
	for _, w := range r.Applied() {
		if w.Value == "ghost-leader-write" {
			t.Fatalf("stale value persisted: %+v", w)
		}
	}
}

// TestFencedResource_RejectsEqualToken pins down the "strictly
// increasing" part of the contract: a token equal to the highest
// already applied is rejected, not silently no-op'd. This matters
// because two leaders that somehow both observed the same token (a
// bug in token issuance) must NOT both be able to write.
func TestFencedResource_RejectsEqualToken(t *testing.T) {
	t.Parallel()

	r := election.NewFencedResource()
	_ = r.Apply(7, "first")

	err := r.Apply(7, "second")
	if !errors.Is(err, election.ErrFencedOff) {
		t.Fatalf("equal token must be rejected: got %v", err)
	}
}

// TestFencedResource_ConcurrentDoesNotPersistStale stresses the
// race between an old and new leader writing concurrently. The
// resource serialises Apply via a mutex; whichever lands first
// becomes the new Highest, and the other (if stale) is rejected.
// Either ordering is acceptable — the invariant is that no write
// strictly older than the highest applied ever lands.
func TestFencedResource_ConcurrentDoesNotPersistStale(t *testing.T) {
	t.Parallel()

	r := election.NewFencedResource()
	_ = r.Apply(10, "baseline")

	var wg sync.WaitGroup
	var staleErr, freshErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		staleErr = r.Apply(5, "stale")
	}()
	go func() {
		defer wg.Done()
		freshErr = r.Apply(20, "fresh")
	}()
	wg.Wait()

	if !errors.Is(staleErr, election.ErrFencedOff) {
		t.Fatalf("stale concurrent write should be rejected: got %v", staleErr)
	}
	if freshErr != nil {
		t.Fatalf("fresh write should succeed: got %v", freshErr)
	}
	if r.Highest() != 20 {
		t.Fatalf("Highest after concurrent: got %d, want 20", r.Highest())
	}
}
