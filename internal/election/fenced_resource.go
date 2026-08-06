package election

import (
	"errors"
	"sync"
)

// ErrFencedOff is returned by FencedResource.Apply when the supplied
// fencing token is lower than the highest token the resource has
// already accepted. The DDIA Ch9 motivation is the classic GC-pause
// scenario: the old leader thinks it still holds the lease, but a
// new leader has since acquired and written. When the old leader
// finally wakes up and tries to write, the downstream rejects it
// because the token it carries is now stale.
var ErrFencedOff = errors.New("election: fenced off — token is older than the highest applied")

// FencedResource is a minimal in-memory downstream that enforces the
// fencing-token contract: it accepts a write only if the token
// strictly increases past every token it has previously applied. It's
// the storage-side half of fencing — without this half, having the
// leader carry a token does no work.
//
// In production this role is filled by:
//   - The storage system itself (e.g. ZooKeeper's zxid, HBase's
//     region-server epoch).
//   - The downstream service receiving the leader's writes (a
//     CompareAndSet on a fencing-token column).
//   - etcd's revision number, which acts as the fence on transactions
//     conditioned by lease ID.
//
// FencedResource is deliberately tiny because the demo lives in the
// *protocol*, not in any clever storage code: the asymmetry between
// "leader thinks it can write" and "downstream knows the highest
// token it has applied" is what fencing exploits.
type FencedResource struct {
	mu         sync.Mutex
	highest    int64
	applied    []AppliedWrite
}

// AppliedWrite is a record of a successful Apply call. Kept for
// observability in demos and tests.
type AppliedWrite struct {
	Token int64
	Value string
}

// NewFencedResource constructs an empty resource. Its initial highest
// token is 0, so any positive token can write the first record.
func NewFencedResource() *FencedResource {
	return &FencedResource{}
}

// Apply records a write under the supplied fencing token. The write
// is accepted (returns nil) only if token > highest token ever
// applied; otherwise it returns ErrFencedOff and the resource state
// is unchanged. Concurrent Applies are serialised by an internal
// mutex — the contract makes no claim about which of two equally-old
// tokens wins; it just guarantees no token strictly older than the
// highest applied is accepted.
func (r *FencedResource) Apply(token int64, value string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if token <= r.highest {
		return ErrFencedOff
	}
	r.highest = token
	r.applied = append(r.applied, AppliedWrite{Token: token, Value: value})
	return nil
}

// Highest returns the highest token the resource has accepted. Useful
// for tests and demos that want to assert "no stale write got
// through" without inspecting the full applied list.
func (r *FencedResource) Highest() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.highest
}

// Applied returns a snapshot of the accepted writes in order. Mostly
// for tests; production downstream code does not expose its history.
func (r *FencedResource) Applied() []AppliedWrite {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]AppliedWrite, len(r.applied))
	copy(out, r.applied)
	return out
}
