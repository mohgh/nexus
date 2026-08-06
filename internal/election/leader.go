// Package election implements leader election for Nexus using Redis.
//
// Ch10 teaching points:
//
//  1. Leader election is a coordination problem: only one node should
//     act as leader at a time (e.g. running the outbox worker, batch
//     aggregator, or shard rebalancer).
//
//  2. The implementation here is a single-Redis SET NX PX lease, NOT
//     the multi-Redis Redlock algorithm and NOT consensus. Redis can
//     fail, network partitions can split-brain, and clock drift on
//     the client can defeat TTL — all of which mean two nodes can
//     simultaneously believe they hold the lease.
//
//  3. That last failure mode is what fencing tokens are for. Every
//     time the lease is acquired, the Elector issues a *monotonically
//     increasing* fencing token via Redis INCR. Each protected
//     operation carries its token to downstream storage, which
//     rejects any write from an older token than it has already
//     applied. So even if an old "leader" survives a GC pause past
//     its lease and tries to write, the storage system rejects it.
//
//  4. The same property cannot be achieved by the lease alone — a
//     stale leader has no way to know its lease has expired until it
//     talks to Redis again, and by then it may already have started
//     a write the downstream system can't safely accept. Fencing
//     tokens move the safety check *to the downstream*, which is the
//     only place that has authoritative knowledge of what it has
//     already applied.
//
// In production, etcd lease-based election (with the lease ID acting
// as the fencing token) or a Temporal workflow gives you durable
// leader election with the fencing guarantee at the protocol level.
// The Redis implementation here is a teaching primitive.
package election

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultTTL        = 15 * time.Second
	defaultRenewEvery = 5 * time.Second
)

// Elector manages a Redis-backed leader lease with monotonic fencing
// token issuance.
//
// Concurrency:
//   - isLeader and fencingToken are atomic so handlers and protected
//     operations can read them concurrently with the run-loop without
//     a data race.
//   - All mutations happen in TryAcquire / Renew / Release, which the
//     RunAsLeader loop serialises.
type Elector struct {
	client    *redis.Client
	leaseKey  string // e.g. "nexus:leader:outbox-worker"
	tokenKey  string // e.g. "nexus:leader:outbox-worker:token"
	nodeID    string // unique identifier for this process
	ttl       time.Duration

	isLeader      atomic.Bool
	fencingToken  atomic.Int64
}

// NewElector creates an elector for the named role.
// nodeID must be unique per process (use ULID from Ch10 idgen package).
func NewElector(client *redis.Client, role, nodeID string) *Elector {
	return &Elector{
		client:   client,
		leaseKey: fmt.Sprintf("nexus:leader:%s", role),
		tokenKey: fmt.Sprintf("nexus:leader:%s:token", role),
		nodeID:   nodeID,
		ttl:      defaultTTL,
	}
}

// acquireScript atomically:
//  1. checks that the lease key is free (key does not exist),
//  2. INCRs the fencing-token counter,
//  3. SETs the lease to "<nodeID>:<token>" with the configured TTL.
//
// Returns the new fencing token on acquisition, or 0 if the lease is
// held by another node. The INCR only happens when we win the race, so
// the token counter stays clean across many losing acquisitions.
var acquireScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
    return 0
end
local token = redis.call("INCR", KEYS[2])
redis.call("SET", KEYS[1], ARGV[1] .. ":" .. token, "PX", ARGV[2])
return token
`)

// TryAcquire attempts to acquire the leader lease and obtain a fresh
// fencing token. Returns true if this node is now the leader.
//
// Ch10: notice that the SET NX side and the INCR side are wrapped in
// a single Lua script so the token issuance and the lease grant happen
// atomically. Splitting them across two round-trips would let the
// counter advance for nodes that never won the lease.
func (e *Elector) TryAcquire(ctx context.Context) (bool, error) {
	ttlMs := strconv.FormatInt(e.ttl.Milliseconds(), 10)
	token, err := acquireScript.Run(ctx, e.client,
		[]string{e.leaseKey, e.tokenKey},
		e.nodeID, ttlMs,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("election: acquire: %w", err)
	}
	if token == 0 {
		e.isLeader.Store(false)
		return false, nil
	}
	e.fencingToken.Store(token)
	e.isLeader.Store(true)
	return true, nil
}

// renewScript checks the lease still belongs to this node (by
// matching the "<nodeID>:<token>" value we wrote) and extends its TTL.
// Returns 1 on renewal, 0 if we no longer hold the lease.
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
`)

// Renew extends the lease TTL if this node is still the current
// leader. The fencing token is unchanged across renewals — it only
// advances on a fresh acquisition.
func (e *Elector) Renew(ctx context.Context) (bool, error) {
	token := e.fencingToken.Load()
	if token == 0 {
		// We've never acquired; nothing to renew.
		e.isLeader.Store(false)
		return false, nil
	}
	expected := fmt.Sprintf("%s:%d", e.nodeID, token)
	ttlMs := strconv.FormatInt(e.ttl.Milliseconds(), 10)
	result, err := renewScript.Run(ctx, e.client,
		[]string{e.leaseKey},
		expected, ttlMs,
	).Int()
	if err != nil {
		return false, fmt.Errorf("election: renew: %w", err)
	}
	ok := result == 1
	e.isLeader.Store(ok)
	return ok, nil
}

// releaseScript only deletes the lease if it still belongs to us.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
`)

// Release voluntarily gives up the leader lease. The fencing token
// is *not* zeroed — a future TryAcquire will get a higher one, and
// downstream resources will compare against the highest they've seen.
func (e *Elector) Release(ctx context.Context) error {
	token := e.fencingToken.Load()
	if token == 0 {
		e.isLeader.Store(false)
		return nil
	}
	expected := fmt.Sprintf("%s:%d", e.nodeID, token)
	_, err := releaseScript.Run(ctx, e.client,
		[]string{e.leaseKey},
		expected,
	).Int()
	e.isLeader.Store(false)
	return err
}

// IsLeader returns whether this node currently believes it is the
// leader. Note that this is a local read — for a strict answer you
// would call Renew, but the local read is appropriate for handlers
// rendering status pages.
func (e *Elector) IsLeader() bool {
	return e.isLeader.Load()
}

// FencingToken returns the token issued at this node's last
// successful TryAcquire. Returns 0 if this node has never held the
// lease in the current process lifetime.
//
// The contract: callers must only act on this token while IsLeader()
// is true. If you're checking a token at the downstream (the safety
// side of the fence) you don't care about IsLeader at all — you only
// compare the incoming token to the highest you've ever applied.
func (e *Elector) FencingToken() int64 {
	return e.fencingToken.Load()
}

// CurrentLeader returns the node ID of the current leader (without
// the fencing-token suffix), or empty string if no node holds the
// lease right now.
func (e *Elector) CurrentLeader(ctx context.Context) (string, error) {
	val, err := e.client.Get(ctx, e.leaseKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	// Stored as "nodeID:token". Strip the token suffix for display.
	if idx := strings.LastIndex(val, ":"); idx > 0 {
		return val[:idx], nil
	}
	return val, nil
}

// CurrentFencingToken returns the latest globally-issued fencing
// token for this role, regardless of which node currently holds the
// lease. Reads the token counter key directly. Useful for the leader
// status endpoint, and for downstream resources that want to know the
// highest token they could ever see.
func (e *Elector) CurrentFencingToken(ctx context.Context) (int64, error) {
	val, err := e.client.Get(ctx, e.tokenKey).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("election: parse fencing token %q: %w", val, err)
	}
	return n, nil
}

// RunAsLeader starts a background loop that:
//  1. Tries to acquire the lease every 5s.
//  2. Calls fn when this node becomes leader. fn receives a context
//     that is cancelled when leadership is lost (or RunAsLeader's
//     parent context is cancelled).
//  3. The function should periodically check IsLeader/FencingToken
//     before acting on shared state, or pass the token to the
//     downstream so the downstream can fence stale writes.
func (e *Elector) RunAsLeader(ctx context.Context, fn func(ctx context.Context)) {
	var cancel context.CancelFunc

	ticker := time.NewTicker(defaultRenewEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if cancel != nil {
				cancel()
			}
			_ = e.Release(context.Background())
			return

		case <-ticker.C:
			if e.IsLeader() {
				ok, _ := e.Renew(ctx)
				if !ok && cancel != nil {
					cancel()
					cancel = nil
				}
			} else {
				ok, _ := e.TryAcquire(ctx)
				if ok {
					var leaderCtx context.Context
					leaderCtx, cancel = context.WithCancel(ctx)
					go fn(leaderCtx)
				}
			}
		}
	}
}
