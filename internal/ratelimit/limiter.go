// Package ratelimit provides request throttling for Nexus.
//
// The headline use is per-tenant rate limiting: each authenticated tenant
// gets its own token bucket sized by its plan (free / pro / enterprise), so
// one noisy tenant can't exhaust capacity for the others. Admin (operator)
// keys get a generous ceiling of their own.
//
// The Limiter interface keeps the algorithm swappable. The bundled
// MemoryLimiter is an in-process token bucket (golang.org/x/time/rate),
// which is correct and fast for a single-node deployment — matching Nexus's
// single-node pool tuning. In a multi-instance deploy each instance keeps
// its own buckets, so the effective limit is N× the configured value; a
// Redis-backed Limiter (token bucket via a Lua script) is the drop-in
// upgrade for a true global limit.
package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// Limit is a token-bucket configuration: sustained requests per second and
// the burst the bucket can absorb.
type Limit struct {
	RPS   float64
	Burst int
}

// Result is the outcome of an Allow check.
type Result struct {
	Allowed    bool
	RetryAfter time.Duration // how long until a token frees up (only meaningful when !Allowed)
	Remaining  int           // approximate tokens left in the bucket
}

// Limiter decides whether a request keyed by `key` may proceed under `lim`.
type Limiter interface {
	Allow(key string, lim Limit) Result
}

// Plan-tiered defaults. The Tenant.Plan field selects one of these.
// Deliberately hard-coded (not env-configurable) to keep the surface small;
// change here if you re-price the tiers.
var (
	freeLimit       = Limit{RPS: 10, Burst: 20}
	proLimit        = Limit{RPS: 100, Burst: 200}
	enterpriseLimit = Limit{RPS: 1000, Burst: 2000}
	adminLimit      = Limit{RPS: 2000, Burst: 4000}
)

// LimitForPlan maps a tenant plan to its bucket. An unknown/empty plan
// falls back to the most conservative (free) tier.
func LimitForPlan(plan string) Limit {
	switch plan {
	case "enterprise":
		return enterpriseLimit
	case "pro":
		return proLimit
	case "free":
		return freeLimit
	default:
		return freeLimit
	}
}

// AdminLimit is the generous ceiling applied to operator (admin) keys.
func AdminLimit() Limit { return adminLimit }

// MemoryLimiter is an in-process, per-key token bucket. Idle keys are
// evicted by a background janitor so memory stays bounded under churn.
type MemoryLimiter struct {
	mu      sync.Mutex
	entries map[string]*entry
	idleTTL time.Duration
	now     func() time.Time
	stop    chan struct{}
	stopOne sync.Once
}

type entry struct {
	lim      *rate.Limiter
	lastSeen time.Time
}

// NewMemoryLimiter returns a limiter with a background janitor that evicts
// keys unused for idleTTL. Call Close to stop the janitor.
func NewMemoryLimiter() *MemoryLimiter {
	m := &MemoryLimiter{
		entries: make(map[string]*entry),
		idleTTL: 10 * time.Minute,
		now:     time.Now,
		stop:    make(chan struct{}),
	}
	go m.janitor(time.Minute)
	return m
}

var _ Limiter = (*MemoryLimiter)(nil)

// Allow consumes one token from key's bucket, creating it on first use. If
// the bucket's configured limit differs from lim (e.g. the tenant changed
// plan), it is retuned in place without discarding accumulated tokens.
func (m *MemoryLimiter) Allow(key string, lim Limit) Result {
	m.mu.Lock()
	e, ok := m.entries[key]
	if !ok {
		e = &entry{lim: rate.NewLimiter(rate.Limit(lim.RPS), lim.Burst)}
		m.entries[key] = e
	} else {
		if float64(e.lim.Limit()) != lim.RPS {
			e.lim.SetLimit(rate.Limit(lim.RPS))
		}
		if e.lim.Burst() != lim.Burst {
			e.lim.SetBurst(lim.Burst)
		}
	}
	e.lastSeen = m.now()
	l := e.lim
	m.mu.Unlock()

	// rate.Limiter is internally synchronised, so we can operate on it
	// after releasing our own lock.
	res := l.Reserve()
	if !res.OK() {
		// Only happens if a single request exceeds the burst — impossible
		// with cost 1 and burst >= 1, but handle defensively.
		return Result{Allowed: false, RetryAfter: time.Second, Remaining: 0}
	}
	if delay := res.Delay(); delay > 0 {
		res.Cancel() // return the token; we're rejecting rather than waiting
		return Result{Allowed: false, RetryAfter: delay, Remaining: tokens(l)}
	}
	return Result{Allowed: true, Remaining: tokens(l)}
}

func tokens(l *rate.Limiter) int {
	t := int(l.Tokens())
	if t < 0 {
		return 0
	}
	return t
}

func (m *MemoryLimiter) janitor(interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
			cutoff := m.now().Add(-m.idleTTL)
			m.mu.Lock()
			for k, e := range m.entries {
				if e.lastSeen.Before(cutoff) {
					delete(m.entries, k)
				}
			}
			m.mu.Unlock()
		}
	}
}

// Close stops the background janitor. Safe to call more than once.
func (m *MemoryLimiter) Close() {
	m.stopOne.Do(func() { close(m.stop) })
}
