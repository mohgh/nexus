package chaos

import (
	"context"

	"github.com/mohgh/nexus/internal/domain"
)

// EventRepository wraps a domain.EventRepository with the Ch09
// fault-injection toggles. Live profile mutations from the chaos
// endpoint take effect on the next call — no restart needed.
//
// Wire it INSIDE the resilience wrapper in main.go — directly
// around the real storage repo — so that everything it injects
// happens underneath the circuit breaker and its per-call timeout:
//
//	resilience.NewResilientEventRepository(
//	    chaos.NewEventRepository(profile,
//	        pgstore.NewEventRepository(replicaPool)), ...)
//
// A decorator can only inject faults into the layers BELOW it. If
// chaos were the outermost layer, MaybeError would return before
// the breaker was ever entered and MaybeDelayDB would sleep outside
// the scope of the 5s timeout — the breaker would observe neither.
//
// With chaos on the inside, the "slow vs dead" demo works:
// db_delay_ms=15000 forces a 15-second pause, the resilience
// wrapper's 5s per-call timeout fires, and the breaker tallies a
// failure. With db_delay_ms=4000 the call completes inside the
// timeout and the breaker stays healthy. Likewise error_rate=100
// produces failures the breaker actually counts. The two cases
// produce different observable system behavior.
type EventRepository struct {
	profile *Profile
	inner   domain.EventRepository
}

func NewEventRepository(profile *Profile, inner domain.EventRepository) *EventRepository {
	return &EventRepository{profile: profile, inner: inner}
}

var _ domain.EventRepository = (*EventRepository)(nil)

func (r *EventRepository) Create(ctx context.Context, e *domain.Event) error {
	if err := r.profile.MaybeError(); err != nil {
		return err
	}
	r.profile.MaybeDelayDB(ctx)
	return r.inner.Create(ctx, e)
}

// ListByTenant — chaos applies on reads too, for symmetry. A
// student investigating "the cache is now serving stale data
// during read failures" can flip error_rate while the cache miss
// path hits this repo.
func (r *EventRepository) ListByTenant(ctx context.Context, tenantID string, limit int) ([]*domain.Event, error) {
	if err := r.profile.MaybeError(); err != nil {
		return nil, err
	}
	r.profile.MaybeDelayDB(ctx)
	return r.inner.ListByTenant(ctx, tenantID, limit)
}

func (r *EventRepository) Search(ctx context.Context, tenantID, query string, limit int) ([]*domain.Event, error) {
	if err := r.profile.MaybeError(); err != nil {
		return nil, err
	}
	r.profile.MaybeDelayDB(ctx)
	return r.inner.Search(ctx, tenantID, query, limit)
}
