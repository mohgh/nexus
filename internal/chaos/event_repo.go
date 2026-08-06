package chaos

import (
	"context"

	"github.com/mohgh/nexus/internal/domain"
)

// EventRepository wraps a domain.EventRepository with the Ch09
// fault-injection toggles. Live profile mutations from the chaos
// endpoint take effect on the next call — no restart needed.
//
// Wire it as the OUTERMOST event-repo layer in main.go, so chaos
// gets to inject before either the resilience (circuit breaker)
// or the sharded routing layer sees the request:
//
//	chaos.NewEventRepository(profile,
//	    resilience.NewResilientEventRepository(
//	        pgstore.NewEventRepository(replicaPool), ...))
//
// That ordering is what makes the "slow vs dead" demo work:
// db_delay_ms=15000 forces a 15-second pause, the inner timeout
// (5s in the resilience wrapper) fires, the breaker tallies a
// failure. With db_delay_ms=4000 the call eventually succeeds and
// the breaker stays healthy. The two cases produce different
// observable system behavior.
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
