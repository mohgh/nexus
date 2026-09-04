package resilience

import (
	"context"
	"time"

	"github.com/mohgh/nexus/internal/domain"
	"go.uber.org/zap"
)

// ResilientEventRepository wraps domain.EventRepository with circuit breaking
// and per-call timeouts.
//
// Ch09 teaching point: the caller (HTTP handler) doesn't know or care that
// the underlying repo is protected by a circuit breaker — it still uses the
// same domain.EventRepository interface. The circuit breaker is transparent.
type ResilientEventRepository struct {
	inner   domain.EventRepository
	create  *Breaker[struct{}]
	list    *Breaker[[]*domain.Event]
	search  *Breaker[[]*domain.Event]
	timeout time.Duration
}

// Compile-time assertion.
var _ domain.EventRepository = (*ResilientEventRepository)(nil)

// DefaultCallTimeout is the per-call deadline applied to every
// operation. It is the "slow" half of the slow-vs-dead demo: a
// dependency that takes longer than this is treated as a failure and
// counted by the breaker. DefaultSettings.Interval is chosen relative
// to this value — see the arithmetic on DefaultSettings.
const DefaultCallTimeout = 5 * time.Second

// NewResilientEventRepository wraps repo with three circuit breakers:
// one each for Create, List, and Search. They trip independently —
// a slow search doesn't affect ingestion.
//
// Wrap the chaos repo (if any) BELOW this one — resilience → chaos →
// storage — so injected faults are visible to the breakers.
func NewResilientEventRepository(repo domain.EventRepository, reg *Registry, logger *zap.Logger) *ResilientEventRepository {
	return NewResilientEventRepositoryWith(repo, reg, logger, DefaultSettings, DefaultCallTimeout)
}

// NewResilientEventRepositoryWith is NewResilientEventRepository with
// explicit breaker settings and per-call timeout. Production uses the
// defaults; tests use it to run the same code paths on sub-second
// timescales instead of the 5s/30s production ones.
func NewResilientEventRepositoryWith(
	repo domain.EventRepository,
	reg *Registry,
	logger *zap.Logger,
	s Settings,
	timeout time.Duration,
) *ResilientEventRepository {
	createCB := NewBreaker[struct{}]("event.create", s, logger)
	listCB := NewBreaker[[]*domain.Event]("event.list", s, logger)
	searchCB := NewBreaker[[]*domain.Event]("event.search", s, logger)

	reg.Register("event.create", createCB)
	reg.Register("event.list", listCB)
	reg.Register("event.search", searchCB)

	return &ResilientEventRepository{
		inner:   repo,
		create:  createCB,
		list:    listCB,
		search:  searchCB,
		timeout: timeout,
	}
}

func (r *ResilientEventRepository) Create(ctx context.Context, e *domain.Event) error {
	_, err := Call(ctx, r.create, r.timeout, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, r.inner.Create(ctx, e)
	})
	return err
}

func (r *ResilientEventRepository) ListByTenant(ctx context.Context, tenantID string, limit int) ([]*domain.Event, error) {
	return Call(ctx, r.list, r.timeout, func(ctx context.Context) ([]*domain.Event, error) {
		return r.inner.ListByTenant(ctx, tenantID, limit)
	})
}

func (r *ResilientEventRepository) Search(ctx context.Context, tenantID, query string, limit int) ([]*domain.Event, error) {
	return Call(ctx, r.search, r.timeout, func(ctx context.Context) ([]*domain.Event, error) {
		return r.inner.Search(ctx, tenantID, query, limit)
	})
}
