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

// NewResilientEventRepository wraps repo with three circuit breakers:
// one each for Create, List, and Search. They trip independently —
// a slow search doesn't affect ingestion.
func NewResilientEventRepository(repo domain.EventRepository, reg *Registry, logger *zap.Logger) *ResilientEventRepository {
	createCB := NewBreaker[struct{}]("event.create", DefaultSettings, logger)
	listCB := NewBreaker[[]*domain.Event]("event.list", DefaultSettings, logger)
	searchCB := NewBreaker[[]*domain.Event]("event.search", DefaultSettings, logger)

	reg.Register("event.create", createCB)
	reg.Register("event.list", listCB)
	reg.Register("event.search", searchCB)

	return &ResilientEventRepository{
		inner:   repo,
		create:  createCB,
		list:    listCB,
		search:  searchCB,
		timeout: 5 * time.Second,
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
