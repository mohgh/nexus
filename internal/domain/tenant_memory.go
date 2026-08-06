package domain

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryTenantRepository is the Chapter 01 implementation of TenantRepository.
//
// It is replaced by PostgresTenantRepository in Chapter 03 when we connect
// Nexus to a real database. The interface stays identical — callers notice
// nothing. That is the point of the abstraction.
type InMemoryTenantRepository struct {
	mu      sync.RWMutex
	tenants map[string]*Tenant
}

func NewInMemoryTenantRepository() *InMemoryTenantRepository {
	return &InMemoryTenantRepository{
		tenants: make(map[string]*Tenant),
	}
}

func (r *InMemoryTenantRepository) List(_ context.Context) ([]*Tenant, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]*Tenant, 0, len(r.tenants))
	for _, t := range r.tenants {
		list = append(list, t)
	}
	return list, nil
}

func (r *InMemoryTenantRepository) Get(_ context.Context, id string) (*Tenant, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	t, ok := r.tenants[id]
	if !ok {
		return nil, fmt.Errorf("tenant %q: %w", id, ErrNotFound)
	}
	return t, nil
}

func (r *InMemoryTenantRepository) Create(_ context.Context, t *Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.tenants[t.ID] = t
	return nil
}
