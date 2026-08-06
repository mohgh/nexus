package domain

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when a requested resource does not exist.
var ErrNotFound = errors.New("not found")

// Tenant is the core SaaS primitive. Every piece of data in Nexus is
// anchored to a tenant_id — it is the top-level isolation boundary.
//
// The Plan field drives feature gating, rate limiting, and billing throughout
// the course. Nexus supports three tiers: free, pro, enterprise.
type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Plan      string    `json:"plan"` // "free" | "pro" | "enterprise"
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TenantRepository is the persistence interface for tenants.
//
// Chapter 01 ships with InMemoryTenantRepository.
// Chapter 03 replaces it with PostgresTenantRepository — callers notice nothing.
// That swap is the whole point of defining the interface here.
type TenantRepository interface {
	List(ctx context.Context) ([]*Tenant, error)
	Get(ctx context.Context, id string) (*Tenant, error)
	Create(ctx context.Context, t *Tenant) error
}
