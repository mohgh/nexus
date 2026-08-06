package domain

import (
	"context"
	"errors"
	"time"
)

// TenantCredit is the per-tenant prepaid balance used by the Ch08
// billing flow. Storing money as a BIGINT in cents — float arithmetic
// on currency is one of the classic ways to lose customer money.
type TenantCredit struct {
	TenantID     string    `json:"tenant_id"`
	BalanceCents int64     `json:"balance_cents"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ErrInsufficientCredit is returned by Deduct when the requested
// amount exceeds the tenant's current balance. The caller decides
// whether to surface this as 402 Payment Required, queue a top-up,
// or fail the workflow.
var ErrInsufficientCredit = errors.New("insufficient credit")

// TenantCreditsRepository is the persistence boundary for prepaid
// credit balances. The interface intentionally stays narrow — three
// operations cover every use the chapter touches.
type TenantCreditsRepository interface {
	// GetBalance returns the current balance for a tenant. Returns
	// ErrNotFound if no credits row exists; the migration's trigger
	// auto-creates a zero-balance row on tenant insert, so this
	// generally indicates a stale tenant ID.
	GetBalance(ctx context.Context, tenantID string) (int64, error)

	// AddCredit increments the balance by amount. Idempotent at the
	// API boundary via the caller's idempotency_key — the repo itself
	// just SUMs.
	AddCredit(ctx context.Context, tenantID string, amount int64) error

	// Deduct subtracts amount from the balance, returning
	// ErrInsufficientCredit if the balance would go negative. Uses
	// SELECT ... FOR UPDATE under the hood to prevent the lost-update
	// anomaly on concurrent charges.
	Deduct(ctx context.Context, tenantID string, amount int64) error
}
