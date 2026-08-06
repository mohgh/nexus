package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/mohgh/nexus/internal/domain"
)

// TenantCreditsRepository persists tenant credit balances to PostgreSQL.
type TenantCreditsRepository struct {
	pool *Pool
}

func NewTenantCreditsRepository(pool *Pool) *TenantCreditsRepository {
	return &TenantCreditsRepository{pool: pool}
}

var _ domain.TenantCreditsRepository = (*TenantCreditsRepository)(nil)

// GetBalance returns the current balance for tenantID.
func (r *TenantCreditsRepository) GetBalance(ctx context.Context, tenantID string) (int64, error) {
	var balance int64
	err := r.pool.QueryRow(ctx,
		`SELECT balance_cents FROM tenant_credits WHERE tenant_id = $1`,
		tenantID,
	).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, domain.ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("credits: get balance: %w", err)
	}
	return balance, nil
}

// AddCredit adds amount to the balance using a single atomic UPDATE.
// No FOR UPDATE needed: the UPDATE itself takes a row lock, and we read
// no value into application memory before writing. Atomic at the SQL
// level, immune to lost update.
//
// (Compare with Deduct, which has to read-then-write because the read
// drives the can-we-go-negative decision.)
func (r *TenantCreditsRepository) AddCredit(ctx context.Context, tenantID string, amount int64) error {
	if amount <= 0 {
		return fmt.Errorf("credits: add credit: amount must be positive, got %d", amount)
	}
	tag, err := r.pool.Exec(ctx,
		`UPDATE tenant_credits
		    SET balance_cents = balance_cents + $1,
		        updated_at = NOW()
		  WHERE tenant_id = $2`,
		amount, tenantID,
	)
	if err != nil {
		return fmt.Errorf("credits: add credit: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("credits: add credit: %w", domain.ErrNotFound)
	}
	return nil
}

// Deduct subtracts amount from the balance, returning
// ErrInsufficientCredit if it would go negative.
//
// Uses SELECT ... FOR UPDATE inside a transaction: the lock blocks any
// concurrent Deduct against the same tenant until this transaction
// commits, so the read-balance/decide/write sequence is serial per
// tenant. Compare with the lost_update_test.go demo which shows what
// happens without the FOR UPDATE.
//
// The CHECK (balance_cents >= 0) constraint on the column is a
// belt-and-suspenders against a logic bug in this method — it will
// trip the UPDATE if we somehow miscompute and try to go negative.
func (r *TenantCreditsRepository) Deduct(ctx context.Context, tenantID string, amount int64) error {
	if amount <= 0 {
		return fmt.Errorf("credits: deduct: amount must be positive, got %d", amount)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("credits: deduct: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := DeductCreditTx(ctx, tx, tenantID, amount); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("credits: deduct: commit: %w", err)
	}
	return nil
}

// DeductCreditTx performs the lock-balance/check/decrement steps
// inside a caller-supplied transaction. Used directly by Deduct, and
// by ChargeTenant in this package which threads a single transaction
// across the credit deduction *and* the billing record insert so both
// commit atomically.
func DeductCreditTx(ctx context.Context, tx pgx.Tx, tenantID string, amount int64) error {
	if amount <= 0 {
		return fmt.Errorf("credits: deduct: amount must be positive, got %d", amount)
	}

	var balance int64
	err := tx.QueryRow(ctx,
		`SELECT balance_cents FROM tenant_credits WHERE tenant_id = $1 FOR UPDATE`,
		tenantID,
	).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("credits: deduct: %w", domain.ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("credits: deduct: select for update: %w", err)
	}

	if balance < amount {
		return domain.ErrInsufficientCredit
	}

	if _, err := tx.Exec(ctx,
		`UPDATE tenant_credits
		    SET balance_cents = balance_cents - $1,
		        updated_at = NOW()
		  WHERE tenant_id = $2`,
		amount, tenantID,
	); err != nil {
		return fmt.Errorf("credits: deduct: update: %w", err)
	}
	return nil
}
