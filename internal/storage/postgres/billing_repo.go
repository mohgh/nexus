package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mohgh/nexus/internal/domain"
)

// BillingRepository persists billing records to PostgreSQL.
type BillingRepository struct {
	pool *Pool
}

func NewBillingRepository(pool *Pool) *BillingRepository {
	return &BillingRepository{pool: pool}
}

var _ domain.BillingRepository = (*BillingRepository)(nil)

// Create inserts a billing_records row outside of any transaction.
// Callers that need atomicity across billing_records and tenant_credits
// (the canonical Ch08 pattern) should use ChargeWithCreditDeduction
// instead — Create is kept for non-transactional admin paths and tests.
func (r *BillingRepository) Create(ctx context.Context, rec *domain.BillingRecord) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO billing_records
		 (id, tenant_id, idempotency_key, amount_cents, currency, status, description, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		rec.ID, rec.TenantID, rec.IdempotencyKey,
		rec.AmountCents, rec.Currency, rec.Status,
		rec.Description, rec.CreatedAt,
	)
	if err != nil {
		if strings.Contains(err.Error(), "billing_idempotency") {
			return domain.ErrDuplicateKey
		}
		return fmt.Errorf("billing: create: %w", err)
	}
	return nil
}

// ChargeWithCreditDeduction is the chapter's headline transactional
// operation: in a single Postgres transaction it
//
//  1. SELECT ... FOR UPDATE on tenant_credits to lock the row,
//  2. checks the balance and bails with ErrInsufficientCredit if it
//     would go negative,
//  3. UPDATE tenant_credits to apply the deduction, and
//  4. INSERT into billing_records.
//
// Either both rows commit or neither does — atomicity across two
// tables. Concurrent charges on the same tenant serialise at step 1;
// concurrent charges on different tenants don't block each other.
//
// Returns ErrDuplicateKey if a billing_records row already exists for
// (tenant_id, idempotency_key). Temporal's workflow-ID dedupe normally
// catches duplicates upstream, but the unique constraint is the
// authoritative safety net — if it ever fires, the credit deduction
// is rolled back along with the failed insert.
func ChargeWithCreditDeduction(ctx context.Context, pool *Pool, rec *domain.BillingRecord) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("charge: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if err := DeductCreditTx(ctx, tx, rec.TenantID, rec.AmountCents); err != nil {
		return err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO billing_records
		 (id, tenant_id, idempotency_key, amount_cents, currency, status, description, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		rec.ID, rec.TenantID, rec.IdempotencyKey,
		rec.AmountCents, rec.Currency, rec.Status,
		rec.Description, rec.CreatedAt,
	); err != nil {
		if strings.Contains(err.Error(), "billing_idempotency") {
			return domain.ErrDuplicateKey
		}
		return fmt.Errorf("charge: insert billing: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("charge: commit: %w", err)
	}
	return nil
}

func (r *BillingRepository) GetByIdempotencyKey(ctx context.Context, tenantID, key string) (*domain.BillingRecord, error) {
	rec := &domain.BillingRecord{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, tenant_id, idempotency_key, amount_cents, currency,
		        status, description, created_at, outbox_sent_at
		 FROM billing_records
		 WHERE tenant_id = $1 AND idempotency_key = $2`,
		tenantID, key,
	).Scan(
		&rec.ID, &rec.TenantID, &rec.IdempotencyKey,
		&rec.AmountCents, &rec.Currency, &rec.Status,
		&rec.Description, &rec.CreatedAt, &rec.OutboxSentAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("billing: get by idempotency key: %w", err)
	}
	return rec, nil
}

func (r *BillingRepository) PendingOutbox(ctx context.Context, limit int) ([]*domain.BillingRecord, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, tenant_id, idempotency_key, amount_cents, currency,
		        status, description, created_at, outbox_sent_at
		 FROM billing_records
		 WHERE outbox_sent_at IS NULL
		 ORDER BY created_at
		 LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("billing: pending outbox: %w", err)
	}
	defer rows.Close()

	var out []*domain.BillingRecord
	for rows.Next() {
		rec := &domain.BillingRecord{}
		if err := rows.Scan(
			&rec.ID, &rec.TenantID, &rec.IdempotencyKey,
			&rec.AmountCents, &rec.Currency, &rec.Status,
			&rec.Description, &rec.CreatedAt, &rec.OutboxSentAt,
		); err != nil {
			return nil, fmt.Errorf("billing: scan: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (r *BillingRepository) MarkOutboxSent(ctx context.Context, id string) error {
	now := time.Now().UTC()
	_, err := r.pool.Exec(ctx,
		`UPDATE billing_records SET outbox_sent_at = $1 WHERE id = $2`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("billing: mark outbox sent: %w", err)
	}
	return nil
}
