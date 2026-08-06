package temporal

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/mohgh/nexus/internal/domain"
	pgstore "github.com/mohgh/nexus/internal/storage/postgres"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

// nonRetryable wraps err in a Temporal application error tagged
// non-retryable, so the workflow's RetryPolicy doesn't burn attempts
// on cases that can't succeed on retry. errType is reported back to
// Temporal as the failure type — search by it in the Temporal UI.
func nonRetryable(errType string, err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), errType, err)
}

// Activities holds the dependencies needed by the billing activities.
// Injected via worker.go — no global state.
//
// pool is held directly (not behind an interface) because RecordCharge
// needs a transaction that spans tenant_credits and billing_records —
// that's the chapter's headline lesson on atomicity-across-tables.
// Hiding the transaction behind a domain interface obscures exactly
// what the chapter is teaching, so we accept the pgx coupling here.
type Activities struct {
	tenants domain.TenantRepository
	billing domain.BillingRepository
	pool    *pgstore.Pool
}

func NewActivities(tenants domain.TenantRepository, billing domain.BillingRepository, pool *pgstore.Pool) *Activities {
	return &Activities{tenants: tenants, billing: billing, pool: pool}
}

// ValidateCharge checks that the tenant exists and is on a plan that
// allows billing. Read-only and safe to retry on transient failures,
// but business-rule failures (tenant not found, free plan) are wrapped
// as Temporal non-retryable errors so the RetryPolicy doesn't burn
// attempts on cases that can't succeed on retry.
func (a *Activities) ValidateCharge(ctx context.Context, input ChargeInput) error {
	logger := activity.GetLogger(ctx)

	tenant, err := a.tenants.Get(ctx, input.TenantID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nonRetryable("TenantNotFound",
				fmt.Errorf("tenant %q not found", input.TenantID))
		}
		// Transient lookup failure — let Temporal retry.
		return fmt.Errorf("validate charge: get tenant: %w", err)
	}

	if tenant.Plan == "free" {
		return nonRetryable("PlanDoesNotAllowBilling",
			fmt.Errorf("tenant %q is on free plan — billing not enabled", input.TenantID))
	}

	logger.Info("charge validated",
		"tenant_id", input.TenantID,
		"plan", tenant.Plan,
		"amount_cents", input.AmountCents,
	)
	return nil
}

// RecordCharge atomically deducts the charge amount from the tenant's
// credit balance and inserts a billing_records row. Either both happen
// or neither — that's the Ch08 lesson on transactions across two
// tables.
//
// Idempotency: a retry after a partial failure returns the existing
// record rather than creating a duplicate charge. Two layers:
//
//  1. Fast path: GetByIdempotencyKey returns existing -> done, no
//     transaction opened. (Crucially: this also short-circuits the
//     credit deduction, so a retry after a successful charge never
//     decrements credits twice.)
//
//  2. Race path: ChargeWithCreditDeduction itself can return
//     ErrDuplicateKey when two workers race past step 1 — Postgres'
//     unique constraint catches it and the entire transaction (credit
//     deduction included) rolls back. We then re-fetch and return the
//     winner's record.
//
// ErrInsufficientCredit is wrapped as a Temporal non-retryable error:
// retrying a charge against a tenant with too little balance won't
// succeed until they top up, which is out-of-band of this workflow.
//
// The inserted row carries status='completed' from the start — the
// atomic transaction either committed (charge succeeded, status
// belongs as 'completed') or rolled back (no row exists at all).
// There is no transient 'pending' window in this implementation.
//
// Critically: this activity does NOT touch outbox_sent_at. The row is
// left at NULL so the outbox worker can pick it up and publish to
// Kafka asynchronously. An earlier draft of the workflow had a
// PublishChargeEvent step that called MarkOutboxSent here, which
// silently disabled the outbox on the happy path — keep this property
// in mind if you ever consider adding a "publish" step back.
func (a *Activities) RecordCharge(ctx context.Context, input ChargeInput) (ChargeResult, error) {
	// Idempotent fast path. Skips the tx entirely on retry of an
	// already-completed charge — and skips re-deducting credits.
	existing, err := a.billing.GetByIdempotencyKey(ctx, input.TenantID, input.IdempotencyKey)
	if err == nil {
		return ChargeResult{BillingRecordID: existing.ID, Status: string(existing.Status)}, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return ChargeResult{}, fmt.Errorf("record charge: lookup: %w", err)
	}

	rec := &domain.BillingRecord{
		ID:             uuid.New().String(),
		TenantID:       input.TenantID,
		IdempotencyKey: input.IdempotencyKey,
		AmountCents:    input.AmountCents,
		Currency:       input.Currency,
		Status:         domain.BillingStatusCompleted,
		Description:    input.Description,
		CreatedAt:      time.Now().UTC(),
	}

	if err := pgstore.ChargeWithCreditDeduction(ctx, a.pool, rec); err != nil {
		if errors.Is(err, domain.ErrInsufficientCredit) {
			// Non-retryable: the balance won't change by retrying.
			return ChargeResult{}, nonRetryable("InsufficientCredit", err)
		}
		if errors.Is(err, domain.ErrDuplicateKey) {
			// Race past the idempotency fast path. Re-fetch the row
			// the winning transaction inserted and return its result —
			// the credit deduction in *this* transaction was rolled
			// back, so we haven't double-charged.
			existing, fetchErr := a.billing.GetByIdempotencyKey(ctx, input.TenantID, input.IdempotencyKey)
			if fetchErr != nil {
				return ChargeResult{}, fmt.Errorf("record charge: fetch after duplicate: %w", fetchErr)
			}
			return ChargeResult{BillingRecordID: existing.ID, Status: string(existing.Status)}, nil
		}
		// Transient (DB connection, etc.) — let Temporal retry.
		return ChargeResult{}, fmt.Errorf("record charge: %w", err)
	}

	return ChargeResult{BillingRecordID: rec.ID, Status: string(rec.Status)}, nil
}

// (PublishChargeEvent was removed from this file.
//
// The previous implementation called MarkOutboxSent inside the
// workflow, which set outbox_sent_at = NOW() immediately after the
// transaction committed — so the outbox worker's PendingOutbox query
// (WHERE outbox_sent_at IS NULL) never found the row on the happy
// path, and nothing was ever published to Kafka. Calling it
// "PublishChargeEvent" was therefore actively misleading.
//
// The fix is to delete the activity entirely: the outbox worker is
// the only thing that should ever mark a row sent, and only after it
// has successfully published. The workflow stops after RecordCharge.)
