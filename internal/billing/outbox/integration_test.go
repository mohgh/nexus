//go:build integration

// Live-Postgres integration tests for the outbox pattern.
//
// The unit tests in worker_test.go cover the worker's behavior in
// isolation using fake repo + publisher. These integration tests
// pin down the multi-component contract that the unit tests can't
// reach: a successful charge leaves outbox_sent_at = NULL, and the
// worker is the one that eventually sets it.
//
// This is the regression test for the original Ch08 bug where the
// Temporal workflow itself called MarkOutboxSent — defeating the
// outbox pattern silently on the happy path. If a future refactor
// reintroduces that bug, this test fails at "outbox_sent_at should
// still be NULL".
//
// Run via: make ch08-anomalies, or POSTGRES_DSN=... go test -tags=integration

package outbox_test

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/mohgh/nexus/internal/billing/outbox"
	"github.com/mohgh/nexus/internal/domain"
	pgstore "github.com/mohgh/nexus/internal/storage/postgres"
	"go.uber.org/zap"
)

func dsnOrSkip(t *testing.T) string {
	t.Helper()
	v := os.Getenv("POSTGRES_DSN")
	if v == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}
	return v
}

func setupPool(t *testing.T) *pgstore.Pool {
	t.Helper()
	pool, err := pgstore.NewPool(context.Background(), dsnOrSkip(t))
	if err != nil {
		t.Fatalf("pgstore.NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// setupTenant inserts a one-off tenant, tops its credits row to
// startBalance, and registers cleanup that deletes the tenant
// (cascading to credits + billing).
func setupTenant(t *testing.T, pool *pgstore.Pool, startBalance int64) string {
	t.Helper()
	id := uuid.New().String()
	now := time.Now().UTC()

	if _, err := pool.Exec(context.Background(),
		`INSERT INTO tenants (id, name, plan, created_at, updated_at)
		 VALUES ($1, $2, 'pro', $3, $3)`,
		id, "outbox-int-test-"+id[:8], now,
	); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, id)
	})

	if _, err := pool.Exec(context.Background(),
		`UPDATE tenant_credits SET balance_cents = $1, updated_at = NOW() WHERE tenant_id = $2`,
		startBalance, id,
	); err != nil {
		t.Fatalf("top up credits: %v", err)
	}
	return id
}

// countingPublisher counts the publish calls it receives. Used to
// assert that the worker DID publish (vs. it being a no-op because
// some other code already marked the row sent).
type countingPublisher struct {
	called atomic.Int32
}

func (p *countingPublisher) Publish(_ context.Context, _ *domain.BillingRecord) error {
	p.called.Add(1)
	return nil
}

// TestOutbox_ChargeLeavesRowPendingUntilWorkerPublishes is the
// regression test for the original "workflow defeats the outbox" bug.
//
// Contract under test:
//
//  1. ChargeWithCreditDeduction commits a row with status='completed'
//     and outbox_sent_at = NULL. That NULL is load-bearing — it's what
//     makes the outbox worker pick the row up. If anyone marks it
//     sent before publishing, the worker stops seeing it and Kafka
//     never gets the event.
//
//  2. The outbox worker is the ONLY component that calls
//     MarkOutboxSent, and only after Publisher.Publish succeeds.
//
// Either assertion failing would mean the outbox pattern has been
// silently broken in a way that's hard to spot in production.
func TestOutbox_ChargeLeavesRowPendingUntilWorkerPublishes(t *testing.T) {
	ctx := context.Background()
	pool := setupPool(t)

	tenantID := setupTenant(t, pool, 100_000)
	idempotencyKey := "outbox-int-" + uuid.New().String()
	recordID := uuid.New().String()

	rec := &domain.BillingRecord{
		ID:             recordID,
		TenantID:       tenantID,
		IdempotencyKey: idempotencyKey,
		AmountCents:    1_000,
		Currency:       "USD",
		Status:         domain.BillingStatusCompleted,
		Description:    "integration test charge",
		CreatedAt:      time.Now().UTC(),
	}

	// Step 1: run the atomic charge — the same call path the Temporal
	// activity uses. We bypass Temporal here because that lets the
	// test focus on the Postgres contract, not the workflow runtime.
	if err := pgstore.ChargeWithCreditDeduction(ctx, pool, rec); err != nil {
		t.Fatalf("ChargeWithCreditDeduction: %v", err)
	}

	// Read back the row and pin down the post-charge state.
	var status string
	var sentAt *time.Time
	if err := pool.QueryRow(ctx,
		`SELECT status, outbox_sent_at FROM billing_records WHERE id = $1`,
		recordID,
	).Scan(&status, &sentAt); err != nil {
		t.Fatalf("read billing_records: %v", err)
	}
	if status != string(domain.BillingStatusCompleted) {
		t.Fatalf("status: got %q, want %q (the activity must persist the final state, "+
			"not leave it as 'pending' for a follow-up update that doesn't exist)",
			status, domain.BillingStatusCompleted)
	}
	if sentAt != nil {
		t.Fatalf("outbox_sent_at: got %v, want NULL.\n"+
			"This is the regression case — if the charge path is marking rows sent\n"+
			"before publishing, the outbox worker will never see them and Kafka\n"+
			"will never receive the event. See activities.go for the prior bug.",
			*sentAt)
	}

	// Step 2: run the outbox worker for a short window with a
	// publisher we control. The worker should pick up our row,
	// publish it (incrementing the counter), and then mark it sent.
	pub := &countingPublisher{}
	billingRepo := pgstore.NewBillingRepository(pool)
	w := outbox.New(billingRepo, pub, zap.NewNop(), outbox.Config{
		PollInterval: 50 * time.Millisecond,
	})

	wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		_ = w.Run(wctx)
		close(done)
	}()

	// Wait until the worker reports at least one publish, then stop.
	deadline := time.After(2 * time.Second)
poll:
	for {
		if pub.called.Load() >= 1 && w.Stats().Published >= 1 {
			break poll
		}
		select {
		case <-deadline:
			t.Fatalf("worker did not publish our row within 2s. publish_calls=%d, stats=%+v",
				pub.called.Load(), w.Stats())
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-done

	// Step 3: the row should now be marked sent — by the worker, not
	// by anyone else.
	if err := pool.QueryRow(ctx,
		`SELECT outbox_sent_at FROM billing_records WHERE id = $1`,
		recordID,
	).Scan(&sentAt); err != nil {
		t.Fatalf("read billing_records after sweep: %v", err)
	}
	if sentAt == nil {
		t.Fatalf("outbox_sent_at: still NULL after the worker ran (stats=%+v)", w.Stats())
	}
	if pub.called.Load() != 1 {
		t.Fatalf("publish call count: got %d, want 1", pub.called.Load())
	}
}
