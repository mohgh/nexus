//go:build integration

package anomalies

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/mohgh/nexus/internal/domain"
	pgstore "github.com/mohgh/nexus/internal/storage/postgres"
)

// openPgstorePool opens a Nexus *pgstore.Pool against POSTGRES_DSN.
// (Distinct from openPool which returns the raw pgxpool.Pool used by
// the scratch-table tests.)
func openPgstorePool(t *testing.T) *pgstore.Pool {
	t.Helper()
	pool, err := pgstore.NewPool(context.Background(), dsn(t))
	if err != nil {
		t.Fatalf("pgstore.NewPool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// TestTenantCredits_LostUpdateOnRealTable mirrors the scratch-table
// lost-update test, but on the actual `tenant_credits` table that
// production billing uses. The lesson is the same — naive
// read-modify-write loses updates under READ COMMITTED — but the
// failure now lives on the real domain object, so a regression in
// the credits repo's safety properties surfaces here.
//
// The "fix" branch uses the production repo's Deduct path (which
// internally does SELECT ... FOR UPDATE). Both branches must hold:
// a green result on the unsafe branch would mean Postgres no longer
// permits the anomaly, and a red result on the safe branch would mean
// the repo regressed.
func TestTenantCredits_LostUpdateOnRealTable(t *testing.T) {
	ctx := context.Background()
	pool := openPgstorePool(t)
	repo := pgstore.NewTenantCreditsRepository(pool)

	tenantID := setupTestTenant(t, pool, 100)

	// Phase 1 — anomaly: two concurrent naive read-modify-write
	// deductions at READ COMMITTED. Both observe 100, both write 50.
	// Final balance: 50 — one decrement was lost.
	resetBalance(t, pool, tenantID, 100)
	concurrentNaiveDeducts(t, pool, tenantID, 50)

	balance, err := repo.GetBalance(ctx, tenantID)
	if err != nil {
		t.Fatalf("read balance: %v", err)
	}
	if balance != 50 {
		t.Fatalf("expected lost-update anomaly (final balance = 50), got %d.\n"+
			"If the naive read-modify-write no longer loses an update, the chapter "+
			"narrative needs updating — this test guards that.",
			balance)
	}
	t.Logf("anomaly reproduced on tenant_credits: balance = %d (correct serial result is 0)", balance)

	// Phase 2 — fix: same scenario, but each deduct goes through the
	// production repo. The repo uses SELECT ... FOR UPDATE, so the
	// two transactions serialise and both decrements take effect.
	resetBalance(t, pool, tenantID, 100)
	concurrentRepoDeducts(t, repo, tenantID, 50)

	balance, err = repo.GetBalance(ctx, tenantID)
	if err != nil {
		t.Fatalf("read balance: %v", err)
	}
	if balance != 0 {
		t.Fatalf("repo.Deduct should serialise concurrent deducts: expected 0, got %d", balance)
	}
	t.Logf("repo.Deduct preserved the invariant: balance = %d", balance)
}

// TestTenantCredits_DeductRejectsInsufficient verifies the
// ErrInsufficientCredit path: deducting 80 from a 50 balance must
// return ErrInsufficientCredit and leave the balance unchanged.
func TestTenantCredits_DeductRejectsInsufficient(t *testing.T) {
	ctx := context.Background()
	pool := openPgstorePool(t)
	repo := pgstore.NewTenantCreditsRepository(pool)

	tenantID := setupTestTenant(t, pool, 50)

	err := repo.Deduct(ctx, tenantID, 80)
	if !errors.Is(err, domain.ErrInsufficientCredit) {
		t.Fatalf("expected ErrInsufficientCredit, got %v", err)
	}

	bal, err := repo.GetBalance(ctx, tenantID)
	if err != nil {
		t.Fatalf("get balance: %v", err)
	}
	if bal != 50 {
		t.Fatalf("balance should be unchanged after rejected deduct: got %d, want 50", bal)
	}
}

// ─── Test helpers ────────────────────────────────────────────────────────────

// setupTestTenant inserts a one-off tenant, registers cleanup that
// deletes it (cascades to tenant_credits + billing_records via FK),
// and tops the auto-created credits row to startingBalance.
func setupTestTenant(t *testing.T, pool *pgstore.Pool, startingBalance int64) string {
	t.Helper()
	id := uuid.New().String()
	now := time.Now().UTC()

	if _, err := pool.Exec(context.Background(),
		`INSERT INTO tenants (id, name, plan, created_at, updated_at)
		 VALUES ($1, $2, 'pro', $3, $3)`,
		id, "credits-anomaly-test-"+id[:8], now,
	); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, id)
	})

	resetBalance(t, pool, id, startingBalance)
	return id
}

func resetBalance(t *testing.T, pool *pgstore.Pool, tenantID string, balance int64) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`UPDATE tenant_credits SET balance_cents = $1, updated_at = NOW() WHERE tenant_id = $2`,
		balance, tenantID,
	); err != nil {
		t.Fatalf("reset balance: %v", err)
	}
}

// concurrentNaiveDeducts runs two transactions at READ COMMITTED that
// each read the balance, sleep briefly so the other read lands inside
// the anomaly window, then write balance - amount.
func concurrentNaiveDeducts(t *testing.T, pool *pgstore.Pool, tenantID string, amount int64) {
	t.Helper()
	ctx := context.Background()

	ready := make(chan struct{}, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	work := func() {
		defer wg.Done()
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
		if err != nil {
			t.Errorf("begin: %v", err)
			return
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		var balance int64
		if err := tx.QueryRow(ctx,
			`SELECT balance_cents FROM tenant_credits WHERE tenant_id = $1`,
			tenantID,
		).Scan(&balance); err != nil {
			t.Errorf("select: %v", err)
			return
		}

		ready <- struct{}{}
		time.Sleep(50 * time.Millisecond)

		if _, err := tx.Exec(ctx,
			`UPDATE tenant_credits SET balance_cents = $1, updated_at = NOW() WHERE tenant_id = $2`,
			balance-amount, tenantID,
		); err != nil {
			t.Errorf("update: %v", err)
			return
		}
		if err := tx.Commit(ctx); err != nil {
			t.Errorf("commit: %v", err)
		}
	}

	go work()
	go work()
	<-ready
	<-ready
	wg.Wait()
}

// concurrentRepoDeducts runs the same scenario but through the
// production repo, which uses SELECT ... FOR UPDATE. Both decrements
// must succeed and apply.
func concurrentRepoDeducts(t *testing.T, repo *pgstore.TenantCreditsRepository, tenantID string, amount int64) {
	t.Helper()
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer wg.Done()
			if err := repo.Deduct(ctx, tenantID, amount); err != nil {
				t.Errorf("deduct: %v", err)
			}
		}()
	}
	wg.Wait()
}
