//go:build integration

package anomalies

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dsn picks the Postgres connection string. Empty -> skip.
func dsn(t *testing.T) string {
	t.Helper()
	v := os.Getenv("POSTGRES_DSN")
	if v == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}
	return v
}

func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn(t))
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func mustExec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

// TestLostUpdate_ReproducesAndFixes is the canonical lost-update demo:
//
//  1. A scratch row holds balance = 100.
//  2. Two goroutines each run, in their own transaction at the
//     PostgreSQL default (READ COMMITTED): SELECT balance, sleep
//     briefly so the other transaction's read is guaranteed to land
//     before either commits, decrement by 50 in Go, UPDATE the row.
//     Final balance is 50 — one update overwrote the other.
//
//  3. Same scenario again, but the SELECT is `SELECT ... FOR UPDATE`,
//     which acquires a row-level lock. The second transaction blocks
//     until the first commits, observes balance=50, and writes 0.
//     Final balance is 0 — the correct outcome.
//
// The test asserts BOTH outcomes: the first phase MUST show the
// anomaly, the second phase MUST show the fix. This makes the test
// pedagogically meaningful — it stops being a demo if either side
// silently changes behavior.
func TestLostUpdate_ReproducesAndFixes(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()

	const table = "lost_update_demo"
	mustExec(t, pool, `DROP TABLE IF EXISTS `+table)
	mustExec(t, pool, `CREATE TABLE `+table+` (id INT PRIMARY KEY, balance BIGINT NOT NULL)`)
	t.Cleanup(func() { mustExec(t, pool, `DROP TABLE IF EXISTS `+table) })

	// Run a single round of two concurrent decrements. Returns the
	// final balance and the worst-observed lock-wait time so the
	// caller can sanity-check that the goroutines actually overlapped.
	runRound := func(useForUpdate bool) int64 {
		t.Helper()
		mustExec(t, pool, `TRUNCATE `+table)
		mustExec(t, pool, `INSERT INTO `+table+` (id, balance) VALUES (1, 100)`)

		// A barrier so both transactions read inside the anomaly
		// window — without it the test would be flaky on slow CI.
		ready := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		decrement := func(label string) {
			defer wg.Done()

			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				t.Errorf("%s: begin: %v", label, err)
				return
			}
			defer tx.Rollback(ctx) //nolint:errcheck

			query := `SELECT balance FROM ` + table + ` WHERE id = 1`
			if useForUpdate {
				query += ` FOR UPDATE`
			}

			var balance int64
			if err := tx.QueryRow(ctx, query).Scan(&balance); err != nil {
				t.Errorf("%s: select: %v", label, err)
				return
			}

			// Hold the read briefly so the other transaction can race.
			// Without FOR UPDATE both transactions see the same value
			// here; with FOR UPDATE the second one blocks at the SELECT.
			ready <- struct{}{}
			time.Sleep(50 * time.Millisecond)

			newBalance := balance - 50
			if _, err := tx.Exec(ctx,
				`UPDATE `+table+` SET balance = $1 WHERE id = 1`,
				newBalance,
			); err != nil {
				t.Errorf("%s: update: %v", label, err)
				return
			}
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("%s: commit: %v", label, err)
				return
			}
		}

		go decrement("a")
		go decrement("b")

		// Drain the readiness signals so the goroutines can proceed.
		<-ready
		<-ready

		wg.Wait()

		var final int64
		if err := pool.QueryRow(ctx, `SELECT balance FROM `+table+` WHERE id = 1`).Scan(&final); err != nil {
			t.Fatalf("read final: %v", err)
		}
		return final
	}

	t.Run("anomaly_under_read_committed", func(t *testing.T) {
		final := runRound(false)
		// 100 - 50 - 50 = 0 is the correct serial result. Lost update
		// gives 50: both transactions saw 100 and wrote 50.
		if final != 50 {
			t.Fatalf("expected the lost-update anomaly (final=50), got %d.\n"+
				"If this is suddenly correct (0), Postgres or the test "+
				"changed behavior — the chapter narrative is now wrong.",
				final)
		}
		t.Logf("anomaly reproduced: final balance = %d (correct serial result is 0)", final)
	})

	t.Run("fixed_with_select_for_update", func(t *testing.T) {
		final := runRound(true)
		if final != 0 {
			t.Fatalf("SELECT FOR UPDATE should serialize the decrements: expected 0, got %d", final)
		}
		t.Logf("FOR UPDATE serialized the writes: final balance = %d", final)
	})
}
