//go:build integration

package anomalies

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestWriteSkew_ReproducesAndFixes is the on-call doctors / admin-quorum
// scenario from DDIA Ch7:
//
//   - There are two admins, both currently on call. The invariant the
//     application wants to preserve is "at least one admin must be on
//     call at all times."
//
//   - Two transactions run concurrently. Each reads the count of admins
//     currently on call. Each sees 2 (the other admin is still on call,
//     in the snapshot). Each then sets ITSELF off-call, since the
//     invariant appears to permit it. Both commit. End state: zero
//     admins on call. Invariant violated.
//
//   - SNAPSHOT ISOLATION (Postgres REPEATABLE READ) does not catch this:
//     the two transactions read overlapping data (the same set of
//     admin rows) and write to disjoint rows (each writes only its own
//     row), so MVCC sees no conflict. This is write skew.
//
//   - SERIALIZABLE catches it via SSI: Postgres tracks the read-write
//     dependency, detects that the two transactions cannot be
//     serialized, and aborts one with SQLSTATE 40001 ("could not
//     serialize access due to read/write dependencies"). The
//     application is expected to retry.
//
// The test asserts both outcomes — REPEATABLE READ produces the
// violation, SERIALIZABLE prevents it — so a future regression in
// either Postgres behavior or the test is a hard failure rather than
// a silently changed lesson.
func TestWriteSkew_ReproducesAndFixes(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()

	const table = "oncall_demo"
	mustExec(t, pool, `DROP TABLE IF EXISTS `+table)
	mustExec(t, pool, `CREATE TABLE `+table+` (id INT PRIMARY KEY, on_call BOOLEAN NOT NULL)`)
	t.Cleanup(func() { mustExec(t, pool, `DROP TABLE IF EXISTS `+table) })

	resetState := func(t *testing.T) {
		t.Helper()
		mustExec(t, pool, `TRUNCATE `+table)
		mustExec(t, pool, `INSERT INTO `+table+` (id, on_call) VALUES (1, true), (2, true)`)
	}

	// runRound starts two transactions at the chosen isolation level,
	// each tries to take itself off-call if "another admin is on call,"
	// and returns:
	//   - finalOnCall: count of admins still on_call after both finish
	//   - aborted: number of transactions that failed with serialization conflict
	runRound := func(t *testing.T, level pgx.TxIsoLevel) (finalOnCall int, aborted int) {
		t.Helper()
		resetState(t)

		// barrier: both transactions read before either writes
		var afterReads sync.WaitGroup
		afterReads.Add(2)

		var abortedCount atomic.Int32
		var wg sync.WaitGroup
		wg.Add(2)

		takeOffCall := func(label string, myID int) {
			defer wg.Done()

			tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: level})
			if err != nil {
				t.Errorf("%s: begin: %v", label, err)
				return
			}
			defer tx.Rollback(ctx) //nolint:errcheck

			var count int
			if err := tx.QueryRow(ctx,
				`SELECT COUNT(*) FROM `+table+` WHERE on_call = true`,
			).Scan(&count); err != nil {
				t.Errorf("%s: count: %v", label, err)
				return
			}
			afterReads.Done()
			afterReads.Wait()

			// Pause briefly so both reads land before either write
			// commits — guarantees overlapping snapshots.
			time.Sleep(20 * time.Millisecond)

			if count < 2 {
				// Invariant says we can't go off call.
				_ = tx.Rollback(ctx)
				return
			}

			if _, err := tx.Exec(ctx,
				`UPDATE `+table+` SET on_call = false WHERE id = $1`, myID,
			); err != nil {
				if isSerializationFailure(err) {
					abortedCount.Add(1)
					return
				}
				t.Errorf("%s: update: %v", label, err)
				return
			}
			if err := tx.Commit(ctx); err != nil {
				if isSerializationFailure(err) {
					abortedCount.Add(1)
					return
				}
				t.Errorf("%s: commit: %v", label, err)
			}
		}

		go takeOffCall("a", 1)
		go takeOffCall("b", 2)
		wg.Wait()

		var n int
		if err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM `+table+` WHERE on_call = true`,
		).Scan(&n); err != nil {
			t.Fatalf("final count: %v", err)
		}
		return n, int(abortedCount.Load())
	}

	t.Run("anomaly_under_repeatable_read", func(t *testing.T) {
		final, aborted := runRound(t, pgx.RepeatableRead)
		if final != 0 {
			t.Fatalf("expected write-skew violation (final on_call = 0), got %d.\n"+
				"If both transactions visibly serialized, snapshot isolation "+
				"semantics changed and the chapter's lesson is now wrong.",
				final)
		}
		if aborted != 0 {
			t.Fatalf("REPEATABLE READ must NOT abort either transaction, but %d aborted", aborted)
		}
		t.Logf("write skew reproduced: %d admins on call after both txns committed (invariant violated)", final)
	})

	t.Run("fixed_with_serializable", func(t *testing.T) {
		final, aborted := runRound(t, pgx.Serializable)
		if final == 0 {
			t.Fatalf("SERIALIZABLE should preserve the invariant: expected at least 1 admin on call, got 0")
		}
		if aborted == 0 {
			t.Fatalf("SERIALIZABLE should abort one transaction with 40001; none did")
		}
		t.Logf("SSI caught the conflict: %d transaction(s) aborted, %d admin(s) still on call", aborted, final)
	})
}

// isSerializationFailure detects Postgres SQLSTATE 40001
// ("could not serialize access due to read/write dependencies").
// Returned by the SERIALIZABLE level when SSI detects a conflict;
// the application is expected to retry.
func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "40001" {
		return true
	}
	// Older clients sometimes surface the message text only.
	return strings.Contains(strings.ToLower(err.Error()), "could not serialize access")
}

// silence unused import in case future edits remove the only pgxpool use
var _ pgxpool.Pool
