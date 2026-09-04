//go:build integration

// Live-Postgres test for the projection runner's catch-up loop.
//
// This is the test the unit tier could not write. The in-memory
// fakeStore now models uncommitted positions explicitly, but it only
// models them because we told it to; the point of this file is that
// the interleaving is not a modelling choice, it is what a BIGSERIAL
// and MVCC actually do.
//
// The bug it pins: stream_position is handed out at INSERT time and
// the row appears at COMMIT time. A slow transaction holds position N
// while a fast one commits N+1. A runner that reads "everything I can
// see after N-1", gets [N+1], and bookmarks N+1 never reads N once it
// commits a moment later. The event is gone, the projection is short
// one row, and /api/v1/projections reports lag 0 because
// MAX(stream_position) is N+1 too.
//
// Run via:
//
//	POSTGRES_DSN=postgres://nexus:nexus_secret@127.0.0.1:55432/nexus?sslmode=disable \
//	    go test -tags=integration -v -run Uncommitted ./internal/projections/...

package projections_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/eventstore"
	"github.com/mohgh/nexus/internal/projections"
)

// recordingProjection is a real Projection with no storage: it starts
// at a position we choose and records what it is fed. Using it rather
// than TenantStatsProjection keeps the test on the thing under test —
// the store/runner handshake — instead of on tenant fixtures.
type recordingProjection struct {
	mu       sync.Mutex
	startPos int64
	applied  []int64
	last     int64
}

func (p *recordingProjection) Name() string { return "gap-probe" }

func (p *recordingProjection) LastPosition() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last
}

func (p *recordingProjection) LoadPosition(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.last = p.startPos
	return nil
}

func (p *recordingProjection) Apply(_ context.Context, e eventstore.StoredEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applied = append(p.applied, e.StreamPosition)
	p.last = e.StreamPosition
	return nil
}

func (p *recordingProjection) Reset(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.applied, p.last = nil, 0
	return nil
}

func (p *recordingProjection) snapshot() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int64(nil), p.applied...)
}

func dsnOrSkipPG(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}
	return dsn
}

// TestRunner_DoesNotSkipEventsHeldByAnUncommittedTransaction is the
// regression test for the silent event loss.
//
// Against the old implementation it fails at "the runner applied
// position N+1 over uncommitted position N": ReadAllFrom served the
// fast event, the bookmark jumped the hole, and the slow event was
// never read again.
func TestRunner_DoesNotSkipEventsHeldByAnUncommittedTransaction(t *testing.T) {
	dsn := dsnOrSkipPG(t)
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	const stream = "projections-gap-it"

	// ─── The slow writer: INSERT, then hold the transaction open ──────────
	slowConn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer slowConn.Close(ctx)

	slowTx, err := slowConn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin slow tx: %v", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = slowTx.Rollback(ctx)
		}
	}()

	var slowPos int64
	err = slowTx.QueryRow(ctx,
		`INSERT INTO events_store (stream_name, event_type, data, metadata, occurred_at)
		 VALUES ($1, 'SlowCommit', '{"n":1}', '{}', now())
		 RETURNING stream_position`,
		stream,
	).Scan(&slowPos)
	if err != nil {
		t.Fatalf("slow insert: %v", err)
	}

	// ─── The fast writer: INSERT and COMMIT while the slow one waits ──────
	var fastPos int64
	err = pool.QueryRow(ctx,
		`INSERT INTO events_store (stream_name, event_type, data, metadata, occurred_at)
		 VALUES ($1, 'FastCommit', '{"n":2}', '{}', now())
		 RETURNING stream_position`,
		stream,
	).Scan(&fastPos)
	if err != nil {
		t.Fatalf("fast insert: %v", err)
	}
	if fastPos != slowPos+1 {
		t.Fatalf("expected the classic interleaving (slow=%d, fast=%d must be adjacent)", slowPos, fastPos)
	}
	t.Logf("slow (uncommitted) position=%d, fast (committed) position=%d", slowPos, fastPos)

	// ─── The runner, parked one position below the hole ───────────────────
	store := eventstore.NewStore(pool)
	proj := &recordingProjection{startPos: slowPos - 1}
	r := projections.NewRunner(store, []projections.Projection{proj}, nil,
		projections.Config{PollInterval: 20 * time.Millisecond})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = r.Run(runCtx) }()

	// Several sweeps' worth of opportunity to step over the hole.
	time.Sleep(300 * time.Millisecond)

	for _, pos := range proj.snapshot() {
		if pos > slowPos {
			t.Fatalf("the runner applied position %d over uncommitted position %d — "+
				"its bookmark has jumped the hole and %d is lost for good",
				pos, slowPos, slowPos)
		}
	}

	// The lag endpoint must not call this a healthy zero.
	lags, err := r.LagFor(ctx)
	if err != nil {
		t.Fatalf("LagFor: %v", err)
	}
	if len(lags) != 1 {
		t.Fatalf("len(lags)=%d, want 1", len(lags))
	}
	l := lags[0]
	t.Logf("while blocked: last=%d head=%d safe_head=%d lag=%d ready_lag=%d",
		l.LastPosition, l.HeadPosition, l.SafeHeadPosition, l.Lag, l.ReadyLag)
	if l.Lag == 0 {
		t.Fatalf("lag reported 0 while position %d is stranded behind an open transaction", slowPos)
	}
	if l.SafeHeadPosition >= slowPos {
		t.Fatalf("safe head=%d, want below the uncommitted position %d", l.SafeHeadPosition, slowPos)
	}

	// ─── The slow writer finally commits ──────────────────────────────────
	if err := slowTx.Commit(ctx); err != nil {
		t.Fatalf("commit slow tx: %v", err)
	}
	committed = true

	deadline := time.Now().Add(5 * time.Second)
	var got []int64
	for time.Now().Before(deadline) {
		got = proj.snapshot()
		if len(got) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !equal(got, []int64{slowPos, fastPos}) {
		t.Fatalf("applied %v, want [%d %d] — the once-uncommitted event must be picked up in order",
			got, slowPos, fastPos)
	}

	cancel()
	<-done
}
