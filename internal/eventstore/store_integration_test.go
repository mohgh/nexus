//go:build integration

// Live-Postgres tests for the event store.
//
// The unit tests next door cover the trim rule and the horizon
// arithmetic in isolation. These cover what only a real database can
// show: that a BIGSERIAL hands out stream_position at INSERT time
// while the row appears at COMMIT time, and that the store refuses to
// serve events that sit above a position a live transaction still
// holds.
//
// events_store is append-only — the migration says so, and these
// tests honour it. Nothing here deletes rows; every test derives its
// own starting position instead, so repeated runs simply append.
//
// Run via:
//
//	POSTGRES_DSN=postgres://nexus:nexus_secret@127.0.0.1:55432/nexus?sslmode=disable \
//	    go test -tags=integration -v ./internal/eventstore/...

package eventstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// insertHeld inserts one event on a dedicated connection inside a
// transaction that is left open, and returns the stream_position the
// sequence handed it plus a commit func. Until commit is called the
// row is invisible to everyone else while its position is spoken for
// — exactly the interleaving that used to lose events.
func insertHeld(t *testing.T, stream, eventType string) (int64, func()) {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, os.Getenv("POSTGRES_DSN"))
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	var pos int64
	err = tx.QueryRow(ctx,
		`INSERT INTO events_store (stream_name, event_type, data, metadata, occurred_at)
		 VALUES ($1, $2, '{}', '{}', now())
		 RETURNING stream_position`,
		stream, eventType,
	).Scan(&pos)
	if err != nil {
		t.Fatalf("held insert: %v", err)
	}

	committed := false
	t.Cleanup(func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
		_ = conn.Close(ctx)
	})
	return pos, func() {
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit held insert: %v", err)
		}
		committed = true
	}
}

func testEvent(stream, eventType string) StoredEvent {
	return StoredEvent{
		StreamName: stream,
		EventType:  eventType,
		Data:       []byte(`{"tenant_id":"t","event_type":"x","value":1}`),
		Metadata:   []byte(`{}`),
		OccurredAt: time.Now().UTC(),
	}
}

func uniqueStream(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("es-it-%s-%s", t.Name(), uuid.NewString()[:8])
}

// TestStore_AppendAndReadStream: Append writes atomically and hands
// the events consecutive positions; ReadStream replays them in order.
func TestStore_AppendAndReadStream(t *testing.T) {
	ctx := context.Background()
	s := NewStore(openPool(t))
	stream := uniqueStream(t)

	if err := s.Append(ctx,
		testEvent(stream, "TenantCreated"),
		testEvent(stream, "EventIngested"),
		testEvent(stream, "EventIngested"),
	); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := s.ReadStream(ctx, stream)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len=%d, want 3", len(got))
	}
	if got[0].EventType != "TenantCreated" {
		t.Fatalf("first event type = %q, want TenantCreated", got[0].EventType)
	}
	for i := 1; i < len(got); i++ {
		if got[i].StreamPosition != got[i-1].StreamPosition+1 {
			t.Fatalf("positions are not consecutive within one Append: %d then %d",
				got[i-1].StreamPosition, got[i].StreamPosition)
		}
	}
	if string(got[1].Data) == "" || string(got[1].Metadata) == "" {
		t.Fatalf("payload round-trip lost data: %+v", got[1])
	}
}

// TestStore_AppendIsAtomic: a failing event in the middle of an
// Append leaves nothing behind. (Empty Data violates the NOT NULL
// on a JSONB column, which is a convenient way to fail mid-batch.)
func TestStore_AppendIsAtomic(t *testing.T) {
	ctx := context.Background()
	s := NewStore(openPool(t))
	stream := uniqueStream(t)

	bad := testEvent(stream, "EventIngested")
	bad.Data = nil
	err := s.Append(ctx, testEvent(stream, "EventIngested"), bad)
	if err == nil {
		t.Fatal("Append with an invalid event should fail")
	}

	got, readErr := s.ReadStream(ctx, stream)
	if readErr != nil {
		t.Fatalf("ReadStream: %v", readErr)
	}
	if len(got) != 0 {
		t.Fatalf("a failed Append left %d event(s) behind; it must be all-or-nothing", len(got))
	}
}

// TestStore_HeadPositionTracksTheLastAppend.
func TestStore_HeadPositionTracksTheLastAppend(t *testing.T) {
	ctx := context.Background()
	s := NewStore(openPool(t))
	stream := uniqueStream(t)

	before, err := s.HeadPosition(ctx)
	if err != nil {
		t.Fatalf("HeadPosition: %v", err)
	}
	if err := s.Append(ctx, testEvent(stream, "EventIngested")); err != nil {
		t.Fatalf("Append: %v", err)
	}
	after, err := s.HeadPosition(ctx)
	if err != nil {
		t.Fatalf("HeadPosition: %v", err)
	}
	if after <= before {
		t.Fatalf("head did not advance: %d -> %d", before, after)
	}

	written, err := s.ReadStream(ctx, stream)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if after != written[0].StreamPosition {
		t.Fatalf("head=%d, want the position of the event just written (%d)",
			after, written[0].StreamPosition)
	}
}

// TestStore_ReadAllFromReturnsEverythingAfterAPosition, and honours
// the batch limit.
func TestStore_ReadAllFromReturnsEverythingAfterAPosition(t *testing.T) {
	ctx := context.Background()
	s := NewStore(openPool(t))
	stream := uniqueStream(t)

	if err := s.Append(ctx,
		testEvent(stream, "EventIngested"),
		testEvent(stream, "EventIngested"),
		testEvent(stream, "EventIngested"),
	); err != nil {
		t.Fatalf("Append: %v", err)
	}
	written, err := s.ReadStream(ctx, stream)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	// Start one below our first event so the read is unaffected by
	// whatever else is (or is not) in the table.
	from := written[0].StreamPosition - 1

	got, err := s.ReadAllFrom(ctx, from, 100)
	if err != nil {
		t.Fatalf("ReadAllFrom: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("len=%d, want 3 (from %d)", len(got), from)
	}
	if got[0].StreamPosition != written[0].StreamPosition {
		t.Fatalf("first position = %d, want %d", got[0].StreamPosition, written[0].StreamPosition)
	}

	limited, err := s.ReadAllFrom(ctx, from, 2)
	if err != nil {
		t.Fatalf("ReadAllFrom limited: %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("limit ignored: len=%d, want 2", len(limited))
	}

	drained, err := s.ReadAllFrom(ctx, written[2].StreamPosition, 100)
	if err != nil {
		t.Fatalf("ReadAllFrom drained: %v", err)
	}
	if len(drained) != 0 {
		t.Fatalf("reading past the last event returned %d events", len(drained))
	}
}

// TestStore_ReadAllFromWithholdsEventsAboveAnUncommittedPosition is
// the store-level version of the bug.
//
// Position N is held open by a live transaction; position N+1 commits
// first. Serving N+1 is what let the projection runner move its
// bookmark past N and lose it for good, so the store must withhold
// N+1 until N lands.
func TestStore_ReadAllFromWithholdsEventsAboveAnUncommittedPosition(t *testing.T) {
	ctx := context.Background()
	pool := openPool(t)
	s := NewStore(pool)
	stream := uniqueStream(t)

	heldPos, commitHeld := insertHeld(t, stream, "SlowCommit")

	if err := s.Append(ctx, testEvent(stream, "FastCommit")); err != nil {
		t.Fatalf("Append fast event: %v", err)
	}
	fast, err := s.ReadStream(ctx, stream)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(fast) != 1 || fast[0].StreamPosition != heldPos+1 {
		t.Fatalf("expected exactly the fast event at %d, got %+v", heldPos+1, fast)
	}

	from := heldPos - 1

	got, err := s.ReadAllFrom(ctx, from, 100)
	if err != nil {
		t.Fatalf("ReadAllFrom: %v", err)
	}
	for _, e := range got {
		if e.StreamPosition > heldPos {
			t.Fatalf("store served position %d over uncommitted position %d — "+
				"a consumer bookmarking that would lose %d permanently",
				e.StreamPosition, heldPos, heldPos)
		}
	}

	head, err := s.HeadPosition(ctx)
	if err != nil {
		t.Fatalf("HeadPosition: %v", err)
	}
	safe, err := s.SafeHeadPosition(ctx, from)
	if err != nil {
		t.Fatalf("SafeHeadPosition: %v", err)
	}
	if head < heldPos+1 {
		t.Fatalf("head=%d, want at least %d", head, heldPos+1)
	}
	if safe >= heldPos {
		t.Fatalf("safe head=%d, want below the uncommitted position %d "+
			"(this is what stops the lag gauge reporting a comfortable 0)", safe, heldPos)
	}

	commitHeld()

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err = s.ReadAllFrom(ctx, from, 100)
		if err != nil {
			t.Fatalf("ReadAllFrom after commit: %v", err)
		}
		if len(got) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("after commit the store still returns %d event(s) from %d; "+
				"positions %d and %d should both be readable", len(got), from, heldPos, heldPos+1)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got[0].StreamPosition != heldPos || got[1].StreamPosition != heldPos+1 {
		t.Fatalf("got positions %d,%d after commit, want %d,%d",
			got[0].StreamPosition, got[1].StreamPosition, heldPos, heldPos+1)
	}
}

// TestStore_ReadAllFromStepsOverARolledBackPosition: withholding on a
// gap is only correct if dead gaps eventually clear. A rolled back
// insert burns a sequence value forever, and contiguity alone would
// park the reader in front of it for the life of the process. The
// settled horizon is what unblocks it.
func TestStore_ReadAllFromStepsOverARolledBackPosition(t *testing.T) {
	ctx := context.Background()
	s := NewStore(openPool(t))
	stream := uniqueStream(t)

	deadPos := insertRolledBack(t, stream)

	if err := s.Append(ctx, testEvent(stream, "AfterTheHole")); err != nil {
		t.Fatalf("Append: %v", err)
	}

	from := deadPos - 1
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := s.ReadAllFrom(ctx, from, 100)
		if err != nil {
			t.Fatalf("ReadAllFrom: %v", err)
		}
		if len(got) > 0 {
			if got[0].StreamPosition <= deadPos {
				t.Fatalf("position %d was rolled back and must never be served", got[0].StreamPosition)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reader is still parked in front of the dead position %d after 5s; "+
				"the settled horizon never advanced", deadPos)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// insertRolledBack burns one sequence value and throws it away,
// leaving a hole that no commit will ever fill. Sequences are not
// transactional: the number is spent whether or not the transaction
// survives.
func insertRolledBack(t *testing.T, stream string) int64 {
	t.Helper()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, os.Getenv("POSTGRES_DSN"))
	if err != nil {
		t.Fatalf("pgx.Connect: %v", err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	var pos int64
	err = tx.QueryRow(ctx,
		`INSERT INTO events_store (stream_name, event_type, data, metadata, occurred_at)
		 VALUES ($1, 'RolledBack', '{}', '{}', now())
		 RETURNING stream_position`,
		stream,
	).Scan(&pos)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	return pos
}
