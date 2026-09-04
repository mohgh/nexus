// Seed script — populates Nexus with realistic demo data.
//
// Usage: go run ./scripts/seed/main.go
//    or: make seed
//
// Creates 10,000 events across the seeded tenants with varied event types,
// realistic payloads (including PII for Ch14 masking demos), and timestamps
// spread over the last 30 days.
//
// The seeder writes the SAME two tables the real ingest path writes
// (internal/storage/postgres/event_repo.go Create): the append-only
// `events_store` log first, then the `events` read projection, both in
// one transaction. An earlier version inserted into `events` only,
// which left the log that the architecture calls the source of truth
// holding a fraction of a percent of the data — and made
// cmd/projection-rebuild faithfully rebuild a read model of ~10 events
// for a database of 10,008. Anything that replays the log (projection
// rebuild, the Ch13 catch-up subscriptions, GDPR erasure by stream)
// only sees what is in `events_store`, so a seeder that skips it isn't
// seeding the system, only one of its projections.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/config"
)

// batchSize is how many events share one transaction. 10,000 separate
// transactions is ~10,000 fsyncs; one transaction for all 10,000 holds
// a single long write and makes a mid-run failure all-or-nothing.
// Batching in the middle keeps the seeder fast while bounding how much
// work a failure throws away.
const batchSize = 500

// seedEvent is one generated event, held just long enough to be
// written to both tables in the same transaction.
type seedEvent struct {
	id         string
	tenantID   string
	eventType  string
	payload    []byte
	value      float64
	occurredAt time.Time
}

// insertBatch writes a slice of events to events_store and events in a
// single transaction, mirroring EventRepository.Create. Both statements
// for a given event are queued adjacently so the log row and its
// projection row commit together and in that order.
func insertBatch(ctx context.Context, pool *pgxpool.Pool, events []seedEvent) error {
	if len(events) == 0 {
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	batch := &pgx.Batch{}
	for _, e := range events {
		// The canonical record. Shape matches event_repo.go exactly —
		// the projections unmarshal tenant_id/event_type/value out of
		// this JSON, so a different shape here would silently produce
		// zeroed projections.
		storePayload, err := json.Marshal(map[string]any{
			"tenant_id":  e.tenantID,
			"event_type": e.eventType,
			"payload":    json.RawMessage(e.payload),
			"value":      e.value,
			"id":         e.id,
		})
		if err != nil {
			return fmt.Errorf("marshal store payload: %w", err)
		}

		batch.Queue(
			`INSERT INTO events_store (stream_name, event_type, data, metadata, occurred_at)
			 VALUES ($1, 'EventIngested', $2, '{}'::jsonb, $3)`,
			"tenant-"+e.tenantID, storePayload, e.occurredAt,
		)
		batch.Queue(
			`INSERT INTO events (id, tenant_id, event_type, payload, value, occurred_at)
			 VALUES ($1, $2, $3, $4, $5, $6)`,
			e.id, e.tenantID, e.eventType, e.payload, e.value, e.occurredAt,
		)
	}

	results := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("batch statement %d: %w", i, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close batch: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func main() {
	cfg := config.Load()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "postgres: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Fetch existing tenant IDs from the seed data in migration 001.
	rows, err := pool.Query(ctx, `SELECT id FROM tenants`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query tenants: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var tenantIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		tenantIDs = append(tenantIDs, id)
	}
	if len(tenantIDs) == 0 {
		fmt.Fprintln(os.Stderr, "no tenants found — run 'make migrate-up' first")
		os.Exit(1)
	}

	fmt.Printf("Found %d tenants. Seeding 10,000 events...\n", len(tenantIDs))

	rng := rand.New(rand.NewSource(42)) // deterministic seed for reproducibility
	eventTypes := []string{"page_view", "click", "purchase", "signup", "logout", "api_call"}
	pages := []string{"/home", "/pricing", "/docs", "/dashboard", "/settings", "/api/v1/events"}
	emails := []string{"alice@example.com", "bob@test.co", "carol@acme.org", "dave@globex.net"}
	ips := []string{"192.168.1.42", "10.0.0.5", "172.16.0.100", "203.0.113.7"}

	now := time.Now().UTC()
	inserted := 0
	pending := make([]seedEvent, 0, batchSize)

	flush := func() {
		if len(pending) == 0 {
			return
		}
		if err := insertBatch(ctx, pool, pending); err != nil {
			// A batch failure is fatal rather than skipped: the old
			// per-row loop logged and continued, which could leave the
			// log and its projection at different lengths. Both tables
			// move together or the run stops.
			fmt.Fprintf(os.Stderr, "insert batch (events %d–%d): %v\n",
				inserted+1, inserted+len(pending), err)
			os.Exit(1)
		}
		inserted += len(pending)
		pending = pending[:0]
		if inserted%1000 == 0 || inserted == 10000 {
			fmt.Printf("  %d events inserted...\n", inserted)
		}
	}

	for range 10000 {
		tenantID := tenantIDs[rng.Intn(len(tenantIDs))]
		eventType := eventTypes[rng.Intn(len(eventTypes))]
		daysAgo := rng.Intn(30)
		hoursAgo := rng.Intn(24)
		occurredAt := now.Add(-time.Duration(daysAgo)*24*time.Hour - time.Duration(hoursAgo)*time.Hour)
		value := float64(rng.Intn(10000)) / 100.0 // 0.00 – 99.99

		// Build a payload with realistic fields — some include PII (email, IP)
		// so Ch14's PII masker has real data to detect.
		payload := map[string]any{
			"page":        pages[rng.Intn(len(pages))],
			"duration_ms": rng.Intn(5000),
			"user_agent":  "Mozilla/5.0 (course seed data)",
		}
		// ~30% of events include PII fields
		if rng.Float64() < 0.3 {
			payload["user_email"] = emails[rng.Intn(len(emails))]
			payload["client_ip"] = ips[rng.Intn(len(ips))]
		}
		if eventType == "purchase" {
			payload["amount"] = value
			payload["currency"] = "USD"
		}

		payloadJSON, _ := json.Marshal(payload)

		// The ID is generated here rather than left to the events.id
		// column default, because the same ID has to appear inside the
		// events_store payload — that is what ties a log entry to its
		// projection row, exactly as the real ingest path does.
		//
		// Deliberately NOT drawn from rng: the seeder is re-runnable
		// (`make seed` twice, workshop resets), and a reproducible ID
		// sequence would make the second run die on the events primary
		// key. rng still drives all the content, so the *shape* of the
		// seed data stays reproducible.
		pending = append(pending, seedEvent{
			id:         uuid.NewString(),
			tenantID:   tenantID,
			eventType:  eventType,
			payload:    payloadJSON,
			value:      value,
			occurredAt: occurredAt,
		})
		if len(pending) == batchSize {
			flush()
		}
	}
	flush()

	fmt.Printf("Done. %d events seeded across %d tenants over the last 30 days.\n",
		inserted, len(tenantIDs))
	fmt.Println("~30% of events contain PII (email, IP) for Ch14 masking demos.")

	// Top up tenant_credits so the Ch08 billing/charge endpoint actually
	// has a balance to deduct from. The migration's AFTER INSERT trigger
	// on tenants creates a zero-balance credits row automatically; we
	// just bump it to $1000 (100,000 cents) for every seeded tenant.
	const seedCreditCents = 100000
	for _, id := range tenantIDs {
		if _, err := pool.Exec(ctx,
			`UPDATE tenant_credits
			    SET balance_cents = $1, updated_at = NOW()
			  WHERE tenant_id = $2`,
			seedCreditCents, id,
		); err != nil {
			fmt.Fprintf(os.Stderr, "top-up credits for %s: %v\n", id, err)
		}
	}
	fmt.Printf("Topped up tenant_credits to $%d.00 for %d tenants (Ch08 charge demos).\n",
		seedCreditCents/100, len(tenantIDs))
}
