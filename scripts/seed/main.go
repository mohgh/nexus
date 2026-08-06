// Seed script — populates Nexus with realistic demo data.
//
// Usage: go run ./scripts/seed/main.go
//    or: make seed
//
// Creates 10,000 events across the seeded tenants with varied event types,
// realistic payloads (including PII for Ch14 masking demos), and timestamps
// spread over the last 30 days.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/config"
)

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

	for i := range 10000 {
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

		_, err := pool.Exec(ctx,
			`INSERT INTO events (tenant_id, event_type, payload, value, occurred_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			tenantID, eventType, payloadJSON, value, occurredAt,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "insert event %d: %v\n", i, err)
			continue
		}
		inserted++

		if inserted%1000 == 0 {
			fmt.Printf("  %d events inserted...\n", inserted)
		}
	}

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
