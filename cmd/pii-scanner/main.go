// pii-scanner — scans events.payload for unmasked PII and reports.
//
// Usage:
//
//	go run ./cmd/pii-scanner [-tenant <id>] [-limit 1000] [-since 24h]
//
// The chapter teaches that PII regularly leaks into JSONB analytics
// payloads — emails in form-submission events, IPs in click events,
// phone numbers in lead-capture events. The application masker only
// runs on writes that go through the masking layer; legacy data and
// data ingested via paths that bypass masking can carry unmasked
// values for years.
//
// This binary is the offline complement to the live PIIDetect
// middleware. It reads events.payload, applies the same regex
// detector, and prints a report so an operator can quantify the
// problem before deciding what to do with it (anonymise via the
// GDPR service, run a backfill, change the upstream contract).
//
// Output is text-table by default and JSON with -json. Exit code 0
// always — this is a report tool, not a CI gate. Use 'jq' on the
// JSON output if you want to assert thresholds in CI.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/config"
	"github.com/mohgh/nexus/internal/pii"
)

type finding struct {
	EventID    string         `json:"event_id"`
	TenantID   string         `json:"tenant_id"`
	EventType  string         `json:"event_type"`
	OccurredAt time.Time      `json:"occurred_at"`
	Categories []pii.Category `json:"pii_categories"`
}

func main() {
	var (
		tenantFilter string
		since        time.Duration
		limit        int
		asJSON       bool
	)
	flag.StringVar(&tenantFilter, "tenant", "", "scan only one tenant (empty = all)")
	flag.DurationVar(&since, "since", 0, "scan only events newer than this (e.g. 24h, 7d). 0 = all.")
	flag.IntVar(&limit, "limit", 10000, "max events to scan")
	flag.BoolVar(&asJSON, "json", false, "emit JSON instead of a text table")
	flag.Parse()

	cfg := config.Load()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		fail("postgres: %v", err)
	}
	defer pool.Close()

	masker := pii.NewMasker()

	// Build the WHERE clause. Keeping it stitched here rather than
	// using sqlbuilder libraries — the surface is small enough.
	var (
		clauses []string
		args    []any
	)
	clauses = append(clauses, "pii_erased = FALSE") // already-anonymised rows aren't interesting
	if tenantFilter != "" {
		clauses = append(clauses, fmt.Sprintf("tenant_id = $%d", len(args)+1))
		args = append(args, tenantFilter)
	}
	if since > 0 {
		clauses = append(clauses, fmt.Sprintf("occurred_at >= $%d", len(args)+1))
		args = append(args, time.Now().Add(-since))
	}
	args = append(args, limit)

	query := fmt.Sprintf(
		`SELECT id, tenant_id, event_type, payload, occurred_at
		 FROM events
		 WHERE %s
		 ORDER BY occurred_at DESC
		 LIMIT $%d`,
		strings.Join(clauses, " AND "), len(args),
	)

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		fail("query: %v", err)
	}
	defer rows.Close()

	var (
		findings  []finding
		scanned   int
		piiCounts = map[pii.Category]int{}
	)
	for rows.Next() {
		var (
			id, tenantID, eventType string
			payload                 []byte
			occurredAt              time.Time
		)
		if err := rows.Scan(&id, &tenantID, &eventType, &payload, &occurredAt); err != nil {
			fail("scan: %v", err)
		}
		scanned++
		cats := masker.Detect(json.RawMessage(payload))
		if len(cats) == 0 {
			continue
		}
		for _, c := range cats {
			piiCounts[c]++
		}
		findings = append(findings, finding{
			EventID:    id,
			TenantID:   tenantID,
			EventType:  eventType,
			OccurredAt: occurredAt,
			Categories: cats,
		})
	}
	if err := rows.Err(); err != nil {
		fail("rows: %v", err)
	}

	if asJSON {
		out := map[string]any{
			"scanned":            scanned,
			"events_with_pii":    len(findings),
			"pii_category_counts": piiCounts,
			"findings":           findings,
		}
		_ = json.NewEncoder(os.Stdout).Encode(out)
		return
	}

	fmt.Printf("scanned %d events; %d carry unmasked PII\n", scanned, len(findings))
	if len(findings) == 0 {
		fmt.Println("clean ✓")
		return
	}
	fmt.Println()
	fmt.Println("category counts:")
	for cat, n := range piiCounts {
		fmt.Printf("  %-12s %6d\n", cat, n)
	}
	fmt.Println()
	fmt.Println("first 20 findings (event_id  tenant_id  event_type  categories  occurred_at):")
	max := 20
	if len(findings) < max {
		max = len(findings)
	}
	for i := 0; i < max; i++ {
		f := findings[i]
		fmt.Printf("  %s  %s  %-12s  %v  %s\n",
			f.EventID, f.TenantID, f.EventType, f.Categories,
			f.OccurredAt.Format(time.RFC3339),
		)
	}
	if len(findings) > max {
		fmt.Printf("  ... and %d more (use -json to inspect all)\n", len(findings)-max)
	}
	fmt.Println()
	fmt.Println("To anonymise: POST /api/v1/gdpr/{tenantID}/anonymise (Ch14 service)")
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "pii-scanner: "+format+"\n", args...)
	os.Exit(1)
}
