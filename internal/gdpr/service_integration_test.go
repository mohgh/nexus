//go:build integration

// Live-Postgres integration tests for the GDPR service. The
// EraseTenantData correctness regression — silently swallowing
// per-table errors and never deleting the tenant row — only
// reproduces against a real Postgres because the failure mode is
// the txn-aborted-after-first-error semantic.
//
// Run via:
//
//	POSTGRES_DSN=postgres://nexus:nexus_secret@localhost:5432/nexus?sslmode=disable \
//	    go test -tags=integration -v ./internal/gdpr/...

package gdpr_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/audit"
	"github.com/mohgh/nexus/internal/consent"
	"github.com/mohgh/nexus/internal/gdpr"
	"go.uber.org/zap"
)

func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// setupTenantWithData creates a tenant + at least one event + at
// least one billing record + a consent row, returning the tenant ID.
// Cleanup is registered so even a partial run doesn't pollute the DB.
func setupTenantWithData(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	id := uuid.New().String()
	now := time.Now().UTC()

	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, name, plan, created_at, updated_at)
		 VALUES ($1, $2, 'pro', $3, $3)`,
		id, "gdpr-test-"+id[:8], now,
	); err != nil {
		t.Fatalf("insert tenant: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM tenants WHERE id = $1`, id)
	})

	if _, err := pool.Exec(ctx,
		`INSERT INTO events (id, tenant_id, event_type, payload, value, occurred_at)
		 VALUES ($1, $2, 'page_view', '{}'::jsonb, 1, $3)`,
		uuid.New().String(), id, now,
	); err != nil {
		t.Fatalf("insert event: %v", err)
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO billing_records (id, tenant_id, idempotency_key, amount_cents, status)
		 VALUES ($1, $2, $3, 100, 'completed')`,
		uuid.New().String(), id, "key-"+id[:8],
	); err != nil {
		t.Fatalf("insert billing: %v", err)
	}

	if _, err := pool.Exec(ctx,
		`INSERT INTO consent_records (tenant_id, purpose, granted, granted_at)
		 VALUES ($1, 'analytics', TRUE, NOW())`,
		id,
	); err != nil {
		t.Fatalf("insert consent: %v", err)
	}

	return id
}

func countWhere(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// TestEraseTenantData_DeletesEverythingAndTenant is the headline
// regression test for the audit's "swallows errors and never deletes
// the tenant" finding. After erasure: the tenant row is gone, all
// FK-cascade'd children are gone, and the audit_log entry survives.
func TestEraseTenantData_DeletesEverythingAndTenant(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()

	tenantID := setupTenantWithData(t, pool)

	auditLog := audit.NewLog(pool)
	consentStore := consent.NewStore(pool)
	svc := gdpr.NewService(pool, auditLog, consentStore, zap.NewNop())

	if err := svc.EraseTenantData(ctx, tenantID); err != nil {
		t.Fatalf("EraseTenantData: %v", err)
	}

	if got := countWhere(t, pool, `SELECT COUNT(*) FROM tenants WHERE id = $1`, tenantID); got != 0 {
		t.Fatalf("tenant row should be deleted, got %d (this is the audit's specific regression case — "+
			"the original code never deleted the parent row)",
			got)
	}
	if got := countWhere(t, pool, `SELECT COUNT(*) FROM events WHERE tenant_id = $1`, tenantID); got != 0 {
		t.Fatalf("events should cascade-delete, got %d remaining", got)
	}
	if got := countWhere(t, pool, `SELECT COUNT(*) FROM billing_records WHERE tenant_id = $1`, tenantID); got != 0 {
		t.Fatalf("billing_records should cascade-delete, got %d remaining", got)
	}
	if got := countWhere(t, pool, `SELECT COUNT(*) FROM consent_records WHERE tenant_id = $1`, tenantID); got != 0 {
		t.Fatalf("consent_records should cascade-delete, got %d remaining", got)
	}

	// The audit log entry MUST survive — it's the legal record that
	// the erasure happened.
	if got := countWhere(t, pool,
		`SELECT COUNT(*) FROM audit_log WHERE tenant_id = $1 AND action = 'erasure'`,
		tenantID,
	); got < 1 {
		t.Fatalf("audit log should retain the erasure entry, got %d", got)
	}
}

// TestEraseTenantData_404OnUnknownTenant pins down that asking to
// erase a tenant that doesn't exist returns ErrTenantNotFound (the
// handler maps to 404). Earlier code returned 204 silently.
func TestEraseTenantData_404OnUnknownTenant(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()

	auditLog := audit.NewLog(pool)
	consentStore := consent.NewStore(pool)
	svc := gdpr.NewService(pool, auditLog, consentStore, zap.NewNop())

	err := svc.EraseTenantData(ctx, uuid.New().String())
	if !errors.Is(err, gdpr.ErrTenantNotFound) {
		t.Fatalf("expected ErrTenantNotFound, got %v", err)
	}
}

// TestAuditLog_AppendOnlyTriggerRejectsUpdates verifies the trigger
// added in migration 011 actually fires. The audit log being
// append-only is a chapter claim and a compliance property; the
// trigger turns it from "by convention" into "by the database."
func TestAuditLog_AppendOnlyTriggerRejectsUpdates(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()

	tenantID := uuid.New().String() // not a real tenant; audit_log has no FK
	auditLog := audit.NewLog(pool)
	if err := auditLog.Record(ctx, audit.Entry{
		TenantID:     tenantID,
		Actor:        "test",
		Action:       audit.ActionRead,
		ResourceType: "test_resource",
		ResourceID:   "x",
	}); err != nil {
		t.Fatalf("seed: Record: %v", err)
	}
	t.Cleanup(func() {
		// Cleanup must bypass the trigger. We intentionally don't —
		// the row stays. The audit table is small in tests; this is
		// fine for the chapter milestone. (A test-only superuser
		// path is the production way around this.)
	})

	if _, err := pool.Exec(ctx,
		`UPDATE audit_log SET action = 'tampered' WHERE tenant_id = $1`,
		tenantID,
	); err == nil {
		t.Fatal("UPDATE on audit_log should be rejected by the append-only trigger")
	}

	if _, err := pool.Exec(ctx,
		`DELETE FROM audit_log WHERE tenant_id = $1`,
		tenantID,
	); err == nil {
		t.Fatal("DELETE on audit_log should be rejected by the append-only trigger")
	}
}

// TestAnonymiseTenantEvents_RedactsBothEventsAndEventStore is the
// regression test for the audit's "anonymise misses the canonical
// log" finding. Ch13 made events_store the source of truth, but
// AnonymiseTenantEvents originally only scrubbed the events
// projection — leaving the original PII intact in events_store
// where any future projection rebuild would re-derive it.
//
// The test ingests an event with PII through the live write path
// (which dual-writes to events_store + events), runs anonymise,
// and asserts the email is redacted in BOTH tables.
func TestAnonymiseTenantEvents_RedactsBothEventsAndEventStore(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()

	tenantID := setupTenantWithData(t, pool)
	piiEventID := uuid.New().String()
	piiPayload := `{"email":"alice@example.com","client_ip":"203.0.113.7"}`
	streamName := "tenant-" + tenantID

	// Insert into both tables explicitly (matching what
	// EventRepository.Create does in one tx). Direct SQL keeps the
	// test independent of the storage layer's call signature.
	if _, err := pool.Exec(ctx,
		`INSERT INTO events (id, tenant_id, event_type, payload, value, occurred_at)
		 VALUES ($1, $2, 'signup', $3::jsonb, 1, NOW())`,
		piiEventID, tenantID, piiPayload,
	); err != nil {
		t.Fatalf("insert events: %v", err)
	}
	storePayload := `{"id":"` + piiEventID + `","tenant_id":"` + tenantID +
		`","event_type":"signup","value":1,"payload":` + piiPayload + `}`
	if _, err := pool.Exec(ctx,
		`INSERT INTO events_store (stream_name, event_type, data, occurred_at)
		 VALUES ($1, 'EventIngested', $2::jsonb, NOW())`,
		streamName, storePayload,
	); err != nil {
		t.Fatalf("insert events_store: %v", err)
	}

	auditLog := audit.NewLog(pool)
	consentStore := consent.NewStore(pool)
	svc := gdpr.NewService(pool, auditLog, consentStore, zap.NewNop())

	result, err := svc.AnonymiseTenantEvents(ctx, tenantID)
	if err != nil {
		t.Fatalf("AnonymiseTenantEvents: %v", err)
	}
	if result.EventsAnonymised < 1 {
		t.Fatalf("expected events to be anonymised, got %+v", result)
	}
	if result.EventStoreAnonymised < 1 {
		t.Fatalf("expected events_store to be anonymised, got %+v.\n"+
			"This is the audit's regression case — the canonical event log "+
			"must be scrubbed too, or a projection rebuild reintroduces the PII.",
			result)
	}

	// events: payload redacted, pii_erased=true.
	var eventsPayload []byte
	var erased bool
	if err := pool.QueryRow(ctx,
		`SELECT payload, pii_erased FROM events WHERE id = $1`,
		piiEventID,
	).Scan(&eventsPayload, &erased); err != nil {
		t.Fatalf("read events row: %v", err)
	}
	if !erased {
		t.Fatalf("pii_erased should be TRUE")
	}
	if strings.Contains(string(eventsPayload), "alice@example.com") {
		t.Fatalf("events.payload still carries the original email: %s", eventsPayload)
	}

	// events_store: data column also scrubbed.
	var storeData []byte
	if err := pool.QueryRow(ctx,
		`SELECT data FROM events_store WHERE stream_name = $1 ORDER BY stream_position DESC LIMIT 1`,
		streamName,
	).Scan(&storeData); err != nil {
		t.Fatalf("read events_store row: %v", err)
	}
	if strings.Contains(string(storeData), "alice@example.com") {
		t.Fatalf("events_store.data still carries the original email: %s\n"+
			"This is the regression case — the canonical log was missed.",
			storeData)
	}
	if !strings.Contains(string(storeData), "[REDACTED]") {
		t.Fatalf("events_store.data should carry the redaction placeholder, got %s", storeData)
	}
}

// TestExportAnonymise_404OnMissingTenant pins down the consistent
// 404 contract across /gdpr/{id}/* — without these checks export
// would 500 (ErrNoRows surfacing as a generic error) and anonymise
// would 200 with zero counts.
func TestExportAnonymise_404OnMissingTenant(t *testing.T) {
	pool := openPool(t)
	ctx := context.Background()

	auditLog := audit.NewLog(pool)
	consentStore := consent.NewStore(pool)
	svc := gdpr.NewService(pool, auditLog, consentStore, zap.NewNop())

	missing := uuid.New().String()

	if _, err := svc.ExportTenantData(ctx, missing); !errors.Is(err, gdpr.ErrTenantNotFound) {
		t.Fatalf("ExportTenantData on missing tenant: got %v, want ErrTenantNotFound", err)
	}
	if _, err := svc.AnonymiseTenantEvents(ctx, missing); !errors.Is(err, gdpr.ErrTenantNotFound) {
		t.Fatalf("AnonymiseTenantEvents on missing tenant: got %v, want ErrTenantNotFound", err)
	}
}
