// Package gdpr provides the GDPR compliance service for Nexus.
//
// It implements DataExporter, DataEraser, and ConsentManager from the
// handlers package, coordinating across multiple repositories.
package gdpr

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/audit"
	"github.com/mohgh/nexus/internal/consent"
	"github.com/mohgh/nexus/internal/pii"
	"go.uber.org/zap"
)

// Service coordinates GDPR operations across repositories.
type Service struct {
	pool    *pgxpool.Pool
	audit   *audit.Log
	consent *consent.Store
	masker  *pii.Masker
	logger  *zap.Logger
}

// NewService creates a GDPR service.
func NewService(pool *pgxpool.Pool, auditLog *audit.Log, consentStore *consent.Store, logger *zap.Logger) *Service {
	return &Service{
		pool:    pool,
		audit:   auditLog,
		consent: consentStore,
		masker:  pii.NewMasker(),
		logger:  logger,
	}
}

// ExportTenantData collects all data for a tenant and returns it as a
// machine-readable structure. PII is included (this is the tenant's own data).
//
// GDPR Article 20: data portability — return data in a structured,
// commonly used, machine-readable format.
//
// Returns ErrTenantNotFound if the tenant does not exist (handler
// maps to 404). Earlier this surfaced as a generic 500 because
// QueryRow's ErrNoRows wasn't translated.
func (s *Service) ExportTenantData(ctx context.Context, tenantID string) (any, error) {
	if err := s.assertTenantExists(ctx, tenantID); err != nil {
		return nil, err
	}
	// Collect tenant record. We've already confirmed existence;
	// any error here is a real DB problem.
	var tenant map[string]any
	err := s.pool.QueryRow(ctx,
		`SELECT json_build_object(
		    'id', id, 'name', name, 'plan', plan,
		    'created_at', created_at, 'updated_at', updated_at
		 ) FROM tenants WHERE id = $1`, tenantID,
	).Scan(&tenant)
	if err != nil {
		return nil, fmt.Errorf("gdpr: export tenant: %w", err)
	}

	// Collect events.
	rows, err := s.pool.Query(ctx,
		`SELECT json_build_object(
		    'id', id, 'event_type', event_type,
		    'payload', payload, 'value', value, 'occurred_at', occurred_at
		 ) FROM events WHERE tenant_id = $1 ORDER BY occurred_at`, tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("gdpr: export events: %w", err)
	}
	defer rows.Close()

	var events []map[string]any
	for rows.Next() {
		var e map[string]any
		if err := rows.Scan(&e); err != nil {
			return nil, fmt.Errorf("gdpr: scan event: %w", err)
		}
		events = append(events, e)
	}

	// Collect consent records.
	consents, err := s.consent.ListByTenant(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("gdpr: export consent: %w", err)
	}

	// Audit the export.
	_ = s.audit.Record(ctx, audit.Entry{
		TenantID:     tenantID,
		Actor:        "system",
		Action:       audit.ActionExport,
		ResourceType: "tenant_data",
		ResourceID:   tenantID,
	})

	return map[string]any{
		"tenant":   tenant,
		"events":   events,
		"consents": consents,
	}, nil
}

// EraseTenantData deletes all data for a tenant across all tables.
// The audit log entry for the erasure is retained (legal requirement).
//
// GDPR Article 17: right to erasure / right to be forgotten.
//
// Behaviour (the bug this rewrite is fixing):
//
// The previous version logged-and-continued on per-table delete
// errors. That doesn't work in Postgres: once any statement in a
// transaction fails, every subsequent statement in the same tx
// fails with "current transaction is aborted." The loop silently
// skipped every remaining table and the final commit also failed —
// but the failure was swallowed by the same logger.Warn pattern, so
// the handler returned 204 No Content while leaving most of the
// tenant's data intact. AND it never deleted the tenants row, so
// even on the green path none of the cascading children went away.
//
// The fix is fail-closed:
//
//   - Every delete is unguarded: any error rolls back the tx and
//     bubbles up to the handler, which returns 500.
//
//   - Tables without a foreign key cascade to tenants are deleted
//     explicitly (events_store, tenant_window_stats — TEXT-keyed
//     to tenant_id but no FK).
//
//   - The tenants row is deleted LAST. The cascading FKs on
//     billing_records, events, tenant_event_counts, daily_event_counts,
//     consent_records, and tenant_credits then drop those rows
//     automatically. RowsAffected is checked: a missing tenant
//     returns ErrNotFound (404).
//
//   - The audit log entry is written AFTER the commit. The audit
//     table has no FK to tenants on purpose — the entry survives
//     the erasure as the legal record that erasure happened.
//
// Tables NOT touched by erasure:
//   - audit_log: legal hold, must outlive the tenant.
//   - projection_positions: not tenant-scoped.
//   - processed_idempotency_keys: not tenant-scoped (24h TTL anyway).
func (s *Service) EraseTenantData(ctx context.Context, tenantID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("gdpr: begin erasure tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Tables that hold tenant data but DON'T cascade from tenants.
	// These have to be deleted explicitly before the cascading
	// delete on tenants runs.
	if _, err := tx.Exec(ctx,
		`DELETE FROM events_store WHERE stream_name = $1`,
		"tenant-"+tenantID,
	); err != nil {
		return fmt.Errorf("gdpr: erase events_store: %w", err)
	}

	if _, err := tx.Exec(ctx,
		`DELETE FROM tenant_window_stats WHERE tenant_id = $1`,
		tenantID,
	); err != nil {
		return fmt.Errorf("gdpr: erase tenant_window_stats: %w", err)
	}

	// The cascade-anchor delete. ON DELETE CASCADE on the FK columns
	// in billing_records, events, tenant_event_counts,
	// daily_event_counts, consent_records, and tenant_credits drops
	// those rows automatically as part of this statement.
	tag, err := tx.Exec(ctx,
		`DELETE FROM tenants WHERE id = $1`,
		tenantID,
	)
	if err != nil {
		return fmt.Errorf("gdpr: erase tenants: %w", err)
	}
	if tag.RowsAffected() != 1 {
		// Either the tenant never existed or it was already erased.
		// Either way, nothing else has been deleted (we only got
		// here because the earlier DELETEs of the non-cascading
		// tables succeeded against zero rows). Surface a 404 via
		// domain.ErrNotFound so the handler renders accordingly.
		return fmt.Errorf("gdpr: erase tenant %q: %w", tenantID, ErrTenantNotFound)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("gdpr: commit erasure: %w", err)
	}

	// Audit AFTER commit. The audit table has no FK to tenants — the
	// entry persists as the legal record of the erasure.
	if err := s.audit.Record(ctx, audit.Entry{
		TenantID:     tenantID,
		Actor:        "system",
		Action:       audit.ActionErasure,
		ResourceType: "tenant_data",
		ResourceID:   tenantID,
	}); err != nil {
		// Audit failure after a successful erasure is a compliance
		// problem but not an erasure failure. Log loudly; the
		// erasure itself stands.
		s.logger.Error("gdpr: audit record failed after successful erasure",
			zap.String("tenant_id", tenantID),
			zap.Error(err),
		)
	}

	s.logger.Info("gdpr: tenant data erased", zap.String("tenant_id", tenantID))
	return nil
}

// ErrTenantNotFound is returned by every tenant-scoped GDPR
// operation when the tenant does not exist. The handler maps this
// to a 404 so the /gdpr/{id}/* surface has a consistent contract.
var ErrTenantNotFound = errTenantNotFound{}

type errTenantNotFound struct{}

func (errTenantNotFound) Error() string { return "gdpr: tenant not found" }

// assertTenantExists returns ErrTenantNotFound when no tenants row
// matches. Used at the top of export/erase/anonymise so the API
// surface is uniform — without it, ExportTenantData would surface
// ErrNoRows as a generic 500 and AnonymiseTenantEvents would 200
// with zero counts.
func (s *Service) assertTenantExists(ctx context.Context, tenantID string) error {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`,
		tenantID,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("gdpr: check tenant exists: %w", err)
	}
	if !exists {
		return ErrTenantNotFound
	}
	return nil
}

// AnonymiseResult reports what AnonymiseTenantEvents did. It tracks
// both tables because the audit's regression case was that we
// anonymised the projection (events) while leaving the canonical
// log (events_store) untouched.
type AnonymiseResult struct {
	EventsScanned        int `json:"events_scanned"`
	EventsAnonymised     int `json:"events_anonymised"`
	EventStoreScanned    int `json:"event_store_scanned"`
	EventStoreAnonymised int `json:"event_store_anonymised"`
}

// AnonymiseTenantEvents replaces PII in tenant event payloads with
// the masker's redaction placeholder and marks each event as
// anonymised. Unlike EraseTenantData this preserves the row, the
// event_type, the value, and the occurred_at — useful when an
// auditor needs to confirm "user X had Y events of type Z" is true
// or false in aggregate WITHOUT identifying any actual user.
//
// The chapter README distinguishes "anonymisation in place" from
// "deletion": anonymisation keeps the structural record so
// analytics over historical periods stay sound; deletion drops the
// row entirely. GDPR allows both.
//
// Two tables are scrubbed in one transaction:
//
//   1. events  — the synchronous primary projection used by the
//      fast read paths.
//   2. events_store — the canonical event log Ch13 introduced as
//      the source of truth. The chapter's lesson on event sourcing
//      is "the log is immutable" — but GDPR's right to erasure
//      trumps the architectural invariant. Skipping the log was
//      the audit's regression case: a projection rebuild from
//      events_store would re-derive the *original PII* into the
//      newly-rebuilt projection, undoing the anonymisation.
//
//      Production alternatives that preserve append-only:
//        a. Crypto-shredding — encrypt each event's PII with a
//           per-tenant key; "anonymise" = throw away the key.
//        b. Tombstone events + projection logic that skips them.
//        c. Compaction with rewrite, similar to Kafka log
//           compaction.
//      All are more elaborate than the in-place UPDATE chosen here.
//      The README documents the trade-off explicitly.
//
// Idempotent: rows already marked pii_erased=true are skipped via
// the partial index events_pending_anonymisation. event_store
// idempotency is checked by a regex match against [REDACTED] in
// the data column — re-running the operation finds nothing to do.
//
// Returns ErrTenantNotFound if the tenant does not exist.
func (s *Service) AnonymiseTenantEvents(ctx context.Context, tenantID string) (AnonymiseResult, error) {
	if err := s.assertTenantExists(ctx, tenantID); err != nil {
		return AnonymiseResult{}, err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AnonymiseResult{}, fmt.Errorf("gdpr: anonymise begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	eventsResult, err := s.anonymiseEventsTable(ctx, tx, tenantID)
	if err != nil {
		return AnonymiseResult{}, err
	}
	storeResult, err := s.anonymiseEventStore(ctx, tx, tenantID)
	if err != nil {
		return AnonymiseResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return AnonymiseResult{}, fmt.Errorf("gdpr: anonymise commit: %w", err)
	}

	if err := s.audit.Record(ctx, audit.Entry{
		TenantID:     tenantID,
		Actor:        "system",
		Action:       audit.ActionErasure, // anonymisation is the lightweight cousin of erasure
		ResourceType: "tenant_events_anonymised",
		ResourceID:   tenantID,
	}); err != nil {
		s.logger.Warn("gdpr: audit anonymisation failed",
			zap.String("tenant_id", tenantID),
			zap.Error(err),
		)
	}

	return AnonymiseResult{
		EventsScanned:        eventsResult.scanned,
		EventsAnonymised:     eventsResult.anonymised,
		EventStoreScanned:    storeResult.scanned,
		EventStoreAnonymised: storeResult.anonymised,
	}, nil
}

// anonymisePassResult is the per-table return shape. Kept private
// because callers see the merged AnonymiseResult.
type anonymisePassResult struct {
	scanned    int
	anonymised int
}

func (s *Service) anonymiseEventsTable(ctx context.Context, tx pgxQuerier, tenantID string) (anonymisePassResult, error) {
	rows, err := tx.Query(ctx,
		`SELECT id, payload FROM events
		 WHERE tenant_id = $1 AND pii_erased = FALSE`,
		tenantID,
	)
	if err != nil {
		return anonymisePassResult{}, fmt.Errorf("gdpr: anonymise events: query: %w", err)
	}

	type row struct {
		id      string
		payload []byte
	}
	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.payload); err != nil {
			rows.Close()
			return anonymisePassResult{}, fmt.Errorf("gdpr: anonymise events: scan: %w", err)
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return anonymisePassResult{}, fmt.Errorf("gdpr: anonymise events: rows: %w", err)
	}

	out := anonymisePassResult{scanned: len(batch)}
	for _, r := range batch {
		masked, cats := s.masker.Mask(r.payload)
		if len(cats) == 0 {
			if _, err := tx.Exec(ctx,
				`UPDATE events SET pii_erased = TRUE WHERE id = $1`,
				r.id,
			); err != nil {
				return out, fmt.Errorf("gdpr: anonymise events: mark %s: %w", r.id, err)
			}
		} else {
			if _, err := tx.Exec(ctx,
				`UPDATE events SET payload = $1, pii_erased = TRUE WHERE id = $2`,
				[]byte(masked), r.id,
			); err != nil {
				return out, fmt.Errorf("gdpr: anonymise events: update %s: %w", r.id, err)
			}
		}
		out.anonymised++
	}
	return out, nil
}

// anonymiseEventStore scrubs the canonical event log. The masker
// operates on the entire JSONB payload — events_store.data wraps
// the original event payload, so a single Mask pass over the JSONB
// bytes catches PII regardless of which JSON field carries it.
func (s *Service) anonymiseEventStore(ctx context.Context, tx pgxQuerier, tenantID string) (anonymisePassResult, error) {
	streamName := "tenant-" + tenantID
	rows, err := tx.Query(ctx,
		`SELECT stream_position, data FROM events_store
		 WHERE stream_name = $1`,
		streamName,
	)
	if err != nil {
		return anonymisePassResult{}, fmt.Errorf("gdpr: anonymise events_store: query: %w", err)
	}

	type row struct {
		pos  int64
		data []byte
	}
	var batch []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.pos, &r.data); err != nil {
			rows.Close()
			return anonymisePassResult{}, fmt.Errorf("gdpr: anonymise events_store: scan: %w", err)
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return anonymisePassResult{}, fmt.Errorf("gdpr: anonymise events_store: rows: %w", err)
	}

	out := anonymisePassResult{scanned: len(batch)}
	for _, r := range batch {
		masked, cats := s.masker.Mask(r.data)
		if len(cats) == 0 {
			// Already clean (or already anonymised). Nothing to update.
			continue
		}
		if _, err := tx.Exec(ctx,
			`UPDATE events_store SET data = $1 WHERE stream_position = $2`,
			[]byte(masked), r.pos,
		); err != nil {
			return out, fmt.Errorf("gdpr: anonymise events_store: update pos=%d: %w", r.pos, err)
		}
		out.anonymised++
	}
	return out, nil
}

// pgxQuerier is the slice of pgx.Tx the anonymisation passes use.
// Defining it locally lets both tx and pool satisfy it for tests
// without dragging the pgx tx interface across packages.
type pgxQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Grant records consent for a purpose at the given policy version.
// version=0 defaults to 1 (single-policy setups).
func (s *Service) Grant(ctx context.Context, tenantID, purpose string, version int) error {
	if err := s.consent.Grant(ctx, tenantID, consent.Purpose(purpose), version); err != nil {
		return err
	}
	return s.audit.Record(ctx, audit.Entry{
		TenantID:     tenantID,
		Actor:        "system",
		Action:       audit.ActionConsentGiven,
		ResourceType: "consent",
		ResourceID:   purpose,
	})
}

// Revoke withdraws consent for a purpose.
func (s *Service) Revoke(ctx context.Context, tenantID, purpose string) error {
	if err := s.consent.Revoke(ctx, tenantID, consent.Purpose(purpose)); err != nil {
		return err
	}
	return s.audit.Record(ctx, audit.Entry{
		TenantID:     tenantID,
		Actor:        "system",
		Action:       audit.ActionConsentRevoked,
		ResourceType: "consent",
		ResourceID:   purpose,
	})
}

// ListByTenant returns consent records.
func (s *Service) ListByTenant(ctx context.Context, tenantID string) (any, error) {
	return s.consent.ListByTenant(ctx, tenantID)
}
