// Package audit provides an immutable audit log for compliance.
//
// Ch14 teaching points:
//  1. Every data access and modification is recorded — who did what, when.
//  2. The audit log is append-only (like the event store in Ch13) — entries
//     are never updated or deleted.
//  3. The log is separate from the events table — events are business data,
//     audit entries are compliance/security data. They have different
//     retention policies and access controls.
//  4. Audit entries include the actor (API key, user ID), the action, and
//     the target (resource type + ID). This supports both security forensics
//     and regulatory audits.
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Action constants for audit entries.
const (
	ActionCreate       = "create"
	ActionRead         = "read"
	ActionUpdate       = "update"
	ActionDelete       = "delete"
	ActionExport       = "export"
	ActionErasure      = "erasure"
	ActionConsentGiven = "consent_given"
	ActionConsentRevoked = "consent_revoked"
)

// Entry is a single audit log record.
type Entry struct {
	ID           int64           `json:"id"`
	TenantID     string          `json:"tenant_id"`
	Actor        string          `json:"actor"`         // who: API key, user ID, "system"
	Action       string          `json:"action"`        // what: create, read, update, delete, export, erasure
	ResourceType string          `json:"resource_type"` // what kind: "event", "tenant", "billing_record"
	ResourceID   string          `json:"resource_id"`   // which one
	Details      json.RawMessage `json:"details"`       // additional context (redacted payload, etc.)
	OccurredAt   time.Time       `json:"occurred_at"`
}

// Log writes audit entries to PostgreSQL.
type Log struct {
	pool *pgxpool.Pool
}

// NewLog creates an audit logger.
func NewLog(pool *pgxpool.Pool) *Log {
	return &Log{pool: pool}
}

// Record appends an audit entry. This never fails silently — an audit
// write failure should block the operation (fail closed, not fail open).
func (l *Log) Record(ctx context.Context, e Entry) error {
	if e.OccurredAt.IsZero() {
		e.OccurredAt = time.Now().UTC()
	}
	if len(e.Details) == 0 {
		e.Details = json.RawMessage(`{}`)
	}

	_, err := l.pool.Exec(ctx,
		`INSERT INTO audit_log
		 (tenant_id, actor, action, resource_type, resource_id, details, occurred_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		e.TenantID, e.Actor, e.Action,
		e.ResourceType, e.ResourceID,
		[]byte(e.Details), e.OccurredAt,
	)
	if err != nil {
		return fmt.Errorf("audit: record: %w", err)
	}
	return nil
}

// ListByTenant returns audit entries for a tenant, most recent first.
func (l *Log) ListByTenant(ctx context.Context, tenantID string, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := l.pool.Query(ctx,
		`SELECT id, tenant_id, actor, action, resource_type, resource_id, details, occurred_at
		 FROM audit_log
		 WHERE tenant_id = $1
		 ORDER BY occurred_at DESC
		 LIMIT $2`,
		tenantID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("audit: list: %w", err)
	}
	defer rows.Close()

	var out []Entry
	for rows.Next() {
		var e Entry
		var details []byte
		if err := rows.Scan(&e.ID, &e.TenantID, &e.Actor, &e.Action,
			&e.ResourceType, &e.ResourceID, &details, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("audit: scan: %w", err)
		}
		e.Details = json.RawMessage(details)
		out = append(out, e)
	}
	return out, rows.Err()
}
