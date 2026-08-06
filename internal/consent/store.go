// Package consent tracks data processing consent for GDPR compliance.
//
// Ch14 teaching points:
//  1. Under GDPR, data processing requires a legal basis. Consent is one
//     of six legal bases. If consent is revoked, processing must stop.
//  2. Consent is granular: a tenant may consent to analytics but not marketing.
//     Each (tenant_id, purpose) pair has its own consent state.
//  3. Consent is auditable: every grant and revocation is logged in the
//     audit log (see internal/audit). The consent table holds only the
//     current state; the audit log holds the history.
package consent

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Purpose represents a data processing purpose.
type Purpose string

const (
	PurposeAnalytics Purpose = "analytics"
	PurposeMarketing Purpose = "marketing"
	PurposeBilling   Purpose = "billing"
)

// Record is the current consent state for a tenant + purpose.
type Record struct {
	TenantID  string     `json:"tenant_id"`
	Purpose   Purpose    `json:"purpose"`
	Granted   bool       `json:"granted"`
	Version   int        `json:"version"` // privacy-policy version the consent was given against
	GrantedAt time.Time  `json:"granted_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// Store manages consent records in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a consent store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Grant records that a tenant has given consent for a purpose at the
// given privacy-policy version. Pass version=0 to default to 1
// (single-version setups).
//
// GDPR's "informed consent" requirement maps to versioning: consent
// is bound to a specific privacy policy. When the policy revises,
// existing consent rows can be invalidated and re-prompted.
func (s *Store) Grant(ctx context.Context, tenantID string, purpose Purpose, version int) error {
	if version <= 0 {
		version = 1
	}
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx,
		`INSERT INTO consent_records (tenant_id, purpose, granted, version, granted_at)
		 VALUES ($1, $2, true, $3, $4)
		 ON CONFLICT (tenant_id, purpose)
		 DO UPDATE SET granted = true, version = $3, granted_at = $4, revoked_at = NULL`,
		tenantID, string(purpose), version, now,
	)
	if err != nil {
		return fmt.Errorf("consent: grant: %w", err)
	}
	return nil
}

// Revoke records that a tenant has withdrawn consent for a purpose.
func (s *Store) Revoke(ctx context.Context, tenantID string, purpose Purpose) error {
	now := time.Now().UTC()
	_, err := s.pool.Exec(ctx,
		`UPDATE consent_records
		 SET granted = false, revoked_at = $1
		 WHERE tenant_id = $2 AND purpose = $3`,
		now, tenantID, string(purpose),
	)
	if err != nil {
		return fmt.Errorf("consent: revoke: %w", err)
	}
	return nil
}

// IsGranted checks if a tenant has active consent for a purpose.
//
// "Not granted" here covers BOTH no-record and explicit-revoke. The
// ConsentGate middleware needs to distinguish those two cases — see
// State for the three-valued version.
func (s *Store) IsGranted(ctx context.Context, tenantID string, purpose Purpose) (bool, error) {
	var granted bool
	err := s.pool.QueryRow(ctx,
		`SELECT granted FROM consent_records
		 WHERE tenant_id = $1 AND purpose = $2`,
		tenantID, string(purpose),
	).Scan(&granted)
	if err == pgx.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("consent: check: %w", err)
	}
	return granted, nil
}

// State constants — kept as integers so the middleware (which
// imports neither pgx nor anything heavy) can compare against
// well-known values without round-tripping a string.
const (
	StateNoRecord = 0
	StateGranted  = 1
	StateRevoked  = 2
)

// ConsentState returns the three-valued consent state for the
// (tenant, purpose) pair. The middleware in internal/api/middleware
// uses this to distinguish "no record yet" (lenient default: allow)
// from "explicitly revoked" (always deny).
func (s *Store) ConsentState(ctx context.Context, tenantID, purpose string) (int, error) {
	var granted bool
	err := s.pool.QueryRow(ctx,
		`SELECT granted FROM consent_records
		 WHERE tenant_id = $1 AND purpose = $2`,
		tenantID, purpose,
	).Scan(&granted)
	if err == pgx.ErrNoRows {
		return StateNoRecord, nil
	}
	if err != nil {
		return StateNoRecord, fmt.Errorf("consent: state: %w", err)
	}
	if granted {
		return StateGranted, nil
	}
	return StateRevoked, nil
}

// ListByTenant returns all consent records for a tenant.
func (s *Store) ListByTenant(ctx context.Context, tenantID string) ([]Record, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT tenant_id, purpose, granted, version, granted_at, revoked_at
		 FROM consent_records
		 WHERE tenant_id = $1
		 ORDER BY purpose`,
		tenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("consent: list: %w", err)
	}
	defer rows.Close()

	var out []Record
	for rows.Next() {
		var r Record
		var purpose string
		if err := rows.Scan(&r.TenantID, &purpose, &r.Granted, &r.Version, &r.GrantedAt, &r.RevokedAt); err != nil {
			return nil, fmt.Errorf("consent: scan: %w", err)
		}
		r.Purpose = Purpose(purpose)
		out = append(out, r)
	}
	return out, rows.Err()
}
