package domain

import (
	"context"
	"time"
)

// BillingStatus represents the lifecycle of a billing charge.
type BillingStatus string

const (
	BillingStatusPending   BillingStatus = "pending"
	BillingStatusCompleted BillingStatus = "completed"
	BillingStatusFailed    BillingStatus = "failed"
)

// BillingRecord is created when a tenant is charged for Nexus usage.
//
// Ch08 teaching points:
//  1. Idempotency key prevents duplicate charges if the client retries.
//  2. The outbox_sent_at column powers the transactional outbox pattern:
//     a background job reads rows where outbox_sent_at IS NULL and
//     publishes them to Kafka, then marks them sent. No distributed transaction
//     needed between Postgres and Kafka.
type BillingRecord struct {
	ID             string        `json:"id"`
	TenantID       string        `json:"tenant_id"`
	IdempotencyKey string        `json:"idempotency_key"` // client-supplied deduplication token
	AmountCents    int64         `json:"amount_cents"`    // always in cents to avoid float rounding
	Currency       string        `json:"currency"`        // ISO 4217 e.g. "USD"
	Status         BillingStatus `json:"status"`
	Description    string        `json:"description"`
	CreatedAt      time.Time     `json:"created_at"`
	OutboxSentAt   *time.Time    `json:"outbox_sent_at,omitempty"` // nil = not yet published
}

// BillingRepository persists billing records.
type BillingRepository interface {
	// Create inserts a new record inside the caller's transaction.
	// Returns ErrDuplicateKey if idempotency_key already exists for this tenant.
	Create(ctx context.Context, r *BillingRecord) error

	// GetByIdempotencyKey fetches a record by tenant + idempotency key.
	// Returns ErrNotFound if no match.
	GetByIdempotencyKey(ctx context.Context, tenantID, key string) (*BillingRecord, error)

	// PendingOutbox returns up to limit records not yet sent to Kafka.
	PendingOutbox(ctx context.Context, limit int) ([]*BillingRecord, error)

	// MarkOutboxSent marks a record as published to Kafka.
	MarkOutboxSent(ctx context.Context, id string) error
}

// ErrDuplicateKey is returned when an idempotency key already exists.
var ErrDuplicateKey = NewSentinelError("duplicate idempotency key")

// SentinelError is a typed error that can be checked with errors.Is.
type SentinelError struct{ msg string }

func NewSentinelError(msg string) *SentinelError { return &SentinelError{msg: msg} }
func (e *SentinelError) Error() string           { return e.msg }
