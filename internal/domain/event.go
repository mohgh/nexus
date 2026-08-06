package domain

import (
	"context"
	"encoding/json"
	"time"
)

// Event is a single analytics event ingested by Nexus.
//
// The design deliberately mixes relational and document models — a core
// Ch03 DDIA lesson. The envelope (tenant_id, event_type, value, occurred_at)
// is strongly typed and queryable with indexes. The Payload is JSONB:
// schema-on-write for the envelope, schema-on-read for the payload.
//
// This lets Nexus accept arbitrary event shapes from tenants without
// needing a schema migration for every new event type.
type Event struct {
	ID         string          `json:"id"`
	TenantID   string          `json:"tenant_id"`
	EventType  string          `json:"event_type"` // "page_view" | "click" | "purchase" | ...
	Payload    json.RawMessage `json:"payload"`    // JSONB — interpret based on event_type
	Value      float64         `json:"value"`      // e.g. purchase amount, session duration
	OccurredAt time.Time       `json:"occurred_at"`
}

// EventRepository is the persistence interface for events.
//
// Ch03: PostgresEventRepository — JSONB storage, trigram search.
// Ch04: ClickHouseEventRepository — columnar OLAP queries.
// Ch12: consumed and projected from Kafka.
type EventRepository interface {
	Create(ctx context.Context, e *Event) error
	ListByTenant(ctx context.Context, tenantID string, limit int) ([]*Event, error)

	// Search performs a case-insensitive substring search over the JSONB payload.
	// Ch03 shows that PostgreSQL can do this with pg_trgm — but at scale it
	// becomes slow. Ch04 moves full-text search to Elasticsearch.
	Search(ctx context.Context, tenantID, query string, limit int) ([]*Event, error)
}
