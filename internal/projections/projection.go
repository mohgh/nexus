// Package projections implements read-side projections for the CQRS
// pattern. A projection is a denormalized view derived from the
// event store; it is disposable — you can delete the projection
// table, reset its position, and rebuild it from the event log.
//
// Ch13 teaching points:
//
//   1. Projections are catch-up consumers, not transaction
//      participants. The event store is the source of truth; each
//      projection reads from it on a poll cycle and applies events
//      idempotently.
//
//   2. Persisted position. Each projection stores its last processed
//      stream_position in projection_positions. On restart it
//      resumes from there. On replay it's reset to 0.
//
//   3. Idempotent Apply. Projections must be safe under at-least-once
//      re-delivery (e.g. after a restart that processed an event but
//      crashed before writing the position). ON CONFLICT DO UPDATE
//      is the typical Postgres pattern.
//
// The interface here is small enough that adding a third or fourth
// projection later is a one-file change.
package projections

import (
	"context"

	"github.com/mohgh/nexus/internal/eventstore"
)

// Projection is the contract every read model implements. The
// Runner walks the event log on a poll cycle and feeds each
// projection its slice of events. Projections own their position
// state — persisting / loading / resetting happens behind these
// methods.
type Projection interface {
	// Name is the projection's stable identifier, used as the
	// projection_positions.projection_name primary key. Renaming
	// loses the position; pick once.
	Name() string

	// LastPosition is the position the projection has confirmed it
	// has applied through. The Runner asks the event store for
	// events strictly after this position.
	LastPosition() int64

	// LoadPosition fetches the persisted position from the
	// projection_positions table into in-memory state.
	LoadPosition(ctx context.Context) error

	// Apply processes a single event and advances LastPosition on
	// success. Implementations should be idempotent (re-applying
	// the same event must not double-count). Apply returns any
	// transient or fatal error verbatim; the Runner decides whether
	// to keep going.
	Apply(ctx context.Context, e eventstore.StoredEvent) error

	// Reset truncates the projection's read model and sets its
	// persisted position to 0. Used by the rebuild command (and by
	// tests). After Reset, the next Runner sweep re-derives the
	// projection from the event store from position 0.
	Reset(ctx context.Context) error
}
