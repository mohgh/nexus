// Package idgen provides monotonically sortable ID generation using ULID.
//
// Ch10 teaching point: UUID v4 IDs are random — they fragment B-tree indexes
// because each insert lands at a random position. ULID (Universally Unique
// Lexicographically Sortable Identifier) encodes a millisecond timestamp in
// the first 48 bits, so new IDs are always inserted at the end of the index.
//
// ULID vs UUID:
//
//	UUID v4:  entirely random  → random index insertions → B-tree fragmentation
//	ULID:     time + random   → monotonically increasing → sequential inserts
//	UUID v7:  time + random   → same as ULID, different encoding (RFC 9562)
//
// For Nexus: tenant IDs use UUID v4 (created rarely, random is fine).
// Event IDs and billing record IDs use ULID (high-volume, sequential matters).
//
// DDIA reference: Chapter 9 — ordering guarantees and the role of physical clocks.
package idgen

import (
	"crypto/rand"
	"io"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// Generator produces ULIDs using a monotonic entropy source.
// Safe for concurrent use.
type Generator struct {
	mu      sync.Mutex
	entropy io.Reader
}

// NewGenerator creates a ULID generator backed by crypto/rand.
// The monotone reader ensures ULIDs are strictly increasing within the same
// millisecond — even if the wall clock doesn't advance.
func NewGenerator() *Generator {
	return &Generator{
		entropy: ulid.Monotonic(rand.Reader, 0),
	}
}

// New generates a new ULID using the current UTC time.
func (g *Generator) New() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now().UTC()), g.entropy).String()
}

// NewAt generates a ULID for a specific timestamp. Useful for testing
// and for replaying events with their original timestamp.
func (g *Generator) NewAt(t time.Time) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return ulid.MustNew(ulid.Timestamp(t.UTC()), g.entropy).String()
}

// Parse decodes a ULID string and returns the embedded timestamp.
// Use this to extract the creation time of any Nexus resource by its ID.
func Parse(id string) (time.Time, error) {
	u, err := ulid.ParseStrict(id)
	if err != nil {
		return time.Time{}, err
	}
	return ulid.Time(u.Time()), nil
}
