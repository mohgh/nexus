package idgen_test

import (
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/idgen"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerator_Monotonic(t *testing.T) {
	g := idgen.NewGenerator()

	const n = 1000
	ids := make([]string, n)
	for i := range ids {
		ids[i] = g.New()
	}

	// ULIDs must be lexicographically sorted (monotonically increasing).
	for i := 1; i < n; i++ {
		assert.Greater(t, ids[i], ids[i-1],
			"ULID[%d] %q should be greater than ULID[%d] %q", i, ids[i], i-1, ids[i-1])
	}
}

func TestGenerator_EmbeddedTimestamp(t *testing.T) {
	g := idgen.NewGenerator()
	before := time.Now().UTC().Truncate(time.Millisecond)
	id := g.New()
	after := time.Now().UTC().Add(time.Millisecond)

	ts, err := idgen.Parse(id)
	require.NoError(t, err)

	assert.True(t, !ts.Before(before), "timestamp should be >= before")
	assert.True(t, !ts.After(after), "timestamp should be <= after")
}

func TestGenerator_ConcurrentSafe(t *testing.T) {
	g := idgen.NewGenerator()
	done := make(chan string, 100)

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				done <- g.New()
			}
		}()
	}

	seen := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := <-done
		assert.False(t, seen[id], "duplicate ULID: %s", id)
		seen[id] = true
	}
}
