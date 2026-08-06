package batch_test

import (
	"testing"
	"time"

	"github.com/mohgh/nexus/internal/batch"
)

// TestChecksum_OrderIndependent verifies the canonical-form claim:
// re-ordering the input slice MUST NOT change the checksum, because
// Postgres GROUP BY doesn't guarantee output order. Two backfill
// runs that produce the same set of stats in different orders are
// the same aggregation and must hash identically.
func TestChecksum_OrderIndependent(t *testing.T) {
	t.Parallel()
	day := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	a := []batch.DailyStat{
		{TenantID: "t1", EventType: "view", Date: day, EventCount: 3, TotalValue: 6.0},
		{TenantID: "t2", EventType: "click", Date: day, EventCount: 1, TotalValue: 0.0},
	}
	b := []batch.DailyStat{
		{TenantID: "t2", EventType: "click", Date: day, EventCount: 1, TotalValue: 0.0},
		{TenantID: "t1", EventType: "view", Date: day, EventCount: 3, TotalValue: 6.0},
	}
	if got, want := batch.Checksum(a), batch.Checksum(b); got != want {
		t.Fatalf("Checksum should be order-independent: %s != %s", got, want)
	}
}

// TestChecksum_DistinguishesDifferences guards the other direction:
// any change to any field — count, sum, tenant, event_type, date —
// produces a different checksum. Without this, a regression that
// silently dropped a row would still pass Validate().
func TestChecksum_DistinguishesDifferences(t *testing.T) {
	t.Parallel()
	day := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	base := batch.DailyStat{TenantID: "t1", EventType: "view", Date: day, EventCount: 3, TotalValue: 6.0}
	baseHash := batch.Checksum([]batch.DailyStat{base})

	cases := []struct {
		name string
		stat batch.DailyStat
	}{
		{"different tenant", batch.DailyStat{TenantID: "t2", EventType: "view", Date: day, EventCount: 3, TotalValue: 6.0}},
		{"different event_type", batch.DailyStat{TenantID: "t1", EventType: "click", Date: day, EventCount: 3, TotalValue: 6.0}},
		{"different date", batch.DailyStat{TenantID: "t1", EventType: "view", Date: day.Add(24 * time.Hour), EventCount: 3, TotalValue: 6.0}},
		{"different count", batch.DailyStat{TenantID: "t1", EventType: "view", Date: day, EventCount: 4, TotalValue: 6.0}},
		{"different sum", batch.DailyStat{TenantID: "t1", EventType: "view", Date: day, EventCount: 3, TotalValue: 6.5}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h := batch.Checksum([]batch.DailyStat{c.stat})
			if h == baseHash {
				t.Fatalf("Checksum should differ when %s changes; both produced %s", c.name, h)
			}
		})
	}
}

// TestChecksum_EmptyInputStable: an empty stat slice produces a
// deterministic checksum (the SHA-256 of the empty string). This
// matters for the "no events on this date" path — Validate() must
// be able to confirm "source has no rows, sink has no rows" via
// matching empty-checksums rather than special-casing the empty
// case.
func TestChecksum_EmptyInputStable(t *testing.T) {
	t.Parallel()
	// sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
	want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got := batch.Checksum(nil); got != want {
		t.Fatalf("Checksum(nil): got %s, want %s", got, want)
	}
	if got := batch.Checksum([]batch.DailyStat{}); got != want {
		t.Fatalf("Checksum([]): got %s, want %s", got, want)
	}
}
