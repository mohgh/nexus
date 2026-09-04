package stream

import (
	"fmt"
	"testing"

	"github.com/segmentio/kafka-go"
)

// The stakes, repeated in every failure message below because whoever
// trips this test needs them at the moment they trip it:
//
// EventProducer.Publish sets Key = tenant_id and the codebase treats
// per-tenant ordering as a guarantee. Kafka only orders within a
// partition, so that guarantee holds if and only if the balancer
// actually routes on the key. kafka.LeastBytes does not — it ignores
// the key entirely and picks whichever partition it has written the
// fewest bytes to — which is how this shipped once already, with the
// key sitting there decoratively next to a comment claiming ordering.
//
// The consequence is not "events arrive shuffled". WindowAggregator
// keeps ONE global high watermark (window.go) and routes an event to
// the late bucket when its event_time is before
// watermark - allowedLateness. Split one tenant across partitions
// drained at different rates and a perfectly punctual event lands
// after the watermark has already jumped ahead: it is booked late and
// the on-time aggregate silently under-counts. IsClosed then declares
// buckets flushable off that same watermark, so the flusher can close
// and persist a bucket while in-order-produced events for that tenant
// are still queued behind a slower partition. The failure mode is a
// wrong number in the analytics, and nothing crashes.
//
// These tests assert the *property* (the balancer is key-aware), not
// the concrete type, so any future key-routing balancer — a custom
// murmur2 for Java-client compatibility, a composite-key balancer for
// hot tenants — passes without edit. They need no Kafka: a Balancer
// is a pure function of (message, partitions).

const balancerStakes = "\n\n" +
	"Why this matters: Publish() keys by tenant_id and the codebase (consumer.go teaching\n" +
	"point 5, window.go's watermark, IsClosed/the flusher) treats per-tenant ordering as a\n" +
	"guarantee. Kafka orders only within a partition, so a balancer that ignores the key\n" +
	"breaks that guarantee. The symptom is NOT visible reordering — it is punctual events\n" +
	"getting booked into the late bucket because the single global watermark ran ahead on a\n" +
	"faster partition, i.e. silently under-counted window aggregates. If you are here after\n" +
	"changing the balancer for partition-balance reasons, the fix is a key-aware balancer\n" +
	"over a finer key (tenant + sub-stream), not a key-blind one."

// TestProducer_BalancerRoutesOnTheTenantKey is the regression guard
// for the LeastBytes → Hash fix. It asserts the two halves of "the
// balancer is key-aware": the same key is sticky regardless of
// message size, and distinct keys do not all collapse onto one
// partition.
func TestProducer_BalancerRoutesOnTheTenantKey(t *testing.T) {
	t.Parallel()

	p := NewEventProducer([]string{"kafka.invalid:9092"})
	balancer := p.writer.Balancer
	if balancer == nil {
		t.Fatalf("EventProducer has no balancer configured, so kafka-go falls back to its "+
			"default round-robin, which is key-blind.%s", balancerStakes)
	}

	// A realistic partition count. Any count > 1 exercises the property.
	partitions := []int{0, 1, 2, 3, 4, 5}

	tenants := []string{
		"tenant-a", "tenant-b", "tenant-c", "tenant-d",
		"tenant-e", "tenant-f", "tenant-g", "tenant-h",
	}

	// Half one: stickiness. Each tenant's key must map to the same
	// partition across messages of wildly different sizes. Size is the
	// variable specifically because LeastBytes routes on accumulated
	// bytes — a size-invariant result is exactly what distinguishes a
	// key-aware balancer from a load-aware one.
	chosen := make(map[string]int, len(tenants))
	for _, tenant := range tenants {
		first := balancer.Balance(
			kafka.Message{Key: []byte(tenant), Value: []byte("x")},
			partitions...,
		)
		for i := range 25 {
			msg := kafka.Message{
				Key:   []byte(tenant),
				Value: make([]byte, 1+100*i),
			}
			got := balancer.Balance(msg, partitions...)
			if got != first {
				t.Fatalf("balancer %T is not key-aware: tenant key %q routed to partition %d, "+
					"then to partition %d on message %d (%d bytes). Two events for one tenant on "+
					"two partitions have no ordering relationship at all.%s",
					balancer, tenant, first, got, i, len(msg.Value), balancerStakes)
			}
		}
		chosen[tenant] = first
	}

	// Half two: the balancer must still spread load. A balancer that
	// pinned every key to partition 0 would pass the stickiness check
	// above while making the partition count meaningless.
	distinct := map[int]bool{}
	for _, part := range chosen {
		distinct[part] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("balancer %T mapped all %d tenant keys onto a single partition (%v). "+
			"Stickiness alone is not enough — a key-aware balancer must also distribute, "+
			"or the topic's partition count buys nothing.%s",
			balancer, len(tenants), chosen, balancerStakes)
	}
	t.Logf("balancer %T: %d tenant keys stably spread over %d of %d partitions",
		balancer, len(tenants), len(distinct), len(partitions))
}

// TestProducer_BalancerIsStableAcrossInstances pins the other half of
// the ordering story: routing must depend only on the key, not on the
// balancer's accumulated state. Two producers (two API replicas, or
// the same replica before and after a restart) publishing for one
// tenant have to reach the same partition, or ordering is broken
// across processes even though each process is internally consistent.
func TestProducer_BalancerIsStableAcrossInstances(t *testing.T) {
	t.Parallel()

	partitions := []int{0, 1, 2, 3, 4, 5}
	replicaA := NewEventProducer([]string{"kafka.invalid:9092"}).writer.Balancer
	replicaB := NewEventProducer([]string{"kafka.invalid:9092"}).writer.Balancer

	// Give replica A a lopsided history first: if the balancer routes
	// on anything other than the key, A and B will now disagree.
	for i := range 50 {
		replicaA.Balance(kafka.Message{
			Key:   []byte(fmt.Sprintf("warmup-tenant-%d", i)),
			Value: make([]byte, 1+500*i),
		}, partitions...)
	}

	for _, tenant := range []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"} {
		msg := kafka.Message{Key: []byte(tenant), Value: []byte("payload")}
		gotA := replicaA.Balance(msg, partitions...)
		gotB := replicaB.Balance(msg, partitions...)
		if gotA != gotB {
			t.Fatalf("balancer %T routes on producer-local state, not on the key: tenant %q "+
				"went to partition %d on a warmed-up producer and partition %d on a fresh one. "+
				"Two API replicas would then split one tenant's stream across partitions.%s",
				replicaA, tenant, gotA, gotB, balancerStakes)
		}
	}
}
