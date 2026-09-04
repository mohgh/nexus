// Package stream provides Kafka producer and consumer wrappers.
//
// Ch05: EventProducer publishes events to Kafka after they are written to
//       PostgreSQL. The tenant_id becomes the Kafka message key, and the
//       writer uses the Hash balancer so that key actually decides the
//       partition — every event for a tenant lands on the same partition
//       and is therefore consumed in produce order.
//
// Ch12: EventConsumer and window aggregation are added here.
package stream

import (
	"context"
	"fmt"

	"github.com/mohgh/nexus/internal/domain"
	"github.com/mohgh/nexus/internal/encoding/protobuf"
	"github.com/segmentio/kafka-go"
)

const TopicEvents = "nexus.events"

// EventProducer publishes domain.Event messages to Kafka encoded as Protobuf.
//
// Ch05 teaching point: compare this binary payload to sending JSON.
// Protobuf is ~3–5× smaller on the wire and ~10× faster to parse —
// critical when you're publishing millions of events per day.
type EventProducer struct {
	writer *kafka.Writer
}

// NewEventProducer creates a producer that writes to the given Kafka brokers.
//
// The balancer is Hash, NOT LeastBytes. This matters, and an earlier
// version got it wrong in a way that was invisible from the code:
// the message Key was already set to the tenant ID with a comment
// claiming per-tenant ordering, but LeastBytes ignores the key
// entirely and routes by which partition it has written the fewest
// bytes to. The key was decorative; two events for the same tenant
// could land on different partitions, and Kafka only orders within a
// partition. Three places in this codebase depend on the guarantee
// the comment claimed:
//
//   1. EventConsumer's contract (consumer.go, teaching point 5)
//      states outright that "since the producer keys by tenant_id,
//      all events for a tenant arrive in partition order". With
//      LeastBytes that sentence was false.
//
//   2. WindowAggregator's watermark (window.go). The aggregator
//      routes an event to the "late" bucket when its event_time is
//      before high_watermark - allowedLateness. Split a tenant's
//      stream across partitions consumed at different rates and a
//      perfectly punctual event can arrive after the watermark has
//      already jumped ahead — it gets booked as late, and the
//      on-time window silently under-counts. That is a wrong number
//      in the analytics, not just a reordering.
//
//   3. The 1-minute / 1-hour aggregates and their flush to
//      tenant_window_stats inherit the same defect: IsClosed()
//      declares a bucket flushable based on the watermark, so a
//      bucket can be closed and flushed while in-order-produced
//      events for that tenant are still queued behind a slower
//      partition.
//
// internal/billing/outbox/kafka_publisher.go already made this
// choice correctly (`&kafka.Hash{}`, "partition by tenant_id key");
// this brings the event topic in line with it.
//
// The cost of Hash is real but acceptable: partitions are only as
// balanced as the tenant key distribution, so one very large tenant
// makes one hot partition. Ordering is a correctness property here
// and byte-balance is a performance property, so correctness wins.
// If a hot tenant ever becomes the bottleneck, the fix is a
// composite key (tenant + sub-stream) that preserves ordering within
// whatever unit actually needs it — not a return to LeastBytes.
//
// Note on hash compatibility: kafka-go's Hash uses FNV-1a with the
// same int32 conversion as Sarama's hashPartitioner. It is NOT
// wire-compatible with the Java client's murmur2, so a non-Go
// producer writing this topic would place the same tenant on a
// different partition. Every producer to nexus.events is kafka-go,
// so this holds today; it is a constraint to remember if that
// changes.
func NewEventProducer(brokers []string) *EventProducer {
	return &EventProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  TopicEvents,
			Balancer:               &kafka.Hash{},
			AllowAutoTopicCreation: true,
		},
	}
}

// Publish serialises e as Protobuf and sends it to Kafka.
// The message key is tenant_id so events from the same tenant
// always land on the same partition — guaranteeing per-tenant ordering.
func (p *EventProducer) Publish(ctx context.Context, e *domain.Event) error {
	payload, err := protobuf.Marshal(e)
	if err != nil {
		return fmt.Errorf("producer: marshal: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(e.TenantID), // partition key → per-tenant ordering
		Value: payload,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(e.EventType)},
			{Key: "content_type", Value: []byte("application/x-protobuf")},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("producer: write: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying Kafka writer.
func (p *EventProducer) Close() error {
	return p.writer.Close()
}
