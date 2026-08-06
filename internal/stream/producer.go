// Package stream provides Kafka producer and consumer wrappers.
//
// Ch05: EventProducer publishes events to Kafka after they are written to
//       PostgreSQL. The event_type becomes the Kafka message key so events
//       from the same tenant/type land on the same partition (ordering).
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
// The writer uses LeastBytes balancer — events spread across partitions by
// volume. Use Hash(key) balancer when ordering within a tenant matters.
func NewEventProducer(brokers []string) *EventProducer {
	return &EventProducer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  TopicEvents,
			Balancer:               &kafka.LeastBytes{},
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
