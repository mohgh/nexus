package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mohgh/nexus/internal/domain"
	"github.com/segmentio/kafka-go"
)

// TopicBilling is the Kafka topic the outbox writes billing events to.
// Kept distinct from nexus.events so analytics consumers don't have to
// filter billing payloads out of the stream.
const TopicBilling = "nexus.billing"

// KafkaPublisher delivers billing records as JSON-encoded Kafka messages.
// Messages are keyed by tenant_id so all billing events for a tenant
// land on the same partition (preserving per-tenant ordering).
//
// JSON rather than Protobuf for billing: the schema is small, the
// volume is low, and on-the-wire human-readability helps a lot when
// debugging an unfamiliar billing flow. Ch12's analytics events use
// Protobuf precisely because their volume makes that overhead matter;
// billing is the wrong place to repeat that optimization.
type KafkaPublisher struct {
	writer *kafka.Writer
}

// NewKafkaPublisher opens a Kafka writer pointed at TopicBilling.
// The caller owns the writer lifetime via Close.
func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  TopicBilling,
			Balancer:               &kafka.Hash{}, // partition by tenant_id key
			AllowAutoTopicCreation: true,
		},
	}
}

// Publish serialises rec as JSON and writes it to TopicBilling.
// The message key is tenant_id for per-tenant ordering.
func (p *KafkaPublisher) Publish(ctx context.Context, rec *domain.BillingRecord) error {
	body, err := json.Marshal(rec)
	if err != nil {
		// Marshal failure on a struct we control should never happen at
		// runtime; surface it as a publisher error so the worker leaves
		// the row unmarked and a human can investigate.
		return fmt.Errorf("kafka publisher: marshal: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(rec.TenantID),
		Value: body,
		Headers: []kafka.Header{
			{Key: "record_id", Value: []byte(rec.ID)},
			{Key: "idempotency_key", Value: []byte(rec.IdempotencyKey)},
			{Key: "content_type", Value: []byte("application/json")},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka publisher: write: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying Kafka writer.
func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
