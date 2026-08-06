package stream

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

// TopicEventsDLQ is the Kafka topic the consumer routes
// poison/exhausted messages to. Operators replay from here once the
// underlying bug is fixed.
const TopicEventsDLQ = "nexus.events.dlq"

// KafkaDLQPublisher writes failed messages to TopicEventsDLQ with
// provenance headers (original topic / partition / offset, failure
// reason, attempt count, last error). The body is the original
// message bytes verbatim — no decoding, no re-encoding — so the
// downstream replay tool can attempt redelivery without guessing at
// the original schema.
type KafkaDLQPublisher struct {
	writer *kafka.Writer
}

// NewKafkaDLQPublisher opens a Kafka writer pointed at TopicEventsDLQ.
// Caller owns the writer lifetime via Close.
func NewKafkaDLQPublisher(brokers []string) *KafkaDLQPublisher {
	return &KafkaDLQPublisher{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  TopicEventsDLQ,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
	}
}

// Publish sends entry to the DLQ topic. Returns an error only when
// the Kafka write fails — in that case the consumer leaves the
// original message uncommitted so the next process attempts redelivery.
func (p *KafkaDLQPublisher) Publish(ctx context.Context, entry DLQEntry) error {
	msg := kafka.Message{
		Key:   entry.OriginalKey,
		Value: entry.OriginalValue,
		Headers: []kafka.Header{
			DLQHeader("original_topic", entry.OriginalTopic),
			DLQHeader("original_partition", entry.OriginalPartition),
			DLQHeader("original_offset", entry.OriginalOffset),
			DLQHeader("reason", entry.Reason),
			DLQHeader("attempts", entry.Attempts),
			DLQHeader("last_error", entry.LastError),
			DLQHeader("failed_at", entry.FailedAt.Format("2006-01-02T15:04:05Z07:00")),
		},
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("dlq: write: %w", err)
	}
	return nil
}

// Close flushes and closes the underlying Kafka writer.
func (p *KafkaDLQPublisher) Close() error {
	return p.writer.Close()
}
