package stream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mohgh/nexus/internal/domain"
	"github.com/mohgh/nexus/internal/encoding/protobuf"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// EventHandler processes a single decoded event from Kafka.
// Return nil to commit the offset, error to drive the retry/DLQ path.
type EventHandler func(ctx context.Context, event *domain.Event) error

// messageReader is the slice of kafka.Reader the consumer actually
// uses. Extracting it as an interface lets the tests inject a fake
// reader so the retry/DLQ logic can be exercised without a live
// Kafka. Production wires *kafka.Reader; nothing in the consumer's
// hot path touches anything outside this interface.
type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// DLQPublisher delivers messages that the consumer could not safely
// process — either because they were malformed (couldn't decode) or
// because the handler returned an error on every retry. The DLQ is
// what makes "skip the bad message and keep moving" honest: nothing
// is silently dropped, every diverted message is durably recorded
// for human review.
type DLQPublisher interface {
	Publish(ctx context.Context, entry DLQEntry) error
}

// DLQEntry is the payload the consumer hands to the DLQ when a
// message fails. The original bytes are preserved verbatim so the
// message can be replayed if the bug is later fixed; the metadata
// gives the operator enough context to triage.
type DLQEntry struct {
	OriginalTopic     string
	OriginalPartition int
	OriginalOffset    int64
	OriginalKey       []byte
	OriginalValue     []byte
	Reason            string // e.g. "unmarshal_failed", "handler_failed_after_retries"
	Attempts          int
	LastError         string
	FailedAt          time.Time
}

// RetryPolicy controls how the consumer retries a transient handler
// failure before giving up and sending the message to the DLQ.
//
// Zero values resolve to defaults via fillDefaults().
type RetryPolicy struct {
	MaxAttempts int           // total attempts including the first; default 3
	BaseDelay   time.Duration // delay before the second attempt; default 100ms
	MaxDelay    time.Duration // cap on the exponential backoff; default 5s
}

func (p *RetryPolicy) fillDefaults() {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 100 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 5 * time.Second
	}
}

// EventConsumer reads Protobuf-encoded events from the nexus.events
// topic and routes them to either the handler or the DLQ.
//
// Ch12 teaching points:
//
//  1. Consumer groups: multiple consumers share a group ID so Kafka
//     assigns each partition to exactly one consumer — scaling out
//     means adding consumers up to the partition count.
//
//  2. At-least-once delivery: a message's offset is committed only
//     after the consumer has either successfully invoked the handler
//     OR durably diverted the message to the DLQ. A handler error
//     does NOT commit. This is the central correctness invariant of
//     the file — an earlier draft committed on every handler return
//     regardless of outcome, which silently dropped events on the
//     happy-path-but-handler-failed branch.
//
//  3. Retry + DLQ: transient handler failures get RetryPolicy
//     attempts with exponential backoff. After exhaustion the message
//     is published to the DLQ topic with full provenance metadata,
//     and only then is the offset committed.
//
//  4. Poison messages: messages that cannot be decoded skip the
//     handler entirely and go straight to the DLQ. They cannot be
//     processed on retry — the bug is in the message, not the
//     consumer — so retry would burn cycles forever.
//
//  5. Ordering: events are ordered within a partition. Since the
//     producer keys by tenant_id, all events for a tenant arrive in
//     partition order.
type EventConsumer struct {
	reader  messageReader
	dlq     DLQPublisher // optional; nil disables DLQ, malformed/exhausted messages are committed with a log line
	retry   RetryPolicy
	logger  *zap.Logger
}

// Config bundles the constructor inputs. Zero values resolve to
// sensible defaults via the RetryPolicy/fillDefaults path.
type Config struct {
	Brokers  []string
	GroupID  string
	Topic    string // defaults to TopicEvents
	Retry    RetryPolicy
	DLQ      DLQPublisher
	Logger   *zap.Logger
}

// NewEventConsumer creates a consumer reading from cfg.Topic.
func NewEventConsumer(cfg Config) *EventConsumer {
	topic := cfg.Topic
	if topic == "" {
		topic = TopicEvents
	}
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg.Retry.fillDefaults()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		Topic:    topic,
		GroupID:  cfg.GroupID,
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	return &EventConsumer{
		reader: reader,
		dlq:    cfg.DLQ,
		retry:  cfg.Retry,
		logger: logger,
	}
}

// newConsumerWithReader is the test seam: lets unit tests inject a
// fake reader without going through NewEventConsumer (which opens a
// real Kafka connection).
func newConsumerWithReader(reader messageReader, dlq DLQPublisher, retry RetryPolicy, logger *zap.Logger) *EventConsumer {
	retry.fillDefaults()
	if logger == nil {
		logger = zap.NewNop()
	}
	return &EventConsumer{reader: reader, dlq: dlq, retry: retry, logger: logger}
}

// Run reads messages in a loop and routes each to handler / DLQ.
// Blocks until ctx is cancelled. Returns nil on clean shutdown.
//
// The contract: when this function returns nil (clean shutdown), every
// message ever fetched has either been processed by handler OR
// published to the DLQ. No message is silently dropped.
func (c *EventConsumer) Run(ctx context.Context, handler EventHandler) error {
	c.logger.Info("consumer: starting")

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("consumer: fetch: %w", err)
		}

		if err := c.process(ctx, msg, handler); err != nil {
			// process() returns an error only when we could neither
			// handle the message nor durably divert it. Don't commit
			// — leave the message in the topic so the next fetch
			// retries from this offset.
			c.logger.Warn("consumer: leaving message uncommitted for retry",
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("consumer: commit: %w", err)
		}
	}
}

// process handles a single message and returns nil exactly when the
// caller is safe to commit. Returns an error when the message is
// neither handled nor durably diverted (DLQ unavailable) — in that
// case the caller must NOT commit.
func (c *EventConsumer) process(ctx context.Context, msg kafka.Message, handler EventHandler) error {
	// Decode. On decode failure the message is poison — retry won't
	// help — so we go straight to DLQ.
	event := &domain.Event{}
	if err := protobuf.Unmarshal(msg.Value, event); err != nil {
		c.logger.Error("consumer: unmarshal failed",
			zap.Int64("offset", msg.Offset),
			zap.Error(err),
		)
		return c.toDLQ(ctx, msg, "unmarshal_failed", 1, err)
	}

	// Try the handler with retries.
	var lastErr error
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Shutting down. The message stays uncommitted so the
			// next process to pick up this partition retries it.
			return ctxErr
		}
		if err := handler(ctx, event); err == nil {
			if attempt > 1 {
				c.logger.Info("consumer: handler succeeded after retry",
					zap.String("event_id", event.ID),
					zap.Int("attempt", attempt),
				)
			}
			return nil
		} else {
			lastErr = err
			if attempt < c.retry.MaxAttempts {
				delay := c.retry.backoff(attempt)
				c.logger.Warn("consumer: handler failed; will retry",
					zap.String("event_id", event.ID),
					zap.Int("attempt", attempt),
					zap.Duration("next_delay", delay),
					zap.Error(err),
				)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
			}
		}
	}

	c.logger.Error("consumer: handler failed after all retries",
		zap.String("event_id", event.ID),
		zap.Int("attempts", c.retry.MaxAttempts),
		zap.Error(lastErr),
	)
	return c.toDLQ(ctx, msg, "handler_failed_after_retries", c.retry.MaxAttempts, lastErr)
}

// toDLQ publishes msg to the DLQ. If the DLQ publish succeeds, the
// caller is safe to commit (the message is durably elsewhere). If it
// fails, returns the error so the caller leaves the message
// uncommitted — losing the DLQ destination must not lose the message.
//
// If no DLQ is configured we log and commit. That's a deliberate
// choice for single-node / teaching deployments where there's no
// downstream to feed the DLQ to; it's marked clearly so a production
// operator knows to plug one in.
func (c *EventConsumer) toDLQ(ctx context.Context, msg kafka.Message, reason string, attempts int, lastErr error) error {
	if c.dlq == nil {
		c.logger.Warn("consumer: no DLQ configured — dropping message after failure (configure a DLQ for at-least-once semantics)",
			zap.Int64("offset", msg.Offset),
			zap.String("reason", reason),
			zap.Error(lastErr),
		)
		return nil
	}

	entry := DLQEntry{
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,
		OriginalKey:       msg.Key,
		OriginalValue:     msg.Value,
		Reason:            reason,
		Attempts:          attempts,
		FailedAt:          time.Now().UTC(),
	}
	if lastErr != nil {
		entry.LastError = lastErr.Error()
	}

	if err := c.dlq.Publish(ctx, entry); err != nil {
		return fmt.Errorf("dlq publish: %w", err)
	}
	c.logger.Info("consumer: message diverted to DLQ",
		zap.Int64("offset", msg.Offset),
		zap.String("reason", reason),
	)
	return nil
}

// backoff returns the delay before attempt N (1-indexed). Exponential
// with a cap.
func (p RetryPolicy) backoff(attempt int) time.Duration {
	d := p.BaseDelay << (attempt - 1) // BaseDelay * 2^(attempt-1)
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	return d
}

// Close shuts down the underlying reader.
func (c *EventConsumer) Close() error {
	return c.reader.Close()
}

// DLQHeader returns a string header value for a DLQ entry field —
// convenience for KafkaDLQPublisher to keep its headers in one
// place. Not part of the consumer's hot path.
func DLQHeader(name string, value any) kafka.Header {
	switch v := value.(type) {
	case string:
		return kafka.Header{Key: name, Value: []byte(v)}
	case int:
		return kafka.Header{Key: name, Value: []byte(strconv.Itoa(v))}
	case int64:
		return kafka.Header{Key: name, Value: []byte(strconv.FormatInt(v, 10))}
	default:
		return kafka.Header{Key: name, Value: []byte(fmt.Sprintf("%v", v))}
	}
}

// (sentinel kept for tests that want a typed error to compare against)
var ErrConsumerShuttingDown = errors.New("consumer: shutting down")
