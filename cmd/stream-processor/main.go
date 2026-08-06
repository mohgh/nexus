// Stream processor — consumes events from Kafka and maintains real-time
// tumbling-window counters in Redis.
//
// Usage:
//
//	go run ./cmd/stream-processor
//
// Ch12: this is a separate binary from cmd/server and cmd/batch-aggregator.
// The three binaries represent three different processing paradigms:
//   - cmd/server:            request/response (synchronous, low latency)
//   - cmd/batch-aggregator:  batch (high throughput, high latency)
//   - cmd/stream-processor:  streaming (continuous, near-real-time)
//
// In DDIA terms: the batch aggregator gives you "correct" (full reprocess),
// and the stream processor gives you "approximate but fast" (lambda architecture).
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/config"
	"github.com/mohgh/nexus/internal/domain"
	chstore "github.com/mohgh/nexus/internal/storage/clickhouse"
	"github.com/mohgh/nexus/internal/stream"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

func main() {
	cfg := config.Load()

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Redis for tumbling window counters.
	redisOpts, err := redis.ParseURL(cfg.RedisDSN)
	if err != nil {
		logger.Fatal("redis: parse DSN", zap.Error(err))
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	if err := redisClient.Ping(ctx).Err(); err != nil {
		logger.Fatal("redis: ping", zap.Error(err))
	}

	// Postgres pool for the flusher target. The stream processor is
	// otherwise stateless — the only reason it needs Postgres is to
	// move closed window aggregates out of the Redis hot path and
	// into a queryable historical table (Ch12).
	pgPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Fatal("postgres: connect", zap.Error(err))
	}
	defer pgPool.Close()

	// Two parallel window aggregators: 1-minute (for live dashboard
	// freshness) and 1-hour (for trend / billing views). Same Redis,
	// distinct key namespaces. Each event lands in BOTH — they don't
	// compete.
	minuteWindow := stream.NewWindow(redisClient, time.Minute, stream.DefaultAllowedLateness, logger)
	hourWindow := stream.NewWindow(redisClient, time.Hour, stream.DefaultAllowedLateness, logger)

	// DLQ for malformed messages and exhausted retries. Same Kafka
	// cluster, separate topic (nexus.events.dlq). Without this the
	// consumer would have to either drop bad messages on the floor
	// (losing events) or block the partition forever (losing
	// throughput).
	dlq := stream.NewKafkaDLQPublisher(cfg.KafkaBrokers)
	defer dlq.Close() //nolint:errcheck

	// Kafka consumer — reads from nexus.events topic, routes
	// poison/exhausted messages to nexus.events.dlq.
	consumer := stream.NewEventConsumer(stream.Config{
		Brokers: cfg.KafkaBrokers,
		GroupID: "stream-processor",
		DLQ:     dlq,
		Logger:  logger,
	})
	defer consumer.Close()

	// Ch04: optional ClickHouse sink. The same events that drive
	// the Redis windows are also written to ClickHouse so the OLAP
	// endpoint (/tenants/{id}/daily-stats) has data to query.
	// ClickHouse is append-only with async merging — a write here
	// may not be visible to a read seconds later, which is fine for
	// the analytics use case.
	var clickhouseEvents *chstore.EventRepository
	if cfg.ClickHouseDSN != "" {
		ch, err := chstore.NewClient(ctx, cfg.ClickHouseDSN)
		if err != nil {
			logger.Warn("clickhouse: not available — OLAP sink disabled",
				zap.Error(err))
		} else {
			defer ch.Close()
			clickhouseEvents = chstore.NewEventRepository(ch)
		}
	}

	// Handler: for each event, increment both window counters,
	// bucketed by event_time (OccurredAt) rather than processing
	// time. Stragglers older than (watermark - allowedLateness) are
	// routed to the late bucket so they don't corrupt the on-time
	// aggregate after the window has been considered closed.
	// Plus the ClickHouse sink if wired.
	handler := func(ctx context.Context, event *domain.Event) error {
		// event.ID threads into Add for per-bucket idempotency. A
		// Kafka redelivery (process crash between handler success
		// and offset commit) replays the same event_id; the window's
		// applied-set check turns that into a no-op rather than
		// double-counting.
		if err := minuteWindow.Add(ctx, event.TenantID, event.EventType, event.ID, event.Value, event.OccurredAt); err != nil {
			return err
		}
		if err := hourWindow.Add(ctx, event.TenantID, event.EventType, event.ID, event.Value, event.OccurredAt); err != nil {
			return err
		}
		if clickhouseEvents != nil {
			if err := clickhouseEvents.Insert(ctx, event); err != nil {
				// Fail-closed: a ClickHouse error must propagate so
				// the consumer's retry+DLQ path engages. Earlier
				// drafts logged-and-ignored and returned nil, which
				// committed the Kafka offset and permanently dropped
				// the event from OLAP — silently breaking the
				// chapter's "same data, different engine" claim. A
				// transient ClickHouse outage now causes Kafka
				// backpressure (the retry path) rather than data
				// divergence.
				return fmt.Errorf("clickhouse insert: %w", err)
			}
		}
		logger.Debug("stream: event processed",
			zap.String("tenant_id", event.TenantID),
			zap.String("event_type", event.EventType),
			zap.Float64("value", event.Value),
			zap.Time("occurred_at", event.OccurredAt),
		)
		return nil
	}

	// Flusher: copies closed Redis windows into tenant_window_stats.
	// Runs alongside the consumer; either can exit independently
	// (we wait on both via wait group below).
	flusher := stream.NewFlusher(redisClient, pgPool,
		[]*stream.WindowAggregator{minuteWindow, hourWindow},
		logger, stream.FlusherConfig{},
	)

	logger.Info("stream processor starting",
		zap.Strings("brokers", cfg.KafkaBrokers),
		zap.String("group_id", "stream-processor"),
	)

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		if err := flusher.Run(ctx); err != nil {
			logger.Error("flusher: exited with error", zap.Error(err))
		}
	}()

	if err := consumer.Run(ctx, handler); err != nil {
		logger.Fatal("stream processor failed", zap.Error(err))
	}

	<-flushDone
	logger.Info("stream processor stopped")
}
