// Batch aggregator — reads events from PostgreSQL, writes daily rollups to ClickHouse.
//
// Usage:
//
//	go run ./cmd/batch-aggregator                     # aggregate yesterday
//	go run ./cmd/batch-aggregator -date 2025-06-01    # aggregate a specific day
//	go run ./cmd/batch-aggregator -from 2025-01-01 -to 2025-06-01  # backfill range
//
// Ch11: this is a separate binary from cmd/server. In production it would be
// triggered by a cron job (Kubernetes CronJob, Temporal scheduled workflow,
// or the leader-elected outbox worker from Ch10).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/batch"
	"github.com/mohgh/nexus/internal/config"
	"go.uber.org/zap"
)

func main() {
	dateFlag := flag.String("date", "", "aggregate a single day (YYYY-MM-DD)")
	fromFlag := flag.String("from", "", "start of range (YYYY-MM-DD)")
	toFlag := flag.String("to", "", "end of range exclusive (YYYY-MM-DD)")
	flag.Parse()

	cfg := config.Load()

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	ctx := context.Background()

	// Connect to PostgreSQL (source).
	pgPool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Fatal("postgres: connect failed", zap.Error(err))
	}
	defer pgPool.Close()

	// Connect to ClickHouse (sink).
	chOpts, err := chdriver.ParseDSN(cfg.ClickHouseDSN)
	if err != nil {
		logger.Fatal("clickhouse: parse DSN", zap.Error(err))
	}
	chConn, err := chdriver.Open(chOpts)
	if err != nil {
		logger.Fatal("clickhouse: open", zap.Error(err))
	}
	defer chConn.Close()

	if err := chConn.Ping(ctx); err != nil {
		logger.Fatal("clickhouse: ping", zap.Error(err))
	}

	agg := batch.NewAggregator(pgPool, chConn, logger)

	switch {
	case *fromFlag != "" && *toFlag != "":
		from, err := time.Parse("2006-01-02", *fromFlag)
		if err != nil {
			logger.Fatal("invalid -from", zap.Error(err))
		}
		to, err := time.Parse("2006-01-02", *toFlag)
		if err != nil {
			logger.Fatal("invalid -to", zap.Error(err))
		}
		if err := agg.RunRange(ctx, from, to); err != nil {
			logger.Fatal("batch range failed", zap.Error(err))
		}

	case *dateFlag != "":
		day, err := time.Parse("2006-01-02", *dateFlag)
		if err != nil {
			logger.Fatal("invalid -date", zap.Error(err))
		}
		if err := agg.RunDay(ctx, day); err != nil {
			logger.Fatal("batch day failed", zap.Error(err))
		}

	default:
		// No flags: aggregate yesterday.
		yesterday := time.Now().UTC().Add(-24 * time.Hour)
		if err := agg.RunDay(ctx, yesterday); err != nil {
			logger.Fatal("batch yesterday failed", zap.Error(err))
		}
	}

	logger.Info("batch aggregator finished")
}
