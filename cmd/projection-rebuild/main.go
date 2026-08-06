// projection-rebuild — wipes a projection's read model + position
// and replays the event store from position 0.
//
// Usage:
//
//	go run ./cmd/projection-rebuild -projection tenant_event_counts
//	go run ./cmd/projection-rebuild -projection daily_event_counts
//
// Ch13 teaching point: a projection is *disposable*. The event log
// is the source of truth; the read model is a derived artefact you
// can throw away and reconstruct any time. That's what makes
// schema-of-projection changes painless: ship the new code, run
// rebuild, point readers at the new shape.
//
// This binary is the operational counterpart to that idea. It runs
// at a wall-clock cadence, not at "live event arrival" pace, so
// it's safe to run while the server is also handling new writes —
// every Apply is idempotent under at-least-once redelivery.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mohgh/nexus/internal/config"
	"github.com/mohgh/nexus/internal/eventstore"
	"github.com/mohgh/nexus/internal/projections"
	"go.uber.org/zap"
)

func main() {
	var (
		projectionName string
		batchSize      int
		yes            bool
	)
	flag.StringVar(&projectionName, "projection", "", "name of the projection to rebuild (required): tenant_event_counts | daily_event_counts")
	flag.IntVar(&batchSize, "batch-size", 1000, "events per ReadAllFrom call")
	flag.BoolVar(&yes, "yes", false, "skip the confirm prompt; required for non-interactive runs")
	flag.Parse()

	if projectionName == "" {
		flag.Usage()
		fmt.Fprintln(os.Stderr, "\nerror: -projection is required")
		os.Exit(2)
	}

	cfg := config.Load()

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.PostgresDSN)
	if err != nil {
		logger.Fatal("postgres: connect", zap.Error(err))
	}
	defer pool.Close()

	store := eventstore.NewStore(pool)

	proj, err := buildProjection(projectionName, pool, logger)
	if err != nil {
		logger.Fatal("projection-rebuild: unknown projection",
			zap.String("name", projectionName),
			zap.Error(err))
	}

	if !yes {
		fmt.Fprintf(os.Stderr,
			"This will TRUNCATE the %s read model and replay the event store from position 0.\n"+
				"Re-run with -yes to proceed.\n",
			projectionName)
		os.Exit(2)
	}

	logger.Info("projection-rebuild: starting",
		zap.String("projection", projectionName),
		zap.Int("batch_size", batchSize),
	)
	start := time.Now()

	if err := proj.Reset(ctx); err != nil {
		logger.Fatal("projection-rebuild: reset failed", zap.Error(err))
	}

	// Drain the event store batch by batch.
	var applied int
	for {
		if ctx.Err() != nil {
			logger.Warn("projection-rebuild: cancelled mid-rebuild",
				zap.Int("applied", applied),
			)
			return
		}
		events, err := store.ReadAllFrom(ctx, proj.LastPosition(), batchSize)
		if err != nil {
			logger.Fatal("projection-rebuild: read events", zap.Error(err))
		}
		if len(events) == 0 {
			break
		}
		for _, e := range events {
			if err := proj.Apply(ctx, e); err != nil {
				logger.Fatal("projection-rebuild: apply failed",
					zap.Int64("position", e.StreamPosition),
					zap.Error(err))
			}
			applied++
		}
		if applied%5000 == 0 {
			logger.Info("projection-rebuild: progress",
				zap.Int("applied", applied),
				zap.Int64("position", proj.LastPosition()),
			)
		}
		if len(events) < batchSize {
			break
		}
	}

	logger.Info("projection-rebuild: done",
		zap.String("projection", projectionName),
		zap.Int("applied", applied),
		zap.Int64("final_position", proj.LastPosition()),
		zap.Duration("elapsed", time.Since(start)),
	)
}

func buildProjection(name string, pool *pgxpool.Pool, logger *zap.Logger) (projections.Projection, error) {
	switch name {
	case projections.TenantStatsName:
		return projections.NewTenantStatsProjection(pool, logger), nil
	case projections.DailyStatsName:
		return projections.NewDailyEventCountsProjection(pool, logger), nil
	default:
		return nil, fmt.Errorf("unknown projection %q (known: %s, %s)",
			name, projections.TenantStatsName, projections.DailyStatsName)
	}
}
